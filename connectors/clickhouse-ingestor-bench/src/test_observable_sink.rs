//! `TestObservableSink` — bench-only scripting surface that lifts
//! Phase 6's `BenchSink` hooks to a trait so the same correctness
//! scenarios run against both the in-memory `BenchSink` and the
//! production `ClickHouseSink<OtlpLogsClickHouseAdapter>` (via
//! [`crate::real_ch::RealClickHouseSink`]) end-to-end.
//!
//! Phase 7 design §`TestObservableSink` trait specifies this as
//! the scaffolding for running the 4 named `ack_invariant_checks`
//! against the production sink — without it, the real-CH
//! `correctness.json` is counts-only and the temporal /
//! scripted-failure invariants stay BenchSink-only.
//!
//! Lives in the bench crate (not the runtime crate) because the
//! trait + every supporting type are bench-scripting concerns and
//! shouldn't appear in `opendata-ingest-runtime`'s public surface.

use std::sync::Arc;

use async_trait::async_trait;
use opendata_ingest_runtime::identity::CommitIdentity;
use opendata_ingest_runtime::sink::Sink;
use tokio_util::sync::CancellationToken;

/// High-level scripted-outcome enum the trait exposes. The
/// `*ThenOk` variants describe a multi-step script per
/// `Sink::write` call: the first attempt returns the failure, the
/// retry returns `Ok`. Implementations translate to their internal
/// representation (BenchSink uses a `Vec<low-level outcome>`
/// pushed once per attempt; RealClickHouseSink uses a one-shot
/// "force this outcome on the next call to seq, then delegate to
/// the inner sink").
#[derive(Debug, Clone)]
pub enum ScriptedWrite {
    /// First attempt resolves `Ok` (BenchSink default; trivial for
    /// the trait completeness — most scenarios don't need to
    /// force this and just rely on the sink's default).
    Ok,
    /// First attempt resolves `SinkCommitFailure::MaybeCommitted`,
    /// the runtime's `check_committed → retry` path replays, and
    /// the second attempt resolves `Ok`. Used to exercise the
    /// retry path end-to-end **without** the inner sink's side
    /// effect happening on the first attempt — so the test proves
    /// identity stability across retries but not the dedupe path.
    MaybeCommittedThenOk,
    /// First attempt **does commit at the inner sink** (the side
    /// effect lands — for `RealClickHouseSink`, the row hits
    /// ClickHouse with its idempotency token) but the wrapper
    /// reports `MaybeCommitted` to the runtime. The runtime's
    /// `check_committed → retry` path then drives a second inner
    /// write whose token matches the first; the inner sink's
    /// idempotency mechanism must suppress the duplicate (for
    /// ClickHouse, the `insert_deduplication_token` URL parameter
    /// at INSERT time). Used by
    /// `maybe_committed_replay_idempotent` to pin the load-bearing
    /// invariant: "CH may have committed, then the runtime replays;
    /// the duplicate insert must not produce a second visible row."
    CommitButReportMaybeCommittedThenRetry,
}

/// Captured-writes log entry. One entry per resolved
/// `Sink::write` call.
#[derive(Debug, Clone)]
pub struct CapturedWrite {
    pub identity: CommitIdentity,
    pub outcome: WriteOutcome,
}

#[derive(Debug, Clone)]
pub enum WriteOutcome {
    Committed { rows: u64 },
    Failure(SinkCommitFailureKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkCommitFailureKind {
    NotCommitted,
    MaybeCommitted,
    Fatal,
}

/// Commit observer — synchronous callback fired after a
/// successful `Sink::write` resolves to `Ok`, before the runtime's
/// `WriteCompletion::Committed` lands on the actor's completion
/// channel. The temporal ordering (sink commit → observer →
/// completion send → ack_through) is what lets the
/// `no_ack_before_sink_commit` scenario pin
/// INV-NO-ACK-BEFORE-COMMIT temporally rather than via the looser
/// "all writes ever happened" check.
pub trait CommitObserver: Send + Sync {
    fn record_commit(&self, identity: &CommitIdentity);
}

/// Blanket impl over closures so callers can install observers
/// inline without boxing types they own.
impl<F> CommitObserver for F
where
    F: Fn(&CommitIdentity) + Send + Sync,
{
    fn record_commit(&self, identity: &CommitIdentity) {
        (self)(identity);
    }
}

/// Bench-scripting trait both `BenchSink` and `RealClickHouseSink`
/// implement. Scenarios under
/// `clickhouse-ingestor-bench::correctness` are generic over this
/// trait so the same temporal / scripted-failure /
/// backpressure-bounded check logic runs against both sinks.
#[async_trait]
pub trait TestObservableSink: Sink {
    /// Block the next `write(seq)` call on the returned
    /// `CancellationToken`. Concurrent writes of other sequences
    /// proceed unimpeded. Returning the token (rather than taking
    /// one) lets the caller hold the gate from outside.
    /// `CancellationToken` is lost-wakeup-safe — `cancel()`
    /// resolves both already-waiting and future waiters.
    fn set_per_sequence_block(&self, seq: u64) -> CancellationToken;

    /// Script a multi-step outcome for the next `write(seq)` call.
    /// `ScriptedWrite::MaybeCommittedThenOk` returns
    /// `SinkCommitFailure::MaybeCommitted` on the first attempt
    /// and `Ok` on the runtime's retry — the production sink
    /// would never produce `MaybeCommitted` against
    /// testcontainers CH organically, so this is the only way to
    /// exercise the runtime's `check_committed → retry` path.
    fn set_per_sequence_forced_outcome(&self, seq: u64, outcome: ScriptedWrite);

    /// Install a commit observer; fires synchronously after each
    /// successful inner-sink commit, before the writer worker
    /// emits `WriteCompletion::Committed` upstream. Used by
    /// `no_ack_before_sink_commit` to push a SinkCommitOk event
    /// onto a shared ordered log alongside the runtime's
    /// `AckThroughObserver` events.
    fn set_commit_observer(&self, observer: Arc<dyn CommitObserver>);

    /// Drain the captured-writes log accumulated since the last
    /// drain. One entry per resolved `Sink::write` call, in the
    /// order they resolved. Backs the `evidence` JSONL the
    /// `correctness.json` per-check entry points at.
    fn drain_captured_writes(&self) -> Vec<CapturedWrite>;
}
