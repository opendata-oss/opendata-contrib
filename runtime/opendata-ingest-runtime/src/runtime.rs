//! Pipelined runtime: per-source actor + bounded fetch/decode workers + shared sink writer pool.
//!
//! Row 6.1 stood up the actor + channel scaffolding with a single
//! internal worker. Row 6.2 split fetch into N parallel workers
//! driven by `source_defaults.fetch_concurrency`. Row 6.3 layered M
//! decode workers driven by `source_defaults.decode_concurrency`
//! and wired the decode-time byte reconciliation step. Row 6.4
//! lifts the single sink writer task into a `SinkWriterPool` of W
//! workers (driven by `sink.max_concurrent_commits`); fatal errors
//! propagate through a `hard_abort_token` that aborts every stage
//! immediately, distinct from the external `shutdown` (admission)
//! token's graceful-drain semantics.
//!
//! ```text
//!   actor       fetch pool (N)         decode pool (M)         writer pool (W)
//!   ─────       ──────────────         ───────────────         ───────────────
//!   admit ─▶ [desc rx MPMC] ──▶ N tasks ─▶ [fetched rx MPMC] ──▶ M tasks
//!                                                              ─▶ reconcile bytes
//!                                                              ─▶ [SinkCommit rx MPMC] ─▶ W tasks
//!                                                                                       ─▶ write_with_retry
//!   actor ◀────────────────────────────[WriteCompletion mpsc]─────────────────────────────
//! ```
//!
//! The actor owns `&mut BufferSource` and `&mut AckCoordinator`;
//! admission and completion both run as `select!` arms on the same
//! task, so `register_pending` (admission arm) and
//! `mark_committed` / `advance_frontier` / `ack_through` /
//! `flush_acks` (completion arm) need no `Arc<Mutex<_>>`. All four
//! channels are `async_channel::bounded` (MPMC; cloneable
//! receivers fan into the worker pools) sized to
//! `source_defaults.max_inflight_batches`; the completion channel
//! is `tokio::sync::mpsc` since the actor is its sole consumer.
//!
//! Invariants pinned in this row:
//!
//! - INV-ADMISSION-CONTIGUOUS — the actor's admission arm is the
//!   only call site for `AckCoordinator::register_pending`. Each
//!   admission cycle calls `next_descriptors(K)` once, then
//!   synchronously calls `register_pending` for **every** returned
//!   descriptor in source-sequence order **before** sending any of
//!   them on `descriptor_tx`. At K=1 this degenerates to today's
//!   register-then-send pattern; at K>1 the two-pass register-all
//!   then send-all structure preserves the invariant.
//! - INV-FRONTIER-NEVER-OVER-HOLE — completion arm calls
//!   `mark_committed` + `advance_frontier` on each completion.
//! - INV-ACK-CALLED-ON-ADVANCE — `ack_through` only fires when
//!   the coordinator's `frontier()` strictly exceeds the actor's
//!   `last_ack_sent`.
//! - INV-DESCRIPTOR-LOSS-FATAL — admission's `descriptor_tx.send`
//!   returning `Err` halts the runtime with a `Pipeline` error.
//! - INV-SINK-RETRY-IDEMPOTENT — `write_with_retry` re-issues the
//!   same `SinkCommit` (with a byte-identical `CommitIdentity`)
//!   on `NotCommitted` / `MaybeCommitted`.
//! - INV-MAYBE-COMMITTED-RESOLVES — `MaybeCommitted` triggers
//!   `Sink::check_committed(&identity)` before retry.
//!
//! # Hard-abort cancellation exception: synchronous decode
//!
//! `hard_abort_token` propagates through every blocking `.await`
//! the worker tasks own — `descriptor_rx.recv()`,
//! `fetch_handle.fetch(...)`, `sink.write(...)`,
//! `sink.check_committed(...)`, the inter-attempt retry sleep —
//! by wrapping each in `tokio::select!`. The one stage it
//! **cannot** preempt mid-execution is the synchronous
//! `Decoder::decode(SourceBatch)` call inside `decode_one`. The
//! decode worker wraps the helper in `select!` against the abort
//! token, but a synchronous body inside that future runs to its
//! first suspension point before the abort branch can fire — for
//! a pure CPU-bound decoder, that's the end of the call. The
//! runtime assumes microsecond-scale decode time (the OTel logs
//! decoder is structural mapping over an already-parsed protobuf
//! tree); a decoder that needs slow CPU work belongs behind a
//! future async-decode contract paired with `spawn_blocking` at
//! this layer. See `decoder::Decoder::decode` for the trait-level
//! note.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::ack_coordinator::AckCoordinators;
use crate::decoded_batch::{DecodedBatch, DecodedRecords};
use crate::decoder::Decoder;
use crate::envelope::{
    ConfiguredEnvelope, PayloadEncoding, SignalType, decode_envelopes, validate_consistent,
};
use crate::error::{RuntimeError, RuntimeResult};
use crate::identity::{CommitIdentity, SequenceRange};
use crate::sink::{CommitStatus, Sink, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId};
use crate::source::{
    BufferSource, BufferSourceFetchHandle, SourceBatch, SourceBatchDescriptor, SourceBudget,
    SourceId,
};
use crate::source_budget::{ByteReservation, SourceByteBudget};

/// Per-stage in-flight byte counters for the
/// `runtime_stage_inflight_bytes{stage=...}` gauge. Each stage's
/// `Arc<AtomicU64>` is attached to a `ByteReservation` (see
/// [`ByteReservation::attach_stage`]) when the unit enters the
/// stage; the reservation's `Drop` and `reconcile` impls keep the
/// atomic in sync without manual decrement bookkeeping on every
/// early-return / error path. The sum across all four atomics
/// matches `budget.in_flight()` modulo a single-instruction
/// window during a stage transition.
///
/// For the single-sink runtime today, `sink_dispatch` also drives
/// `runtime_sink_inflight_bytes{sink}` — the gauge samples
/// `sink_dispatch.load()` in the writer worker, which gives the
/// sum across concurrent in-flight commits (replaces the row-6.5
/// per-envelope `set(reservation.held())` that only showed the
/// last commit's reservation size).
#[derive(Clone)]
struct StageInflightBytes {
    /// Reservations admitted by the actor (descriptor channel or
    /// just before sending). Detaches when a fetch worker calls
    /// `attach_stage(stage.fetch.clone())`.
    source: Arc<AtomicU64>,
    /// Held by fetch workers + bytes parked in `fetched_rx`.
    fetch: Arc<AtomicU64>,
    /// Held by decode workers + bytes parked in `sink_commit_rx`.
    /// Reconcile-grow / shrink reflects here via the reservation.
    decode: Arc<AtomicU64>,
    /// Held by writer workers from `sink_commit_rx.recv()` until
    /// the reservation drops at the end of the write. Doubles as
    /// the source of `runtime_sink_inflight_bytes{sink}`.
    sink_dispatch: Arc<AtomicU64>,
}

impl StageInflightBytes {
    fn new() -> Self {
        Self {
            source: Arc::new(AtomicU64::new(0)),
            fetch: Arc::new(AtomicU64::new(0)),
            decode: Arc::new(AtomicU64::new(0)),
            sink_dispatch: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Threshold for `runtime_backpressure_reason` emission, per
/// design §Observability ("≥ 10 ms park-time gating").
const BACKPRESSURE_TIMER_THRESHOLD: Duration = Duration::from_millis(10);

/// Wrap a backpressure-prone `.await` so the runtime emits
/// `runtime_backpressure_reason{source,reason}` once when the
/// future hasn't resolved within 10 ms. Returns the wrapped
/// future's output unchanged.
///
/// Implementation: a single `select!` polls the wrapped future
/// (pinned in place so the timer arm can drop in and continue
/// awaiting it). The counter increments at most once per call —
/// after the timer fires the function re-awaits the inner future
/// without arming a second timer.
async fn with_backpressure_timer<T>(
    source_id: &SourceId,
    reason: crate::metrics::BackpressureReason,
    metrics: &crate::metrics::RuntimeMetrics,
    fut: impl Future<Output = T>,
) -> T {
    let mut fut = std::pin::pin!(fut);
    tokio::select! {
        biased;
        out = &mut fut => out,
        () = tokio::time::sleep(BACKPRESSURE_TIMER_THRESHOLD) => {
            metrics
                .backpressure_reason
                .get_or_create(&crate::metrics::SourceReasonLabels {
                    source: source_id.0.clone(),
                    reason: reason.as_label().to_string(),
                })
                .inc();
            fut.await
        }
    }
}

/// Test instrumentation: record each `(source_id, sequence)` pair the
/// per-source actor passes to
/// [`AckCoordinator::register_pending`](crate::ack_coordinator::AckCoordinator::register_pending)
/// in its admission arm, in admission order. Pins
/// INV-ADMISSION-CONTIGUOUS under parallel fetch (row 6.2). Plain
/// `pub` — gating it behind `#[cfg(test)]` would hide it from
/// integration tests under `tests/`, which link against the runtime
/// crate as a published library and do not see test-only items.
pub type AdmissionRecorder = Arc<std::sync::Mutex<Vec<(SourceId, u64)>>>;

/// Test instrumentation: record each frontier value the per-source
/// actor passes to [`BufferSource::ack_through`] from its completion
/// arm, in call order. Pins INV-ACK-CALLED-ON-ADVANCE (§1.3c
/// closeout): the actor must call `ack_through(f)` only when the
/// coordinator's frontier strictly exceeds `last_ack_sent` (so out-
/// of-order completions that don't advance the frontier produce no
/// extra calls). A test asserts the recorded sequence is strictly
/// monotonic and the final value matches the highest committed
/// sequence.
///
/// Plain `pub` for the same reason as [`AdmissionRecorder`].
pub type AckThroughRecorder = Arc<std::sync::Mutex<Vec<u64>>>;

/// Test instrumentation: callback fired immediately before the
/// per-source actor calls [`BufferSource::ack_through(f)`]. Lets
/// a test push the ack event onto a shared ordered event log
/// alongside sink-side commit events emitted by a programmable
/// sink — the resulting interleaved log proves the temporal
/// invariant `INV-NO-ACK-BEFORE-COMMIT` (every `ack_through(f)`
/// is preceded in the log by `sink_commit_ok(s)` for every
/// `s ∈ [low..=f]`).
///
/// The §1.3c [`AckThroughRecorder`] is sufficient for the
/// monotonicity / final-value checks, but cannot pin the temporal
/// relationship to sink commits because it doesn't observe sink-
/// side events. The bench harness's
/// `no_ack_before_sink_commit` scenario uses an observer that
/// pushes onto the same `Arc<Mutex<Vec<_>>>` the sink writes to.
pub type AckThroughObserver = Arc<dyn Fn(u64) + Send + Sync>;

/// Test instrumentation: inject artificial latency at the fetch
/// stage so a test can stress the actor's admission ordering under
/// uneven fetch completion times (row 6.2 §Test Plan
/// `pipeline_register_pending_called_in_admission_order`). The
/// closure receives the descriptor's `buffer_sequence` and returns
/// the `Duration` the fetch worker should sleep before invoking
/// `BufferSourceFetchHandle::fetch`. Not used in production.
pub type TestFetchDelayFn = Arc<dyn Fn(u64) -> Duration + Send + Sync>;

/// Test instrumentation: ungracefully unwind every fetch worker
/// without going through the supervisor's `hard_abort_token`. When
/// the token is cancelled, each fetch worker's `select!` fires the
/// killswitch arm at its next descriptor-recv suspension point,
/// returns `Ok(())`, and drops its `descriptor_rx` clone. With
/// `fetch_concurrency = 1`, the channel then has zero receivers and
/// the actor's next `descriptor_tx.send` fails — that's the
/// `pipeline_dropped_descriptor_send_halts_runtime` (§1.3a) path
/// that pins INV-DESCRIPTOR-LOSS-FATAL.
///
/// Distinct from `hard_abort_token` because the killswitch leaves
/// the actor and writer-pool tasks untouched — they only see the
/// failure mode the test is trying to provoke (a closed descriptor
/// channel), not a generic hard-abort cascade.
///
/// Plain `pub` for the same reason as
/// [`AdmissionRecorder`] / [`TestFetchDelayFn`]: integration tests
/// link the runtime as an external crate where `#[cfg(test)]`
/// items are invisible.
pub type TestFetchKillswitch = CancellationToken;

// =========================================================================
// Public configuration types
// =========================================================================

/// How often the runtime calls [`BufferSource::flush_acks`] after
/// acking a source range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum AckFlushPolicy {
    /// Flush after every committed source range. Default. Replay
    /// window after a crash is at most one source range.
    #[default]
    EveryCommitGroup,
    /// Flush after `n` committed source ranges. Operator opt-in for
    /// higher throughput at the cost of an explicit replay window.
    EveryN { n: u32 },
}

/// Per-source backpressure + pool sizing. Default values produce the
/// pipelined behavior Phase 6's bench gate expects (`64`/`8`/`4`); a
/// test that wants the Phase 5 serial shape can override
/// `max_inflight_batches`, `fetch_concurrency`, `decode_concurrency`
/// each to `1`.
///
/// Row 6.1 implements only the byte/slot bookkeeping; `fetch_concurrency`
/// and `decode_concurrency` are configured but the internal worker is
/// single-threaded until 6.2 / 6.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceBackpressureOptions {
    /// Max in-flight `SourceBatch`es per source across the fetch
    /// and decode stages. Bounds the descriptor channel capacity.
    pub max_inflight_batches: u32,
    /// Max in-flight bytes per source, measured as **post-decode
    /// retained memory** — `decoded.estimated_bytes() +
    /// source_coords_bytes` summed over every in-flight unit.
    /// Reserved pessimistically at admission, reconciled at decode
    /// (row 6.3), released at sink commit.
    pub max_inflight_bytes: u64,
    /// Pessimistic per-batch reservation at admission, sized for
    /// the post-decode footprint. Reconciled to actual post-decode
    /// bytes at decode completion (row 6.3).
    pub estimated_max_batch_bytes: u64,
    /// Number of fetch workers per source. Row 6.2 wires this; row
    /// 6.1 ignores the value (single internal worker).
    pub fetch_concurrency: u32,
    /// Number of decode workers per source. Row 6.3 wires this.
    pub decode_concurrency: u32,
    /// Cap on the post-decode byte size of any single batch,
    /// expressed as a multiple of `estimated_max_batch_bytes`. Row
    /// 6.3 wires this into the decode worker.
    pub oversize_fault_multiplier: u32,
}

impl Default for SourceBackpressureOptions {
    fn default() -> Self {
        Self {
            max_inflight_batches: 64,
            max_inflight_bytes: 256 * 1024 * 1024,
            estimated_max_batch_bytes: 4 * 1024 * 1024,
            fetch_concurrency: 8,
            decode_concurrency: 4,
            oversize_fault_multiplier: 4,
        }
    }
}

impl SourceBackpressureOptions {
    /// Phase 5 serial-equivalent profile (`max_inflight_batches = 1`,
    /// `fetch_concurrency = 1`, `decode_concurrency = 1`). Useful for
    /// tests that want to assert behavior under the Phase 6 supervisor
    /// without any parallelism.
    pub fn serial() -> Self {
        Self {
            max_inflight_batches: 1,
            max_inflight_bytes: 256 * 1024 * 1024,
            estimated_max_batch_bytes: 4 * 1024 * 1024,
            fetch_concurrency: 1,
            decode_concurrency: 1,
            oversize_fault_multiplier: 4,
        }
    }
}

/// Shared sink writer pool sizing + retry. The sink itself is
/// singular (RFC 0002 single-sink scope); fields here apply to the
/// one configured sink. Row 6.1 doesn't construct a writer pool —
/// the worker calls `Sink::write` inline — but the config shape is
/// in place for 6.4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkPoolOptions {
    /// Max concurrent `Sink::write` calls in flight across all
    /// sources. Row 6.4 wires this against the shared writer pool.
    pub max_concurrent_commits: u32,
    /// Per-`SinkCommit` retry budget. Shadows the legacy
    /// `RuntimeOptions.max_retry_attempts` when both are set.
    pub retry_max_attempts: u32,
    /// Initial retry backoff (exponential growth with jitter is
    /// future work; row 6.1 uses a constant backoff).
    pub retry_initial_backoff_ms: u64,
}

impl Default for SinkPoolOptions {
    fn default() -> Self {
        Self {
            max_concurrent_commits: 4,
            retry_max_attempts: 6,
            retry_initial_backoff_ms: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeOptions {
    /// Configured envelope every entry must match (validated per
    /// source batch before the decoder runs).
    pub configured_envelope: ConfiguredEnvelope,
    /// Ack flush cadence.
    pub ack_flush_policy: AckFlushPolicy,
    /// When true, the runtime runs the full decode pipeline but
    /// skips [`Sink::write`] and the Buffer ack/flush. Used by
    /// integration tests and by the binary's `--dry-run` mode.
    pub dry_run: bool,
    /// How long to sleep when [`BufferSource::next_descriptors`]
    /// returns an empty descriptor list (no more visible batches).
    pub poll_interval: Duration,
    /// Maximum descriptors to request from `next_descriptors` per
    /// admission cycle. Default `8` — amortizes the per-cycle
    /// manifest GET across up to K descriptors so a saturated source
    /// hits one manifest GET per ~K admissions instead of one per
    /// descriptor.
    ///
    /// Backpressure invariant preserved at any K: the admission arm's
    /// blocking gate still parks until at least one batch permit + one
    /// byte reservation are in hand. After the gate opens, the arm
    /// opportunistically claims up to `K - 1` additional permits +
    /// reservations non-blockingly, then calls
    /// `next_descriptors(gates.len())`. Excess gates (when the buffer
    /// returns fewer descriptors than the loop acquired) are released
    /// via Drop. Registration with the AckCoordinator is synchronous,
    /// in source-sequence order, before any descriptor leaves the arm
    /// — INV-ADMISSION-CONTIGUOUS holds at any K.
    pub max_descriptors_per_poll: usize,
    /// Max retries per source range before a non-fatal sink failure
    /// is bubbled up as `RuntimeError::Sink`. Shadowed by
    /// `sink.retry_max_attempts` when the per-pool value is
    /// configured; the more-specific knob wins.
    pub max_retry_attempts: u32,
    /// Sleep between retry attempts.
    pub retry_backoff: Duration,
    /// Per-source backpressure + pool sizing. Applied identically
    /// to every source unless `source_overrides` is non-empty.
    pub source_defaults: SourceBackpressureOptions,
    /// Optional per-source overrides keyed by `SourceId`. Lookup
    /// falls back to `source_defaults` when no override exists for
    /// the source.
    pub source_overrides: HashMap<SourceId, SourceBackpressureOptions>,
    /// Shared sink writer pool sizing + retry.
    pub sink: SinkPoolOptions,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            configured_envelope: ConfiguredEnvelope {
                version: 1,
                signal_type: SignalType::Logs,
                encoding: PayloadEncoding::OtlpProtobuf,
            },
            ack_flush_policy: AckFlushPolicy::default(),
            dry_run: true,
            poll_interval: Duration::from_millis(250),
            max_descriptors_per_poll: 8,
            max_retry_attempts: 3,
            retry_backoff: Duration::from_millis(100),
            source_defaults: SourceBackpressureOptions::default(),
            source_overrides: HashMap::new(),
            sink: SinkPoolOptions::default(),
        }
    }
}

impl RuntimeOptions {
    /// Look up the [`SourceBackpressureOptions`] for `source`. Falls
    /// back to `source_defaults` if no override is registered.
    pub fn backpressure_for(&self, source: &SourceId) -> SourceBackpressureOptions {
        self.source_overrides
            .get(source)
            .copied()
            .unwrap_or(self.source_defaults)
    }

    /// Resolved per-`SinkCommit` retry budget. `sink.retry_max_attempts`
    /// shadows the legacy `max_retry_attempts` when set to a
    /// non-default value (phase06 design §SinkPoolOptions); the
    /// legacy field stays for backwards compatibility with Phase 5
    /// fixtures that constructed `RuntimeOptions` field-by-field.
    pub fn effective_retry_max_attempts(&self) -> u32 {
        let default_sink_attempts = SinkPoolOptions::default().retry_max_attempts;
        if self.sink.retry_max_attempts != default_sink_attempts {
            self.sink.retry_max_attempts
        } else {
            self.max_retry_attempts
        }
    }

    /// Resolved per-`SinkCommit` retry backoff. Same shadowing rules
    /// as [`effective_retry_max_attempts`](Self::effective_retry_max_attempts).
    pub fn effective_retry_backoff(&self) -> Duration {
        let default_sink_backoff_ms = SinkPoolOptions::default().retry_initial_backoff_ms;
        if self.sink.retry_initial_backoff_ms != default_sink_backoff_ms {
            Duration::from_millis(self.sink.retry_initial_backoff_ms)
        } else {
            self.retry_backoff
        }
    }
}

/// Counters published via the watch channel for tests/metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeProgress {
    pub batches_read: u64,
    pub source_ranges_committed: u64,
    pub records_written: u64,
    pub last_decoded_sequence: Option<u64>,
    pub last_acked_sequence: Option<u64>,
    /// Number of registered-but-not-yet-committed ranges across
    /// all sources hosted by this runtime.
    pub pending_ranges_total: usize,
}

// =========================================================================
// Internal channel envelopes
// =========================================================================

/// Output of the per-source actor's admission arm. Carries the
/// descriptor plus the byte reservation and batch-slot permit
/// that travel with the unit through every stage.
struct AdmittedDescriptor {
    descriptor: SourceBatchDescriptor,
    /// Bytes held against the source's budget. Reserved at
    /// admission (pessimistic). Released on Drop alongside the
    /// batch permit when the worker finishes handling the unit.
    reservation: ByteReservation,
    /// One slot from the per-source batch-count semaphore.
    /// Released on Drop.
    batch_permit: tokio::sync::OwnedSemaphorePermit,
}

/// Output of a fetch worker. The reservation + batch_permit travel
/// with the unit through decode + sink; they drop when the sink
/// worker emits a `WriteCompletion` (success or fatal).
struct FetchedBatch {
    source_batch: SourceBatch,
    reservation: ByteReservation,
    batch_permit: tokio::sync::OwnedSemaphorePermit,
    /// Original descriptor sequence — used by the decode worker to
    /// validate the decoded range matches what admission registered.
    admitted_sequence: u64,
}

/// Output of a decode worker. The decode worker reconciled the
/// `reservation` to actual post-decode bytes before emitting this;
/// the sink writer carries it through `Sink::write` and drops it on
/// completion.
struct SinkCommitEnvelope {
    commit: SinkCommit,
    /// Pending range registered at admission. The actor uses this
    /// for `mark_committed` once the write completes.
    range: (u64, u64),
    reservation: ByteReservation,
    batch_permit: tokio::sync::OwnedSemaphorePermit,
}

/// Worker → actor completion message.
enum WriteCompletion {
    Committed(CommittedReport),
    Fatal(RuntimeError),
}

struct CommittedReport {
    /// Source-range covered by this commit. Must equal the range
    /// the actor registered on admission (Phase 6 contract: one
    /// SinkCommit per source range, low == high == descriptor.sequence).
    range: (u64, u64),
    /// Authoritative row count from the sink. In dry-run, the
    /// decoded record count.
    rows_written: u64,
}

// =========================================================================
// Runtime + RuntimeBuilder
// =========================================================================

pub struct Runtime {
    source: BufferSource,
    decoder: Arc<dyn Decoder>,
    sink: Arc<dyn Sink>,
    coordinators: AckCoordinators,
    options: RuntimeOptions,
    progress_tx: watch::Sender<RuntimeProgress>,
    progress_rx: watch::Receiver<RuntimeProgress>,
    /// Per-source byte budget. Constructed at build time so tests
    /// can clone the `Arc` via [`Runtime::source_byte_budget`] and
    /// observe `in_flight()` mid-run.
    source_byte_budget: Arc<SourceByteBudget>,
    /// Stage-1 typed metric surface (see
    /// `crate::metrics::RuntimeMetrics`). Stored even when the
    /// builder didn't supply one — fallback is a fresh
    /// `Arc::new(RuntimeMetrics::new())` so call sites can emit
    /// unconditionally regardless of whether the bin wired a shared
    /// registry. Wiring at call sites lands in C2.
    runtime_metrics: Arc<crate::metrics::RuntimeMetrics>,
    admission_recorder: Option<AdmissionRecorder>,
    ack_through_recorder: Option<AckThroughRecorder>,
    ack_through_observer: Option<AckThroughObserver>,
    test_fetch_delay: Option<TestFetchDelayFn>,
    test_fetch_killswitch: Option<TestFetchKillswitch>,
}

pub struct RuntimeBuilder {
    source: Option<BufferSource>,
    decoder: Option<Arc<dyn Decoder>>,
    sink: Option<Arc<dyn Sink>>,
    options: RuntimeOptions,
    runtime_metrics: Option<Arc<crate::metrics::RuntimeMetrics>>,
    admission_recorder: Option<AdmissionRecorder>,
    ack_through_recorder: Option<AckThroughRecorder>,
    ack_through_observer: Option<AckThroughObserver>,
    test_fetch_delay: Option<TestFetchDelayFn>,
    test_fetch_killswitch: Option<TestFetchKillswitch>,
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder {
            source: None,
            decoder: None,
            sink: None,
            options: RuntimeOptions::default(),
            runtime_metrics: None,
            admission_recorder: None,
            ack_through_recorder: None,
            ack_through_observer: None,
            test_fetch_delay: None,
            test_fetch_killswitch: None,
        }
    }

    /// Stage-1 typed metric struct (see
    /// `crate::metrics::RuntimeMetrics`). C2 routes every `metrics::*!`
    /// call site through this Arc; for now it's plumbing only.
    pub fn runtime_metrics(&self) -> &Arc<crate::metrics::RuntimeMetrics> {
        &self.runtime_metrics
    }

    pub fn options(&self) -> &RuntimeOptions {
        &self.options
    }

    /// Subscribe to a stream of progress snapshots; tests use this to
    /// observe forward progress without poking at internals.
    pub fn progress(&self) -> watch::Receiver<RuntimeProgress> {
        self.progress_rx.clone()
    }

    /// Clone the per-source byte budget. Tests use this to observe
    /// `in_flight()` mid-run (e.g. to verify decode-time
    /// reconciliation grew the reservation past the pessimistic
    /// admission size).
    pub fn source_byte_budget(&self) -> Arc<SourceByteBudget> {
        Arc::clone(&self.source_byte_budget)
    }

    /// Run until cancellation. On `shutdown.cancelled()` admission
    /// stops, the in-flight units drain, the durable ack frontier is
    /// flushed, and the function returns `Ok(())`. Any pipeline /
    /// sink / source error halts the runtime with the corresponding
    /// `RuntimeError`.
    pub async fn run(self, shutdown: CancellationToken) -> RuntimeResult<()> {
        let Runtime {
            source,
            decoder,
            sink,
            mut coordinators,
            options,
            progress_tx,
            progress_rx: _progress_rx,
            source_byte_budget,
            runtime_metrics,
            admission_recorder,
            ack_through_recorder,
            ack_through_observer,
            test_fetch_delay,
            test_fetch_killswitch,
        } = self;

        info!(
            source = %source.id(),
            sink = %sink.id(),
            dry_run = options.dry_run,
            "starting runtime"
        );

        let source_id = source.id().clone();
        let sink_id = sink.id().clone();
        let bp = options.backpressure_for(&source_id);
        let fetch_concurrency = bp.fetch_concurrency.max(1) as usize;
        let decode_concurrency = bp.decode_concurrency.max(1) as usize;
        let writer_pool_size = options.sink.max_concurrent_commits.max(1) as usize;
        let channel_depth = bp.max_inflight_batches.max(1) as usize;

        let batch_semaphore = Arc::new(Semaphore::new(bp.max_inflight_batches.max(1) as usize));

        let (descriptor_tx, descriptor_rx) =
            async_channel::bounded::<AdmittedDescriptor>(channel_depth);
        let (fetched_tx, fetched_rx) = async_channel::bounded::<FetchedBatch>(channel_depth);
        let (sink_commit_tx, sink_commit_rx) =
            async_channel::bounded::<SinkCommitEnvelope>(channel_depth);
        let (completion_tx, completion_rx) = mpsc::channel::<WriteCompletion>(channel_depth);

        let mut coordinator = coordinators.take(&source_id).ok_or_else(|| {
            RuntimeError::Ack(format!("no coordinator registered for source {source_id}",))
        })?;

        // Two cancellation tokens. The external `shutdown` is the
        // admission token (graceful drain on cancel). `hard_abort`
        // is internally cancelled on fatal errors; every worker
        // checks it via `select!` and exits immediately.
        let hard_abort_token = CancellationToken::new();

        // Per-stage in-flight byte counters. Each stage's atomic
        // is attached to the in-stage reservation via
        // `ByteReservation::attach_stage`; Drop / reconcile keep
        // the atomic in sync without manual decrement bookkeeping
        // on early-return paths.
        let stage_bytes = StageInflightBytes::new();

        // N fetch workers — RFC 0003 §Concurrency Model: `fetch(&self,
        // ...)` is safe to call from N tasks against distinct
        // descriptors. The descriptor channel is async_channel
        // (cloneable receiver = MPMC).
        let mut fetch_handles = Vec::with_capacity(fetch_concurrency);
        for worker_idx in 0..fetch_concurrency {
            fetch_handles.push(tokio::spawn(fetch_worker(
                source.fetch_handle(),
                descriptor_rx.clone(),
                fetched_tx.clone(),
                completion_tx.clone(),
                source_id.clone(),
                worker_idx,
                test_fetch_delay.clone(),
                test_fetch_killswitch.clone(),
                stage_bytes.clone(),
                hard_abort_token.clone(),
                Arc::clone(&runtime_metrics),
            )));
        }
        drop(descriptor_rx);
        drop(fetched_tx);

        // M decode workers. `decoder: Arc<dyn Decoder>` is
        // `Send + Sync`. Each worker reconciles its reservation to
        // actual post-decode bytes before forwarding the
        // `SinkCommitEnvelope` downstream.
        let mut decode_handles = Vec::with_capacity(decode_concurrency);
        for worker_idx in 0..decode_concurrency {
            decode_handles.push(tokio::spawn(decode_worker(
                Arc::clone(&decoder),
                options.clone(),
                sink_id.clone(),
                source_id.clone(),
                worker_idx,
                fetched_rx.clone(),
                sink_commit_tx.clone(),
                completion_tx.clone(),
                stage_bytes.clone(),
                hard_abort_token.clone(),
                Arc::clone(&runtime_metrics),
            )));
        }
        drop(fetched_rx);
        drop(sink_commit_tx);

        // W writer workers — share `sink_commit_rx` MPMC. Each runs
        // `write_with_retry` and emits `WriteCompletion` back to
        // the actor. On a non-recoverable error the worker sends
        // `WriteCompletion::Fatal(e)` to the actor with the typed
        // `RuntimeError` and exits; the supervisor cancels
        // `hard_abort_token` after the actor returns `Err(...)` so
        // peer workers unwind via their `select!` arms (worker-driven
        // cancel would race the completion send and turn a typed
        // `RuntimeError::Sink(...)` into a generic
        // `Pipeline("hard abort …")`).
        let mut writer_handles = Vec::with_capacity(writer_pool_size);
        for worker_idx in 0..writer_pool_size {
            writer_handles.push(tokio::spawn(writer_worker(
                Arc::clone(&sink),
                options.clone(),
                source_id.clone(),
                sink_id.clone(),
                worker_idx,
                sink_commit_rx.clone(),
                completion_tx.clone(),
                stage_bytes.clone(),
                hard_abort_token.clone(),
                Arc::clone(&runtime_metrics),
            )));
        }
        drop(sink_commit_rx);
        drop(completion_tx);

        let actor_result = per_source_actor(
            source,
            &mut coordinator,
            options.clone(),
            descriptor_tx,
            completion_rx,
            Arc::clone(&source_byte_budget),
            batch_semaphore,
            sink_id,
            shutdown,
            hard_abort_token.clone(),
            progress_tx.clone(),
            admission_recorder,
            ack_through_recorder,
            ack_through_observer,
            stage_bytes.clone(),
            Arc::clone(&runtime_metrics),
        )
        .await;

        // If the actor exited with an error, cancel the abort token
        // so still-running workers unwind promptly. Successful exits
        // close channels naturally and workers exit on
        // `recv() == Err(Closed)`.
        if actor_result.is_err() {
            hard_abort_token.cancel();
        }

        let mut fetch_result: RuntimeResult<()> = Ok(());
        for handle in fetch_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if fetch_result.is_ok() {
                        fetch_result = Err(e);
                    }
                }
                Err(join_err) => {
                    if fetch_result.is_ok() {
                        fetch_result = Err(RuntimeError::Pipeline(format!(
                            "fetch worker panicked: {join_err}"
                        )));
                    }
                }
            }
        }

        let mut decode_result: RuntimeResult<()> = Ok(());
        for handle in decode_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if decode_result.is_ok() {
                        decode_result = Err(e);
                    }
                }
                Err(join_err) => {
                    if decode_result.is_ok() {
                        decode_result = Err(RuntimeError::Pipeline(format!(
                            "decode worker panicked: {join_err}"
                        )));
                    }
                }
            }
        }

        let mut sink_result: RuntimeResult<()> = Ok(());
        for handle in writer_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if sink_result.is_ok() {
                        sink_result = Err(e);
                    }
                }
                Err(join_err) => {
                    if sink_result.is_ok() {
                        sink_result = Err(RuntimeError::Pipeline(format!(
                            "writer worker panicked: {join_err}"
                        )));
                    }
                }
            }
        }

        // Surface the most informative error: actor errors take
        // precedence (they reflect coordinator / source state) but
        // a worker fatal that the actor never observed is still a
        // failure.
        match (actor_result, fetch_result, decode_result, sink_result) {
            (Err(e), _, _, _) => Err(e),
            (Ok(()), Err(e), _, _) => Err(e),
            (Ok(()), Ok(()), Err(e), _) => Err(e),
            (Ok(()), Ok(()), Ok(()), Err(e)) => Err(e),
            (Ok(()), Ok(()), Ok(()), Ok(())) => {
                info!("runtime exited cleanly");
                Ok(())
            }
        }
    }
}

impl RuntimeBuilder {
    pub fn add_source(mut self, source: BufferSource) -> Self {
        self.source = Some(source);
        self
    }

    pub fn add_decoder<D>(mut self, decoder: D) -> Self
    where
        D: Decoder,
    {
        self.decoder = Some(Arc::new(decoder));
        self
    }

    pub fn set_sink<S>(mut self, sink: S) -> Self
    where
        S: Sink,
    {
        self.sink = Some(Arc::new(sink));
        self
    }

    pub fn with_options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
        self
    }

    /// Attach a test-only [`AdmissionRecorder`]. Each
    /// `(source_id, sequence)` pair the per-source actor passes to
    /// `register_pending` lands in the recorder's `Vec` in
    /// admission order. Pin INV-ADMISSION-CONTIGUOUS in integration
    /// tests where you can't observe the coordinator directly.
    pub fn with_admission_recorder(mut self, recorder: AdmissionRecorder) -> Self {
        self.admission_recorder = Some(recorder);
        self
    }

    /// Attach a test-only [`AckThroughRecorder`]. Each frontier
    /// value the per-source actor passes to
    /// [`BufferSource::ack_through`] lands in the recorder's `Vec`
    /// in call order. Pin INV-ACK-CALLED-ON-ADVANCE in integration
    /// tests where you can't wrap `BufferSource` with an observer:
    /// the actor must only call `ack_through(f)` when `f >
    /// last_ack_sent`, so the recorded sequence is strictly
    /// monotonic.
    pub fn with_ack_through_recorder(mut self, recorder: AckThroughRecorder) -> Self {
        self.ack_through_recorder = Some(recorder);
        self
    }

    /// Attach a test-only [`AckThroughObserver`] callback. Invoked
    /// synchronously immediately before each
    /// [`BufferSource::ack_through(f)`] call. Use when a test needs
    /// to record the ack event onto a shared ordered log
    /// alongside sink-side events (the bench harness's
    /// `no_ack_before_sink_commit` scenario does exactly this).
    pub fn with_ack_through_observer(mut self, observer: AckThroughObserver) -> Self {
        self.ack_through_observer = Some(observer);
        self
    }

    /// Attach a test-only [`TestFetchDelayFn`]. Each fetch worker
    /// sleeps for `delay(descriptor.sequence)` before invoking the
    /// underlying `BufferSourceFetchHandle::fetch`. Used to stress
    /// admission ordering under uneven fetch completion times.
    pub fn with_test_fetch_delay(mut self, delay: TestFetchDelayFn) -> Self {
        self.test_fetch_delay = Some(delay);
        self
    }

    /// Attach a test-only [`TestFetchKillswitch`]. When the token is
    /// cancelled, every fetch worker observes it at its next
    /// `descriptor_rx.recv` suspension point, returns `Ok(())`, and
    /// drops its `descriptor_rx` clone. With `fetch_concurrency = 1`,
    /// the descriptor channel then has zero receivers and the actor's
    /// next admission send fails — that's the
    /// `pipeline_dropped_descriptor_send_halts_runtime` (§1.3a) shape
    /// that pins INV-DESCRIPTOR-LOSS-FATAL without a `#[cfg(test)]`
    /// fetch-worker variant.
    pub fn with_test_fetch_killswitch(mut self, killswitch: TestFetchKillswitch) -> Self {
        self.test_fetch_killswitch = Some(killswitch);
        self
    }

    /// Supply the typed metric struct the runtime will emit into.
    /// Stage 1 plumbing only — call sites still emit through
    /// `metrics::*!` until C2. When absent the builder default-
    /// constructs an unregistered `RuntimeMetrics` so emission code
    /// has somewhere to write regardless of the bin wiring.
    pub fn with_runtime_metrics(
        mut self,
        metrics: Arc<crate::metrics::RuntimeMetrics>,
    ) -> Self {
        self.runtime_metrics = Some(metrics);
        self
    }

    pub fn build(self) -> RuntimeResult<Runtime> {
        let source = self
            .source
            .ok_or_else(|| RuntimeError::Config("no source configured".into()))?;
        let decoder = self
            .decoder
            .ok_or_else(|| RuntimeError::Config("no decoder configured".into()))?;
        let sink = self
            .sink
            .ok_or_else(|| RuntimeError::Config("no sink configured".into()))?;

        // Validate the byte-budget shape per source. The admission
        // arm pessimistically reserves `estimated_max_batch_bytes`
        // against the per-source `SourceByteBudget` whose capacity
        // is `max_inflight_bytes`. If the reservation can never fit
        // (estimated > capacity), `SourceByteBudget::reserve` parks
        // forever and admission deadlocks before the first
        // descriptor enters the pipeline. Reject the config at
        // build time so the operator sees a typed
        // `RuntimeError::Config` rather than a silent hang.
        let primary_bp = self.options.backpressure_for(source.id());
        validate_backpressure(source.id(), &primary_bp)?;
        for (override_source, override_bp) in &self.options.source_overrides {
            validate_backpressure(override_source, override_bp)?;
        }

        let mut coordinators = AckCoordinators::new();
        coordinators.register_source(source.id().clone(), source.last_acked_sequence())?;
        let (progress_tx, progress_rx) = watch::channel(RuntimeProgress::default());
        let source_byte_budget =
            SourceByteBudget::new(source.id().clone(), primary_bp.max_inflight_bytes);
        let runtime_metrics = self
            .runtime_metrics
            .unwrap_or_else(|| Arc::new(crate::metrics::RuntimeMetrics::new()));
        Ok(Runtime {
            source,
            decoder,
            sink,
            coordinators,
            options: self.options,
            progress_tx,
            progress_rx,
            source_byte_budget,
            runtime_metrics,
            admission_recorder: self.admission_recorder,
            ack_through_recorder: self.ack_through_recorder,
            ack_through_observer: self.ack_through_observer,
            test_fetch_delay: self.test_fetch_delay,
            test_fetch_killswitch: self.test_fetch_killswitch,
        })
    }
}

/// Reject `SourceBackpressureOptions` shapes that would deadlock
/// admission on the per-source byte budget. The admission arm calls
/// `SourceByteBudget::reserve(estimated_max_batch_bytes)` against a
/// budget whose capacity is `max_inflight_bytes`; if the
/// reservation exceeds the capacity, `reserve` parks forever before
/// the first descriptor flows.
///
/// Also rejects pool-sizing knobs set to zero (the runtime treats
/// zero as one via `.max(1)`, but documenting "zero is silently
/// treated as one" in a Config error is friendlier than letting it
/// pass).
fn validate_backpressure(source: &SourceId, bp: &SourceBackpressureOptions) -> RuntimeResult<()> {
    if bp.estimated_max_batch_bytes > bp.max_inflight_bytes {
        return Err(RuntimeError::Config(format!(
            "source {source}: estimated_max_batch_bytes ({}) > max_inflight_bytes ({}); \
             admission would deadlock on SourceByteBudget::reserve. Raise max_inflight_bytes \
             or lower estimated_max_batch_bytes so the pessimistic reservation fits.",
            bp.estimated_max_batch_bytes, bp.max_inflight_bytes,
        )));
    }
    if bp.max_inflight_batches == 0 {
        return Err(RuntimeError::Config(format!(
            "source {source}: max_inflight_batches must be >= 1",
        )));
    }
    if bp.fetch_concurrency == 0 {
        return Err(RuntimeError::Config(format!(
            "source {source}: fetch_concurrency must be >= 1",
        )));
    }
    if bp.decode_concurrency == 0 {
        return Err(RuntimeError::Config(format!(
            "source {source}: decode_concurrency must be >= 1",
        )));
    }
    Ok(())
}

// =========================================================================
// Per-source actor
// =========================================================================

/// Drives admission + completion for a single source. Owns
/// `&mut BufferSource` and `&mut AckCoordinator` for the duration of
/// the run; admission and completion share the actor's `&mut self`
/// across distinct `select!` arms.
#[allow(clippy::too_many_arguments)]
async fn per_source_actor(
    mut source: BufferSource,
    coordinator: &mut crate::ack_coordinator::AckCoordinator,
    options: RuntimeOptions,
    descriptor_tx: async_channel::Sender<AdmittedDescriptor>,
    mut completion_rx: mpsc::Receiver<WriteCompletion>,
    budget: Arc<SourceByteBudget>,
    batch_semaphore: Arc<Semaphore>,
    _sink_id: SinkId,
    shutdown: CancellationToken,
    hard_abort_token: CancellationToken,
    progress_tx: watch::Sender<RuntimeProgress>,
    admission_recorder: Option<AdmissionRecorder>,
    ack_through_recorder: Option<AckThroughRecorder>,
    ack_through_observer: Option<AckThroughObserver>,
    stage_bytes: StageInflightBytes,
    metrics: Arc<crate::metrics::RuntimeMetrics>,
) -> RuntimeResult<()> {
    let source_id = source.id().clone();
    let bp = options.backpressure_for(&source_id);
    let dry_run = options.dry_run;

    let mut progress = RuntimeProgress::default();
    let mut last_ack_sent: Option<u64> = source.last_acked_sequence();
    let mut groups_since_flush: u32 = 0;
    let mut in_flight: u64 = 0;
    let mut admission_open = true;

    loop {
        if !admission_open && in_flight == 0 {
            break;
        }

        // Build the admission attempt only when admission is open
        // and the channel has space. Holding a guard-style
        // permit/reservation across the select! would either pin
        // them across the completion branch (wasteful) or risk
        // dropping them on a no-op poll. Pattern: do a *peek* —
        // try_acquire / try_recv — and on success commit by
        // performing the awaited operations in an immediate
        // dedicated arm. The shape below uses an `if admission_open`
        // guard plus an inner `if let Ok(permit) = semaphore.try_acquire_owned()`
        // gate to start the admission flow only when capacity is
        // there.
        tokio::select! {
            biased;

            // 1. Hard abort — a worker hit a non-recoverable error
            //    (sink Fatal / retry-budget exhausted / oversize batch
            //    / lost descriptor) and cancelled the abort token.
            //    Drop in-flight units; the un-committed range
            //    replays on the next start.
            _ = hard_abort_token.cancelled() => {
                return Err(RuntimeError::Pipeline(format!(
                    "hard abort on source {source_id}"
                )));
            }

            // 2. External shutdown — close admission. The runtime
            //    continues draining completions until in_flight == 0.
            _ = shutdown.cancelled(), if admission_open => {
                debug!("admission token cancelled; draining in-flight commits");
                admission_open = false;
                // Closing the sender flushes the worker's
                // descriptor_rx after it drains.
                descriptor_tx.close();
            }

            // 2. Completion arm — drain writer completions promptly
            //    so the byte budget recovers and admission can
            //    park-then-resume cleanly. The arm body is timed and
            //    recorded as `runtime_stage_latency_seconds{stage=source}`
            //    so the Phase 7 stage-latency bench sees the source
            //    actor's per-cycle work cost (see phase07-clickhouse-
            //    throughput-design.md §Bottleneck Attribution
            //    Methodology > Underlying instrument semantics).
            completion = completion_rx.recv(), if in_flight > 0 => {
                let stage_start = std::time::Instant::now();
                match completion {
                    Some(WriteCompletion::Committed(report)) => {
                        let ack_lag_start = std::time::Instant::now();
                        let (low, high) = report.range;
                        coordinator.mark_committed(low, high)?;
                        coordinator.advance_frontier();
                        in_flight -= 1;

                        progress.batches_read = progress.batches_read.saturating_add(1);
                        progress.last_decoded_sequence = Some(high);
                        progress.records_written = progress
                            .records_written
                            .saturating_add(report.rows_written);
                        progress.source_ranges_committed = progress
                            .source_ranges_committed
                            .saturating_add(1);

                        let frontier = coordinator.frontier();
                        if !dry_run
                            && let Some(f) = frontier
                        {
                            let should_ack = match last_ack_sent {
                                Some(prev) => f > prev,
                                None => true,
                            };
                            if should_ack {
                                // INV-ACK-CALLED-ON-ADVANCE: the
                                // recorder hook fires immediately
                                // before the call so a test sees
                                // exactly the same sequence of
                                // values the source observes —
                                // strictly monotonic by the
                                // `f > prev` guard above. The
                                // observer callback fires at the
                                // same point — used by the bench
                                // harness to push onto a shared
                                // ordered event log alongside sink
                                // commit events (pins
                                // INV-NO-ACK-BEFORE-COMMIT
                                // temporally rather than via the
                                // looser "all writes ever
                                // happened" property).
                                if let Some(recorder) = ack_through_recorder.as_ref() {
                                    recorder.lock().unwrap().push(f);
                                }
                                if let Some(observer) = ack_through_observer.as_ref() {
                                    observer(f);
                                }
                                source.ack_through(f).await?;
                                last_ack_sent = Some(f);
                                groups_since_flush = groups_since_flush.saturating_add(1);
                                let should_flush = match options.ack_flush_policy {
                                    AckFlushPolicy::EveryCommitGroup => true,
                                    AckFlushPolicy::EveryN { n } => {
                                        groups_since_flush >= n.max(1)
                                    }
                                };
                                if should_flush {
                                    source.flush_acks().await?;
                                    groups_since_flush = 0;
                                }
                                progress.last_acked_sequence = Some(f);
                                metrics
                                    .ack_lag_seconds
                                    .get_or_create(&crate::metrics::SourceLabels {
                                        source: source_id.0.clone(),
                                    })
                                    .observe(ack_lag_start.elapsed().as_secs_f64());
                            }
                            metrics
                                .ack_frontier
                                .get_or_create(&crate::metrics::SourceLabels {
                                    source: source_id.0.clone(),
                                })
                                .set(f as i64);
                        }
                        progress.pending_ranges_total = coordinator.pending_count();
                        let _ = progress_tx.send(progress);

                        metrics
                            .pending_ranges
                            .get_or_create(&crate::metrics::SourceLabels {
                                source: source_id.0.clone(),
                            })
                            .set(coordinator.pending_count() as i64);
                        metrics
                            .buffer_consumer_seq_lag
                            .get_or_create(&crate::metrics::SourceLabels {
                                source: source_id.0.clone(),
                            })
                            .set(source.pending_count() as i64);
                        // Per-stage breakdown (§1.4 closeout). Each
                        // stage atomic tracks the bytes attached to
                        // reservations currently owned by that
                        // stage; `Drop` / `reconcile` keep them in
                        // sync without manual decrement on early-
                        // return paths. Sum equals
                        // `budget.in_flight()` under quiescence.
                        emit_stage_inflight_gauges(&stage_bytes, &source_id, &metrics);
                    }
                    Some(WriteCompletion::Fatal(e)) => {
                        return Err(e);
                    }
                    None => {
                        // Worker dropped its completion_tx while we
                        // still have in-flight units. That's a
                        // structural failure.
                        return Err(RuntimeError::Pipeline(format!(
                            "internal worker exited with {in_flight} in-flight unit(s) on source {source_id}",
                        )));
                    }
                }
                metrics
                    .stage_latency_seconds
                    .get_or_create(&crate::metrics::StageLabels {
                        stage: "source".to_string(),
                        source: source_id.0.clone(),
                    })
                    .observe(stage_start.elapsed().as_secs_f64());
            }

            // 3. Admission arm — only when admission is open AND
            //    backpressure permits.
            biased_arm = with_backpressure_timer(
                &source_id,
                crate::metrics::BackpressureReason::SourceBudget,
                &metrics,
                admission_attempt(
                    &source_id,
                    &budget,
                    &batch_semaphore,
                    in_flight,
                    &bp,
                ),
            ), if admission_open => {
                // Time the admission arm body and record one
                // `runtime_stage_latency_seconds{stage=source}` sample
                // per arm execution (one per cycle, not one per
                // descriptor — the cycle is the unit of work). The
                // parked time on backpressure is already accounted
                // for by `with_backpressure_timer` above; this only
                // measures the synchronous + the `next_descriptors`
                // + the `descriptor_tx.send` work.
                let stage_start = std::time::Instant::now();

                // K>1 admission protocol (see plans/odb-high-
                // throughput/phase06-k-gt-1-admission-impl.md §4.1).
                //
                // Step 1 — blocking gate already opened: `biased_arm`
                // carries one batch_permit + one byte reservation.
                let AdmissionGate { batch_permit, mut reservation } = biased_arm;
                // Step 2 — attach the first reservation to the
                // source-stage atomic so it shows up in
                // `runtime_stage_inflight_bytes{stage=source}` while
                // the manifest GET is in flight. Drop / reconcile
                // keep the atomic in sync automatically; the fetch
                // worker re-attaches to `stage.fetch` on recv.
                reservation.attach_stage(Arc::clone(&stage_bytes.source));
                let mut gates: Vec<AdmissionGate> = Vec::with_capacity(
                    options.max_descriptors_per_poll.max(1),
                );
                gates.push(AdmissionGate { batch_permit, reservation });

                // Step 3 — compute advisory K_target. Saturating sub
                // because `in_flight` can transiently exceed
                // `capacity` (oversize batches, reconcile-grow at
                // decode). The authoritative gates are
                // `try_acquire_owned` and `try_reserve`.
                let estimated = bp.estimated_max_batch_bytes.max(1);
                let room_bytes = budget.capacity().saturating_sub(budget.in_flight());
                let room_units = (room_bytes / estimated) as usize;
                let k_target = options
                    .max_descriptors_per_poll
                    .max(1)
                    .min(1usize.saturating_add(batch_semaphore.available_permits()))
                    .min(1usize.saturating_add(room_units));

                // Step 4–5 — opportunistically extend the gate Vec
                // up to K_target.
                let extras_target = k_target.saturating_sub(1);
                for _ in 0..extras_target {
                    let Ok(permit) = Arc::clone(&batch_semaphore).try_acquire_owned()
                    else {
                        break;
                    };
                    let Some(mut extra) = budget.try_reserve(bp.estimated_max_batch_bytes)
                    else {
                        // Permit acquired but no byte room — drop the
                        // permit (back to the semaphore) and stop
                        // extending this cycle.
                        drop(permit);
                        break;
                    };
                    extra.attach_stage(Arc::clone(&stage_bytes.source));
                    gates.push(AdmissionGate { batch_permit: permit, reservation: extra });
                }

                // Step 7 — one manifest GET per cycle, returning up
                // to gates.len() descriptors.
                let descriptors = source
                    .next_descriptors(gates.len(), SourceBudget::default())
                    .await?;
                // Step 13a — count the call regardless of how many
                // descriptors came back (including the empty case).
                metrics
                    .admission_next_descriptors_calls
                    .get_or_create(&crate::metrics::SourceLabels {
                        source: source_id.0.clone(),
                    })
                    .inc();
                // Step 13b — histogram observes descriptors.len()
                // (may be 0 in the empty-poll branch below).
                metrics
                    .admission_descriptors_per_call
                    .get_or_create(&crate::metrics::SourceLabels {
                        source: source_id.0.clone(),
                    })
                    .observe(descriptors.len() as f64);

                // Step 8 — empty poll. Drop all gates (releases
                // permits + bytes, detaches the source-stage atomic
                // via Drop), record stage latency, sleep, continue.
                if descriptors.is_empty() {
                    let released = gates.len() as u64;
                    drop(gates);
                    metrics
                        .admission_extension_releases
                        .get_or_create(&crate::metrics::SourceLabels {
                            source: source_id.0.clone(),
                        })
                        .inc_by(released);
                    metrics
                        .stage_latency_seconds
                        .get_or_create(&crate::metrics::StageLabels {
                            stage: "source".to_string(),
                            source: source_id.0.clone(),
                        })
                        .observe(stage_start.elapsed().as_secs_f64());
                    tokio::time::sleep(options.poll_interval).await;
                    continue;
                }

                // Step 9 — truncate excess gates if buffer returned
                // fewer descriptors than the loop acquired. Count
                // `gates.len() - descriptors.len()` (NOT
                // `k_target - descriptors.len()` — the extension
                // loop may have broken before reaching k_target).
                let admitted = descriptors.len();
                if admitted < gates.len() {
                    let released = (gates.len() - admitted) as u64;
                    gates.truncate(admitted);
                    metrics
                        .admission_extension_releases
                        .get_or_create(&crate::metrics::SourceLabels {
                            source: source_id.0.clone(),
                        })
                        .inc_by(released);
                }
                debug_assert_eq!(gates.len(), admitted);

                // Step 10 — register-all pass. Synchronous, in
                // source-sequence order. INV-ADMISSION-CONTIGUOUS:
                // every descriptor's `register_pending` is observed
                // before any descriptor leaves the arm on
                // `descriptor_tx.send`.
                for descriptor in &descriptors {
                    let seq = descriptor.sequence;
                    if let Some(recorder) = admission_recorder.as_ref() {
                        recorder.lock().unwrap().push((source_id.clone(), seq));
                    }
                    coordinator.register_pending(seq, seq)?;
                    in_flight = in_flight.saturating_add(1);
                    metrics
                        .descriptors_handed_out
                        .get_or_create(&crate::metrics::SourceLabels {
                            source: source_id.0.clone(),
                        })
                        .inc();
                }

                // Step 11 — send-all pass. The two passes are
                // separated so a send-failure mid-batch leaves the
                // remaining undriven descriptors + their gates owned
                // by this stack; their Drop releases permits + bytes
                // on the error return. Already-sent descriptors are
                // in-flight at workers and replay on restart per RFC
                // 0003.
                let mut send_iter = descriptors.into_iter().zip(gates.into_iter());
                while let Some((descriptor, gate)) = send_iter.next() {
                    let seq = descriptor.sequence;
                    if descriptor_tx
                        .send(AdmittedDescriptor {
                            descriptor,
                            reservation: gate.reservation,
                            batch_permit: gate.batch_permit,
                        })
                        .await
                        .is_err()
                    {
                        // Remaining (descriptor, gate) tuples drop
                        // here via Drop of send_iter on early return.
                        return Err(RuntimeError::Pipeline(format!(
                            "descriptor lost: source={source_id} seq={seq} cause=worker-stage-closed",
                        )));
                    }
                }

                metrics
                    .stage_latency_seconds
                    .get_or_create(&crate::metrics::StageLabels {
                        stage: "source".to_string(),
                        source: source_id.0.clone(),
                    })
                    .observe(stage_start.elapsed().as_secs_f64());
            }
        }
    }

    if !dry_run {
        source.flush_acks().await?;
    }

    // Final progress sample on the way out.
    progress.pending_ranges_total = coordinator.pending_count();
    let _ = progress_tx.send(progress);
    Ok(())
}

/// Emit the four `runtime_stage_inflight_bytes{stage=...,source}`
/// gauges from the stage-counter snapshot. Called by the actor's
/// completion arm after every committed unit; covers all stages
/// (`source` / `fetch` / `decode` / `sink_dispatch`) and is the
/// canonical emission site for the per-stage breakdown.
fn emit_stage_inflight_gauges(
    stage_bytes: &StageInflightBytes,
    source_id: &SourceId,
    metrics: &crate::metrics::RuntimeMetrics,
) {
    let source_label = source_id.0.clone();
    for (stage_label, atomic) in [
        ("source", &stage_bytes.source),
        ("fetch", &stage_bytes.fetch),
        ("decode", &stage_bytes.decode),
        ("sink_dispatch", &stage_bytes.sink_dispatch),
    ] {
        metrics
            .stage_inflight_bytes
            .get_or_create(&crate::metrics::StageLabels {
                stage: stage_label.to_string(),
                source: source_label.clone(),
            })
            .set(atomic.load(Ordering::SeqCst) as i64);
    }
}

/// Output of a successful admission attempt: the actor still has to
/// poll the buffer and `register_pending` synchronously, but the
/// per-source backpressure axes have already been claimed.
struct AdmissionGate {
    batch_permit: tokio::sync::OwnedSemaphorePermit,
    reservation: ByteReservation,
}

/// Park until the per-source budget + batch-slot semaphore both have
/// room for the next descriptor. Cancellation-safe under `select!`:
/// dropping the returned future before it resolves leaves the budget
/// at its pre-call value (no held bytes / no held permit).
async fn admission_attempt(
    _source: &SourceId,
    budget: &Arc<SourceByteBudget>,
    batch_semaphore: &Arc<Semaphore>,
    _in_flight: u64,
    bp: &SourceBackpressureOptions,
) -> AdmissionGate {
    let batch_permit = Arc::clone(batch_semaphore)
        .acquire_owned()
        .await
        .expect("batch semaphore not closed before actor exit");
    let reservation = budget.reserve(bp.estimated_max_batch_bytes).await;
    AdmissionGate {
        batch_permit,
        reservation,
    }
}

// =========================================================================
// Per-source fetch workers (row 6.2)
// =========================================================================

/// One of N fetch workers per source. Each holds a clone of the
/// source's `BufferSourceFetchHandle` (wrapping
/// `Arc<buffer::ConsumerFetchHandle>`, RFC 0003 — `fetch(&self, ...)`
/// is safe to call from concurrent tasks against distinct
/// descriptors). The shared `descriptor_rx` is an
/// `async_channel::Receiver` clone; recv is MPMC. Re-fetch safety
/// holds by RFC 0003 §Concurrency Model, though the actor's
/// admission arm sends each descriptor exactly once so re-fetch is
/// purely defense-in-depth.
///
/// The worker emits two metrics per fetch:
/// - `runtime_stage_queue_depth{stage=fetch,source}` — sampled
///   before the recv awaits.
/// - `runtime_stage_latency_seconds{stage=fetch,source}` — observed
///   per fetch invocation.
#[allow(clippy::too_many_arguments)]
async fn fetch_worker(
    fetch_handle: BufferSourceFetchHandle,
    descriptor_rx: async_channel::Receiver<AdmittedDescriptor>,
    fetched_tx: async_channel::Sender<FetchedBatch>,
    completion_tx: mpsc::Sender<WriteCompletion>,
    source_id: SourceId,
    worker_idx: usize,
    test_fetch_delay: Option<TestFetchDelayFn>,
    test_fetch_killswitch: Option<TestFetchKillswitch>,
    stage_bytes: StageInflightBytes,
    hard_abort_token: CancellationToken,
    metrics: Arc<crate::metrics::RuntimeMetrics>,
) -> RuntimeResult<()> {
    let source_label = source_id.0.clone();
    loop {
        metrics
            .stage_queue_depth
            .get_or_create(&crate::metrics::StageLabels {
                stage: "fetch".to_string(),
                source: source_label.clone(),
            })
            .set(descriptor_rx.len() as i64);
        metrics
            .stage_inflight_bytes
            .get_or_create(&crate::metrics::StageLabels {
                stage: "fetch".to_string(),
                source: source_label.clone(),
            })
            .set(stage_bytes.fetch.load(Ordering::SeqCst) as i64);

        let admitted = tokio::select! {
            biased;
            _ = hard_abort_token.cancelled() => return Ok(()),
            // Test-only killswitch (§1.3a). In production
            // `test_fetch_killswitch` is `None` and the arm parks
            // forever (`std::future::pending`), never preempting
            // the recv. When `Some(token)`, cancelling the token
            // unwinds this worker without going through the
            // supervisor's hard_abort_token — the actor and writer
            // pool stay live and observe the descriptor-channel
            // closure as the failure mode.
            _ = async {
                match test_fetch_killswitch.as_ref() {
                    Some(token) => token.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            } => return Ok(()),
            recv = descriptor_rx.recv() => match recv {
                Ok(a) => a,
                Err(_) => return Ok(()), // descriptor channel closed; graceful exit
            },
        };
        let AdmittedDescriptor {
            descriptor,
            mut reservation,
            batch_permit,
        } = admitted;
        // Transition the reservation from `source` to `fetch`
        // stage. `attach_stage` detaches the prior atomic
        // (subtracts `held` from `source`) and adds `held` to
        // `fetch`. If the worker errors before forwarding, Drop
        // decrements `fetch` automatically.
        reservation.attach_stage(Arc::clone(&stage_bytes.fetch));
        let admitted_sequence = descriptor.sequence;

        if let Some(delay_fn) = test_fetch_delay.as_ref() {
            let delay = delay_fn(admitted_sequence);
            if !delay.is_zero() && !sleep_with_abort(delay, &hard_abort_token).await {
                return Ok(());
            }
        }

        let stage_start = std::time::Instant::now();
        let fetch_result = tokio::select! {
            biased;
            _ = hard_abort_token.cancelled() => return Ok(()),
            r = fetch_handle.fetch(descriptor) => r,
        };
        let source_batch = match fetch_result {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    error = %e,
                    source = %source_id,
                    worker = worker_idx,
                    "fetch worker fatal",
                );
                let _ = completion_tx.send(WriteCompletion::Fatal(e)).await;
                return Ok(());
            }
        };
        metrics
            .stage_latency_seconds
            .get_or_create(&crate::metrics::StageLabels {
                stage: "fetch".to_string(),
                source: source_label.clone(),
            })
            .observe(stage_start.elapsed().as_secs_f64());
        // §4 fetch-stage byte throughput. `_count` on
        // STAGE_LATENCY_SECONDS already gives the batches-fetched
        // rate; this counter gives the byte rate against the
        // producer's wire bytes so we can answer "is the fetcher
        // keeping up?" without sampling per-batch sizes.
        let fetched_bytes: u64 = source_batch
            .entries
            .iter()
            .map(|e| e.raw_bytes.len() as u64 + e.raw_metadata.len() as u64)
            .sum();
        metrics
            .bytes_fetched
            .get_or_create(&crate::metrics::SourceLabels {
                source: source_label.clone(),
            })
            .inc_by(fetched_bytes);

        let send_result = with_backpressure_timer(
            &source_id,
            crate::metrics::BackpressureReason::DecodeBudget,
            &metrics,
            fetched_tx.send(FetchedBatch {
                source_batch,
                reservation,
                batch_permit,
                admitted_sequence,
            }),
        )
        .await;
        if send_result.is_err() {
            // Decode stage closed unexpectedly. INV-DESCRIPTOR-LOSS-FATAL.
            // The rejected FetchedBatch is dropped along with its
            // reservation; the fetch-stage atomic decrements
            // automatically via the reservation's Drop impl.
            let _ = completion_tx
                .send(WriteCompletion::Fatal(RuntimeError::Pipeline(format!(
                    "fetched-batch lost: source={source_id} seq={admitted_sequence} cause=decode-stage-closed",
                ))))
                .await;
            return Ok(());
        }
    }
}

// =========================================================================
// Per-source decode workers (row 6.3) — M parallel tasks.
//
// Stateless. Each worker pulls one `FetchedBatch` from the shared
// `fetched_rx`, decodes, **reconciles its `ByteReservation` to the
// actual post-decode bytes**, builds a `SinkCommitEnvelope`, and
// forwards to the sink writer task. Like fetch workers, no direct
// shutdown token — graceful drain happens via channel close.
//
// The oversize-fault gate caps the worst-case over-subscription that
// reconcile-grow can cause: if a decoded batch's actual bytes exceed
// `estimated_max_batch_bytes × oversize_fault_multiplier`, the worker
// emits `RuntimeError::Pipeline("oversize decoded batch: …")`. Default
// multiplier is 4×.
// =========================================================================

/// Post-decode bytes for the source-coordinate columns. Phase 6
/// design §Algorithms uses `decoded.estimated_bytes() +
/// source_coords_bytes(decoded)` as the byte-budget total — the
/// records crate already accounts for its own payload via
/// `TypedRecords::estimated_bytes`; this helper adds the
/// runtime-owned coordinate columns on top.
fn source_coords_bytes(coords: &crate::decoded_batch::SourceCoordinateColumns) -> u64 {
    let strings = coords.manifest_path.len() + coords.data_path.len();
    let n = coords.sequences.len();
    // u64 (sequence) + u32 (entry) + u32 (record) + i64 (ts) per row.
    let per_row = 8 + 4 + 4 + 8;
    (strings + n * per_row) as u64
}

#[allow(clippy::too_many_arguments)]
async fn decode_worker(
    decoder: Arc<dyn Decoder>,
    options: RuntimeOptions,
    sink_id: SinkId,
    source_id: SourceId,
    worker_idx: usize,
    fetched_rx: async_channel::Receiver<FetchedBatch>,
    sink_commit_tx: async_channel::Sender<SinkCommitEnvelope>,
    completion_tx: mpsc::Sender<WriteCompletion>,
    stage_bytes: StageInflightBytes,
    hard_abort_token: CancellationToken,
    metrics: Arc<crate::metrics::RuntimeMetrics>,
) -> RuntimeResult<()> {
    let source_label = source_id.0.clone();
    let bp = options.backpressure_for(&source_id);
    loop {
        metrics
            .stage_queue_depth
            .get_or_create(&crate::metrics::StageLabels {
                stage: "decode".to_string(),
                source: source_label.clone(),
            })
            .set(fetched_rx.len() as i64);
        metrics
            .stage_inflight_bytes
            .get_or_create(&crate::metrics::StageLabels {
                stage: "decode".to_string(),
                source: source_label.clone(),
            })
            .set(stage_bytes.decode.load(Ordering::SeqCst) as i64);

        let fetched = tokio::select! {
            biased;
            _ = hard_abort_token.cancelled() => return Ok(()),
            recv = fetched_rx.recv() => match recv {
                Ok(f) => f,
                Err(_) => return Ok(()),
            },
        };
        let FetchedBatch {
            source_batch,
            mut reservation,
            batch_permit,
            admitted_sequence,
        } = fetched;
        // Transition from `fetch` to `decode` stage. `attach_stage`
        // detaches the fetch atomic and adds `held` to decode;
        // subsequent `reconcile` calls adjust the decode atomic by
        // the same delta they apply to `budget.in_flight`.
        reservation.attach_stage(Arc::clone(&stage_bytes.decode));

        let stage_start = std::time::Instant::now();
        let outcome = tokio::select! {
            biased;
            _ = hard_abort_token.cancelled() => return Ok(()),
            r = decode_one(
                &decoder,
                &options,
                &sink_id,
                source_batch,
                admitted_sequence,
                &bp,
                &mut reservation,
                &metrics,
            ) => r,
        };
        metrics
            .stage_latency_seconds
            .get_or_create(&crate::metrics::StageLabels {
                stage: "decode".to_string(),
                source: source_label.clone(),
            })
            .observe(stage_start.elapsed().as_secs_f64());

        match outcome {
            Ok(DecodeOutcome::Live { commit, range }) => {
                let send_result = with_backpressure_timer(
                    &source_id,
                    crate::metrics::BackpressureReason::SinkBudget,
                    &metrics,
                    sink_commit_tx.send(SinkCommitEnvelope {
                        commit: *commit,
                        range,
                        reservation,
                        batch_permit,
                    }),
                )
                .await;
                if send_result.is_err() {
                    // Sink stage closed unexpectedly. Rejected
                    // envelope drops; the reservation's Drop
                    // decrements `decode` automatically.
                    let _ = completion_tx
                        .send(WriteCompletion::Fatal(RuntimeError::Pipeline(format!(
                            "sink-commit lost: source={source_id} seq={admitted_sequence} cause=sink-stage-closed",
                        ))))
                        .await;
                    return Ok(());
                }
            }
            Ok(DecodeOutcome::DryRun {
                range,
                rows_written,
            }) => {
                // Dry-run: skip the sink stage entirely; report
                // completion directly. Drop reservation + permit
                // after the message lands.
                let msg = WriteCompletion::Committed(CommittedReport {
                    range,
                    rows_written,
                });
                if completion_tx.send(msg).await.is_err() {
                    return Ok(());
                }
                drop(reservation);
                drop(batch_permit);
            }
            Err(e) => {
                warn!(
                    error = %e,
                    source = %source_id,
                    worker = worker_idx,
                    "decode worker fatal",
                );
                let _ = completion_tx.send(WriteCompletion::Fatal(e)).await;
                return Ok(());
            }
        }
    }
}

enum DecodeOutcome {
    // `SinkCommit` is large (carries the whole `DecodedBatch`); box
    // it so `DryRun` doesn't pad the enum.
    Live {
        commit: Box<SinkCommit>,
        range: (u64, u64),
    },
    DryRun {
        range: (u64, u64),
        rows_written: u64,
    },
}

#[allow(clippy::too_many_arguments)]
async fn decode_one(
    decoder: &Arc<dyn Decoder>,
    options: &RuntimeOptions,
    sink_id: &SinkId,
    source_batch: SourceBatch,
    admitted_sequence: u64,
    bp: &SourceBackpressureOptions,
    reservation: &mut ByteReservation,
    metrics: &crate::metrics::RuntimeMetrics,
) -> RuntimeResult<DecodeOutcome> {
    // Per-entry envelope validation.
    let envelopes =
        decode_envelopes(&source_batch).map_err(|e| RuntimeError::Decoder(Box::new(e)))?;
    validate_consistent(&envelopes, &options.configured_envelope)
        .map_err(|e| RuntimeError::Decoder(Box::new(e)))?;

    if let Some(envelope) = envelopes.first()
        && !decoder.accepts(envelope)
    {
        return Err(RuntimeError::Decoder(
            format!(
                "decoder rejected configured envelope: version={} signal_type={:?} encoding={:?}",
                envelope.version, envelope.signal_type, envelope.encoding,
            )
            .into(),
        ));
    }

    let decoded_batches = decoder.decode(source_batch)?;
    if decoded_batches.is_empty() {
        return Err(RuntimeError::Decoder(
            "decoder returned empty Vec<DecodedBatch>; every source batch must produce at \
             least one DecodedBatch (zero-record allowed) to keep admission contiguous \
             (INV-ADMISSION-CONTIGUOUS)"
                .into(),
        ));
    }
    if decoded_batches.len() != 1 {
        // Phase 6 contract: one SinkCommit per source range.
        return Err(RuntimeError::Decoder(
            format!(
                "Phase 6 expects one DecodedBatch per source batch; got {}",
                decoded_batches.len()
            )
            .into(),
        ));
    }
    let decoded = decoded_batches.into_iter().next().expect("len checked");

    let low = decoded.low_sequence;
    let high = decoded.high_sequence;
    if low != admitted_sequence || high != admitted_sequence {
        return Err(RuntimeError::Pipeline(format!(
            "decoded range {low}..={high} does not match admitted sequence \
             {admitted_sequence} (Phase 6 contract: one DecodedBatch per source batch with \
             low == high == descriptor.sequence)"
        )));
    }

    // Decode-time byte reconciliation. Phase 6 design §Algorithms >
    // Per-Source Decode Workers: actual post-decode memory =
    // `records.estimated_bytes() + source_coords_bytes(coords)`.
    //
    // The oversize-fault gate runs *before* the reconcile so a
    // pathological outlier halts the runtime instead of growing the
    // budget unboundedly. `saturating_mul` keeps an operator who
    // sets `estimated_max_batch_bytes` extremely high from
    // accidentally disabling the fault via u64 overflow.
    let records_bytes = match &decoded.records {
        DecodedRecords::Typed(t) => t.estimated_bytes() as u64,
    };
    let actual_bytes = records_bytes + source_coords_bytes(&decoded.source_columns);
    let fault_limit = bp
        .estimated_max_batch_bytes
        .saturating_mul(bp.oversize_fault_multiplier as u64);
    if actual_bytes > fault_limit {
        return Err(RuntimeError::Pipeline(format!(
            "oversize decoded batch: source range {low}..={high} actual_bytes={actual_bytes} \
             fault_limit={fault_limit} (estimated_max_batch_bytes={} × \
             oversize_fault_multiplier={})",
            bp.estimated_max_batch_bytes, bp.oversize_fault_multiplier,
        )));
    }
    reservation.reconcile(actual_bytes);

    let row_count = match &decoded.records {
        DecodedRecords::Typed(t) => t.record_count() as u64,
    };
    // §4 decode-stage record throughput. Live + dry-run both pay
    // the decode work, so the counter increments before branching
    // on dry_run.
    metrics
        .records_decoded
        .get_or_create(&crate::metrics::SourceLabels {
            source: decoded.source.0.clone(),
        })
        .inc_by(row_count);

    if options.dry_run {
        debug!(low, high, rows = row_count, "dry-run: skipping sink write");
        return Ok(DecodeOutcome::DryRun {
            range: (low, high),
            rows_written: row_count,
        });
    }

    let commit = build_commit(sink_id.clone(), low, high, decoded);
    Ok(DecodeOutcome::Live {
        commit: Box::new(commit),
        range: (low, high),
    })
}

// =========================================================================
// Shared sink writer pool (row 6.4) — W workers per Phase 6 design
// §Algorithms > Shared Sink Writer Pool with Round-Robin Fairness.
//
// Today's runtime is single-source, so workers share a single
// `sink_commit_rx` (async_channel MPMC). The round-robin dispatcher
// across per-source FIFOs lands when multi-source RuntimeBuilder
// support arrives.
//
// Each worker:
// - runs `write_with_retry` against the configured sink (the retry
//   loop wraps `sink.write` / `sink.check_committed` / the
//   inter-attempt sleep in `select!` against `hard_abort_token` so
//   a peer worker's Fatal unwinds this one promptly)
// - emits `WriteCompletion::Committed` on success, threading the
//   range + rows_written back to the per-source actor
// - on retry-budget exhaustion / Fatal, sends
//   `WriteCompletion::Fatal(e)` with the typed `RuntimeError` so
//   the actor's completion arm surfaces the original error
//   (the supervisor cancels `hard_abort_token` after the actor
//   returns `Err(...)`, which is what wakes peer workers parked
//   on slow I/O)
//   immediately
// - emits `runtime_sink_commits_total{source,sink,result}` per
//   outcome; samples `runtime_sink_queue_depth{sink}` pre-recv
// =========================================================================

#[allow(clippy::too_many_arguments)]
async fn writer_worker(
    sink: Arc<dyn Sink>,
    options: RuntimeOptions,
    source_id: SourceId,
    sink_id: SinkId,
    worker_idx: usize,
    sink_commit_rx: async_channel::Receiver<SinkCommitEnvelope>,
    completion_tx: mpsc::Sender<WriteCompletion>,
    stage_bytes: StageInflightBytes,
    hard_abort_token: CancellationToken,
    metrics: Arc<crate::metrics::RuntimeMetrics>,
) -> RuntimeResult<()> {
    let source_label = source_id.0.clone();
    let sink_label = sink_id.0.clone();
    loop {
        metrics
            .sink_queue_depth
            .get_or_create(&crate::metrics::SinkLabels {
                sink: sink_label.clone(),
            })
            .set(sink_commit_rx.len() as i64);
        metrics
            .stage_inflight_bytes
            .get_or_create(&crate::metrics::StageLabels {
                stage: "sink_dispatch".to_string(),
                source: source_label.clone(),
            })
            .set(stage_bytes.sink_dispatch.load(Ordering::SeqCst) as i64);

        let envelope = tokio::select! {
            biased;
            _ = hard_abort_token.cancelled() => return Ok(()),
            recv = sink_commit_rx.recv() => match recv {
                Ok(e) => e,
                Err(_) => return Ok(()),
            },
        };

        let SinkCommitEnvelope {
            commit,
            range,
            mut reservation,
            batch_permit,
        } = envelope;
        // Transition from `decode` to `sink_dispatch` stage. After
        // attach, `stage_bytes.sink_dispatch` reflects the sum of
        // held bytes across every concurrent writer worker — the
        // sink-side "in-flight summed across commits" the §1.4
        // closeout calls for. Drop on the success path / error
        // path / abort path decrements automatically.
        reservation.attach_stage(Arc::clone(&stage_bytes.sink_dispatch));
        metrics
            .sink_inflight_bytes
            .get_or_create(&crate::metrics::SinkLabels {
                sink: sink_label.clone(),
            })
            .set(stage_bytes.sink_dispatch.load(Ordering::SeqCst) as i64);
        let stage_start = std::time::Instant::now();
        let attempt =
            write_with_retry(&sink, commit, &options, &source_id, &hard_abort_token, &metrics)
                .await;
        metrics
            .stage_latency_seconds
            .get_or_create(&crate::metrics::StageLabels {
                stage: "sink_dispatch".to_string(),
                source: source_label.clone(),
            })
            .observe(stage_start.elapsed().as_secs_f64());
        metrics
            .sink_commits
            .get_or_create(&crate::metrics::SourceSinkResultLabels {
                source: source_label.clone(),
                sink: sink_label.clone(),
                result: attempt.outcome.as_label().to_string(),
            })
            .inc();

        match attempt.inner {
            Ok(WriteSuccess::Committed { result }) => {
                let msg = WriteCompletion::Committed(CommittedReport {
                    range,
                    rows_written: result.rows_written,
                });
                if completion_tx.send(msg).await.is_err() {
                    return Ok(()); // actor exited
                }
            }
            Ok(WriteSuccess::VerifiedAlreadyCommitted) => {
                // Range is durably written but no fresh row count.
                // Report zero so progress.records_written doesn't
                // double-count on replay.
                let msg = WriteCompletion::Committed(CommittedReport {
                    range,
                    rows_written: 0,
                });
                if completion_tx.send(msg).await.is_err() {
                    return Ok(());
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    source = %source_id,
                    worker = worker_idx,
                    outcome = ?attempt.outcome,
                    "writer worker fatal",
                );
                // Send the typed error to the actor first so the
                // completion arm sees the original `RuntimeError`
                // variant (not a `Pipeline("hard abort …")` from a
                // racing abort branch). Supervisor cancels
                // `hard_abort_token` after the actor exits with
                // `Err(...)`, which is what tears peer workers down.
                let _ = completion_tx.send(WriteCompletion::Fatal(e)).await;
                return Ok(());
            }
        }

        drop(reservation);
        drop(batch_permit);
        // Re-emit the sink-side gauge AFTER the reservation
        // dropped — without this, the gauge keeps the last
        // non-zero value the writer set when the envelope
        // arrived, and post-drain snapshots read stale state.
        // The post-drop read of `stage_bytes.sink_dispatch` is
        // the now-decremented total (the reservation's Drop
        // updated the atomic before this line ran).
        metrics
            .sink_inflight_bytes
            .get_or_create(&crate::metrics::SinkLabels {
                sink: sink_label.clone(),
            })
            .set(stage_bytes.sink_dispatch.load(Ordering::SeqCst) as i64);
    }
}

fn build_commit(
    sink_id: SinkId,
    low_sequence: u64,
    high_sequence: u64,
    batch: DecodedBatch,
) -> SinkCommit {
    let identity = CommitIdentity {
        source: batch.source.clone(),
        sink: sink_id,
        range: SequenceRange::new(low_sequence, high_sequence),
        schema_version: batch.schema_version,
    };
    SinkCommit { identity, batch }
}

/// Outcome of a write_with_retry call. Carries the metric label
/// the writer worker should emit alongside the inner result, so
/// `verified_already_committed` and `failed_retryable` distinguish
/// themselves from `committed` / `failed_fatal` in
/// `runtime_sink_commits_total`.
struct WriteAttempt {
    inner: RuntimeResult<WriteSuccess>,
    outcome: crate::metrics::SinkCommitOutcome,
}

enum WriteSuccess {
    /// Sink wrote the commit on this attempt. Carries the
    /// authoritative row count.
    Committed { result: SinkCommitResult },
    /// MaybeCommitted resolved to Committed via `check_committed`;
    /// the range is durably written but we don't have a fresh
    /// `SinkCommitResult`. Returns a zero-row result so
    /// `progress.records_written` doesn't double-count on replay.
    VerifiedAlreadyCommitted,
}

async fn write_with_retry(
    sink: &Arc<dyn Sink>,
    commit: SinkCommit,
    options: &RuntimeOptions,
    source_id: &SourceId,
    hard_abort_token: &CancellationToken,
    metrics: &crate::metrics::RuntimeMetrics,
) -> WriteAttempt {
    use crate::metrics::SinkCommitOutcome;
    let mut attempt = 0u32;
    let max_attempts = options.effective_retry_max_attempts();
    let backoff = options.effective_retry_backoff();
    loop {
        // sink.write awaits are wrapped in select! against the
        // abort token so a fatal from a peer worker unwinds this
        // worker promptly — without this, a parked sink call (or a
        // long retry sleep) would keep the worker alive past the
        // supervisor's `hard_abort_token.cancel()`.
        let write_result = tokio::select! {
            biased;
            _ = hard_abort_token.cancelled() => {
                return WriteAttempt {
                    inner: Err(RuntimeError::Pipeline(
                        "hard abort during Sink::write".into(),
                    )),
                    outcome: SinkCommitOutcome::FailedFatal,
                };
            }
            r = sink.write(commit.clone()) => r,
        };
        match write_result {
            Ok(result) => {
                return WriteAttempt {
                    inner: Ok(WriteSuccess::Committed { result }),
                    outcome: SinkCommitOutcome::Committed,
                };
            }
            Err(SinkCommitFailure::Fatal(e)) => {
                return WriteAttempt {
                    inner: Err(RuntimeError::Sink(e)),
                    outcome: SinkCommitOutcome::FailedFatal,
                };
            }
            Err(SinkCommitFailure::NotCommitted(e)) => {
                if attempt >= max_attempts {
                    return WriteAttempt {
                        inner: Err(RuntimeError::Sink(e)),
                        outcome: SinkCommitOutcome::FailedRetryable,
                    };
                }
                let slept = with_backpressure_timer(
                    source_id,
                    crate::metrics::BackpressureReason::Retrying,
                    metrics,
                    sleep_with_abort(backoff, hard_abort_token),
                )
                .await;
                if !slept {
                    return WriteAttempt {
                        inner: Err(RuntimeError::Pipeline(
                            "hard abort during retry sleep".into(),
                        )),
                        outcome: SinkCommitOutcome::FailedFatal,
                    };
                }
                attempt = attempt.saturating_add(1);
            }
            Err(SinkCommitFailure::MaybeCommitted(e)) => {
                let check = tokio::select! {
                    biased;
                    _ = hard_abort_token.cancelled() => {
                        return WriteAttempt {
                            inner: Err(RuntimeError::Pipeline(
                                "hard abort during check_committed".into(),
                            )),
                            outcome: SinkCommitOutcome::FailedFatal,
                        };
                    }
                    s = sink.check_committed(&commit.identity) => s,
                };
                match check {
                    Ok(CommitStatus::Committed) => {
                        return WriteAttempt {
                            inner: Ok(WriteSuccess::VerifiedAlreadyCommitted),
                            outcome: SinkCommitOutcome::VerifiedAlreadyCommitted,
                        };
                    }
                    Ok(CommitStatus::NotCommitted) | Ok(CommitStatus::Unknown) => {
                        if attempt >= max_attempts {
                            return WriteAttempt {
                                inner: Err(RuntimeError::Sink(e)),
                                outcome: SinkCommitOutcome::FailedRetryable,
                            };
                        }
                        let slept = with_backpressure_timer(
                            source_id,
                            crate::metrics::BackpressureReason::Retrying,
                            metrics,
                            sleep_with_abort(backoff, hard_abort_token),
                        )
                        .await;
                        if !slept {
                            return WriteAttempt {
                                inner: Err(RuntimeError::Pipeline(
                                    "hard abort during retry sleep".into(),
                                )),
                                outcome: SinkCommitOutcome::FailedFatal,
                            };
                        }
                        attempt = attempt.saturating_add(1);
                    }
                    Err(check_err) => {
                        // `check_committed` returned a runtime error of
                        // its own (e.g. transport failure). Surface it.
                        return WriteAttempt {
                            inner: Err(check_err),
                            outcome: SinkCommitOutcome::FailedFatal,
                        };
                    }
                }
            }
        }
    }
}

/// Sleep `duration` or return early if `token` cancels. Returns
/// `true` when the sleep ran to completion, `false` on abort.
async fn sleep_with_abort(duration: Duration, token: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = token.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_are_dry_run_logs() {
        let opts = RuntimeOptions::default();
        assert!(opts.dry_run, "default must be dry-run for safety");
        assert_eq!(opts.max_descriptors_per_poll, 8);
        assert!(matches!(
            opts.ack_flush_policy,
            AckFlushPolicy::EveryCommitGroup
        ));
        // New Phase 6 fields default to the pipelined profile.
        assert_eq!(opts.source_defaults.max_inflight_batches, 64);
        assert_eq!(opts.source_defaults.fetch_concurrency, 8);
        assert_eq!(opts.source_defaults.decode_concurrency, 4);
        assert_eq!(opts.sink.max_concurrent_commits, 4);
    }

    #[test]
    fn ack_flush_policy_default_is_every_commit_group() {
        assert_eq!(AckFlushPolicy::default(), AckFlushPolicy::EveryCommitGroup);
    }

    #[test]
    fn serial_profile_reproduces_phase_five_shape() {
        let bp = SourceBackpressureOptions::serial();
        assert_eq!(bp.max_inflight_batches, 1);
        assert_eq!(bp.fetch_concurrency, 1);
        assert_eq!(bp.decode_concurrency, 1);
    }

    #[test]
    fn backpressure_for_falls_back_to_defaults() {
        let opts = RuntimeOptions::default();
        let bp = opts.backpressure_for(&SourceId::from("any"));
        assert_eq!(bp.max_inflight_batches, 64);
    }

    #[test]
    fn backpressure_for_honors_override() {
        let mut opts = RuntimeOptions::default();
        let override_bp = SourceBackpressureOptions {
            max_inflight_batches: 5,
            ..SourceBackpressureOptions::default()
        };
        opts.source_overrides
            .insert(SourceId::from("hot"), override_bp);
        assert_eq!(
            opts.backpressure_for(&SourceId::from("hot"))
                .max_inflight_batches,
            5,
        );
        assert_eq!(
            opts.backpressure_for(&SourceId::from("other"))
                .max_inflight_batches,
            64,
        );
    }

    #[test]
    fn validate_backpressure_rejects_oversized_reservation() {
        // estimated > capacity → admission would deadlock on reserve.
        let bp = SourceBackpressureOptions {
            max_inflight_bytes: 1024,
            estimated_max_batch_bytes: 4096,
            ..SourceBackpressureOptions::default()
        };
        let err = validate_backpressure(&SourceId::from("hot"), &bp).expect_err("must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("estimated_max_batch_bytes"),
            "error should call out the oversize reservation: {msg}",
        );
        assert!(matches!(err, RuntimeError::Config(_)));
    }

    #[test]
    fn validate_backpressure_rejects_zero_concurrency() {
        for field in [
            "max_inflight_batches",
            "fetch_concurrency",
            "decode_concurrency",
        ] {
            let mut bp = SourceBackpressureOptions::default();
            match field {
                "max_inflight_batches" => bp.max_inflight_batches = 0,
                "fetch_concurrency" => bp.fetch_concurrency = 0,
                "decode_concurrency" => bp.decode_concurrency = 0,
                _ => unreachable!(),
            }
            let err = validate_backpressure(&SourceId::from("s"), &bp)
                .expect_err(&format!("must reject {field} = 0"));
            assert!(
                format!("{err}").contains(field),
                "error should name {field}: {err}",
            );
        }
    }

    #[test]
    fn validate_backpressure_accepts_default() {
        validate_backpressure(&SourceId::from("s"), &SourceBackpressureOptions::default())
            .expect("library defaults must validate");
        validate_backpressure(&SourceId::from("s"), &SourceBackpressureOptions::serial())
            .expect("serial profile must validate");
    }
}
