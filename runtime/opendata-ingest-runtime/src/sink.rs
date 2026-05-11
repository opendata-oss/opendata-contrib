//! Sink trait (RFC 0002 rev 6 §`Sink`).
//!
//! `Sink::write` is the source-range commit unit: one call covers
//! one source sequence range for the configured sink. `Ok(_)` means
//! the full commit is durable; retry of the same `SinkCommit` must
//! be idempotent. The runtime branches on `SinkCommitFailure`:
//! `MaybeCommitted` triggers a `check_committed` lookup before
//! retry; `NotCommitted` retries directly; `Fatal` halts. That
//! resolution is wired in the orchestrator (Phase 4.4 `runtime.rs`);
//! this file only defines the contract.

use async_trait::async_trait;
use std::fmt;

use crate::decoded_batch::DecodedBatch;
use crate::error::{BoxError, RuntimeResult};
use crate::idempotency::IdempotencyKey;
use crate::source::SourceId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SinkId(pub String);

impl fmt::Display for SinkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SinkId {
    fn from(s: String) -> Self {
        SinkId(s)
    }
}

impl From<&str> for SinkId {
    fn from(s: &str) -> Self {
        SinkId(s.to_string())
    }
}

/// Maximum bytes-in-flight the runtime should hold for this sink
/// before pausing upstream pulls (RFC 0002 rev 6 §Backpressure
/// Model). Phase 6 wires it; Phase 4.2 carries the type.
#[derive(Debug, Clone, Copy, Default)]
pub struct SinkBudget {
    pub max_bytes_inflight: u64,
    pub max_concurrent_commits: u32,
}

/// One source-range commit unit for the configured sink (RFC 0002
/// rev 6). The runtime issues exactly one `Sink::write(commit)` per
/// `(source, low..=high)` range at a time; `Ok(_)` means the entire
/// range committed, and retry of the same `SinkCommit` is idempotent.
///
/// `source`, `low_sequence`, and `high_sequence` mirror `batch`'s
/// own fields for sink-side ergonomics (logging, metrics) so sinks
/// don't have to reach into the batch for routine attributes. The
/// runtime guarantees the duplicated fields stay consistent.
#[derive(Debug)]
pub struct SinkCommit {
    pub source: SourceId,
    pub sink: SinkId,
    pub low_sequence: u64,
    pub high_sequence: u64,
    pub batch: DecodedBatch,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SinkCommitResult {
    pub bytes_written: u64,
    pub rows_written: u64,
}

#[derive(Debug)]
pub enum SinkCommitFailure {
    /// Sink definitively did not commit. Safe to retry the same
    /// `write` call. Examples: connection refused, 5xx before
    /// request body sent, fast-path validation rejection that
    /// cannot have produced state.
    NotCommitted(BoxError),
    /// Request did not return success, but the sink may have
    /// committed (e.g. timeout after request body fully sent,
    /// connection drop after server-side commit, ClickHouse 200 OK
    /// dropped on the network). The runtime calls
    /// `check_committed(idempotency_key)` before deciding whether
    /// to retry.
    MaybeCommitted(BoxError),
    /// Non-retryable. The runtime halts. Examples: schema mismatch,
    /// permissions error, malformed request that cannot succeed
    /// without code or schema changes.
    Fatal(BoxError),
}

impl fmt::Display for SinkCommitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotCommitted(e) => write!(f, "sink not committed: {e}"),
            Self::MaybeCommitted(e) => write!(f, "sink maybe committed: {e}"),
            Self::Fatal(e) => write!(f, "sink fatal: {e}"),
        }
    }
}

impl std::error::Error for SinkCommitFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotCommitted(e) | Self::MaybeCommitted(e) | Self::Fatal(e) => Some(&**e),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitStatus {
    Committed,
    NotCommitted,
    /// Sink cannot tell from the idempotency key alone (e.g.
    /// ClickHouse insert dedupe window has passed). The runtime
    /// treats `Unknown` like `NotCommitted` for the retry decision
    /// and relies on table-level dedupe to clean up duplicates.
    /// `Unknown` is logged separately so an operator can audit how
    /// often it fires.
    Unknown,
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    fn id(&self) -> &SinkId;

    fn write_budget(&self) -> SinkBudget;

    /// Commit one source-range unit for the configured sink. Returns
    /// `Ok` only when the full commit is durable per RFC 0002 rev 6;
    /// returns the appropriate `SinkCommitFailure` variant otherwise.
    /// The runtime is allowed to retry the same `SinkCommit` after a
    /// non-fatal failure; implementations must keep retry idempotent.
    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure>;

    /// Inspect prior commit state for a given idempotency key. The
    /// runtime calls this on replay (after a crash) and on
    /// `MaybeCommitted` failure (after an ambiguous response from
    /// the sink). Sinks that cannot tell return
    /// `CommitStatus::Unknown`; the runtime then re-attempts the
    /// `write` and relies on the sink's table-level dedupe.
    async fn check_committed(&self, key: &IdempotencyKey) -> RuntimeResult<CommitStatus>;
}
