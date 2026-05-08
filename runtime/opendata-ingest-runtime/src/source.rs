//! Source side of the runtime contract.
//!
//! `SourceReader` and `SourceFetchHandle` mirror RFC 0003's
//! `Consumer` / `ConsumerFetchHandle` split: the manifest owner is
//! `Send + 'static` and mutates ack state through `&mut self`; the
//! fetch handle is `Send + Sync` and clones across N parallel fetch
//! workers. Concrete `BufferSourceReader` / `BufferSourceFetchHandle`
//! impls land in Phase 4.3.

use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;

use crate::error::RuntimeResult;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceId(pub String);

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SourceId {
    fn from(s: String) -> Self {
        SourceId(s)
    }
}

impl From<&str> for SourceId {
    fn from(s: &str) -> Self {
        SourceId(s.to_string())
    }
}

/// Per-range metadata as it flows from the buffer source. Mirrors
/// the per-entry shape that `clickhouse-ingestor::source::RawEntry`
/// carries today; the byte-format envelope (parsed in Phase 4.3)
/// reads from `raw_metadata`.
#[derive(Debug, Clone)]
pub struct SourceRangeMetadata {
    pub raw_metadata: Bytes,
    pub ingestion_time_ms: i64,
}

/// Backpressure budget the source poller is allowed to consume on a
/// single `next_descriptors` call. Phase 6 wires reservations end to
/// end (RFC 0002 rev 5 §Backpressure Model > Byte Budget Accounting);
/// Phase 4.2 only carries the type so trait shapes match.
#[derive(Debug, Clone, Copy, Default)]
pub struct SourceBudget {
    pub bytes_remaining: u64,
    pub batches_remaining: u32,
}

#[derive(Debug, Clone)]
pub struct SourceBatchDescriptor {
    pub source: SourceId,
    pub sequence: u64,
    pub location: String,
    pub per_range_metadata: Vec<SourceRangeMetadata>,
    /// Object size in bytes when the source can supply it without an
    /// extra round trip. Buffer sources pass through
    /// `BatchDescriptor.object_bytes` (RFC 0003), which is `None`
    /// until the manifest format extension lands; the runtime's
    /// budget accounting falls back to
    /// `source.estimated_max_batch_bytes` when this is `None`.
    pub object_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SourceEntry {
    /// Index within the buffer batch. Same index the buffer record
    /// block uses; carried through so sinks can materialize it as
    /// the `_odb_entry_index` system column.
    pub entry_index: u32,
    pub raw_bytes: Bytes,
    pub raw_metadata: Bytes,
    pub ingestion_time_ms: i64,
}

#[derive(Debug, Clone)]
pub struct SourceBatch {
    pub source: SourceId,
    pub sequence: u64,
    pub manifest_path: String,
    pub data_object_path: String,
    pub entries: Vec<SourceEntry>,
}

/// Manifest-owner side of a source. Mutates ack state, so all
/// non-`fetch_handle` methods take `&mut self`. The runtime owns one
/// `SourceReader` per source and drives it from the descriptor
/// poller and the ack coordinator.
#[async_trait]
pub trait SourceReader: Send + 'static {
    /// Stable identifier for this source; used in metric labels and
    /// idempotency keys.
    fn id(&self) -> &SourceId;

    /// Fetch up to `max` new descriptors past the current cursor.
    /// Must not mutate the durable ack frontier. Returning fewer
    /// than `max` is allowed and signals "no more visible right
    /// now"; the runtime sleeps and retries.
    async fn next_descriptors(
        &mut self,
        max: usize,
        budget: SourceBudget,
    ) -> RuntimeResult<Vec<SourceBatchDescriptor>>;

    /// Construct a cloneable handle for fetching descriptors
    /// concurrently. Construction is O(1); the handle holds shared
    /// references to whatever the source needs (object store
    /// handle, HTTP client, etc.) and no manifest state.
    fn fetch_handle(&self) -> Box<dyn SourceFetchHandle>;

    /// Advance the durable ack frontier through (and including)
    /// `sequence`. Implementations honor the in-order requirement
    /// of the underlying source; the runtime guarantees monotonic
    /// advance.
    async fn ack_through(&mut self, sequence: u64) -> RuntimeResult<()>;

    /// Force the underlying source's durable checkpoint. The
    /// runtime calls this on flush boundaries.
    async fn flush_acks(&mut self) -> RuntimeResult<()>;
}

/// Cloneable, concurrency-safe fetch primitive. The runtime calls
/// `fetch` from N workers in parallel against distinct descriptors.
/// Implementations must not touch manifest or ack state here.
#[async_trait]
pub trait SourceFetchHandle: Send + Sync {
    async fn fetch(&self, descriptor: SourceBatchDescriptor) -> RuntimeResult<SourceBatch>;

    /// Object-safe clone. The default `Clone` derive does not work
    /// across `dyn Trait`; implementations return a new boxed
    /// handle.
    fn clone_box(&self) -> Box<dyn SourceFetchHandle>;
}

impl Clone for Box<dyn SourceFetchHandle> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
