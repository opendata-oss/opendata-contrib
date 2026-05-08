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

/// Apply the per-range metadata items in a `buffer::ConsumedBatch` to
/// each record index, producing a flat list of [`SourceEntry`]s.
///
/// The buffer crate represents metadata as `Vec<Metadata>` where each
/// item has a `start_index`; the entry at index `i` belongs to the
/// metadata range whose `start_index` is the largest value `<= i`.
/// Ranges are emitted in order, so we can scan in lockstep with the
/// entries.
///
/// Phase 4.3 moves this from `clickhouse-ingestor::source`. The
/// `BufferSourceFetchHandle` impl in Phase 4.4 calls this to convert
/// the underlying `buffer::ConsumedBatch` into a [`SourceBatch`].
pub fn split_into_raw_entries(
    batch: buffer::ConsumedBatch,
    source: SourceId,
    manifest_path: impl Into<String>,
) -> SourceBatch {
    let buffer::ConsumedBatch {
        entries,
        sequence,
        location,
        metadata,
    } = batch;

    let mut raw = Vec::with_capacity(entries.len());
    // Index into `metadata` of the range that currently covers the
    // entry we're about to emit. We bump it forward as `entry_index`
    // crosses the next range's `start_index`.
    let mut range_idx = 0usize;
    for (i, payload) in entries.into_iter().enumerate() {
        let entry_index = i as u32;
        while range_idx + 1 < metadata.len() && metadata[range_idx + 1].start_index <= entry_index {
            range_idx += 1;
        }
        let (raw_metadata, ingestion_time_ms) = match metadata.get(range_idx) {
            Some(m) => (m.payload.clone(), m.ingestion_time_ms),
            None => (Bytes::new(), 0),
        };
        raw.push(SourceEntry {
            entry_index,
            raw_bytes: payload,
            raw_metadata,
            ingestion_time_ms,
        });
    }

    SourceBatch {
        source,
        sequence,
        manifest_path: manifest_path.into(),
        data_object_path: location,
        entries: raw,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn meta(start_index: u32, signal: u8, ts: i64) -> buffer::Metadata {
        buffer::Metadata {
            start_index,
            ingestion_time_ms: ts,
            payload: Bytes::copy_from_slice(&[1, signal, 1, 0]),
        }
    }

    #[test]
    fn single_metadata_range_applies_to_all_entries() {
        let batch = buffer::ConsumedBatch {
            entries: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            sequence: 7,
            location: "loc".into(),
            metadata: vec![meta(0, 1, 1234)],
        };
        let raw = split_into_raw_entries(batch, "test".into(), "manifest");
        assert_eq!(raw.source, "test".into());
        assert_eq!(raw.sequence, 7);
        assert_eq!(raw.manifest_path, "manifest");
        assert_eq!(raw.data_object_path, "loc");
        assert_eq!(raw.entries.len(), 2);
        for (i, entry) in raw.entries.iter().enumerate() {
            assert_eq!(entry.entry_index, i as u32);
            assert_eq!(&entry.raw_metadata[..], &[1, 1, 1, 0]);
            assert_eq!(entry.ingestion_time_ms, 1234);
        }
    }

    #[test]
    fn multiple_metadata_ranges_split_at_start_indexes() {
        // Three ranges: [0..2) signal=1, [2..5) signal=2, [5..) signal=1.
        let batch = buffer::ConsumedBatch {
            entries: (0..6)
                .map(|i| Bytes::copy_from_slice(format!("e{i}").as_bytes()))
                .collect(),
            sequence: 9,
            location: "loc".into(),
            metadata: vec![meta(0, 1, 100), meta(2, 2, 200), meta(5, 1, 300)],
        };
        let raw = split_into_raw_entries(batch, "test".into(), "manifest");
        let signals: Vec<u8> = raw.entries.iter().map(|e| e.raw_metadata[1]).collect();
        let times: Vec<i64> = raw.entries.iter().map(|e| e.ingestion_time_ms).collect();
        assert_eq!(signals, vec![1, 1, 2, 2, 2, 1]);
        assert_eq!(times, vec![100, 100, 200, 200, 200, 300]);
    }

    #[test]
    fn empty_metadata_yields_empty_envelopes() {
        let batch = buffer::ConsumedBatch {
            entries: vec![Bytes::from_static(b"x")],
            sequence: 1,
            location: "loc".into(),
            metadata: vec![],
        };
        let raw = split_into_raw_entries(batch, "test".into(), "manifest");
        assert_eq!(raw.entries.len(), 1);
        assert!(raw.entries[0].raw_metadata.is_empty());
        assert_eq!(raw.entries[0].ingestion_time_ms, 0);
    }
}
