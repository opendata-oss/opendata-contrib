//! Source-side data types and the concrete `BufferSource` for v1.
//!
//! Phase 6 row 6.1 cut over to the RFC 0003-direct surface
//! (`Consumer::next_descriptors`, `Consumer::fetch_handle`,
//! `Consumer::ack_through`); the Phase 4 sequence-keyed cache and the
//! `first_seen` resume-anchor disappear. `BufferSourceFetchHandle`
//! wraps `Arc<buffer::ConsumerFetchHandle>` and is safe to clone into
//! N fetch worker tasks per RFC 0003 §Concurrency Model.
//!
//! The handle's `fetch(&self, descriptor)` is stateless. Two
//! concurrent fetches against the same handle (or two clones) against
//! distinct descriptors are fully independent; re-fetching the same
//! descriptor twice is safe by the same RFC. Phase 6 row 6.2 wires
//! the parallel fetch worker pool against this surface.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

use crate::error::{RuntimeError, RuntimeResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
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
/// single `next_descriptors` call. Phase 6 row 6.3 wires the byte
/// axis end to end via `SourceByteBudget`; Phase 4 only carried the
/// type so the call shape matches.
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
    /// Buffer-side metadata items reconstructed by the fetch
    /// handle when it converts the descriptor back into a
    /// `buffer::BatchDescriptor`. Kept on the descriptor so the
    /// fetch path is stateless: the consumer doesn't need to
    /// cache anything per sequence.
    pub buffer_metadata: Vec<buffer::Metadata>,
    /// Object size in bytes when the source can supply it without an
    /// extra round trip. `BufferSource` passes through
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

/// Apply the per-range metadata items in a `buffer::ConsumedBatch` to
/// each record index, producing a flat list of [`SourceEntry`]s.
///
/// The buffer crate represents metadata as `Vec<Metadata>` where each
/// item has a `start_index`; the entry at index `i` belongs to the
/// metadata range whose `start_index` is the largest value `<= i`.
/// Ranges are emitted in order, so we can scan in lockstep with the
/// entries.
///
/// Phase 4.3 moved this from `clickhouse-ingestor::source`. The
/// `BufferSourceFetchHandle::fetch` impl calls this to convert each
/// underlying `buffer::ConsumedBatch` into a [`SourceBatch`].
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

/// Cloneable, concurrency-safe fetch primitive paired with
/// [`BufferSource`]. Wraps `Arc<buffer::ConsumerFetchHandle>` from
/// RFC 0003 — the underlying handle is stateless and safe to call
/// from N tasks against distinct descriptors. Each `BufferSource`
/// emits a fresh handle from `BufferSource::fetch_handle()`; cloning
/// the handle into worker tasks is O(1) (Arc bump).
#[derive(Clone)]
pub struct BufferSourceFetchHandle {
    inner: Arc<buffer::ConsumerFetchHandle>,
    source: SourceId,
    manifest_path: String,
}

impl BufferSourceFetchHandle {
    /// Fetch the data object pointed at by `descriptor` and
    /// materialize a [`SourceBatch`]. `&self` per RFC 0003 — calls
    /// against distinct descriptors are fully independent.
    /// Re-fetching the same descriptor twice is safe (RFC 0003
    /// §Concurrency Model > "Re-fetching a descriptor is safe").
    pub async fn fetch(&self, descriptor: SourceBatchDescriptor) -> RuntimeResult<SourceBatch> {
        let buffer_descriptor = buffer::BatchDescriptor {
            sequence: descriptor.sequence,
            location: descriptor.location,
            metadata: descriptor.buffer_metadata,
            object_bytes: descriptor.object_bytes,
        };
        let consumed = self
            .inner
            .fetch(buffer_descriptor)
            .await
            .map_err(|e| RuntimeError::Source(Box::new(e)))?;
        Ok(split_into_raw_entries(
            consumed,
            self.source.clone(),
            self.manifest_path.clone(),
        ))
    }
}

/// Concrete `BufferSource` for v1. Wraps `buffer::Consumer` as the
/// manifest owner (mutates ack state through `&mut self`); paired
/// with a [`BufferSourceFetchHandle`] that wraps a cloneable
/// `Arc<buffer::ConsumerFetchHandle>`.
///
/// The runtime owns exactly one `BufferSource` per configured source.
/// The per-source actor task (Phase 6 row 6.4) holds the `&mut
/// BufferSource` for the source's lifetime; admission and
/// `ack_through` happen on distinct `select!` arms inside that one
/// task, so no synchronization wrapper is needed.
pub struct BufferSource {
    id: SourceId,
    consumer: buffer::Consumer,
    manifest_path: String,
    fetch_handle_inner: Arc<buffer::ConsumerFetchHandle>,
    last_acked: Option<u64>,
    /// Highest sequence advanced by `ack_through` but not yet
    /// durably persisted. Drained on `flush_acks`. Lets the runtime
    /// keep [`AckFlushPolicy::EveryN`] semantics from Phase 5 even
    /// though the buffer-side [`buffer::Consumer::ack_through`] now
    /// dequeues durably on every call (RFC 0003). Without this
    /// indirection, every commit would be a durable boundary and
    /// `EveryN { n }` would degrade to `EveryCommitGroup`.
    pending_durable_ack: Option<u64>,
}

impl BufferSource {
    /// Build a `BufferSource` over an already-constructed
    /// `buffer::Consumer`. Callers own the object-store wiring; this
    /// keeps the runtime crate independent of `slatedb`.
    pub fn new(
        consumer: buffer::Consumer,
        id: impl Into<SourceId>,
        manifest_path: impl Into<String>,
        last_acked_sequence: Option<u64>,
    ) -> Self {
        let manifest_path = manifest_path.into();
        let fetch_handle_inner = Arc::new(consumer.fetch_handle());
        Self {
            id: id.into(),
            consumer,
            manifest_path,
            fetch_handle_inner,
            last_acked: last_acked_sequence,
            pending_durable_ack: None,
        }
    }

    pub fn id(&self) -> &SourceId {
        &self.id
    }

    pub fn manifest_path(&self) -> &str {
        &self.manifest_path
    }

    pub fn last_acked_sequence(&self) -> Option<u64> {
        self.last_acked
    }

    /// Fetch up to `max` new descriptors from the manifest. Delegates
    /// directly to `Consumer::next_descriptors` (RFC 0003) — the
    /// consumer maintains its own read-ahead cursor, so successive
    /// calls return contiguous, monotonically increasing sequences
    /// without re-reading the manifest each time.
    ///
    /// `_budget` is the byte-budget filter that Phase 6 wires end to
    /// end via [`crate::source_budget::SourceByteBudget`]; the
    /// buffer-side `next_descriptors` does not yet accept a budget
    /// arg, so the runtime gates admission on the budget BEFORE
    /// calling this method (Phase 6 design §Algorithms > Per-Source
    /// Actor).
    pub async fn next_descriptors(
        &mut self,
        max: usize,
        _budget: SourceBudget,
    ) -> RuntimeResult<Vec<SourceBatchDescriptor>> {
        let raw = self
            .consumer
            .next_descriptors(max)
            .await
            .map_err(|e| RuntimeError::Source(Box::new(e)))?;
        Ok(raw
            .into_iter()
            .map(|d| SourceBatchDescriptor {
                source: self.id.clone(),
                sequence: d.sequence,
                per_range_metadata: d
                    .metadata
                    .iter()
                    .map(|m| SourceRangeMetadata {
                        raw_metadata: m.payload.clone(),
                        ingestion_time_ms: m.ingestion_time_ms,
                    })
                    .collect(),
                location: d.location,
                buffer_metadata: d.metadata,
                object_bytes: d.object_bytes,
            })
            .collect())
    }

    /// Build a fresh fetch handle. O(1) — `Arc` bump. The handle's
    /// `fetch` method is `&self` per RFC 0003, so the runtime may
    /// clone this into many fetch worker tasks (Phase 6 row 6.2).
    pub fn fetch_handle(&self) -> BufferSourceFetchHandle {
        BufferSourceFetchHandle {
            inner: Arc::clone(&self.fetch_handle_inner),
            source: self.id.clone(),
            manifest_path: self.manifest_path.clone(),
        }
    }

    /// Advance the in-memory ack frontier through (and including)
    /// `sequence`. Does **not** touch the durable manifest — that
    /// happens in [`flush_acks`](Self::flush_acks). Lets the
    /// runtime preserve the Phase 5 `AckFlushPolicy::EveryN`
    /// semantic on top of RFC 0003's durable-immediate
    /// `Consumer::ack_through`.
    pub async fn ack_through(&mut self, sequence: u64) -> RuntimeResult<()> {
        self.pending_durable_ack = Some(sequence);
        self.last_acked = Some(sequence);
        Ok(())
    }

    /// Drain the pending in-memory ack through the buffer's
    /// durable manifest. Equivalent to
    /// `Consumer::ack_through(pending)` in RFC 0003 terms — the
    /// durable dequeue happens here, not on each
    /// [`ack_through`](Self::ack_through) call. Returns `Ok(())`
    /// when there's nothing pending. Surfaces `Error::Fenced` as
    /// [`RuntimeError::Source`] so the per-source actor halts on
    /// fence.
    pub async fn flush_acks(&mut self) -> RuntimeResult<()> {
        if let Some(seq) = self.pending_durable_ack.take()
            && let Err(e) = self.consumer.ack_through(seq).await
        {
            // Re-arm the pending pointer so a retry against the
            // same sequence is safe (matches the per-call
            // fail-atomic contract).
            self.pending_durable_ack = Some(seq);
            return Err(RuntimeError::Source(Box::new(e)));
        }
        Ok(())
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
