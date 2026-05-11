//! Source-side data types and the concrete `BufferSource` for v1.
//!
//! For v1 (RFC 0002 rev 8), the source side is concrete — the
//! runtime owns a `BufferSource` + `Clone` `BufferSourceFetchHandle`
//! that wrap `buffer::Consumer` (and, once RFC 0003 ships, the
//! paired `ConsumerFetchHandle`). This file holds the sink-neutral
//! data types (`SourceId`, `SourceBatchDescriptor`, `SourceBatch`,
//! `SourceEntry`, `SourceBudget`, `SourceRangeMetadata`), the
//! `split_into_raw_entries` materialization helper, and the
//! concrete `BufferSource` / `BufferSourceFetchHandle` pair.
//!
//! `opendata-buffer` v0.2.0 only ships `next_batch / ack / flush`;
//! RFC 0003's `next_descriptors` / `ConsumerFetchHandle` /
//! `ack_through` are not yet released. The Phase 4 `BufferSource`
//! falls back to the serial-`next_batch` path: each
//! `next_descriptors(max, _)` call invokes `next_batch` up to `max`
//! times, stashes the resulting `SourceBatch` in a sequence-keyed
//! cache shared with `BufferSourceFetchHandle`, and returns
//! descriptors. The fetch handle pops the matching `SourceBatch`.
//! `ack_through(seq)` issues per-sequence `Consumer::ack(s)` calls
//! across `(last_acked+1)..=seq` because v0.2.0 lacks bulk ack.
//! Phase 6 swaps this for the RFC 0003 path once the buffer crate
//! releases the read-ahead API.

use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::error::{RuntimeError, RuntimeResult};

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
/// end (RFC 0002 rev 6 §Backpressure Model > Byte Budget Accounting);
/// Phase 4 only carries the type so the call shape matches.
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
/// `BufferSourceFetchHandle::fetch` impl in Phase 4.4 calls this to
/// convert each underlying `buffer::ConsumedBatch` into a
/// [`SourceBatch`].
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

/// Cloneable fetch primitive paired with [`BufferSource`]. Each
/// clone holds the same sequence-keyed `SourceBatch` cache the owner
/// populates via `next_descriptors`. Cloning is O(1) (Arc bump);
/// `fetch` is safe to call from N worker tasks against distinct
/// descriptors, though Phase 4 only exercises the serial path.
#[derive(Clone)]
pub struct BufferSourceFetchHandle {
    cached: Arc<Mutex<HashMap<u64, SourceBatch>>>,
}

impl BufferSourceFetchHandle {
    /// Pop the [`SourceBatch`] that [`BufferSource::next_descriptors`]
    /// stashed for `descriptor.sequence`. Returns an error if the
    /// descriptor was never handed out by this fetch handle's owner.
    pub async fn fetch(&self, descriptor: SourceBatchDescriptor) -> RuntimeResult<SourceBatch> {
        let mut guard = self
            .cached
            .lock()
            .expect("BufferSource cache mutex poisoned");
        guard.remove(&descriptor.sequence).ok_or_else(|| {
            RuntimeError::Source(
                format!("no cached SourceBatch for sequence {}", descriptor.sequence).into(),
            )
        })
    }
}

/// Concrete `BufferSource` for v1. Wraps `buffer::Consumer` as the
/// manifest owner (mutates ack state through `&mut self`); paired
/// with a [`BufferSourceFetchHandle`] that holds a `Clone` reference
/// to the sequence-keyed batch cache.
///
/// The runtime owns exactly one `BufferSource` per configured source
/// and drives it from the serial poll loop in `runtime::Runtime`.
pub struct BufferSource {
    id: SourceId,
    consumer: buffer::Consumer,
    manifest_path: String,
    cached: Arc<Mutex<HashMap<u64, SourceBatch>>>,
    last_acked: Option<u64>,
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
        Self {
            id: id.into(),
            consumer,
            manifest_path: manifest_path.into(),
            cached: Arc::new(Mutex::new(HashMap::new())),
            last_acked: last_acked_sequence,
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

    /// Fetch up to `max` new descriptors. Each call drives
    /// `Consumer::next_batch` (the v0.2.0 serial primitive) up to
    /// `max` times, stashes the resulting `SourceBatch` keyed by
    /// sequence in the shared cache, and returns descriptors. A
    /// `next_batch` returning `Ok(None)` stops the loop early (no
    /// more visible right now); the runtime sleeps and retries.
    ///
    /// `_budget` is reserved for the byte-budget filter that lands
    /// in Phase 6; v0.2.0 batches carry no size hint, so v1 ignores
    /// it.
    pub async fn next_descriptors(
        &mut self,
        max: usize,
        _budget: SourceBudget,
    ) -> RuntimeResult<Vec<SourceBatchDescriptor>> {
        let mut descriptors = Vec::with_capacity(max);
        for _ in 0..max {
            match self.consumer.next_batch().await {
                Ok(Some(batch)) => {
                    let sequence = batch.sequence;
                    let location = batch.location.clone();
                    let per_range_metadata = batch
                        .metadata
                        .iter()
                        .map(|m| SourceRangeMetadata {
                            raw_metadata: m.payload.clone(),
                            ingestion_time_ms: m.ingestion_time_ms,
                        })
                        .collect();
                    let source_batch =
                        split_into_raw_entries(batch, self.id.clone(), self.manifest_path.clone());
                    self.cached
                        .lock()
                        .expect("BufferSource cache mutex poisoned")
                        .insert(sequence, source_batch);
                    descriptors.push(SourceBatchDescriptor {
                        source: self.id.clone(),
                        sequence,
                        location,
                        per_range_metadata,
                        object_bytes: None,
                    });
                }
                Ok(None) => break,
                Err(e) => return Err(RuntimeError::Source(Box::new(e))),
            }
        }
        Ok(descriptors)
    }

    /// Build a fresh fetch handle pointing at the same sequence-keyed
    /// cache. O(1) — `Arc` bump.
    pub fn fetch_handle(&self) -> BufferSourceFetchHandle {
        BufferSourceFetchHandle {
            cached: Arc::clone(&self.cached),
        }
    }

    /// Advance the durable ack frontier through (and including)
    /// `sequence`. `buffer::Consumer::ack` requires strict in-order,
    /// one-at-a-time acks in v0.2.0, so this loops over
    /// `(last_acked+1)..=sequence`. Phase 6 swaps in
    /// `Consumer::ack_through(seq)` once RFC 0003 ships.
    pub async fn ack_through(&mut self, sequence: u64) -> RuntimeResult<()> {
        let start = self.last_acked.map(|s| s.saturating_add(1)).unwrap_or(0);
        for seq in start..=sequence {
            self.consumer
                .ack(seq)
                .await
                .map_err(|e| RuntimeError::Source(Box::new(e)))?;
        }
        self.last_acked = Some(sequence);
        Ok(())
    }

    /// Force the underlying `buffer::Consumer`'s durable manifest
    /// checkpoint. Called on `AckFlushPolicy` boundaries and on
    /// graceful shutdown.
    pub async fn flush_acks(&mut self) -> RuntimeResult<()> {
        self.consumer
            .flush()
            .await
            .map_err(|e| RuntimeError::Source(Box::new(e)))
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
