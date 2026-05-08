//! Decoded record carrier (RFC 0002 rev 5 §`DecodedBatch`).
//!
//! Phase 4.2 ships only the `Typed` variant of `DecodedRecords`; the
//! `Arrow` variant lands in Phase 7 alongside the columnar
//! prototype. Records and source-coordinate columns are
//! reference-counted so multi-route fanout is O(1) Arc clones with
//! no record copies.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::idempotency::SchemaVersion;
use crate::source::SourceId;

/// Schema descriptor exposed by typed records. Phase 7 fleshes this
/// out alongside the schema/mapping document; Phase 4.2 carries an
/// opaque name + version pair so trait shapes line up with RFC 0002
/// rev 5.
#[derive(Debug, Clone)]
pub struct TypedSchema {
    pub name: String,
    pub version: SchemaVersion,
}

pub trait TypedRecords: Send + Sync {
    fn record_count(&self) -> usize;
    fn estimated_bytes(&self) -> usize;
    fn schema(&self) -> &TypedSchema;
    /// Sinks that need a uniform record view downcast through here.
    fn as_any(&self) -> &dyn Any;
}

/// Phase 4.2 ships only the `Typed` variant. The `Arrow` variant
/// (RFC 0002 rev 5) lands in Phase 7 alongside the columnar
/// prototype; once benched, Phase 9 retires `Typed` for OTLP logs.
#[derive(Clone)]
pub enum DecodedRecords {
    Typed(Arc<dyn TypedRecords>),
    // TODO(phase-7): Arrow(Arc<arrow_array::RecordBatch>).
}

impl fmt::Debug for DecodedRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Typed(records) => f
                .debug_struct("DecodedRecords::Typed")
                .field("record_count", &records.record_count())
                .field("estimated_bytes", &records.estimated_bytes())
                .field("schema", records.schema())
                .finish(),
        }
    }
}

/// Source-coordinate columns parallel to records (one entry per
/// record). Sinks project the subset they materialize into target
/// system columns (RFC 0002 rev 5 §System Columns and Source
/// Coordinates).
#[derive(Debug, Clone)]
pub struct SourceCoordinateColumns {
    pub manifest_path: String,
    pub data_path: String,
    pub sequences: Vec<u64>,
    pub entry_indices: Vec<u32>,
    pub record_indices: Vec<u32>,
    pub ingestion_time_ms: Vec<i64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BatchStats {
    /// Total source bytes (post-decompress, pre-decode) the batch
    /// represents. Used by the byte-budget reconciliation step in
    /// Phase 6.
    pub source_byte_count: u64,
    /// Decoder's own estimate of decoded record memory. The runtime
    /// holds this against the source's in-flight budget while the
    /// batch is in any pipeline stage.
    pub decoded_byte_estimate: u64,
}

#[derive(Debug, Clone)]
pub struct DecodedBatch {
    pub source: SourceId,
    pub low_sequence: u64,
    pub high_sequence: u64,
    /// Number of input source entries this batch represents. Lets
    /// the commit group and the ack coordinator advance the input
    /// high-watermark even when `records` is empty.
    pub source_entry_count: u32,
    pub records: DecodedRecords,
    pub source_columns: SourceCoordinateColumns,
    pub stats: BatchStats,
    pub schema_version: SchemaVersion,
}
