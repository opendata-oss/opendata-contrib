//! Decoded record carrier (RFC 0002 §`DecodedBatch`).
//!
//! `DecodedRecords` carries the `Typed` variant; a columnar `Arrow`
//! variant is future work. Records are reference-counted
//! (`Arc<dyn TypedRecords>`) to keep the typed trait object cheap to
//! move across runtime stages.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::identity::SchemaVersion;
use crate::source::SourceId;

/// Schema descriptor exposed by typed records. Carries an opaque
/// name + version pair, matching RFC 0002's trait shapes.
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

/// Carries the `Typed` variant. A columnar `Arrow` variant
/// (RFC 0002) is future work.
#[derive(Clone)]
pub enum DecodedRecords {
    Typed(Arc<dyn TypedRecords>),
    // TODO(future): Arrow(Arc<arrow_array::RecordBatch>).
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
/// system columns (RFC 0002 §System Columns and Source
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
    /// represents. Used by the byte-budget reconciliation step.
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
