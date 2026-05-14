//! Pluggable serializer surface for the ClickHouse writer. Row 7.3
//! extracts today's `render_jsoneachrow` into a `ChunkSerializer`
//! trait so row 7.4 can land a RowBinaryWithNamesAndTypes serializer
//! alongside without touching the writer's retry/dispatch path. The
//! writer holds an `Arc<dyn ChunkSerializer>` chosen via
//! `WriterConfig::serialization_format`.

use std::sync::Arc;

use crate::adapter::InsertChunk;
use crate::writer::WriterError;

pub mod jsoneachrow;
pub mod rowbinary;

pub use jsoneachrow::JsonEachRowSerializer;
pub use rowbinary::RowBinarySerializer;

/// The wire format the writer hands to ClickHouse for the chunk's
/// body. Row 7.3 ships `JsonEachRow` (default; bit-identical with
/// the legacy `render_jsoneachrow` path); row 7.4 ships `RowBinary`
/// alongside.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SerializationFormat {
    #[default]
    JsonEachRow,
    RowBinary,
}

impl SerializationFormat {
    /// Stable label for metrics (`format=...`).
    pub fn as_label(self) -> &'static str {
        match self {
            Self::JsonEachRow => "json_each_row",
            Self::RowBinary => "row_binary",
        }
    }

    /// The `FORMAT <X>` keyword used in the INSERT SQL.
    pub fn format_keyword(self) -> &'static str {
        match self {
            Self::JsonEachRow => "JSONEachRow",
            Self::RowBinary => "RowBinaryWithNamesAndTypes",
        }
    }
}

/// Pluggable chunk serializer. Implementors write the chunk's rows
/// to wire bytes; the writer is responsible for combining them with
/// the INSERT SQL preamble + URL params.
pub trait ChunkSerializer: Send + Sync {
    /// The format this serializer produces (used by the writer to
    /// build the INSERT SQL's `FORMAT <X>` clause and to label
    /// metrics).
    fn format(&self) -> SerializationFormat;
    /// Serialize the chunk to wire bytes.
    fn serialize(&self, chunk: &InsertChunk) -> Result<Vec<u8>, WriterError>;
}

/// Build the serializer for the requested format. Picked once at
/// `ClickHouseWriter::new(...)` and held as `Arc<dyn ChunkSerializer>`.
pub fn build_serializer(format: SerializationFormat) -> Arc<dyn ChunkSerializer> {
    match format {
        SerializationFormat::JsonEachRow => Arc::new(JsonEachRowSerializer),
        SerializationFormat::RowBinary => Arc::new(RowBinarySerializer),
    }
}
