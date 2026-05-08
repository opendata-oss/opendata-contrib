//! `clickhouse-ingestor` — reusable Rust runtime that consumes OpenData
//! Buffer batches and writes them into ClickHouse.
//!
//! Layered following RFC 0003:
//!
//! ```text
//! BufferConsumerRuntime
//!   -> MetadataEnvelopeDecoder    (per-entry envelope parsing)
//!   -> SignalDecoder              (e.g. OtlpLogsDecoder)
//!   -> CommitGroup                (coalesce records across batches)
//!   -> Adapter                    (e.g. OtlpLogsClickHouseAdapter)
//!      -> Vec<InsertChunk>        (deterministic chunking, per-chunk token)
//!   -> ClickHouseWriter           (sync inserts, classified retry)
//!   -> AckController              (range ack, flush)
//! ```
//!
//! Phase 4.3 moved source-batch / envelope / commit-group concepts into
//! `opendata-ingest-runtime`. Phase 4.4a moves the OTLP logs decoder
//! into `opendata-ingest-otel` and the ClickHouse adapter + writer into
//! `opendata-ingest-clickhouse`. They are re-exported under their
//! existing top-level paths so the binary, integration tests, and any
//! external dependent on the alpha keep their imports stable through
//! Phase 4. Phase 4.4c rewires the binary onto `Runtime::builder` and
//! retires the transitional `SignalDecoder` trait + `BufferConsumerRuntime`.

pub mod ack;
pub mod bench;
pub mod config;
pub mod error;
pub mod metrics;
pub mod metrics_server;
pub mod runtime;
pub mod signal;

pub use opendata_ingest_clickhouse::{adapter, writer};
pub use opendata_ingest_runtime::{commit_group, envelope, source};

pub use ack::{AckController, AckFlushPolicy};
pub use adapter::logs::{LogsAdapterConfig, OtlpLogsClickHouseAdapter, logs_table_ddl};
pub use adapter::{Adapter, ClickHouseSettings, InsertChunk, RowValue};
pub use commit_group::{CommitGroup, CommitGroupBatch, CommitGroupThresholds};
pub use config::IngestorConfig;
pub use envelope::{
    ConfiguredEnvelope, EnvelopeError, MetadataEnvelope, PayloadEncoding, SignalType,
    decode_envelopes, validate_consistent,
};
pub use error::{IngestorError, IngestorResult};
pub use runtime::{BufferConsumerRuntime, RuntimeOptions};
pub use signal::{
    DecodedLogRecord, DecodedLogs, OtlpLogsDecoder, SignalDecoder, SourceCoordinates,
};
pub use source::{SourceBatch, SourceEntry, split_into_raw_entries};
pub use writer::{ClickHouseWriter, WriterError, WriterErrorClass};
