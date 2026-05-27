//! `clickhouse-ingestor` — binary crate that wires the sink-neutral
//! `opendata-ingest-runtime` runtime to the OTel logs decoder and the
//! ClickHouse sink plugin.
//!
//! The binary constructs:
//!
//! ```text
//! Runtime::builder()
//!     .add_source(BufferSource::new(buffer::Consumer, ...))
//!     .add_decoder(OtlpLogsDecoder::new())        // opendata-ingest-otel
//!     .set_sink(ClickHouseSink::new(...))         // opendata-ingest-clickhouse
//!     .with_options(...)
//!     .build()?
//!     .run(shutdown_token).await
//! ```
//!
//! This crate keeps the binary's wiring: config loading, metrics
//! recorder install, signal handling, metrics HTTP server. The
//! re-exports below let the existing integration tests
//! (`tests/clickhouse_round_trip.rs`) import legacy names
//! (`OtlpLogsClickHouseAdapter`, `ClickHouseWriter`,
//! `OtlpLogsDecoder`, `DecodedLogRecord`, `InsertChunk`, etc.)
//! through `clickhouse_ingestor::` so the rewrite is import-only
//! when the tests get ported.

pub mod bench;
pub mod config;
pub mod error;
pub mod metrics_registry;
pub mod metrics_server;

pub use opendata_ingest_clickhouse::{adapter, writer};
pub use opendata_ingest_otel::envelope;
pub use opendata_ingest_otel::logs as signal;
pub use opendata_ingest_runtime::source;

pub use adapter::logs::{LogsAdapterConfig, OtlpLogsClickHouseAdapter, logs_table_ddl};
pub use adapter::{
    Adapter, AdapterError, AdapterResult, ClickHouseAdapterBatch, ClickHouseSettings, InsertChunk,
    RowValue,
};
pub use config::IngestorConfig;
pub use envelope::{
    ConfiguredEnvelope, EnvelopeError, MetadataEnvelope, PayloadEncoding, SignalType,
    decode_envelopes, validate_consistent,
};
pub use error::{IngestorError, IngestorResult};
pub use opendata_ingest_clickhouse::ClickHouseSink;
pub use signal::{
    DecodedLogRecord, DecodedLogs, OtelDecodeError, OtlpLogsDecoder, RowSourceCoordinates,
};
pub use source::{SourceBatch, SourceEntry, split_into_raw_entries};
pub use writer::{ClickHouseWriter, WriterError, WriterErrorClass};
