//! ClickHouse sink plugin for the generic ingest runtime.
//!
//! Phase 4.4a moves `clickhouse_ingestor::{adapter, writer}` here.
//! The `opendata_ingest_runtime::Sink` impl that wraps
//! `Adapter::plan(...)` + `ClickHouseWriter::execute_all(...)`
//! into the runtime's `SinkCommit` / `SinkCommitFailure` contract
//! lands in Phase 4.4b.

pub mod adapter;
pub mod writer;

pub use adapter::logs::{LogsAdapterConfig, OtlpLogsClickHouseAdapter, logs_table_ddl};
pub use adapter::{
    Adapter, AdapterError, AdapterResult, ClickHouseSettings, InsertChunk, RowValue,
};
pub use writer::{ClickHouseWriter, WriterError, WriterErrorClass};
