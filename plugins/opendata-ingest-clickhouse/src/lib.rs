//! ClickHouse sink plugin for the generic ingest runtime.
//!
//! Phase 4.1 stub: empty crate. Phase 4.4 moves
//! `clickhouse_ingestor::{adapter, writer}` here and lands the
//! `opendata_ingest_runtime::Sink` impl that drives the existing
//! `Adapter::plan(...)` + `ClickHouseWriter::execute_all(...)`
//! through the runtime's `SinkCommit`/`SinkCommitFailure` contract.
