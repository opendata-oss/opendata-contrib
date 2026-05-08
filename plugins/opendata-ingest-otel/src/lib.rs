//! OTLP signal decoders for the generic ingest runtime.
//!
//! Phase 4.1 stub: empty crate. Phase 4.4 moves
//! `clickhouse_ingestor::signal::OtlpLogsDecoder` here as
//! `opendata_ingest_otel::logs::OtlpLogsDecoder`, implementing
//! `opendata_ingest_runtime::Decoder`. The runtime crate never
//! depends on this crate; the dependency runs OTel→runtime only.
