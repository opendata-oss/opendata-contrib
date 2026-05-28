-- Tutorial logs table. Matches what the clickhouse-ingestor's OTLP-logs
-- adapter writes. `_odb_*` columns are system columns the ingestor emits
-- for every row; together with `_adapter_version` they make the
-- ReplacingMergeTree(_adapter_version) collapse retries on merge.
--
-- The `tutorial` database is created by the CLICKHOUSE_DB env var on the
-- container before this file runs.

CREATE TABLE IF NOT EXISTS tutorial.logs (
    Timestamp           DateTime64(9)               CODEC(Delta, ZSTD),
    ObservedTimestamp   DateTime64(9)               CODEC(Delta, ZSTD),
    SeverityText        LowCardinality(String),
    SeverityNumber      UInt8,
    ServiceName         LowCardinality(String),
    Body                String                      CODEC(ZSTD),
    ResourceAttributes  Map(LowCardinality(String), String),
    LogAttributes       Map(LowCardinality(String), String),
    TraceId             String                      CODEC(ZSTD),
    SpanId              String                      CODEC(ZSTD),
    _odb_sequence            UInt64,
    _odb_entry_index         UInt32,
    _odb_record_index        UInt32,
    _odb_manifest_path       LowCardinality(String),
    _odb_data_path           String,
    _odb_ingestion_time_ms   Int64,
    _adapter_version         UInt32
)
ENGINE = ReplacingMergeTree(_adapter_version)
PARTITION BY toDate(Timestamp)
ORDER BY (toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)
TTL toDate(Timestamp) + INTERVAL 30 DAY;

-- Grant the ingestor user write + read access.
GRANT INSERT, SELECT ON tutorial.logs TO ingestor;
