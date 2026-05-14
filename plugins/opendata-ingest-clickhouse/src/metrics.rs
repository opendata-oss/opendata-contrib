//! Phase 7 ClickHouse-side metric name constants. The writer emits
//! these per chunk (= per HTTP INSERT). Row 7.3 lands the
//! serialization-side trio and the insert-duration histogram;
//! row 7.5 adds the http_mode-labelled in-flight gauge.

/// Histogram. One sample per `execute_chunk` call.
pub const SERIALIZATION_DURATION_SECONDS: &str = "clickhouse_serialization_duration_seconds";

/// Histogram. One sample per `execute_chunk` call. Records the
/// byte length of the serialized chunk body.
pub const SERIALIZED_BYTES: &str = "clickhouse_serialized_bytes";

/// Histogram. One sample per `execute_chunk` call. Records the row
/// count of the chunk.
pub const CHUNK_ROWS: &str = "clickhouse_chunk_rows";

/// Histogram. One sample per HTTP INSERT attempt; labelled with
/// the attempt outcome.
pub const INSERT_DURATION_SECONDS: &str = "clickhouse_insert_duration_seconds";

/// Gauge. Concurrent HTTP requests in flight from the writer (row
/// 7.5). Wired here so the constant lives next to the rest of the
/// writer surface.
pub const HTTP_CONCURRENT_INFLIGHT: &str = "clickhouse_http_concurrent_inflight";

/// Per-attempt result label on `INSERT_DURATION_SECONDS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertResult {
    Ok,
    Retryable,
    NonRetryable,
    RetryBudgetExhausted,
}

impl InsertResult {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Retryable => "retryable",
            Self::NonRetryable => "non_retryable",
            Self::RetryBudgetExhausted => "retry_budget_exhausted",
        }
    }
}
