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

/// Counter. Rows successfully committed to ClickHouse, labelled by
/// `_odb_run_id` (extracted from each record's `LogAttributes` before
/// the adapter plans chunks). Distinguishes loss from delayed drain
/// during the row-8.4 ingestor drain ladder: the harness compares this
/// against loadgen's produced count to attribute backlog to drain rate
/// vs data loss without scanning the CH table.
///
/// Cardinality: one label value per active run_id. Bench workloads
/// emit ~1–3 active run_ids at a time and the harness wipes the
/// registry between cell deploys, so growth is bounded.
pub const ROWS_COMMITTED_TOTAL: &str = "clickhouse_ingestor_rows_committed_total";

/// Counter. Bytes successfully committed to ClickHouse (sum of row
/// approximate byte sizes), aggregated across runs. Companion to
/// [`ROWS_COMMITTED_TOTAL`] for the §4 commit-stage throughput rate.
pub const COMMIT_BYTES_TOTAL: &str = "ingestor_commit_bytes_total";

/// Counter. HTTP-level commit errors broken out by status code (or
/// the synthetic labels `"timeout"`, `"connect"`, `"network"` for
/// transport failures). Lets §4 separate ClickHouse-side 5xx from
/// network-side timeouts when drain rate falls.
pub const INSERT_ERRORS_TOTAL: &str = "clickhouse_insert_errors_total";

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
