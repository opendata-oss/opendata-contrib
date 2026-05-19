//! Phase 7 ClickHouse-side metric name constants + Stage-1 typed
//! `prometheus-client` Family<_, _> surface ([`ClickHouseMetrics`]).
//!
//! The writer + sink emit these per chunk (= per HTTP INSERT) and per
//! commit. Stage 1 of the 2026-05-19 metrics migration (see
//! `plans/odb-high-throughput/stage1-metrics-migration-plan.md`) adds
//! the typed struct; the string constants below stay until the C3
//! call-site migration commit and are deleted there.

use std::sync::Arc;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

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

// =========================================================================
// Stage 1 — typed `prometheus-client` Family<_, _> emission surface.
// Mirrors the runtime crate's [`RuntimeMetrics`]. Bin constructs
// `Arc<ClickHouseMetrics>`, registers it once, and threads it through
// `ClickHouseSink::new` + `ClickHouseWriter::new`.
// =========================================================================

/// Histogram bucket boundaries for `*_seconds` metrics (1 ms → 30 s).
/// Mirrors the runtime crate's `SECONDS_HISTOGRAM_BUCKETS`.
pub const SECONDS_HISTOGRAM_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Histogram bucket boundaries for serialized chunk byte sizes
/// (1 KiB → 64 MiB, log2). Covers ClickHouse-row-binary chunks from
/// tiny smoke runs (KB scale) up to row-8.4's 32 MiB max chunk size.
pub const BYTES_HISTOGRAM_BUCKETS: &[f64] = &[
    1024.0, 4096.0, 16_384.0, 65_536.0, 262_144.0, 1_048_576.0, 4_194_304.0, 16_777_216.0,
    67_108_864.0,
];

/// Histogram bucket boundaries for chunk row counts (100 → 1 M, log10
/// stepped). Covers row-8.4's `max_chunk_rows=500_000` config plus
/// headroom.
pub const ROWS_HISTOGRAM_BUCKETS: &[f64] = &[
    100.0, 500.0, 1000.0, 5000.0, 10_000.0, 50_000.0, 100_000.0, 500_000.0, 1_000_000.0,
];

fn seconds_histogram() -> Histogram {
    Histogram::new(SECONDS_HISTOGRAM_BUCKETS.iter().copied())
}
fn bytes_histogram() -> Histogram {
    Histogram::new(BYTES_HISTOGRAM_BUCKETS.iter().copied())
}
fn rows_histogram() -> Histogram {
    Histogram::new(ROWS_HISTOGRAM_BUCKETS.iter().copied())
}

/// `{run_id}` — only `ROWS_COMMITTED_TOTAL` uses this set.
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct RunIdLabels {
    pub run_id: String,
}

/// `{format}` — shared by the per-chunk histograms emitted by the
/// writer's serializer (`SERIALIZATION_DURATION_SECONDS`,
/// `SERIALIZED_BYTES`, `CHUNK_ROWS`).
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct FormatLabels {
    pub format: String,
}

/// `{format, http_mode, result}` — `INSERT_DURATION_SECONDS`.
/// `result` values come from [`InsertResult::as_label`].
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct FormatHttpResultLabels {
    pub format: String,
    pub http_mode: String,
    pub result: String,
}

/// `{reason}` — `RETRY_COUNT_TOTAL` (moved into the plugin in C3).
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct ReasonLabels {
    pub reason: String,
}

/// `{http_mode}` — `HTTP_CONCURRENT_INFLIGHT` (gauge incremented +1
/// before each request and decremented -1 after, in `execute_once`).
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct HttpModeLabels {
    pub http_mode: String,
}

/// `{status_code}` — `INSERT_ERRORS_TOTAL`. Values are stringified
/// HTTP codes (`"503"`, `"429"`, …) or the synthetic
/// `"timeout"`/`"connect"`/`"network"` labels for transport errors.
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct StatusCodeLabels {
    pub status_code: String,
}

/// Typed ClickHouse-plugin metric surface. One field per metric
/// defined above. Constructed once in the bin; threaded through
/// `ClickHouseSink::new` and `ClickHouseWriter::new` as
/// `Arc<ClickHouseMetrics>`.
pub struct ClickHouseMetrics {
    pub rows_committed: Family<RunIdLabels, Counter>,
    /// Unlabeled — registered directly.
    pub commit_bytes: Counter,
    pub retry_count: Family<ReasonLabels, Counter>,
    pub insert_errors: Family<StatusCodeLabels, Counter>,

    pub http_concurrent_inflight: Family<HttpModeLabels, Gauge>,

    pub serialization_duration_seconds: Family<FormatLabels, Histogram>,
    pub serialized_bytes: Family<FormatLabels, Histogram>,
    pub chunk_rows: Family<FormatLabels, Histogram>,
    pub insert_duration_seconds: Family<FormatHttpResultLabels, Histogram>,
}

impl ClickHouseMetrics {
    pub fn new() -> Self {
        Self {
            rows_committed: Family::<RunIdLabels, Counter>::default(),
            commit_bytes: Counter::default(),
            retry_count: Family::<ReasonLabels, Counter>::default(),
            insert_errors: Family::<StatusCodeLabels, Counter>::default(),

            http_concurrent_inflight: Family::<HttpModeLabels, Gauge>::default(),

            serialization_duration_seconds: Family::<FormatLabels, Histogram>::new_with_constructor(
                seconds_histogram,
            ),
            serialized_bytes: Family::<FormatLabels, Histogram>::new_with_constructor(
                bytes_histogram,
            ),
            chunk_rows: Family::<FormatLabels, Histogram>::new_with_constructor(rows_histogram),
            insert_duration_seconds:
                Family::<FormatHttpResultLabels, Histogram>::new_with_constructor(
                    seconds_histogram,
                ),
        }
    }

    pub fn register(self: &Arc<Self>, registry: &mut Registry) {
        registry.register(
            "clickhouse_ingestor_rows_committed",
            "Rows successfully committed to ClickHouse, labeled by _odb_run_id.",
            self.rows_committed.clone(),
        );
        registry.register(
            "ingestor_commit_bytes",
            "Bytes successfully committed to ClickHouse (sum of serialized row lengths).",
            self.commit_bytes.clone(),
        );
        registry.register(
            "ingestor_retry_count",
            "Per-chunk retry attempts, labeled by reason.",
            self.retry_count.clone(),
        );
        registry.register(
            "clickhouse_insert_errors",
            "ClickHouse insert errors labeled by HTTP status_code (or 'timeout'/'connect'/'network').",
            self.insert_errors.clone(),
        );

        registry.register(
            "clickhouse_http_concurrent_inflight",
            "Concurrent HTTP requests in flight from the ClickHouse writer.",
            self.http_concurrent_inflight.clone(),
        );

        registry.register(
            "clickhouse_serialization_duration_seconds",
            "Per-chunk serialization latency, labeled by format.",
            self.serialization_duration_seconds.clone(),
        );
        registry.register(
            "clickhouse_serialized_bytes",
            "Per-chunk serialized body size in bytes, labeled by format.",
            self.serialized_bytes.clone(),
        );
        registry.register(
            "clickhouse_chunk_rows",
            "Per-chunk row count, labeled by format.",
            self.chunk_rows.clone(),
        );
        registry.register(
            "clickhouse_insert_duration_seconds",
            "Per-attempt ClickHouse insert latency, labeled by format, http_mode, and result.",
            self.insert_duration_seconds.clone(),
        );
    }
}

impl Default for ClickHouseMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus_client::encoding::text::encode;

    #[test]
    fn register_and_encode_round_trip() {
        let metrics = Arc::new(ClickHouseMetrics::new());
        let mut registry = Registry::default();
        metrics.register(&mut registry);

        metrics
            .rows_committed
            .get_or_create(&RunIdLabels {
                run_id: "smoke-test".into(),
            })
            .inc_by(600);
        metrics.commit_bytes.inc_by(12_345);
        metrics
            .http_concurrent_inflight
            .get_or_create(&HttpModeLabels {
                http_mode: "pooled".into(),
            })
            .inc();
        metrics
            .insert_duration_seconds
            .get_or_create(&FormatHttpResultLabels {
                format: "row_binary".into(),
                http_mode: "pooled".into(),
                result: InsertResult::Ok.as_label().into(),
            })
            .observe(0.042);

        let mut buf = String::new();
        encode(&mut buf, &registry).expect("encode");

        assert!(
            buf.contains(
                "clickhouse_ingestor_rows_committed_total{run_id=\"smoke-test\"} 600"
            ),
            "missing rows_committed counter line. Output was:\n{buf}",
        );
        assert!(
            buf.contains("ingestor_commit_bytes_total 12345"),
            "missing commit_bytes counter line. Output was:\n{buf}",
        );
        assert!(
            buf.contains("clickhouse_http_concurrent_inflight{http_mode=\"pooled\"} 1"),
            "missing http_concurrent_inflight gauge line. Output was:\n{buf}",
        );
        assert!(
            buf.contains(
                "clickhouse_insert_duration_seconds_bucket{le=\"0.05\",format=\"row_binary\",http_mode=\"pooled\",result=\"ok\"} 1"
            ),
            "missing insert_duration histogram +0.05s bucket count. Output was:\n{buf}",
        );
    }
}
