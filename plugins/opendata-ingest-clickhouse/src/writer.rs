//! ClickHouse writer.
//!
//! Sync inserts only. Each chunk goes out as one HTTP `INSERT INTO ...
//! FORMAT <X>` request with `async_insert=0`,
//! `insert_deduplication_token=<chunk token>`, and an optional
//! `insert_quorum`. Phase 7.3 made the serializer pluggable via
//! [`ChunkSerializer`]: row 7.3 ships JSONEachRow (default; bit-identical
//! with the pre-7.3 path), row 7.4 lands RowBinaryWithNamesAndTypes
//! alongside.
//!
//! Errors are classified into retryable vs non-retryable so the runtime
//! can apply backoff for retryable failures and halt for the rest.

use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tracing::{debug, warn};

use crate::adapter::InsertChunk;
use crate::metrics::{
    CHUNK_ROWS, HTTP_CONCURRENT_INFLIGHT, INSERT_DURATION_SECONDS, InsertResult,
    SERIALIZATION_DURATION_SECONDS, SERIALIZED_BYTES,
};
use crate::serializer::{ChunkSerializer, SerializationFormat, build_serializer};

/// Metric name shared with `clickhouse-ingestor::metrics`. Emitted
/// per-attempt by the writer; the registry-side `describe_counter!`
/// still lives in the binary crate's metrics module.
const RETRY_COUNT_TOTAL: &str = "ingestor_retry_count_total";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterErrorClass {
    /// Network glitch, timeout, 5xx, 429. Caller should back off and retry
    /// the same chunk with the same token.
    Retryable,
    /// Schema mismatch, 4xx other than 429. The runtime should halt and
    /// surface the error to an operator.
    NonRetryable,
    /// The writer exhausted its internal retry budget on a string of
    /// retryable failures. ClickHouse may or may not have committed
    /// during one of those attempts (timeout after request body sent,
    /// 5xx after server-side commit, connection drop after the 200 OK
    /// was generated). Surface to the sink as `MaybeCommitted` so the
    /// runtime can call `check_committed` before retrying — per RFC
    /// 0002 rev 6 §`Sink`.
    RetryBudgetExhausted,
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("clickhouse insert failed (retryable): {message}")]
    Retryable { message: String },
    #[error("clickhouse insert failed (non-retryable): {message}")]
    NonRetryable { message: String },
    /// Tag attached to a retryable error after `max_attempts` was hit.
    /// Distinct from `NonRetryable` so the sink can promote it to
    /// `SinkCommitFailure::MaybeCommitted` instead of `Fatal`.
    #[error("clickhouse insert failed (retry budget exhausted): {message}")]
    RetryBudgetExhausted { message: String },
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl WriterError {
    pub fn class(&self) -> WriterErrorClass {
        match self {
            WriterError::Retryable { .. } => WriterErrorClass::Retryable,
            WriterError::RetryBudgetExhausted { .. } => WriterErrorClass::RetryBudgetExhausted,
            WriterError::NonRetryable { .. } | WriterError::Serialization(_) => {
                WriterErrorClass::NonRetryable
            }
        }
    }
}

/// How the writer manages its HTTP client. Phase 7.5 lands the
/// pooled variant alongside the legacy per-call mode so row 7.6's
/// matrix sweep can compare them.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum HttpClientMode {
    /// Build a fresh `reqwest::Client` per HTTP attempt. Matches
    /// the pre-7.5 writer; surfaces silent-insert-drop history in
    /// the comment on `http_client`.
    #[default]
    PerCall,
    /// Hold one shared `reqwest::Client` for the writer's lifetime
    /// with a configured idle pool. `reqwest::Client` is `Arc`-shared
    /// internally so the connection pool is reused across cloned
    /// handles.
    Pooled {
        /// `reqwest::ClientBuilder::pool_max_idle_per_host`.
        pool_max_idle_per_host: usize,
        /// `reqwest::ClientBuilder::pool_idle_timeout`.
        pool_idle_timeout_ms: u64,
    },
}

impl HttpClientMode {
    pub fn as_label(&self) -> &'static str {
        match self {
            Self::PerCall => "per_call",
            Self::Pooled { .. } => "pooled",
        }
    }
}

/// Configuration for the writer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub endpoint: String,
    pub user: String,
    pub password: String,
    /// Per-chunk request timeout. Applied via `clickhouse::Client`'s
    /// underlying HTTP client; exceeded → retryable.
    pub request_timeout: Duration,
    /// Maximum retry attempts per chunk. After exhaustion the writer
    /// surfaces a non-retryable error.
    pub max_attempts: u32,
    /// Initial backoff between retry attempts; doubles each attempt.
    pub initial_backoff: Duration,
    /// Wire format the writer hands ClickHouse for each chunk
    /// (`INSERT ... FORMAT <X>`). Default: JSONEachRow (matches the
    /// pre-7.3 writer behavior).
    pub serialization_format: SerializationFormat,
    /// HTTP client management mode. Default: PerCall (matches the
    /// pre-7.5 writer).
    pub http_client_mode: HttpClientMode,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8123".into(),
            user: "default".into(),
            password: String::new(),
            request_timeout: Duration::from_secs(30),
            max_attempts: 6,
            initial_backoff: Duration::from_millis(100),
            serialization_format: SerializationFormat::default(),
            http_client_mode: HttpClientMode::default(),
        }
    }
}

/// ClickHouse insert writer.
///
/// Drives `reqwest` directly so the ingestor can exchange a small,
/// classified error type with the runtime. We don't pull in the official
/// `clickhouse` crate because the alpha uses generic `InsertChunk` rows
/// rather than typed Rust structs, which is the workflow that crate is
/// optimized for.
#[derive(Clone)]
pub struct ClickHouseWriter {
    config: WriterConfig,
    serializer: Arc<dyn ChunkSerializer>,
    /// Some when `http_client_mode == Pooled`; None when PerCall
    /// (each call constructs a fresh client).
    pooled_client: Option<reqwest::Client>,
}

impl ClickHouseWriter {
    pub fn new(config: WriterConfig) -> Self {
        let serializer = build_serializer(config.serialization_format);
        let pooled_client = match &config.http_client_mode {
            HttpClientMode::PerCall => None,
            HttpClientMode::Pooled {
                pool_max_idle_per_host,
                pool_idle_timeout_ms,
            } => match reqwest::Client::builder()
                .timeout(config.request_timeout)
                .http1_only()
                .pool_max_idle_per_host(*pool_max_idle_per_host)
                .pool_idle_timeout(Duration::from_millis(*pool_idle_timeout_ms))
                .build()
            {
                Ok(c) => Some(c),
                Err(e) => {
                    // Fall back to PerCall semantics if the pooled
                    // builder fails — the per-call path then handles
                    // its own builder errors classically.
                    tracing::warn!("pooled reqwest builder failed, falling back to per-call: {e}");
                    None
                }
            },
        };
        Self {
            config,
            serializer,
            pooled_client,
        }
    }

    pub fn config(&self) -> &WriterConfig {
        &self.config
    }

    /// `http_mode` label for the per-attempt
    /// `clickhouse_insert_duration_seconds{http_mode}` metric.
    fn http_mode_label(&self) -> &'static str {
        self.config.http_client_mode.as_label()
    }

    /// Return the HTTP client for this attempt. In pooled mode the
    /// `reqwest::Client` is `Arc`-shared internally and cheap to
    /// clone; in per-call mode a fresh builder runs each call (the
    /// historical no-silent-drop shape).
    fn http_client(&self) -> Result<reqwest::Client, WriterError> {
        if let Some(c) = &self.pooled_client {
            return Ok(c.clone());
        }
        reqwest::Client::builder()
            .timeout(self.config.request_timeout)
            .http1_only()
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| WriterError::NonRetryable {
                message: format!("reqwest client: {e}"),
            })
    }

    /// Execute every chunk in order. Retryable failures back off and
    /// retry the same chunk; non-retryable failures halt the run.
    pub async fn execute_all(&self, chunks: &[InsertChunk]) -> Result<(), WriterError> {
        for chunk in chunks {
            self.execute_chunk(chunk).await?;
        }
        Ok(())
    }

    /// Execute a single chunk with classified retry. Same-token replays
    /// are dedup-safe at the table level. Emits the four Phase 7.3
    /// metric families: `clickhouse_serialization_duration_seconds`,
    /// `clickhouse_serialized_bytes`, and `clickhouse_chunk_rows`
    /// (one sample per chunk), plus `clickhouse_insert_duration_seconds`
    /// (one sample per HTTP INSERT attempt; labelled with the result).
    ///
    /// Per the Phase 7 design's §Bottleneck Attribution Methodology,
    /// `clickhouse_insert_duration_seconds` is defined to **include
    /// serialization time**. The first attempt's timer therefore wraps
    /// `serializer.serialize(...)` + the HTTP send; retries reuse the
    /// already-serialized body so their samples cover only the HTTP
    /// attempt. This keeps `serialize_fraction_of_insert =
    /// Σ serialize_duration / Σ insert_duration` in `[0, 1]` exactly
    /// (the readout's row 7.7 fraction relies on this).
    pub async fn execute_chunk(&self, chunk: &InsertChunk) -> Result<(), WriterError> {
        let format = self.serializer.format();
        let format_label = format.as_label();
        let chunk_start = Instant::now();
        let body = self.serializer.serialize(chunk)?;
        let serialize_secs = chunk_start.elapsed().as_secs_f64();
        metrics::histogram!(SERIALIZATION_DURATION_SECONDS, "format" => format_label)
            .record(serialize_secs);
        metrics::histogram!(SERIALIZED_BYTES, "format" => format_label).record(body.len() as f64);
        metrics::histogram!(CHUNK_ROWS, "format" => format_label).record(chunk.rows_count() as f64);

        let sql = render_insert_sql_clean(chunk, format);
        let http_mode = self.http_mode_label();

        let mut attempt: u32 = 0;
        let mut backoff = self.config.initial_backoff;
        loop {
            attempt += 1;
            // First attempt's timer reaches back to before serialize
            // so `insert_duration` includes serialize per the design.
            // Subsequent attempts reuse `body` and only time HTTP.
            let attempt_start = if attempt == 1 {
                chunk_start
            } else {
                Instant::now()
            };
            let result = self.execute_once(&sql, &body, chunk).await;
            let attempt_secs = attempt_start.elapsed().as_secs_f64();
            let attempt_label = match &result {
                Ok(()) => InsertResult::Ok,
                Err(e) => match e.class() {
                    WriterErrorClass::Retryable => InsertResult::Retryable,
                    WriterErrorClass::NonRetryable => InsertResult::NonRetryable,
                    WriterErrorClass::RetryBudgetExhausted => InsertResult::RetryBudgetExhausted,
                },
            };
            metrics::histogram!(
                INSERT_DURATION_SECONDS,
                "format" => format_label,
                "http_mode" => http_mode,
                "result" => attempt_label.as_label(),
            )
            .record(attempt_secs);

            match result {
                Ok(()) => {
                    debug!(
                        attempt,
                        token = %chunk.idempotency_token,
                        rows = chunk.rows_count(),
                        "clickhouse insert succeeded",
                    );
                    return Ok(());
                }
                Err(err) => match err.class() {
                    WriterErrorClass::Retryable if attempt < self.config.max_attempts => {
                        metrics::counter!(
                            RETRY_COUNT_TOTAL,
                            "reason" => "retryable",
                        )
                        .increment(1);
                        warn!(
                            attempt,
                            token = %chunk.idempotency_token,
                            "clickhouse insert failed retryably, backing off: {err}",
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                    }
                    WriterErrorClass::Retryable => {
                        return Err(WriterError::RetryBudgetExhausted {
                            message: format!(
                                "retry budget ({}) exhausted: {err}",
                                self.config.max_attempts
                            ),
                        });
                    }
                    WriterErrorClass::RetryBudgetExhausted | WriterErrorClass::NonRetryable => {
                        return Err(err);
                    }
                },
            }
        }
    }

    async fn execute_once(
        &self,
        sql_clean: &str,
        body: &[u8],
        chunk: &InsertChunk,
    ) -> Result<(), WriterError> {
        // SQL+data combined in body (no SETTINGS clause); per-request
        // settings ride as URL query params. This matches the curl
        // shape ClickHouse 23.3 accepts most consistently.
        let mut combined = Vec::with_capacity(sql_clean.len() + 2 + body.len());
        combined.extend_from_slice(sql_clean.as_bytes());
        combined.push(b'\n');
        combined.extend_from_slice(body);
        if let Ok(path) = std::env::var("INGESTOR_DUMP_INSERT_BODY") {
            let _ = std::fs::write(&path, &combined);
            tracing::debug!(path = %path, "dumped insert body");
        }

        let url = build_insert_url(&self.config.endpoint, chunk, self.config.request_timeout);
        let body_len = combined.len();
        tracing::debug!(
            url = %url,
            body_len,
            "issuing clickhouse insert",
        );
        let http = self.http_client()?;
        let mut req = http
            .post(&url)
            .header(reqwest::header::CONTENT_LENGTH, body_len.to_string())
            .body(combined);
        if !self.config.user.is_empty() {
            req = req.header("X-ClickHouse-User", &self.config.user);
        }
        if !self.config.password.is_empty() {
            req = req.header("X-ClickHouse-Key", &self.config.password);
        }
        let http_mode = self.http_mode_label();
        metrics::gauge!(HTTP_CONCURRENT_INFLIGHT, "http_mode" => http_mode).increment(1.0);
        let send_result = req.send().await;
        metrics::gauge!(HTTP_CONCURRENT_INFLIGHT, "http_mode" => http_mode).decrement(1.0);
        let resp = send_result.map_err(|e| classify_reqwest(&e))?;
        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            tracing::debug!(
                token = %chunk.idempotency_token,
                rows = chunk.rows_count(),
                response_bytes = resp_body.len(),
                "insert ok"
            );
            return Ok(());
        }
        Err(classify_status(status.as_u16(), &resp_body))
    }

    /// Run a SQL statement (DDL, SELECT, or INSERT-with-data-in-body).
    ///
    /// We POST the SQL as the body rather than via the `?query=`
    /// parameter so reqwest sets a Content-Length header naturally —
    /// older ClickHouse builds reject chunked POSTs with HTTP 411, and
    /// GET implies readonly so it can't run DDL.
    pub async fn execute_statement(&self, sql: &str) -> Result<String, WriterError> {
        let url = self.config.endpoint.trim_end_matches('/').to_string();
        let body_bytes = sql.as_bytes().to_vec();
        let body_len = body_bytes.len();
        let http = self.http_client()?;
        let mut req = http
            .post(url)
            .header(reqwest::header::CONTENT_LENGTH, body_len.to_string())
            .body(body_bytes);
        if !self.config.user.is_empty() {
            req = req.header("X-ClickHouse-User", &self.config.user);
        }
        if !self.config.password.is_empty() {
            req = req.header("X-ClickHouse-Key", &self.config.password);
        }
        let resp = req.send().await.map_err(|e| classify_reqwest(&e))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(body)
        } else {
            Err(classify_status(status.as_u16(), &body))
        }
    }
}

fn classify_reqwest(err: &reqwest::Error) -> WriterError {
    if err.is_timeout() || err.is_connect() {
        return WriterError::Retryable {
            message: format!("network/timeout: {err}"),
        };
    }
    if err.is_status() {
        // Status-class errors are handled by `classify_status` for the
        // body inspection; this branch is the fall-through.
        return WriterError::Retryable {
            message: format!("http status: {err}"),
        };
    }
    WriterError::Retryable {
        message: format!("request error: {err}"),
    }
}

fn classify_status(status: u16, body: &str) -> WriterError {
    if status == 429 || (500..600).contains(&status) {
        return WriterError::Retryable {
            message: format!("status {status}: {body}"),
        };
    }
    WriterError::NonRetryable {
        message: format!("status {status}: {body}"),
    }
}

fn render_insert_sql_clean(chunk: &InsertChunk, format: SerializationFormat) -> String {
    // No SETTINGS clause, no column list. `FORMAT <X>` matches the
    // serializer chosen above. Settings ride as URL query params.
    format!(
        "INSERT INTO {}.{} FORMAT {}",
        chunk.database,
        chunk.table,
        format.format_keyword(),
    )
}

fn build_insert_url(endpoint: &str, chunk: &InsertChunk, timeout: Duration) -> String {
    use std::fmt::Write;
    let mut url = String::new();
    url.push_str(endpoint.trim_end_matches('/'));
    url.push_str("/?async_insert=0");
    url.push_str("&date_time_input_format=best_effort");
    if chunk.settings.apply_deduplication_token {
        let _ = write!(
            &mut url,
            "&insert_deduplication_token={}",
            urlencoding::encode(&chunk.settings.insert_deduplication_token),
        );
    }
    if let Some(quorum) = &chunk.settings.insert_quorum {
        let _ = write!(&mut url, "&insert_quorum={}", urlencoding::encode(quorum));
    }
    if timeout > Duration::ZERO {
        let _ = write!(&mut url, "&max_execution_time={}", timeout.as_secs());
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{ClickHouseSettings, RowValue};

    fn chunk(rows: Vec<Vec<RowValue>>) -> InsertChunk {
        InsertChunk {
            database: "responsive".into(),
            table: "logs".into(),
            columns: vec!["a", "b"],
            rows,
            settings: ClickHouseSettings {
                insert_quorum: Some("auto".into()),
                insert_deduplication_token: "tok".into(),
                apply_deduplication_token: true,
            },
            idempotency_token: "tok".into(),
            chunk_index: 0,
            observability_labels: vec![],
        }
    }

    #[test]
    fn insert_sql_is_well_formed() {
        let c = chunk(vec![]);
        let sql = render_insert_sql_clean(&c, SerializationFormat::JsonEachRow);
        assert_eq!(sql, "INSERT INTO responsive.logs FORMAT JSONEachRow");
    }

    #[test]
    fn classify_status_503_is_retryable() {
        let err = classify_status(503, "service unavailable");
        assert_eq!(err.class(), WriterErrorClass::Retryable);
    }

    #[test]
    fn classify_status_429_is_retryable() {
        let err = classify_status(429, "too many requests");
        assert_eq!(err.class(), WriterErrorClass::Retryable);
    }

    #[test]
    fn classify_status_400_is_non_retryable() {
        let err = classify_status(400, "bad request");
        assert_eq!(err.class(), WriterErrorClass::NonRetryable);
    }

    /// Phase 4 review HIGH-2 regression. After the writer exhausts
    /// its internal retry budget on a string of retryable failures
    /// the error must classify as `RetryBudgetExhausted`, not
    /// `NonRetryable` — otherwise `ClickHouseSink` promotes it to
    /// `Fatal` and the runtime's `check_committed → retry` path is
    /// dead for ambiguous-commit scenarios.
    #[test]
    fn retry_budget_exhausted_classifies_distinctly() {
        let err = WriterError::RetryBudgetExhausted {
            message: "retry budget (3) exhausted: timeout".into(),
        };
        assert_eq!(err.class(), WriterErrorClass::RetryBudgetExhausted);
        // And `RetryBudgetExhausted` is distinct from `NonRetryable`
        // so the sink can branch.
        assert_ne!(
            WriterErrorClass::RetryBudgetExhausted,
            WriterErrorClass::NonRetryable
        );
    }

    /// HIGH-2 end-to-end at the writer level: `execute_chunk` with
    /// `max_attempts: 1` against an unreachable endpoint must return
    /// `RetryBudgetExhausted`. (The first attempt fails retryably
    /// with a connect error; the loop sees `attempt < max_attempts`
    /// is false and falls into the exhausted branch.)
    #[tokio::test]
    async fn execute_chunk_returns_retry_budget_exhausted_after_one_attempt() {
        let writer = ClickHouseWriter::new(WriterConfig {
            // Closed port on localhost; reqwest returns a connect
            // error immediately, which `classify_reqwest` flags as
            // retryable.
            endpoint: "http://127.0.0.1:1".into(),
            user: "default".into(),
            password: String::new(),
            request_timeout: Duration::from_millis(100),
            max_attempts: 1,
            initial_backoff: Duration::from_millis(1),
            ..Default::default()
        });
        // Minimum viable chunk: one row, one column. The body
        // never reaches the network because the connect fails.
        let chunk = InsertChunk {
            database: "test".into(),
            table: "logs".into(),
            columns: vec!["body"],
            rows: vec![vec![RowValue::String("hello".into())]],
            idempotency_token: "test-token".into(),
            chunk_index: 0,
            observability_labels: Vec::new(),
            settings: ClickHouseSettings::default(),
        };
        let err = writer
            .execute_chunk(&chunk)
            .await
            .expect_err("connect failure with max_attempts=1 must surface an error");
        assert_eq!(
            err.class(),
            WriterErrorClass::RetryBudgetExhausted,
            "got {err:?}"
        );
    }

    #[test]
    fn insert_url_carries_settings_as_query_params() {
        let c = chunk(vec![]);
        let url = build_insert_url("http://localhost:8123", &c, Duration::from_secs(5));
        assert!(url.starts_with("http://localhost:8123/?async_insert=0"));
        assert!(url.contains("date_time_input_format=best_effort"));
        assert!(url.contains("insert_deduplication_token=tok"));
        assert!(url.contains("insert_quorum=auto"));
        assert!(url.contains("max_execution_time=5"));
    }

    #[test]
    fn insert_url_omits_token_when_disabled() {
        let mut c = chunk(vec![]);
        c.settings.apply_deduplication_token = false;
        let url = build_insert_url("http://localhost:8123", &c, Duration::from_secs(5));
        assert!(!url.contains("insert_deduplication_token"));
    }
}
