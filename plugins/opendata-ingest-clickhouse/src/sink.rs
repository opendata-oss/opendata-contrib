//! `opendata_ingest_runtime::Sink` impl for ClickHouse.
//!
//! Phase 4.4b shim that wraps the existing
//! [`OtlpLogsClickHouseAdapter::plan`] +
//! [`ClickHouseWriter::execute_all`] path. RFC 0002 rev 6 contract:
//!
//! - `Ok(_)` means the full source-range commit (every chunk for
//!   the range) is durable.
//! - Retry of the same `SinkCommit` is idempotent: the adapter
//!   produces deterministic chunks keyed off `(low_sequence,
//!   high_sequence, chunk_index)`, and ClickHouse dedupes via
//!   `insert_deduplication_token` plus
//!   `ReplacingMergeTree(_adapter_version)`.
//! - `MaybeCommitted(_)` is returned on writer-classified
//!   retryable failures so the runtime can call
//!   [`Sink::check_committed`] before deciding whether to retry.
//!   `check_committed` returns `Unknown` (the short-window
//!   ClickHouse insert dedupe token has expired by the time the
//!   runtime asks); per RFC 0002 rev 6 the runtime treats
//!   `Unknown` like `NotCommitted` and retries, with table-level
//!   dedupe as the long-window backstop.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::info;

use opendata_ingest_otel::logs::{DecodedLogRecord, TypedDecodedLogs};
use opendata_ingest_runtime::decoded_batch::{DecodedBatch, DecodedRecords};
use opendata_ingest_runtime::error::{RuntimeError, RuntimeResult};
use opendata_ingest_runtime::identity::CommitIdentity;
use opendata_ingest_runtime::sink::{
    CommitStatus, Sink, SinkBudget, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId,
};

use crate::adapter::{Adapter, ClickHouseAdapterBatch};
use crate::metrics::{COMMIT_BYTES_TOTAL, ROWS_COMMITTED_TOTAL};
use crate::writer::{ClickHouseWriter, WriterErrorClass};

/// LogAttributes key loadgen stamps on every record to identify the
/// run. The harness uses the same key in its correctness-gate SQL.
const ODB_RUN_ID_ATTR: &str = "_odb_run_id";

/// Records missing the `_odb_run_id` attribute land here. Keeps the
/// counter cardinality bounded even when an upstream producer skips
/// the stamp (a real bug to investigate via this very label).
const UNKNOWN_RUN_ID_LABEL: &str = "unknown";

/// `Sink` impl backed by an [`Adapter`] (planning) + a
/// [`ClickHouseWriter`] (HTTP execution). Generic over the adapter
/// type so production wiring uses `OtlpLogsClickHouseAdapter` and
/// tests can inject misbehaving adapters (e.g. a `DroppingAdapter`)
/// to exercise the row-drop guard introduced for Phase 4 review
/// MED-4a.
pub struct ClickHouseSink<A>
where
    A: Adapter<Input = DecodedLogRecord> + Send + Sync + 'static,
{
    id: SinkId,
    adapter: Arc<A>,
    writer: Arc<ClickHouseWriter>,
    budget: SinkBudget,
}

impl<A> ClickHouseSink<A>
where
    A: Adapter<Input = DecodedLogRecord> + Send + Sync + 'static,
{
    pub fn new(id: impl Into<SinkId>, adapter: Arc<A>, writer: Arc<ClickHouseWriter>) -> Self {
        Self {
            id: id.into(),
            adapter,
            writer,
            budget: SinkBudget::default(),
        }
    }

    pub fn with_budget(mut self, budget: SinkBudget) -> Self {
        self.budget = budget;
        self
    }
}

fn fatal(msg: impl Into<String>) -> SinkCommitFailure {
    SinkCommitFailure::Fatal(msg.into().into())
}

/// Build a `{run_id -> row_count}` map from a batch of decoded log
/// records. Records missing the `_odb_run_id` attribute fall into the
/// `"unknown"` bucket so the metric is still total-count-accurate.
fn bucket_rows_by_run_id(records: &[DecodedLogRecord]) -> HashMap<String, u64> {
    let mut counts: HashMap<String, u64> = HashMap::new();
    for rec in records {
        let run_id = rec
            .log_attributes
            .get(ODB_RUN_ID_ATTR)
            .map(|s| s.as_str())
            .unwrap_or(UNKNOWN_RUN_ID_LABEL);
        *counts.entry(run_id.to_string()).or_insert(0) += 1;
    }
    counts
}

#[async_trait]
impl<A> Sink for ClickHouseSink<A>
where
    A: Adapter<Input = DecodedLogRecord> + Send + Sync + 'static,
{
    fn id(&self) -> &SinkId {
        &self.id
    }

    fn write_budget(&self) -> SinkBudget {
        self.budget
    }

    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        let SinkCommit { identity, batch } = commit;
        // The runtime guarantees identity.range mirrors batch.low/high
        // for the source range Phase 5.9 + RFC 0002 §Runtime/Sink
        // Boundary specify; fail closed if a future runtime change
        // ever breaks the invariant so the sink does not silently
        // emit dedupe tokens that don't match the rows being written.
        if identity.range.low != batch.low_sequence || identity.range.high != batch.high_sequence {
            return Err(fatal(format!(
                "SinkCommit identity range {}..={} disagrees with DecodedBatch range {}..={}",
                identity.range.low, identity.range.high, batch.low_sequence, batch.high_sequence,
            )));
        }
        let DecodedBatch { records, .. } = batch;

        let DecodedRecords::Typed(typed) = records;
        let logs = typed
            .as_any()
            .downcast_ref::<TypedDecodedLogs>()
            .ok_or_else(|| {
                fatal(format!(
                    "ClickHouseSink expects TypedDecodedLogs (schema {}), got {:?}",
                    opendata_ingest_otel::logs::OTLP_LOGS_SCHEMA_NAME,
                    typed.schema(),
                ))
            })?;

        let selected = logs.records().to_vec();
        let input_row_count = selected.len();
        let bytes: usize = selected.iter().map(|r| r.approx_size_bytes()).sum();

        // Row 8.4 instrumentation-gap §1: bucket by _odb_run_id BEFORE
        // the adapter consumes `selected`. The adapter sorts and emits
        // RowValue chunks where the run_id is buried inside a StringMap,
        // so doing it here keeps the lookup a single map probe per
        // record rather than a column reconstruction.
        let rows_by_run_id = bucket_rows_by_run_id(&selected);

        let group = ClickHouseAdapterBatch {
            identity,
            records: selected,
            bytes,
        };

        let chunks = self
            .adapter
            .plan(group)
            .map_err(|e| SinkCommitFailure::Fatal(Box::new(e)))?;

        // Row-drop contract guard (Phase 4 review MED-4): the
        // adapter's planned chunks must cover every input record. An
        // adapter that drops rows would silently advance the ack
        // frontier past records that never reached ClickHouse — the
        // exact regression `runtime_rejects_adapter_that_drops_rows`
        // caught in the legacy `BufferConsumerRuntime`. Fail closed
        // before any HTTP call.
        let chunk_row_total: usize = chunks.iter().map(|c| c.rows_count()).sum();
        if chunk_row_total != input_row_count {
            return Err(fatal(format!(
                "adapter plan covered {chunk_row_total} rows but commit held {input_row_count}; \
                 refusing to insert and ack a partial/dropped chunking"
            )));
        }

        let bytes_written: u64 = chunks
            .iter()
            .map(|c| c.rows.iter().map(|r| r.len() as u64).sum::<u64>())
            .sum();
        let rows_written: u64 = chunks.iter().map(|c| c.rows_count() as u64).sum();

        match self.writer.execute_all(&chunks).await {
            Ok(()) => {
                // Per-run drain counter — §1. Increment exactly once
                // per record per successful commit. The harness reads
                // `rate(...{run_id="..."}[1m])` to track live drain
                // progress without scanning ClickHouse.
                for (run_id, count) in &rows_by_run_id {
                    info!(target: "metric_probe", run_id = %run_id, count = *count, "ROWS_COMMITTED_TOTAL +count");
                    metrics::counter!(
                        ROWS_COMMITTED_TOTAL,
                        "run_id" => run_id.clone(),
                    )
                    .increment(*count);
                }
                // §4 commit-stage byte throughput. `bytes_written` here
                // is the sum of serialized RowValue lengths the writer
                // sent; it's the same number reported in
                // SinkCommitResult.bytes_written.
                info!(target: "metric_probe", bytes = bytes_written, "COMMIT_BYTES_TOTAL +bytes");
                metrics::counter!(COMMIT_BYTES_TOTAL).increment(bytes_written);
                Ok(SinkCommitResult {
                    bytes_written,
                    rows_written,
                })
            }
            Err(e) => match e.class() {
                WriterErrorClass::Retryable | WriterErrorClass::RetryBudgetExhausted => {
                    // ClickHouse can ambiguously commit on retryable
                    // outcomes (timeout after request body sent,
                    // 5xx after server-side commit, connection drop
                    // after the 200 OK was generated). Retryable on
                    // first failure and budget-exhausted after the
                    // writer's internal retries both promote to
                    // MaybeCommitted so the runtime calls
                    // check_committed before the next attempt — per
                    // Phase 4 review HIGH-2.
                    Err(SinkCommitFailure::MaybeCommitted(Box::new(e)))
                }
                WriterErrorClass::NonRetryable => Err(SinkCommitFailure::Fatal(Box::new(e))),
            },
        }
    }

    async fn check_committed(&self, _identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
        // Alpha ClickHouse dedupes at the table layer
        // (`ReplacingMergeTree(_adapter_version)`); the short-
        // window `insert_deduplication_token` is gone by the time
        // the runtime asks. Returning `Unknown` is RFC 0002 rev 6's
        // documented contract for this case. A sink that wanted to
        // answer could recompute its physical token from `identity`
        // plus this adapter's configuration.
        let _ = &self.adapter;
        let _ = &self.writer;
        Ok::<CommitStatus, RuntimeError>(CommitStatus::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::adapter::{AdapterError, AdapterResult, InsertChunk};
    use crate::writer::WriterConfig;
    use opendata_ingest_otel::logs::RowSourceCoordinates;
    use opendata_ingest_runtime::decoded_batch::{BatchStats, SourceCoordinateColumns};
    use opendata_ingest_runtime::identity::{SchemaVersion, SequenceRange};
    use opendata_ingest_runtime::source::SourceId;

    /// Adapter that drops every record (returns no chunks). Mirrors
    /// the legacy `DroppingAdapter` from the in-memory runtime test;
    /// rebuilt here to exercise `ClickHouseSink`'s row-drop guard.
    struct DroppingAdapter;

    impl Adapter for DroppingAdapter {
        type Input = DecodedLogRecord;
        fn plan(
            &self,
            _batch: ClickHouseAdapterBatch<Self::Input>,
        ) -> AdapterResult<Vec<InsertChunk>> {
            Ok(Vec::new())
        }
    }

    fn fake_log_record(seq: u64) -> DecodedLogRecord {
        DecodedLogRecord {
            source: RowSourceCoordinates {
                buffer_sequence: seq,
                entry_index: 0,
                record_index: 0,
                manifest_path: "test/manifest".into(),
                data_path: "test/data".into(),
                ingestion_time_ms: 0,
            },
            timestamp_unix_nano: 0,
            observed_timestamp_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: "hello".into(),
            service_name: None,
            resource_attributes: BTreeMap::new(),
            scope_name: None,
            log_attributes: BTreeMap::new(),
            trace_id_hex: String::new(),
            span_id_hex: String::new(),
        }
    }

    fn fake_sink_commit(records: Vec<DecodedLogRecord>) -> SinkCommit {
        let count = records.len();
        let source = SourceId::from("test");
        let sink = SinkId::from("clickhouse_logs");
        let typed = Arc::new(TypedDecodedLogs::new(records));
        let schema_version = SchemaVersion(1);
        let batch = DecodedBatch {
            source: source.clone(),
            low_sequence: 0,
            high_sequence: 0,
            source_entry_count: count as u32,
            records: DecodedRecords::Typed(typed),
            source_columns: SourceCoordinateColumns {
                manifest_path: "test/manifest".into(),
                data_path: "test/data".into(),
                sequences: vec![0; count],
                entry_indices: vec![0; count],
                record_indices: (0..count as u32).collect(),
                ingestion_time_ms: vec![0; count],
            },
            stats: BatchStats {
                source_byte_count: 0,
                decoded_byte_estimate: 0,
            },
            schema_version,
        };
        SinkCommit {
            identity: CommitIdentity {
                source,
                sink,
                range: SequenceRange::new(0, 0),
                schema_version,
            },
            batch,
        }
    }

    /// Phase 4 review MED-4a regression. `ClickHouseSink::write` must
    /// reject a sink commit where the adapter's planned row total is
    /// less than the input record count — the legacy
    /// `runtime_rejects_adapter_that_drops_rows` regression, now at
    /// the sink layer. The writer is constructed with a fake endpoint
    /// the guard never reaches (it fires before `execute_all` runs).
    #[tokio::test]
    async fn sink_rejects_adapter_that_drops_rows() {
        let adapter = Arc::new(DroppingAdapter);
        let writer = Arc::new(ClickHouseWriter::new(WriterConfig {
            endpoint: "http://localhost:1".into(), // never reached
            ..Default::default()
        }));
        let sink = ClickHouseSink::new("clickhouse_logs", adapter, writer);

        let commit = fake_sink_commit(vec![fake_log_record(0), fake_log_record(1)]);
        let err = sink
            .write(commit)
            .await
            .expect_err("guard must reject a dropping adapter");
        match err {
            SinkCommitFailure::Fatal(boxed) => {
                let msg = boxed.to_string();
                assert!(
                    msg.contains("adapter plan covered 0 rows but commit held 2"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    /// AdapterError unused warning kicker — keeps the import live
    /// for future tests that inspect adapter-error mapping.
    #[allow(dead_code)]
    fn _adapter_error_import_anchor(_: AdapterError) {}

    /// §1 helper: bucket counts records by `_odb_run_id`, falls back
    /// to `"unknown"` for records missing the stamp. Both buckets
    /// must sum to the input size so the counter stays
    /// row-conservation-correct.
    #[test]
    fn bucket_rows_by_run_id_groups_and_falls_back() {
        let mut a = fake_log_record(0);
        a.log_attributes
            .insert("_odb_run_id".into(), "run-a".into());
        let mut b = fake_log_record(1);
        b.log_attributes
            .insert("_odb_run_id".into(), "run-a".into());
        let mut c = fake_log_record(2);
        c.log_attributes
            .insert("_odb_run_id".into(), "run-b".into());
        // d has no _odb_run_id stamp.
        let d = fake_log_record(3);

        let counts = bucket_rows_by_run_id(&[a, b, c, d]);
        assert_eq!(counts.get("run-a").copied(), Some(2));
        assert_eq!(counts.get("run-b").copied(), Some(1));
        assert_eq!(counts.get("unknown").copied(), Some(1));
        let total: u64 = counts.values().sum();
        assert_eq!(total, 4, "row count must be conserved across buckets");
    }
}
