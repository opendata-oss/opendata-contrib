//! OTLP logs ClickHouse adapter.
//!
//! Maps `DecodedLogRecord`s into rows in a `ReplacingMergeTree(_adapter_version)`
//! table whose ORDER BY hybridizes a query-useful prefix with the
//! source-coordinate suffix that gives every record a unique dedupe key
//! (see RFC 0003 "ORDER BY Tradeoff").
//!
//! Chunking is deterministic: rows are emitted in `(sequence, entry_index,
//! record_index)` order, then split into chunks of `max_chunk_rows`. The
//! chunking fingerprint hashes the inputs that affect chunk boundaries so
//! a configuration change between partial-success and ack does not collide
//! tokens with rows produced under different rules.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use opendata_ingest_otel::logs::DecodedLogRecord;

use crate::adapter::{
    Adapter, AdapterError, AdapterResult, ClickHouseAdapterBatch, ClickHouseSettings, InsertChunk,
    RowValue,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogsAdapterConfig {
    pub database: String,
    pub table: String,
    pub adapter_version: u32,
    /// Maximum rows per ClickHouse insert chunk.
    pub max_chunk_rows: usize,
    /// Maximum approximate bytes per ClickHouse insert chunk.
    pub max_chunk_bytes: usize,
    pub insert_quorum: Option<String>,
    /// Whether to apply `insert_deduplication_token` on inserts.
    /// `true` for replicated tables (the production default), `false`
    /// for non-replicated targets (single-node test containers, where
    /// the setting is at best a no-op and historically can drop the
    /// insert silently).
    #[serde(default = "default_apply_dedup_token")]
    pub apply_deduplication_token: bool,
}

fn default_apply_dedup_token() -> bool {
    true
}

impl Default for LogsAdapterConfig {
    fn default() -> Self {
        Self {
            database: "responsive".to_string(),
            table: "logs".to_string(),
            adapter_version: 1,
            max_chunk_rows: 100_000,
            max_chunk_bytes: 32 * 1024 * 1024,
            insert_quorum: Some("auto".to_string()),
            apply_deduplication_token: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OtlpLogsClickHouseAdapter {
    config: LogsAdapterConfig,
    chunking_fingerprint: String,
}

impl OtlpLogsClickHouseAdapter {
    pub fn new(config: LogsAdapterConfig) -> Self {
        let fingerprint = chunking_fingerprint(&config);
        Self {
            config,
            chunking_fingerprint: fingerprint,
        }
    }

    pub fn config(&self) -> &LogsAdapterConfig {
        &self.config
    }

    pub fn chunking_fingerprint(&self) -> &str {
        &self.chunking_fingerprint
    }
}

/// Stable hash of every input that affects chunk boundaries.
fn chunking_fingerprint(config: &LogsAdapterConfig) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"otlp-logs-clickhouse-adapter:v1\n");
    hasher.update(format!("database={}\n", config.database).as_bytes());
    hasher.update(format!("table={}\n", config.table).as_bytes());
    hasher.update(format!("adapter_version={}\n", config.adapter_version).as_bytes());
    hasher.update(format!("max_chunk_rows={}\n", config.max_chunk_rows).as_bytes());
    hasher.update(format!("max_chunk_bytes={}\n", config.max_chunk_bytes).as_bytes());
    // Insert quorum doesn't change chunk boundaries, but a writer setting
    // change shouldn't collide with old tokens either, so include it.
    hasher.update(
        format!(
            "insert_quorum={}\n",
            config.insert_quorum.as_deref().unwrap_or("")
        )
        .as_bytes(),
    );
    let digest = hasher.finalize();
    hex::encode(&digest[..8])
}

// Writer-side INSERT column list. `_odb_clickhouse_inserted_at` is
// intentionally absent — it has `DEFAULT now64(9)` server-side so the
// writer omits it from the projection and CH evaluates the default
// per-row at INSERT materialization time. `_odb_gateway_received_at`
// IS in the list because the adapter writes the gateway stamp (or
// NULL) per row.
pub const COLUMNS: &[&str] = &[
    "Timestamp",
    "ObservedTimestamp",
    "SeverityText",
    "SeverityNumber",
    "ServiceName",
    "Body",
    "ResourceAttributes",
    "LogAttributes",
    "TraceId",
    "SpanId",
    "_odb_sequence",
    "_odb_entry_index",
    "_odb_record_index",
    "_odb_manifest_path",
    "_odb_data_path",
    "_odb_ingestion_time_ms",
    "_adapter_version",
    "_odb_gateway_received_at",
];

impl Adapter for OtlpLogsClickHouseAdapter {
    type Input = DecodedLogRecord;

    fn plan(&self, batch: ClickHouseAdapterBatch<Self::Input>) -> AdapterResult<Vec<InsertChunk>> {
        let ClickHouseAdapterBatch {
            identity,
            mut records,
            bytes: _,
        } = batch;
        // RFC 0002 §Runtime/Sink Boundary: the source-range segment
        // of the dedupe token comes from the runtime's CommitIdentity,
        // not from any per-record / per-DecodedBatch field. The
        // adapter never reads `batch.{low,high}_sequence` for token
        // construction.
        let low_sequence = identity.range.low;
        let high_sequence = identity.range.high;

        // Stable order: (entry_index, record_index) within a sequence,
        // then sequence ascending. Records from the same Buffer entry
        // already arrive in record_index order, but we sort defensively
        // so chunking is deterministic regardless of upstream
        // iteration order.
        records.sort_by_key(|r| {
            (
                r.source.buffer_sequence,
                r.source.entry_index,
                r.source.record_index,
            )
        });

        let mut chunks = Vec::new();

        if records.is_empty() {
            // Zero-row commit groups still need to advance the
            // high-watermark, but they produce no insert chunks. The
            // runtime calls the ack controller directly when the chunk
            // list is empty.
            return Ok(chunks);
        }

        // Manifest path is the same across all records in a single
        // ingestor's batch (the runtime serves one manifest), so we
        // pull it from the first record and fail closed if any other
        // record disagrees. This is defense in depth: if a future
        // runtime change ever started multiplexing manifests through
        // one adapter, the token would silently collide between
        // streams and ClickHouse's insert_deduplication_token would
        // drop one of them — better to halt explicitly.
        let manifest_path = records[0].source.manifest_path.clone();
        if let Some(mismatch) = records
            .iter()
            .find(|r| r.source.manifest_path != manifest_path)
        {
            return Err(AdapterError::Plan(format!(
                "commit group mixes manifest paths: first record has {first}, found {other}",
                first = manifest_path,
                other = mismatch.source.manifest_path,
            )));
        }

        // Byte-aware chunking. Iterate records, accumulating row count
        // and approx bytes; emit a chunk when either threshold trips.
        // This is deterministic given (records, max_chunk_rows,
        // max_chunk_bytes) — the chunking fingerprint hashes both.
        let max_rows = self.config.max_chunk_rows.max(1);
        let max_bytes = self.config.max_chunk_bytes.max(1);
        let mut current: Vec<&DecodedLogRecord> = Vec::new();
        let mut current_bytes: usize = 0;
        let mut chunk_index: u32 = 0;
        let emit = |slice: &[&DecodedLogRecord],
                    chunk_index: u32,
                    config: &LogsAdapterConfig,
                    manifest_path: &str,
                    fingerprint: &str|
         -> InsertChunk {
            let rows: Vec<Vec<RowValue>> = slice.iter().map(|r| log_row(r, config)).collect();
            let token = build_token(
                manifest_path,
                config,
                fingerprint,
                low_sequence,
                high_sequence,
                chunk_index,
            );
            InsertChunk {
                database: config.database.clone(),
                table: config.table.clone(),
                columns: COLUMNS.to_vec(),
                rows,
                settings: ClickHouseSettings {
                    insert_quorum: config.insert_quorum.clone(),
                    insert_deduplication_token: token.clone(),
                    apply_deduplication_token: config.apply_deduplication_token,
                },
                idempotency_token: token,
                chunk_index,
                observability_labels: vec![
                    ("table", config.table.clone()),
                    ("signal", "logs".to_string()),
                ],
            }
        };
        for record in &records {
            let rec_bytes = record.approx_size_bytes();
            // If adding this record would exceed BOTH thresholds and
            // the chunk is non-empty, flush before adding. We test
            // either threshold separately so a row-only or bytes-only
            // limit each correctly chunk.
            let would_exceed_rows = current.len() + 1 > max_rows;
            let would_exceed_bytes = current_bytes + rec_bytes > max_bytes;
            if !current.is_empty() && (would_exceed_rows || would_exceed_bytes) {
                chunks.push(emit(
                    &current,
                    chunk_index,
                    &self.config,
                    &manifest_path,
                    &self.chunking_fingerprint,
                ));
                chunk_index += 1;
                current.clear();
                current_bytes = 0;
            }
            current.push(record);
            current_bytes += rec_bytes;
        }
        if !current.is_empty() {
            chunks.push(emit(
                &current,
                chunk_index,
                &self.config,
                &manifest_path,
                &self.chunking_fingerprint,
            ));
        }
        Ok(chunks)
    }
}

fn build_token(
    manifest_path: &str,
    config: &LogsAdapterConfig,
    fingerprint: &str,
    low: u64,
    high: u64,
    chunk_index: u32,
) -> String {
    // RFC 0003 token format:
    //   {manifest_path}:{database}.{table}:{low}-{high}:{adapter_version}:{fingerprint}:{chunk_index}
    // The manifest segment makes two ingestors writing the same
    // (database, table) from different manifests safe. database.table
    // is included so two adapters writing different tables from one
    // manifest also stay distinct.
    format!(
        "{}:{}.{}:{}-{}:{}:{}:{}",
        manifest_path,
        config.database,
        config.table,
        low,
        high,
        config.adapter_version,
        fingerprint,
        chunk_index,
    )
}

fn log_row(rec: &DecodedLogRecord, config: &LogsAdapterConfig) -> Vec<RowValue> {
    // Extract `_odb_gateway_received_at` from `resource_attributes` and
    // drop it from the map so the typed CH column carries the value and
    // the `ResourceAttributes Map(...)` column doesn't double-store it.
    // The OTel decoder string-coerces every AnyValue (logs.rs's
    // `string_value()` does `i.to_string()` on IntValue), so the
    // attribute arrives as a stringified u64 of unix-epoch nanoseconds.
    // A bad parse → `None` (dropped from the map regardless) keeps the
    // row insertable even if a future producer accidentally writes a
    // non-integer value.
    let mut resource = btree_to_owned(&rec.resource_attributes);
    let gateway_received_at: Option<u64> = resource
        .remove("_odb_gateway_received_at")
        .and_then(|s| s.parse::<u64>().ok());
    let log_attrs = btree_to_owned(&rec.log_attributes);
    vec![
        RowValue::DateTime64Nanos(rec.timestamp_unix_nano),
        RowValue::DateTime64Nanos(rec.observed_timestamp_unix_nano),
        RowValue::LowCardinalityString(rec.severity_text.clone()),
        RowValue::UInt8(severity_to_u8(rec.severity_number)),
        RowValue::LowCardinalityString(rec.service_name.clone().unwrap_or_default()),
        RowValue::String(rec.body.clone()),
        RowValue::StringMap(resource),
        RowValue::StringMap(log_attrs),
        RowValue::String(rec.trace_id_hex.clone()),
        RowValue::String(rec.span_id_hex.clone()),
        RowValue::UInt64(rec.source.buffer_sequence),
        RowValue::UInt32(rec.source.entry_index),
        RowValue::UInt32(rec.source.record_index),
        RowValue::LowCardinalityString(rec.source.manifest_path.clone()),
        RowValue::String(rec.source.data_path.clone()),
        RowValue::Int64(rec.source.ingestion_time_ms),
        RowValue::UInt32(config.adapter_version),
        RowValue::NullableDateTime64Nanos(gateway_received_at),
    ]
}

fn btree_to_owned(input: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    input.clone()
}

fn severity_to_u8(severity_number: i32) -> u8 {
    severity_number.clamp(0, u8::MAX as i32) as u8
}

/// Logs-alpha table DDL. Uses `ReplacingMergeTree(_adapter_version)` so
/// adapter version is the merge tiebreaker (a replay after a row-mapping
/// change resolves to the new mapping). The hybrid `ORDER BY` puts a
/// query-useful prefix in front of the dedupe-identifying suffix.
///
/// IMPORTANT: any change to a column that participates in `ORDER BY`
/// requires a new table plus backfill (RFC 0003 dedupe constraint).
pub fn logs_table_ddl(config: &LogsAdapterConfig) -> String {
    // Two Stage 2 e2e-latency columns appear after `_adapter_version`:
    //   * `_odb_gateway_received_at` is Nullable so the in-place
    //     `ALTER TABLE … ADD COLUMN` against tables that predate
    //     Stage 2 leaves their existing rows with NULL gateway
    //     timestamps; the harness filters those out of quantile math.
    //   * `_odb_clickhouse_inserted_at` uses `DEFAULT now64(9)` so CH
    //     evaluates it server-side per row at INSERT materialization.
    //     The adapter omits this column from its INSERT projection so
    //     the default fires.
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.{table} (\n\
         Timestamp           DateTime64(9)               CODEC(Delta, ZSTD),\n\
         ObservedTimestamp   DateTime64(9)               CODEC(Delta, ZSTD),\n\
         SeverityText        LowCardinality(String),\n\
         SeverityNumber      UInt8,\n\
         ServiceName         LowCardinality(String),\n\
         Body                String                      CODEC(ZSTD),\n\
         ResourceAttributes  Map(LowCardinality(String), String),\n\
         LogAttributes       Map(LowCardinality(String), String),\n\
         TraceId             String                      CODEC(ZSTD),\n\
         SpanId              String                      CODEC(ZSTD),\n\
         _odb_sequence            UInt64,\n\
         _odb_entry_index         UInt32,\n\
         _odb_record_index        UInt32,\n\
         _odb_manifest_path       LowCardinality(String),\n\
         _odb_data_path           String,\n\
         _odb_ingestion_time_ms   Int64,\n\
         _adapter_version         UInt32,\n\
         _odb_gateway_received_at    Nullable(DateTime64(9)),\n\
         _odb_clickhouse_inserted_at DateTime64(9) DEFAULT now64(9)\n\
         )\n\
         ENGINE = ReplacingMergeTree(_adapter_version)\n\
         PARTITION BY toDate(Timestamp)\n\
         ORDER BY (toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)\n\
         TTL toDate(Timestamp) + INTERVAL 30 DAY",
        db = config.database,
        table = config.table,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendata_ingest_otel::logs::RowSourceCoordinates;
    use opendata_ingest_runtime::identity::{CommitIdentity, SchemaVersion, SequenceRange};
    use opendata_ingest_runtime::sink::SinkId;
    use opendata_ingest_runtime::source::SourceId;
    use std::collections::BTreeMap;

    fn cfg() -> LogsAdapterConfig {
        LogsAdapterConfig {
            max_chunk_rows: 2,
            ..LogsAdapterConfig::default()
        }
    }

    fn ident(low: u64, high: u64) -> CommitIdentity {
        CommitIdentity {
            source: SourceId::from("test"),
            sink: SinkId::from("clickhouse_logs"),
            range: SequenceRange::new(low, high),
            schema_version: SchemaVersion(1),
        }
    }

    fn rec(sequence: u64, entry_index: u32, record_index: u32) -> DecodedLogRecord {
        DecodedLogRecord {
            source: RowSourceCoordinates {
                buffer_sequence: sequence,
                entry_index,
                record_index,
                manifest_path: "ingest/test/manifest".into(),
                data_path: "ingest/test/data/abc.batch".into(),
                ingestion_time_ms: 12345,
            },
            timestamp_unix_nano: 1_700_000_000_000_000_000 + sequence,
            observed_timestamp_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: format!("body-{sequence}-{entry_index}-{record_index}"),
            service_name: Some("controller".into()),
            resource_attributes: BTreeMap::from([("k".into(), "v".into())]),
            scope_name: None,
            log_attributes: BTreeMap::from([("topic".into(), "foo".into())]),
            trace_id_hex: "deadbeef".into(),
            span_id_hex: "feedface".into(),
        }
    }

    #[test]
    fn empty_batch_yields_no_chunks() {
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());
        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records: Vec::new(),
                identity: ident(5, 9),
                bytes: 0,
            })
            .expect("plan");
        assert!(chunks.is_empty());
    }

    #[test]
    fn rows_chunk_at_max_chunk_rows() {
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());
        let records = vec![rec(1, 0, 0), rec(1, 0, 1), rec(2, 0, 0)];
        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records,
                identity: ident(1, 2),
                bytes: 0,
            })
            .expect("plan");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].rows_count(), 2);
        assert_eq!(chunks[1].rows_count(), 1);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[1].chunk_index, 1);
    }

    #[test]
    fn token_format_includes_low_high_version_fingerprint_chunk() {
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());
        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![rec(7, 0, 0), rec(7, 0, 1), rec(8, 0, 0)],
                identity: ident(7, 8),
                bytes: 0,
            })
            .expect("plan");
        let token = &chunks[0].idempotency_token;
        // {manifest}:{db}.{table}:low-high:version:fingerprint:chunk_index
        // The test fixture's manifest_path is "ingest/test/manifest"
        // (set in the rec() helper).
        assert!(token.starts_with("ingest/test/manifest:responsive.logs:7-8:1:"));
        assert!(token.ends_with(":0"));
        assert!(chunks[1].idempotency_token.ends_with(":1"));
        assert_ne!(chunks[0].idempotency_token, chunks[1].idempotency_token);
    }

    #[test]
    fn plan_rejects_records_from_mixed_manifests() {
        // Defense in depth: a future runtime change could route
        // records from two manifests through one adapter; the token
        // would silently collide and ClickHouse's
        // insert_deduplication_token would drop one stream's data.
        // Fail closed before any of that.
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());
        let mut a = rec(1, 0, 0);
        a.source.manifest_path = "ingest/otel/logs-a/manifest".into();
        let mut b = rec(1, 0, 1);
        b.source.manifest_path = "ingest/otel/logs-b/manifest".into();
        let err = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![a, b],
                identity: ident(1, 1),
                bytes: 0,
            })
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("commit group mixes manifest paths"),
            "expected manifest mismatch error, got: {msg}"
        );
    }

    #[test]
    fn token_distinguishes_two_manifests_for_same_table() {
        // Two ingestors writing the same (database, table) from
        // different manifests must produce distinct tokens. Without
        // the manifest segment, ClickHouse's
        // insert_deduplication_token would collapse them and silently
        // drop one stream's data.
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());
        let mut a = rec(1, 0, 0);
        a.source.manifest_path = "ingest/otel/logs-a/manifest".into();
        let mut b = rec(1, 0, 0);
        b.source.manifest_path = "ingest/otel/logs-b/manifest".into();
        let plan_a = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![a],
                identity: ident(1, 1),
                bytes: 0,
            })
            .expect("plan a");
        let plan_b = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![b],
                identity: ident(1, 1),
                bytes: 0,
            })
            .expect("plan b");
        assert_ne!(
            plan_a[0].idempotency_token, plan_b[0].idempotency_token,
            "tokens must differ when manifest_path differs"
        );
    }

    #[test]
    fn byte_threshold_chunks_records() {
        // Lower max_chunk_bytes so each row exceeds it; the adapter
        // must split on the byte threshold even when row threshold
        // would not trip.
        let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 1_000_000,
            max_chunk_bytes: 1, // every record's bytes will exceed this
            ..LogsAdapterConfig::default()
        });
        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![rec(1, 0, 0), rec(1, 0, 1), rec(1, 0, 2)],
                identity: ident(1, 1),
                bytes: 0,
            })
            .expect("plan");
        assert_eq!(chunks.len(), 3, "every record should land in its own chunk");
    }

    #[test]
    fn changing_max_chunk_bytes_shifts_chunk_boundaries() {
        // Two configs with identical max_chunk_rows but different
        // max_chunk_bytes must produce different chunk counts and
        // (because the fingerprint includes max_chunk_bytes) different
        // tokens. This pins down the property that an operator change
        // to byte sizing cannot collide tokens with the prior
        // chunking.
        let records = || vec![rec(1, 0, 0), rec(1, 0, 1), rec(1, 0, 2)];
        let adapter_loose = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 1_000_000,
            max_chunk_bytes: 1_000_000,
            ..LogsAdapterConfig::default()
        });
        let adapter_strict = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 1_000_000,
            max_chunk_bytes: 1,
            ..LogsAdapterConfig::default()
        });
        let loose = adapter_loose
            .plan(ClickHouseAdapterBatch {
                records: records(),
                identity: ident(1, 1),
                bytes: 0,
            })
            .expect("plan loose");
        let strict = adapter_strict
            .plan(ClickHouseAdapterBatch {
                records: records(),
                identity: ident(1, 1),
                bytes: 0,
            })
            .expect("plan strict");
        assert_eq!(loose.len(), 1);
        assert_eq!(strict.len(), 3);
        assert_ne!(loose[0].idempotency_token, strict[0].idempotency_token);
    }

    #[test]
    fn replay_with_same_inputs_produces_same_tokens() {
        // Determinism is the property the dedupe model leans on: identical
        // (records, config) => identical chunk boundaries => identical
        // tokens. Two independent invocations must agree.
        let records = || vec![rec(7, 0, 0), rec(7, 0, 1), rec(8, 0, 0)];
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());

        let plan_a = adapter
            .plan(ClickHouseAdapterBatch {
                records: records(),
                identity: ident(7, 8),
                bytes: 0,
            })
            .expect("plan a");
        let plan_b = adapter
            .plan(ClickHouseAdapterBatch {
                records: records(),
                identity: ident(7, 8),
                bytes: 0,
            })
            .expect("plan b");

        let tokens_a: Vec<&str> = plan_a
            .iter()
            .map(|c| c.idempotency_token.as_str())
            .collect();
        let tokens_b: Vec<&str> = plan_b
            .iter()
            .map(|c| c.idempotency_token.as_str())
            .collect();
        assert_eq!(tokens_a, tokens_b);
    }

    #[test]
    fn changing_max_chunk_rows_changes_fingerprint_and_tokens() {
        // Defending the property that any change to a chunking-affecting
        // input causes the fingerprint to differ — without this, two
        // configurations that produced different chunk shapes could share
        // tokens and ClickHouse would silently drop valid data.
        let mut a = cfg();
        let mut b = cfg();
        a.max_chunk_rows = 2;
        b.max_chunk_rows = 3;
        let fa = chunking_fingerprint(&a);
        let fb = chunking_fingerprint(&b);
        assert_ne!(fa, fb);
    }

    #[test]
    fn rows_carry_source_columns_in_correct_positions() {
        let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 100,
            ..LogsAdapterConfig::default()
        });
        let r = rec(11, 3, 7);
        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![r.clone()],
                identity: ident(11, 11),
                bytes: 0,
            })
            .expect("plan");
        let row = &chunks[0].rows[0];
        // Spot-check positional columns. The constants here track
        // adapter::COLUMNS — if columns reorder, this test forces an
        // explicit acknowledgement.
        match &row[10] {
            RowValue::UInt64(v) => assert_eq!(*v, 11),
            other => panic!("_odb_sequence column wrong type: {other:?}"),
        }
        match &row[11] {
            RowValue::UInt32(v) => assert_eq!(*v, 3),
            other => panic!("_odb_entry_index column wrong type: {other:?}"),
        }
        match &row[12] {
            RowValue::UInt32(v) => assert_eq!(*v, 7),
            other => panic!("_odb_record_index column wrong type: {other:?}"),
        }
    }

    #[test]
    fn ddl_uses_replacing_merge_tree_with_adapter_version() {
        let ddl = logs_table_ddl(&LogsAdapterConfig::default());
        assert!(ddl.contains("ReplacingMergeTree(_adapter_version)"));
        assert!(
            ddl.contains("_odb_sequence, _odb_entry_index, _odb_record_index"),
            "ORDER BY must include the source-coordinate suffix"
        );
        assert!(ddl.contains("PARTITION BY toDate(Timestamp)"));
    }

    #[test]
    fn ddl_carries_stage2_e2e_latency_columns() {
        let ddl = logs_table_ddl(&LogsAdapterConfig::default());
        assert!(
            ddl.contains("_odb_gateway_received_at    Nullable(DateTime64(9))"),
            "DDL must declare the gateway column as nullable so the in-place ALTER \
             leaves pre-Stage-2 rows with NULL gateway timestamps: {ddl}"
        );
        assert!(
            ddl.contains("_odb_clickhouse_inserted_at DateTime64(9) DEFAULT now64(9)"),
            "DDL must declare the insert column with a server-side now64(9) default \
             so the adapter doesn't have to populate it explicitly: {ddl}"
        );
    }

    #[test]
    fn columns_const_includes_gateway_but_omits_inserted_at() {
        // _odb_clickhouse_inserted_at MUST NOT appear in the writer's
        // INSERT column projection: that's what makes CH evaluate
        // `now64(9)` server-side. _odb_gateway_received_at MUST appear
        // because the adapter writes the stamped value (or NULL).
        assert!(
            COLUMNS.contains(&"_odb_gateway_received_at"),
            "COLUMNS missing gateway column"
        );
        assert!(
            !COLUMNS.contains(&"_odb_clickhouse_inserted_at"),
            "COLUMNS must NOT include _odb_clickhouse_inserted_at; CH evaluates now64(9) server-side"
        );
        // Row vector length follows COLUMNS length one-for-one.
        assert_eq!(
            COLUMNS.len(),
            18,
            "after Stage 2, the writer emits 18 columns per row"
        );
    }

    #[test]
    fn gateway_attribute_extracted_into_typed_column_and_removed_from_resource_map() {
        let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 100,
            ..LogsAdapterConfig::default()
        });
        let mut r = rec(11, 0, 0);
        // Mirror exactly what the OTel decoder would land in the map:
        // a stringified i64 of unix-epoch nanoseconds.
        let gateway_ts_ns: u64 = 1_700_000_000_123_456_789;
        r.resource_attributes
            .insert("_odb_gateway_received_at".into(), gateway_ts_ns.to_string());
        // A second, unrelated resource attribute that MUST survive.
        r.resource_attributes
            .insert("service.namespace".into(), "responsive".into());

        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![r],
                identity: ident(11, 11),
                bytes: 0,
            })
            .expect("plan");
        let row = &chunks[0].rows[0];

        // Typed column at position 17 carries the parsed timestamp.
        match &row[17] {
            RowValue::NullableDateTime64Nanos(Some(v)) => assert_eq!(*v, gateway_ts_ns),
            other => panic!("position 17 wrong shape: {other:?}"),
        }
        // ResourceAttributes (position 6) keeps unrelated keys but
        // does NOT carry _odb_gateway_received_at any more.
        match &row[6] {
            RowValue::StringMap(m) => {
                assert!(
                    !m.contains_key("_odb_gateway_received_at"),
                    "adapter must drop _odb_gateway_received_at from ResourceAttributes \
                     after extracting it; map still had it: {m:?}"
                );
                assert_eq!(
                    m.get("service.namespace").map(String::as_str),
                    Some("responsive"),
                    "unrelated resource attribute clobbered: {m:?}"
                );
            }
            other => panic!("position 6 wrong shape: {other:?}"),
        }
    }

    #[test]
    fn missing_gateway_attribute_lands_as_null() {
        let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 100,
            ..LogsAdapterConfig::default()
        });
        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![rec(1, 0, 0)], // rec() doesn't stamp the attribute
                identity: ident(1, 1),
                bytes: 0,
            })
            .expect("plan");
        match &chunks[0].rows[0][17] {
            RowValue::NullableDateTime64Nanos(None) => (),
            other => panic!("expected NullableDateTime64Nanos(None), got {other:?}"),
        }
    }

    #[test]
    fn non_numeric_gateway_attribute_is_dropped_and_lands_as_null() {
        // Defensive: if somehow a non-integer string ends up under the
        // key, the row must still insert cleanly with NULL gateway
        // rather than fail the whole batch.
        let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 100,
            ..LogsAdapterConfig::default()
        });
        let mut r = rec(1, 0, 0);
        r.resource_attributes
            .insert("_odb_gateway_received_at".into(), "not-a-number".into());
        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records: vec![r],
                identity: ident(1, 1),
                bytes: 0,
            })
            .expect("plan");
        match &chunks[0].rows[0][17] {
            RowValue::NullableDateTime64Nanos(None) => (),
            other => panic!("expected NullableDateTime64Nanos(None) for bad parse, got {other:?}"),
        }
        // Even on a bad parse, the key must be removed from the map.
        match &chunks[0].rows[0][6] {
            RowValue::StringMap(m) => assert!(
                !m.contains_key("_odb_gateway_received_at"),
                "bad-parse path still removes the key from ResourceAttributes",
            ),
            other => panic!("position 6 wrong shape: {other:?}"),
        }
    }

    #[test]
    fn records_sorted_for_deterministic_chunking() {
        let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 100,
            ..LogsAdapterConfig::default()
        });
        // Feed records out of order; the adapter must still chunk in
        // (sequence, entry, record) order so a replay produces the same
        // shape.
        let records = vec![rec(2, 0, 0), rec(1, 0, 1), rec(1, 0, 0)];
        let chunks = adapter
            .plan(ClickHouseAdapterBatch {
                records,
                identity: ident(1, 2),
                bytes: 0,
            })
            .expect("plan");
        let row_seqs: Vec<u64> = chunks[0]
            .rows
            .iter()
            .map(|row| match &row[10] {
                RowValue::UInt64(v) => *v,
                _ => panic!("expected UInt64"),
            })
            .collect();
        assert_eq!(row_seqs, vec![1, 1, 2]);
    }
}
