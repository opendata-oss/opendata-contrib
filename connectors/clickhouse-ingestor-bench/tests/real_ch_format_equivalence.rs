//! Value-level format-equivalence tests.
//!
//! Plans an `InsertChunk` set from a hand-built batch of decoded
//! log records that exercises every `RowValue` variant used by the
//! OTLP-logs schema (`String`, `LowCardinalityString`, `UInt8`,
//! `UInt32`, `UInt64`, `Int64`, `DateTime64Nanos`, `StringMap`),
//! plus edge values (empty maps, JSON-escapeable bytes, near-bound
//! integers, epoch / large timestamps). Then writes the same
//! chunks through two `ClickHouseWriter`s — one configured for
//! `JsonEachRow`, one for `RowBinaryWithNamesAndTypes` — into two
//! sibling tables in the same DB.
//!
//! Comparing `hash_rows_in` and `hash_columns_in` across the two
//! tables proves: the table contents are byte-identical
//! independently of which wire format produced them. Any encoding
//! drift in a single column localizes via the per-column hash.
//!
//! Docker-gated via the `real-ch` feature.

#![cfg(feature = "real-ch")]

use std::collections::BTreeMap;
use std::time::Duration;

use clickhouse_ingestor::writer::{ClickHouseWriter, WriterConfig};
use clickhouse_ingestor_bench::real_ch::RealClickHouseFixture;
use opendata_ingest_clickhouse::adapter::logs::{
    LogsAdapterConfig, OtlpLogsClickHouseAdapter, logs_table_ddl,
};
use opendata_ingest_clickhouse::adapter::{Adapter, ClickHouseAdapterBatch, InsertChunk};
use opendata_ingest_clickhouse::serializer::SerializationFormat;
use opendata_ingest_otel::logs::{DecodedLogRecord, RowSourceCoordinates};
use opendata_ingest_runtime::identity::{CommitIdentity, SchemaVersion, SequenceRange};
use opendata_ingest_runtime::sink::SinkId;
use opendata_ingest_runtime::source::SourceId;

/// Every column the OTLP-logs DDL declares. Order matters only for
/// the per-column hash report — the test compares hashes by name.
const COLUMNS: &[&str] = &[
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
];

const DATABASE: &str = "phase07_format_equivalence";
const TABLE_JSON: &str = "logs_json";
const TABLE_ROWBINARY: &str = "logs_rowbinary";

/// Build the test fixture and create both the JSON and RowBinary
/// tables in its database.
async fn setup_both_tables() -> RealClickHouseFixture {
    let adapter_cfg = LogsAdapterConfig {
        database: DATABASE.into(),
        table: TABLE_JSON.into(), // fixture default; test creates the second below
        ..Default::default()
    };
    let fixture = RealClickHouseFixture::setup_testcontainers(
        adapter_cfg.database.clone(),
        adapter_cfg.table.clone(),
        adapter_cfg,
    )
    .await
    .expect("setup_testcontainers");

    // Create the RowBinary table as a sibling in the same DB.
    let mut second_cfg = fixture.adapter_config.clone();
    second_cfg.table = TABLE_ROWBINARY.into();
    fixture
        .writer
        .execute_statement(&logs_table_ddl(&second_cfg))
        .await
        .expect("create row-binary table");
    fixture
}

/// Hand-build a batch that exercises every `RowValue` variant the
/// logs adapter emits, plus encoding edge cases. The set is
/// deliberately not derived from `LogWorkload` — that helper grabs
/// `now_ns()` at call time, which would diverge between the two
/// writes and ruin the equivalence check.
fn deterministic_records() -> Vec<DecodedLogRecord> {
    let manifest = "phase07/format-equivalence/manifest".to_string();
    let data = "phase07/format-equivalence/data/abc.batch".to_string();

    let make = |seq: u64,
                entry: u32,
                rec: u32,
                ts: u64,
                obs: u64,
                sev_n: i32,
                sev_t: &str,
                body: &str,
                service: Option<&str>,
                resource: BTreeMap<String, String>,
                log_attrs: BTreeMap<String, String>,
                trace: &str,
                span: &str,
                ingest_ms: i64|
     -> DecodedLogRecord {
        DecodedLogRecord {
            source: RowSourceCoordinates {
                buffer_sequence: seq,
                entry_index: entry,
                record_index: rec,
                manifest_path: manifest.clone(),
                data_path: data.clone(),
                ingestion_time_ms: ingest_ms,
            },
            timestamp_unix_nano: ts,
            observed_timestamp_unix_nano: obs,
            severity_number: sev_n,
            severity_text: sev_t.into(),
            body: body.into(),
            service_name: service.map(str::to_string),
            resource_attributes: resource,
            scope_name: None,
            log_attributes: log_attrs,
            trace_id_hex: trace.into(),
            span_id_hex: span.into(),
        }
    };

    vec![
        // Plain ASCII, single-entry attribute maps.
        make(
            0,
            0,
            0,
            1_700_000_000_000_000_000,
            0,
            9,
            "INFO",
            "first body",
            Some("svc-a"),
            BTreeMap::from([("k1".into(), "v1".into())]),
            BTreeMap::from([("topic".into(), "alpha".into())]),
            "abcd1234",
            "deadbeef",
            12345,
        ),
        // Empty body, empty service_name, larger attribute maps.
        make(
            0,
            0,
            1,
            1_700_000_000_000_000_001,
            1,
            13,
            "WARN",
            "",
            Some(""),
            BTreeMap::from([
                ("ka".into(), "va".into()),
                ("kb".into(), "vb".into()),
                ("kc".into(), "vc".into()),
            ]),
            BTreeMap::from([
                ("topic".into(), "beta".into()),
                ("region".into(), "us-west-2".into()),
            ]),
            "",
            "",
            -1,
        ),
        // Empty maps, no service_name, edge timestamps + large IDs.
        make(
            1,
            0,
            0,
            0,
            u64::MAX / 2,
            0,
            "TRACE",
            "epoch row",
            None,
            BTreeMap::new(),
            BTreeMap::new(),
            "f".repeat(32).as_str(),
            "f".repeat(16).as_str(),
            i64::MIN,
        ),
        // JSON-escapeable bytes in body and attribute values.
        make(
            2,
            1,
            0,
            1_700_000_000_000_500_000,
            1_700_000_000_000_500_000,
            21,
            "ERROR",
            "line\nwith \"quotes\" and \\backslash\t and \u{1F600}",
            Some("svc-b"),
            BTreeMap::from([
                ("k\"weird".into(), "v\nnewline".into()),
                ("unicode".into(), "🎯 emoji".into()),
            ]),
            BTreeMap::from([("topic".into(), "json\\sensitive".into())]),
            "1a2b3c",
            "feedface",
            i64::MAX,
        ),
        // u32::MAX edges, large u64 sequence (still fits the source
        // coordinate columns).
        make(
            u64::MAX / 4,
            u32::MAX - 1,
            u32::MAX - 1,
            1_900_000_000_000_000_000,
            0,
            u8::MAX as i32,
            "FATAL",
            "high-seq",
            Some("svc-c"),
            BTreeMap::from([("k".into(), "v".into())]),
            BTreeMap::from([("topic".into(), "gamma".into())]),
            "00",
            "00",
            999_999_999_999,
        ),
    ]
}

/// Build a chunk-set from the deterministic batch. Caller mutates
/// `database` / `table` on each chunk to direct it at the target
/// format's table.
fn plan_chunks(
    records: Vec<DecodedLogRecord>,
    adapter_cfg: &LogsAdapterConfig,
) -> Vec<InsertChunk> {
    let adapter = OtlpLogsClickHouseAdapter::new(adapter_cfg.clone());
    let identity = CommitIdentity {
        source: SourceId::from("phase07-format-equivalence"),
        sink: SinkId::from("phase07-format-equivalence"),
        range: SequenceRange::new(
            0,
            records
                .iter()
                .map(|r| r.source.buffer_sequence)
                .max()
                .unwrap_or(0),
        ),
        schema_version: SchemaVersion(1),
    };
    let bytes = records
        .iter()
        .map(DecodedLogRecord::approx_size_bytes)
        .sum::<usize>();
    let batch = ClickHouseAdapterBatch {
        identity,
        records,
        bytes,
    };
    adapter.plan(batch).expect("adapter plan")
}

fn build_writer(endpoint: &str, format: SerializationFormat) -> ClickHouseWriter {
    ClickHouseWriter::new(WriterConfig {
        endpoint: endpoint.into(),
        user: "default".into(),
        password: String::new(),
        request_timeout: Duration::from_secs(30),
        max_attempts: 4,
        initial_backoff: Duration::from_millis(50),
        serialization_format: format,
        ..Default::default()
    })
}

/// Drive the same hand-built batch through both formats into two
/// sibling tables, then return (json_rows_hash, rowbinary_rows_hash).
async fn drive_both_formats(fixture: &RealClickHouseFixture) {
    let records = deterministic_records();

    // Two chunk-sets, each parameterized for its target table. The
    // adapter's idempotency token depends on the database+table
    // pair, so re-planning per target keeps the token stable for
    // any retry within a single format.
    let mut adapter_cfg_json = fixture.adapter_config.clone();
    adapter_cfg_json.database = DATABASE.into();
    adapter_cfg_json.table = TABLE_JSON.into();
    let chunks_json = plan_chunks(records.clone(), &adapter_cfg_json);

    let mut adapter_cfg_rb = fixture.adapter_config.clone();
    adapter_cfg_rb.database = DATABASE.into();
    adapter_cfg_rb.table = TABLE_ROWBINARY.into();
    let chunks_rb = plan_chunks(records, &adapter_cfg_rb);

    let json_writer = build_writer(&fixture.endpoint, SerializationFormat::JsonEachRow);
    let rb_writer = build_writer(&fixture.endpoint, SerializationFormat::RowBinary);

    json_writer
        .execute_all(&chunks_json)
        .await
        .expect("json execute_all");
    rb_writer
        .execute_all(&chunks_rb)
        .await
        .expect("rowbinary execute_all");
}

#[tokio::test]
async fn format_swap_produces_identical_row_hashes() {
    let fixture = setup_both_tables().await;
    drive_both_formats(&fixture).await;

    let h_json = fixture
        .hash_rows_in(TABLE_JSON)
        .await
        .expect("hash_rows_in json");
    let h_rb = fixture
        .hash_rows_in(TABLE_ROWBINARY)
        .await
        .expect("hash_rows_in row-binary");
    assert_eq!(
        h_json, h_rb,
        "row-fingerprint mismatch between JsonEachRow ({h_json}) and RowBinary ({h_rb})",
    );
}

#[tokio::test]
async fn format_swap_produces_identical_column_hashes() {
    let fixture = setup_both_tables().await;
    drive_both_formats(&fixture).await;

    let cols_json = fixture
        .hash_columns_in(TABLE_JSON, COLUMNS)
        .await
        .expect("hash_columns_in json");
    let cols_rb = fixture
        .hash_columns_in(TABLE_ROWBINARY, COLUMNS)
        .await
        .expect("hash_columns_in row-binary");
    let mut mismatches: Vec<String> = Vec::new();
    for column in COLUMNS {
        let key = (*column).to_string();
        let j = cols_json.get(&key).copied().unwrap_or_default();
        let r = cols_rb.get(&key).copied().unwrap_or_default();
        if j != r {
            mismatches.push(format!("{column}: json={j} row_binary={r}"));
        }
    }
    assert!(
        mismatches.is_empty(),
        "column-fingerprint mismatches: {mismatches:?}",
    );
}

#[tokio::test]
async fn format_swap_round_trips_every_row_value_variant() {
    let fixture = setup_both_tables().await;
    drive_both_formats(&fixture).await;

    // Each column in this set exercises exactly one RowValue variant
    // (or one variant + one storage-side type). If any of these
    // hashes disagrees, the matching variant's encoder produced
    // different bytes in storage across formats.
    let variant_columns: &[(&str, &str)] = &[
        ("Timestamp", "DateTime64Nanos"),
        ("SeverityText", "LowCardinalityString"),
        ("SeverityNumber", "UInt8"),
        ("Body", "String"),
        ("ResourceAttributes", "StringMap"),
        ("LogAttributes", "StringMap"),
        ("TraceId", "String"),
        ("_odb_sequence", "UInt64"),
        ("_odb_entry_index", "UInt32"),
        ("_odb_ingestion_time_ms", "Int64"),
        ("_adapter_version", "UInt32"),
    ];
    let cols: Vec<&str> = variant_columns.iter().map(|(c, _)| *c).collect();
    let cols_json = fixture
        .hash_columns_in(TABLE_JSON, &cols)
        .await
        .expect("hash_columns_in json");
    let cols_rb = fixture
        .hash_columns_in(TABLE_ROWBINARY, &cols)
        .await
        .expect("hash_columns_in row-binary");

    let mut mismatches: Vec<String> = Vec::new();
    for (col, variant) in variant_columns {
        let j = cols_json.get(*col).copied().unwrap_or_default();
        let r = cols_rb.get(*col).copied().unwrap_or_default();
        if j != r {
            mismatches.push(format!("{col} ({variant}): json={j} row_binary={r}",));
        }
    }
    assert!(
        mismatches.is_empty(),
        "variant-encoding mismatches: {mismatches:?}",
    );
}
