//! End-to-end latency column verification.
//!
//! Exercises the production decode → adapter → writer → CH → SELECT
//! path against a testcontainers ClickHouse to prove the two
//! e2e-latency columns (`_odb_gateway_received_at` and
//! `_odb_clickhouse_inserted_at`) travel end-to-end:
//!
//!   1. Build a `DecodedLogRecord` carrying `_odb_gateway_received_at`
//!      in `resource_attributes` exactly as the OTel decoder would
//!      land it (stringified u64 nanoseconds; what `string_value()`
//!      produces from a Go-side `PutInt`).
//!   2. Plan it through `OtlpLogsClickHouseAdapter`. Assert the typed
//!      column at the row's last position carries the parsed value
//!      and that `ResourceAttributes` no longer contains the key.
//!   3. Write the chunk through the production `ClickHouseWriter`.
//!   4. SELECT the row back: assert the gateway timestamp round-
//!      trips through RowBinary unchanged and the (insert - gateway)
//!      delta is non-negative.
//!   5. Repeat with a record that has no gateway attribute; assert
//!      the column lands NULL — the additive-NULL contract used by
//!      the in-place `ALTER TABLE` migration.
//!
//! Build: `cargo test --features real-ch --test real_ch_e2e_latency`.
//! Skipped silently in `cargo test` without `--features real-ch`,
//! matching `real_ch_smoke.rs` so CI without Docker doesn't fail.

#![cfg(feature = "real-ch")]

use std::collections::BTreeMap;

use clickhouse_ingestor_bench::real_ch::RealClickHouseFixture;
use opendata_ingest_clickhouse::adapter::logs::{LogsAdapterConfig, OtlpLogsClickHouseAdapter};
use opendata_ingest_clickhouse::adapter::{Adapter, ClickHouseAdapterBatch, RowValue};
use opendata_ingest_otel::logs::{DecodedLogRecord, RowSourceCoordinates};
use opendata_ingest_runtime::identity::{CommitIdentity, SchemaVersion, SequenceRange};
use opendata_ingest_runtime::sink::SinkId;
use opendata_ingest_runtime::source::SourceId;

#[tokio::test]
async fn stage2_e2e_latency_columns_populate_and_delta_is_positive() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let adapter_cfg = LogsAdapterConfig {
        database: "stage2_lat_test".into(),
        table: "logs_lat".into(),
        adapter_version: 1,
        max_chunk_rows: 100,
        max_chunk_bytes: 4 * 1024 * 1024,
        insert_quorum: None,
        apply_deduplication_token: false,
    };
    let fx = RealClickHouseFixture::setup_testcontainers(
        adapter_cfg.database.clone(),
        adapter_cfg.table.clone(),
        adapter_cfg.clone(),
    )
    .await
    .expect("setup_testcontainers");

    // Adapter sharing the fixture's exact database+table so the DDL
    // and the row writes match. The fixture creates the table with
    // the post-S2 logs_table_ddl, so the two new columns already
    // exist by the time we INSERT.
    let adapter = OtlpLogsClickHouseAdapter::new(fx.adapter_config.clone());

    // ----- Phase A: record with the gateway stamp. -----

    // Anchor the synthetic record's `Timestamp` to "now" because the
    // logs DDL carries `TTL toDate(Timestamp) + INTERVAL 30 DAY` —
    // historical anchors land in already-expired parts and CH silently
    // drops them at merge time, leaving `count()=0` despite an HTTP-OK
    // INSERT. The gateway stamp itself is independent of the row's
    // Timestamp column; we pick a value 10 s before "now" so the
    // (insert - gateway) delta is small and obviously non-negative.
    let now_ns: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("UNIX_EPOCH preceded now")
        .as_nanos() as u64;
    let gateway_ts_ns: u64 = now_ns - 10_000_000_000; // 10 s ago
    let stamped = stamped_record(1, gateway_ts_ns, "stamped-record", now_ns);
    let chunks = adapter
        .plan(ClickHouseAdapterBatch {
            identity: identity_for(1, 1),
            records: vec![stamped],
            bytes: 0,
        })
        .expect("plan stamped");
    assert_eq!(chunks.len(), 1, "one record → one chunk");
    assert_eq!(chunks[0].rows.len(), 1);
    let row = &chunks[0].rows[0];
    let last = row.len() - 1;
    match &row[last] {
        RowValue::NullableDateTime64Nanos(Some(v)) => assert_eq!(
            *v, gateway_ts_ns,
            "adapter must place the parsed gateway timestamp in the trailing typed column"
        ),
        other => panic!("trailing column wrong shape: {other:?}"),
    }
    // Position 6 is `ResourceAttributes`. The adapter must remove
    // the key from this map after extracting it.
    match &row[6] {
        RowValue::StringMap(m) => assert!(
            !m.contains_key("_odb_gateway_received_at"),
            "adapter must drop _odb_gateway_received_at from ResourceAttributes; still has: {m:?}"
        ),
        other => panic!("position 6 wrong shape: {other:?}"),
    }

    fx.writer
        .execute_chunk(&chunks[0])
        .await
        .expect("execute_chunk stamped");

    // ----- Phase B: record without the gateway stamp. -----

    let bare = bare_record(2, "bare-record", now_ns);
    let chunks_bare = adapter
        .plan(ClickHouseAdapterBatch {
            identity: identity_for(2, 2),
            records: vec![bare],
            bytes: 0,
        })
        .expect("plan bare");
    match &chunks_bare[0].rows[0][last] {
        RowValue::NullableDateTime64Nanos(None) => (),
        other => panic!("expected NullableDateTime64Nanos(None) for bare record, got {other:?}"),
    }
    fx.writer
        .execute_chunk(&chunks_bare[0])
        .await
        .expect("execute_chunk bare");

    // ----- Phase C: CH-side assertions. -----

    let non_null_count = query_scalar_u64(
        &fx,
        &format!(
            "SELECT count() FROM {db}.{table} FINAL \
             WHERE _odb_gateway_received_at IS NOT NULL FORMAT TSV",
            db = fx.database,
            table = fx.table,
        ),
    )
    .await;
    assert_eq!(
        non_null_count, 1,
        "exactly one row has a non-NULL gateway timestamp"
    );

    let null_count = query_scalar_u64(
        &fx,
        &format!(
            "SELECT count() FROM {db}.{table} FINAL \
             WHERE _odb_gateway_received_at IS NULL FORMAT TSV",
            db = fx.database,
            table = fx.table,
        ),
    )
    .await;
    assert_eq!(
        null_count, 1,
        "exactly one row carries NULL gateway timestamp (the bare record)"
    );

    let inserted_at_non_null = query_scalar_u64(
        &fx,
        &format!(
            "SELECT count() FROM {db}.{table} FINAL \
             WHERE _odb_clickhouse_inserted_at IS NOT NULL FORMAT TSV",
            db = fx.database,
            table = fx.table,
        ),
    )
    .await;
    assert_eq!(
        inserted_at_non_null, 2,
        "_odb_clickhouse_inserted_at must be populated by the server-side DEFAULT now64(9) on every row"
    );

    // Round-trip the gateway timestamp through RowBinary → CH →
    // SELECT. The value must come back identical down to the
    // nanosecond. Filter to the stamped row by `_odb_sequence=1`.
    let gateway_round_trip = query_scalar_u64(
        &fx,
        &format!(
            "SELECT toUInt64(toUnixTimestamp64Nano(_odb_gateway_received_at)) \
             FROM {db}.{table} FINAL \
             WHERE _odb_sequence = 1 FORMAT TSV",
            db = fx.database,
            table = fx.table,
        ),
    )
    .await;
    assert_eq!(
        gateway_round_trip, gateway_ts_ns,
        "gateway timestamp must round-trip through CH RowBinary unchanged"
    );

    // The (insert - gateway) delta must be non-negative.
    // `now64(9)` is wall-clock at INSERT materialization; the
    // gateway stamp is 2023-11-14. Don't pin a tight upper bound —
    // CI clocks vary — but the value must fit a sane range
    // (< 100 years).
    let delta_ns = query_scalar_i64(
        &fx,
        &format!(
            "SELECT toInt64(toUnixTimestamp64Nano(_odb_clickhouse_inserted_at)) \
             - toInt64(toUnixTimestamp64Nano(_odb_gateway_received_at)) \
             FROM {db}.{table} FINAL \
             WHERE _odb_sequence = 1 FORMAT TSV",
            db = fx.database,
            table = fx.table,
        ),
    )
    .await;
    assert!(
        delta_ns >= 0,
        "insert_ns - gateway_ns must be non-negative; got {delta_ns}"
    );
    const HUNDRED_YEARS_NS: i64 = 100i64 * 365 * 86_400 * 1_000_000_000;
    assert!(
        delta_ns < HUNDRED_YEARS_NS,
        "delta must be < 100 years; got {delta_ns}"
    );

    // The quantile query the cell-bench harness will run must
    // produce a valid `samples_observed = 1` over this fixture and
    // a non-negative p99 (single sample → p99 == max). This
    // exercises the exact aggregation shape the harness uses.
    let p99_ns = query_scalar_u64(
        &fx,
        &format!(
            "SELECT toUInt64OrZero(toString(quantile(0.99)(delta_ns))) FROM (\
                SELECT \
                    toInt64(toUnixTimestamp64Nano(_odb_clickhouse_inserted_at)) \
                    - toInt64(toUnixTimestamp64Nano(_odb_gateway_received_at)) AS delta_ns \
                FROM {db}.{table} FINAL \
                WHERE _odb_gateway_received_at IS NOT NULL \
            ) FORMAT TSV",
            db = fx.database,
            table = fx.table,
        ),
    )
    .await;
    assert!(
        p99_ns > 0,
        "quantile(0.99) over the single stamped row must be positive; got {p99_ns}"
    );

    let samples_observed = query_scalar_u64(
        &fx,
        &format!(
            "SELECT countIf(delta_ns >= 0) FROM (\
                SELECT \
                    toInt64(toUnixTimestamp64Nano(_odb_clickhouse_inserted_at)) \
                    - toInt64(toUnixTimestamp64Nano(_odb_gateway_received_at)) AS delta_ns \
                FROM {db}.{table} FINAL \
                WHERE _odb_gateway_received_at IS NOT NULL \
            ) FORMAT TSV",
            db = fx.database,
            table = fx.table,
        ),
    )
    .await;
    assert_eq!(
        samples_observed, 1,
        "harness-style samples_observed query must count the stamped row only"
    );
}

// ---- helpers ----

fn identity_for(low: u64, high: u64) -> CommitIdentity {
    CommitIdentity {
        source: SourceId::from("stage2-e2e-test"),
        sink: SinkId::from("clickhouse_logs"),
        range: SequenceRange::new(low, high),
        schema_version: SchemaVersion(1),
    }
}

fn stamped_record(
    sequence: u64,
    gateway_ts_ns: u64,
    body: &str,
    row_timestamp_ns: u64,
) -> DecodedLogRecord {
    let mut rec = bare_record(sequence, body, row_timestamp_ns);
    // Stringified u64 — mirror exactly what
    // `string_value(IntValue(stamp))` in the OTel decoder produces.
    rec.resource_attributes.insert(
        "_odb_gateway_received_at".into(),
        gateway_ts_ns.to_string(),
    );
    rec
}

fn bare_record(sequence: u64, body: &str, row_timestamp_ns: u64) -> DecodedLogRecord {
    DecodedLogRecord {
        source: RowSourceCoordinates {
            buffer_sequence: sequence,
            entry_index: 0,
            record_index: 0,
            manifest_path: "stage2-e2e/manifest".into(),
            data_path: "stage2-e2e/data".into(),
            ingestion_time_ms: 12345,
        },
        // Anchor close to "now" — the table TTL drops rows where
        // `toDate(Timestamp) + INTERVAL 30 DAY < toDate(now())`.
        timestamp_unix_nano: row_timestamp_ns + sequence,
        observed_timestamp_unix_nano: 0,
        severity_number: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        service_name: Some("stage2-e2e".into()),
        resource_attributes: BTreeMap::from([("service.namespace".into(), "responsive".into())]),
        scope_name: None,
        log_attributes: BTreeMap::from([
            ("_odb_run_id".into(), "stage2-e2e-run".into()),
            ("_odb_record_id_in_run".into(), sequence.to_string()),
        ]),
        trace_id_hex: String::new(),
        span_id_hex: String::new(),
    }
}

async fn query_scalar_u64(fx: &RealClickHouseFixture, sql: &str) -> u64 {
    let raw = fx
        .writer
        .execute_statement(sql)
        .await
        .unwrap_or_else(|e| panic!("execute_statement {sql:?} failed: {e}"));
    raw.trim()
        .parse::<u64>()
        .unwrap_or_else(|e| panic!("parse u64 from {raw:?} (sql {sql:?}): {e}"))
}

async fn query_scalar_i64(fx: &RealClickHouseFixture, sql: &str) -> i64 {
    let raw = fx
        .writer
        .execute_statement(sql)
        .await
        .unwrap_or_else(|e| panic!("execute_statement {sql:?} failed: {e}"));
    raw.trim()
        .parse::<i64>()
        .unwrap_or_else(|e| panic!("parse i64 from {raw:?} (sql {sql:?}): {e}"))
}
