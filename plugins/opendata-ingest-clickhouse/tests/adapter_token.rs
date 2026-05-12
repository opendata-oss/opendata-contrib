//! INV-CLICKHOUSE-TOKEN-DETERMINISTIC contract tests for the
//! `OtlpLogsClickHouseAdapter`'s `insert_deduplication_token`.
//! Per Phase 5.0 design rev 6 §Test Plan > Idempotency-key /
//! adapter-token contract tests.
//!
//! The adapter's documented token shape (see `build_token` at
//! `plugins/opendata-ingest-clickhouse/src/adapter/logs.rs:261`):
//!
//! ```text
//! {manifest_path}:{database}.{table}:{low}-{high}:{adapter_version}:{fingerprint}:{chunk_index}
//! ```
//!
//! These tests pin the format AND its per-field sensitivity:
//! any change in `manifest_path`, `database`, `table`,
//! `low_sequence`, `high_sequence`, `adapter_version`, the
//! chunking-fingerprint input fields, or `chunk_index` must
//! produce a different token. The runtime-level
//! `IdempotencyKey` and this adapter token are **independent
//! key spaces** in v1 (the adapter computes its own token from
//! the record stream, not from `SinkCommit.idempotency_key`);
//! unification is Phase 7 schema/mapping work per design
//! §Decisions Q6.

use std::collections::BTreeMap;

use opendata_ingest_clickhouse::{Adapter, LogsAdapterConfig, OtlpLogsClickHouseAdapter};
use opendata_ingest_otel::logs::{DecodedLogRecord, SourceCoordinates};
use opendata_ingest_runtime::commit_group::CommitGroupBatch;

fn make_record(
    manifest_path: &str,
    sequence: u64,
    entry_index: u32,
    record_index: u32,
) -> DecodedLogRecord {
    DecodedLogRecord {
        source: SourceCoordinates {
            sequence,
            entry_index,
            record_index,
            manifest_path: manifest_path.to_string(),
            data_path: "ingest/test/adapter-token/data/0".to_string(),
            ingestion_time_ms: 1_700_000_000_000,
        },
        timestamp_unix_nano: 1_700_000_000_000_000_000,
        observed_timestamp_unix_nano: 1_700_000_000_000_000_001,
        severity_number: 9,
        severity_text: "INFO".into(),
        body: "hello".into(),
        service_name: Some("svc".into()),
        resource_attributes: BTreeMap::new(),
        scope_name: None,
        log_attributes: BTreeMap::new(),
        trace_id_hex: String::new(),
        span_id_hex: String::new(),
    }
}

fn batch(
    manifest_path: &str,
    low_sequence: u64,
    high_sequence: u64,
    n_records: usize,
) -> CommitGroupBatch<DecodedLogRecord> {
    let records: Vec<DecodedLogRecord> = (0..n_records as u32)
        .map(|i| make_record(manifest_path, low_sequence, 0, i))
        .collect();
    let bytes = records
        .iter()
        .map(|r| {
            use opendata_ingest_runtime::commit_group::RecordSize;
            r.approx_size_bytes()
        })
        .sum();
    CommitGroupBatch {
        records,
        low_sequence,
        high_sequence,
        bytes,
    }
}

/// INV-CLICKHOUSE-TOKEN-DETERMINISTIC (positive): the
/// `build_token` format is
/// `{manifest_path}:{database}.{table}:{low}-{high}:{adapter_version}:{fingerprint}:{chunk_index}`.
#[test]
fn adapter_token_matches_documented_build_token_shape() {
    let config = LogsAdapterConfig {
        database: "responsive".into(),
        table: "logs".into(),
        adapter_version: 1,
        max_chunk_rows: 100_000,
        max_chunk_bytes: 32 * 1024 * 1024,
        insert_quorum: Some("auto".into()),
        apply_deduplication_token: true,
    };
    let adapter = OtlpLogsClickHouseAdapter::new(config.clone());
    let fingerprint = adapter.chunking_fingerprint().to_string();

    let chunks = adapter
        .plan(batch("ingest/test/adapter-token/manifest", 7, 9, 1))
        .expect("plan");
    assert_eq!(chunks.len(), 1, "small batch should fit in one chunk");

    let expected = format!(
        "ingest/test/adapter-token/manifest:{}.{}:7-9:{}:{}:0",
        config.database, config.table, config.adapter_version, fingerprint,
    );
    assert_eq!(
        chunks[0].idempotency_token, expected,
        "exact token format mismatch — INV-CLICKHOUSE-TOKEN-DETERMINISTIC broken"
    );
    // Settings carries the same token under
    // `insert_deduplication_token` (writer-side input).
    assert_eq!(chunks[0].settings.insert_deduplication_token, expected);
}

/// INV-CLICKHOUSE-TOKEN-DETERMINISTIC: changing the manifest
/// path shifts the token. (Same code path as build_token's
/// first segment.)
#[test]
fn adapter_token_changes_with_manifest_path() {
    let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig::default());
    let a = adapter.plan(batch("manifests/a", 0, 0, 1)).expect("plan a")[0]
        .idempotency_token
        .clone();
    let b = adapter.plan(batch("manifests/b", 0, 0, 1)).expect("plan b")[0]
        .idempotency_token
        .clone();
    assert_ne!(a, b);
}

/// INV-CLICKHOUSE-TOKEN-DETERMINISTIC: per-field sensitivity
/// for `database`, `table`, `low`, `high`, `adapter_version`.
/// Each pair differs in exactly one input.
#[test]
fn adapter_token_changes_with_database_table_low_high_adapter_version() {
    let baseline_cfg = LogsAdapterConfig::default();
    let baseline = OtlpLogsClickHouseAdapter::new(baseline_cfg.clone())
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan baseline")[0]
        .idempotency_token
        .clone();

    // database
    let mut cfg = baseline_cfg.clone();
    cfg.database = "other_db".into();
    let differ_db = OtlpLogsClickHouseAdapter::new(cfg)
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan db")[0]
        .idempotency_token
        .clone();
    assert_ne!(baseline, differ_db, "database must affect token");

    // table
    let mut cfg = baseline_cfg.clone();
    cfg.table = "other_table".into();
    let differ_table = OtlpLogsClickHouseAdapter::new(cfg)
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan table")[0]
        .idempotency_token
        .clone();
    assert_ne!(baseline, differ_table, "table must affect token");

    // adapter_version
    let mut cfg = baseline_cfg.clone();
    cfg.adapter_version = baseline_cfg.adapter_version + 1;
    let differ_version = OtlpLogsClickHouseAdapter::new(cfg)
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan version")[0]
        .idempotency_token
        .clone();
    assert_ne!(
        baseline, differ_version,
        "adapter_version must affect token"
    );

    // low_sequence
    let differ_low = OtlpLogsClickHouseAdapter::new(baseline_cfg.clone())
        .plan(batch("manifests/base", 1, 1, 1))
        .expect("plan low")[0]
        .idempotency_token
        .clone();
    assert_ne!(baseline, differ_low, "low_sequence must affect token");

    // high_sequence
    let differ_high = OtlpLogsClickHouseAdapter::new(baseline_cfg.clone())
        .plan(batch("manifests/base", 0, 1, 1))
        .expect("plan high")[0]
        .idempotency_token
        .clone();
    assert_ne!(baseline, differ_high, "high_sequence must affect token");
}

/// INV-CLICKHOUSE-TOKEN-DETERMINISTIC: any
/// `LogsAdapterConfig` field that flows into
/// `chunking_fingerprint` (max_chunk_rows, max_chunk_bytes,
/// insert_quorum, plus the database/table/adapter_version
/// already covered) shifts the token via the embedded
/// fingerprint segment.
#[test]
fn adapter_token_changes_with_chunking_fingerprint_inputs() {
    let baseline_cfg = LogsAdapterConfig::default();
    let baseline = OtlpLogsClickHouseAdapter::new(baseline_cfg.clone())
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan baseline")[0]
        .idempotency_token
        .clone();

    // max_chunk_rows
    let mut cfg = baseline_cfg.clone();
    cfg.max_chunk_rows = baseline_cfg.max_chunk_rows + 1;
    let differ_rows = OtlpLogsClickHouseAdapter::new(cfg)
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan rows")[0]
        .idempotency_token
        .clone();
    assert_ne!(baseline, differ_rows, "max_chunk_rows must affect token");

    // max_chunk_bytes
    let mut cfg = baseline_cfg.clone();
    cfg.max_chunk_bytes = baseline_cfg.max_chunk_bytes + 1;
    let differ_bytes = OtlpLogsClickHouseAdapter::new(cfg)
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan bytes")[0]
        .idempotency_token
        .clone();
    assert_ne!(baseline, differ_bytes, "max_chunk_bytes must affect token");

    // insert_quorum
    let mut cfg = baseline_cfg.clone();
    cfg.insert_quorum = Some("3".into());
    let differ_quorum = OtlpLogsClickHouseAdapter::new(cfg)
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan quorum")[0]
        .idempotency_token
        .clone();
    assert_ne!(baseline, differ_quorum, "insert_quorum must affect token");

    // insert_quorum None vs the baseline's Some("auto"). The
    // hasher serializes the quorum value (or `""` when None),
    // so these should differ at the token level even though
    // None and Some("") would collide.
    let mut cfg = baseline_cfg.clone();
    cfg.insert_quorum = None;
    let differ_none = OtlpLogsClickHouseAdapter::new(cfg)
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan none")[0]
        .idempotency_token
        .clone();
    assert_ne!(
        baseline, differ_none,
        "insert_quorum None vs Some(\"auto\") must affect token"
    );

    // Documented current-behavior collision: insert_quorum
    // None and Some("") hash identically because the hasher
    // writes `insert_quorum=\n` (via `as_deref().unwrap_or("")`).
    // Pin it as a sentinel so a future hasher tightening
    // forces an `_adapter_version` bump.
    let mut cfg_empty = baseline_cfg.clone();
    cfg_empty.insert_quorum = Some(String::new());
    let differ_empty = OtlpLogsClickHouseAdapter::new(cfg_empty)
        .plan(batch("manifests/base", 0, 0, 1))
        .expect("plan empty")[0]
        .idempotency_token
        .clone();
    assert_eq!(
        differ_none, differ_empty,
        "insert_quorum None and Some(\"\") currently collide \
         in the chunking fingerprint; if this assertion ever \
         fails, bump LogsAdapterConfig::adapter_version to \
         invalidate prior tokens."
    );
}

/// INV-CLICKHOUSE-TOKEN-DETERMINISTIC: chunk_index monotonicity
/// within a single `plan` call. A batch large enough to span
/// two chunks produces tokens whose chunk_index segment is `0`
/// and `1` in order; all other segments are identical.
#[test]
fn adapter_token_chunk_index_increments_within_a_single_plan() {
    // Force chunking by setting max_chunk_rows = 1.
    let cfg = LogsAdapterConfig {
        database: "responsive".into(),
        table: "logs".into(),
        adapter_version: 1,
        max_chunk_rows: 1,
        max_chunk_bytes: 32 * 1024 * 1024,
        insert_quorum: Some("auto".into()),
        apply_deduplication_token: true,
    };
    let adapter = OtlpLogsClickHouseAdapter::new(cfg.clone());
    let chunks = adapter
        .plan(batch("manifests/chunked", 0, 0, 3))
        .expect("plan");
    assert_eq!(chunks.len(), 3, "max_chunk_rows=1 should produce 3 chunks");
    for (i, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.chunk_index, i as u32);
        assert!(
            chunk.idempotency_token.ends_with(&format!(":{i}")),
            "chunk {i} token must end with `:{i}`; got {}",
            chunk.idempotency_token,
        );
    }
    // Pairwise: every chunk's token is unique.
    let mut seen = std::collections::HashSet::new();
    for chunk in &chunks {
        assert!(
            seen.insert(chunk.idempotency_token.clone()),
            "duplicate token across chunks: {}",
            chunk.idempotency_token,
        );
    }
}
