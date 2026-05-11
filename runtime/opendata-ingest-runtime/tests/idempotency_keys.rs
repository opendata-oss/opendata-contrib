//! INV-IDEMPOTENCY-KEY-IDENTITY contract tests for the
//! runtime-level `DefaultIdempotencyContract`. Per Phase 5.0
//! design rev 6 §Test Plan > Idempotency-key contract tests.
//!
//! The default key shape (RFC 0002 rev 6 §`IdempotencyContract`,
//! confirmed against the implementation at
//! `runtime/opendata-ingest-runtime/src/idempotency.rs`):
//!
//! ```text
//! {source}:{sink}:{low}-{high}:{schema_version}:{fingerprint:016x}
//! ```
//!
//! These tests pin the format AND its per-field sensitivity:
//! any change in `source`, `sink`, `low_sequence`,
//! `high_sequence`, `schema_version`, or `chunking_fingerprint`
//! must produce a different key. Unification between the
//! runtime key and the ClickHouse adapter's
//! `insert_deduplication_token` is deferred to Phase 7 per
//! §Decisions Q6.

use opendata_ingest_runtime::idempotency::{
    DefaultIdempotencyContract, IdempotencyContract, IdempotencyScope, SchemaVersion,
};
use opendata_ingest_runtime::sink::SinkId;
use opendata_ingest_runtime::source::SourceId;

fn scope<'a>(
    source: &'a SourceId,
    sink: &'a SinkId,
    low: u64,
    high: u64,
    schema_version: u32,
    fingerprint: u64,
) -> IdempotencyScope<'a> {
    IdempotencyScope {
        source,
        sink,
        low_sequence: low,
        high_sequence: high,
        schema_version: SchemaVersion(schema_version),
        chunking_fingerprint: fingerprint,
    }
}

/// INV-IDEMPOTENCY-KEY-IDENTITY (positive): the
/// `DefaultIdempotencyContract` produces exactly the documented
/// `{source}:{sink}:{low}-{high}:{schema_version}:{fingerprint:016x}`
/// format. The fingerprint is hex-padded to 16 chars
/// (`{:016x}`) so keys sort lexicographically the same way they
/// would numerically; a change to that padding would silently
/// scramble dedupe state in production.
#[test]
fn default_key_format_is_source_sink_range_schema_fingerprint() {
    let source = SourceId::from("buffer");
    let sink = SinkId::from("logs_clickhouse");
    let key = DefaultIdempotencyContract.key(scope(&source, &sink, 7, 9, 1, 0xABCDEF));
    assert_eq!(
        key.to_string(),
        "buffer:logs_clickhouse:7-9:1:0000000000abcdef",
        "exact format mismatch — INV-IDEMPOTENCY-KEY-IDENTITY broken"
    );

    // Round-trip: fingerprint=0 produces all-zero 16-char hex.
    let key0 = DefaultIdempotencyContract.key(scope(&source, &sink, 0, 0, 1, 0));
    assert_eq!(
        key0.to_string(),
        "buffer:logs_clickhouse:0-0:1:0000000000000000"
    );

    // Round-trip: fingerprint=u64::MAX produces all-f 16-char hex.
    let key_max = DefaultIdempotencyContract.key(scope(&source, &sink, 0, 0, 1, u64::MAX));
    assert_eq!(
        key_max.to_string(),
        "buffer:logs_clickhouse:0-0:1:ffffffffffffffff"
    );
}

/// INV-IDEMPOTENCY-KEY-IDENTITY (per-field sensitivity). Five
/// sub-cases — one per scope field. Each pair differs in
/// exactly one field; the keys must differ.
#[test]
fn default_key_changes_with_each_scope_field() {
    let s1 = SourceId::from("buffer-a");
    let s2 = SourceId::from("buffer-b");
    let k1 = SinkId::from("logs_clickhouse");
    let k2 = SinkId::from("logs_iceberg");

    let baseline = DefaultIdempotencyContract.key(scope(&s1, &k1, 7, 9, 1, 100));

    // (1) source.
    let differ_source = DefaultIdempotencyContract.key(scope(&s2, &k1, 7, 9, 1, 100));
    assert_ne!(baseline, differ_source, "source field must affect key");

    // (2) sink.
    let differ_sink = DefaultIdempotencyContract.key(scope(&s1, &k2, 7, 9, 1, 100));
    assert_ne!(baseline, differ_sink, "sink field must affect key");

    // (3) low_sequence.
    let differ_low = DefaultIdempotencyContract.key(scope(&s1, &k1, 8, 9, 1, 100));
    assert_ne!(baseline, differ_low, "low_sequence must affect key");

    // (4) high_sequence.
    let differ_high = DefaultIdempotencyContract.key(scope(&s1, &k1, 7, 10, 1, 100));
    assert_ne!(baseline, differ_high, "high_sequence must affect key");

    // (5) schema_version.
    let differ_schema = DefaultIdempotencyContract.key(scope(&s1, &k1, 7, 9, 2, 100));
    assert_ne!(baseline, differ_schema, "schema_version must affect key");

    // (6) chunking_fingerprint (bonus — exists in the API).
    let differ_fingerprint = DefaultIdempotencyContract.key(scope(&s1, &k1, 7, 9, 1, 101));
    assert_ne!(
        baseline, differ_fingerprint,
        "chunking_fingerprint must affect key"
    );
}

/// INV-IDEMPOTENCY-KEY-IDENTITY (cross-sink collision
/// resistance). Two sinks committing the same source range
/// must produce different keys — otherwise a single durable
/// store could collide acks between sinks under multi-sink
/// fanout. (v1 is single-sink-per-service; this assertion is
/// defense in depth for any future shape.)
#[test]
fn default_key_collision_resistance_across_sinks_for_same_range() {
    let source = SourceId::from("buffer");
    let sink_a = SinkId::from("logs_clickhouse");
    let sink_b = SinkId::from("logs_iceberg");

    let key_a = DefaultIdempotencyContract.key(scope(&source, &sink_a, 0, 99, 1, 0));
    let key_b = DefaultIdempotencyContract.key(scope(&source, &sink_b, 0, 99, 1, 0));
    assert_ne!(
        key_a, key_b,
        "same source-range across distinct sinks must produce distinct keys"
    );
}
