//! Runtime logical identity contract tests.
//!
//! The runtime exposes a single logical identity for a source-range
//! commit: [`CommitIdentity`]. This struct projection keeps
//! sink-physical concerns out of the runtime surface (an earlier
//! hashed-string `IdempotencyKey` did not).
//!
//! Two properties must hold and are pinned here:
//!
//! 1. **Stability across runtime initializations** — for the same
//!    `(source, sink, range, schema_version)` inputs, two
//!    independently constructed `CommitIdentity` values are
//!    byte-identical (`==`, identical `Display` projection, identical
//!    `Hash`).
//! 2. **Multi-entry batch identity** — when a single Buffer batch
//!    sequence `S` carries multiple OTel entries × records, the
//!    runtime's `CommitIdentity.range` is `S..=S` (not per-row),
//!    proving that row-level coordinates
//!    (`buffer_sequence`, `entry_index`, `record_index`) belong on
//!    the row, not on `CommitIdentity`. The sink-physical token
//!    pins the matching `S..=S` shape via
//!    `plugins/opendata-ingest-clickhouse/tests/adapter_token.rs`.
//!
//! The per-field-sensitivity assertions (`source` / `sink` /
//! `low` / `high` / `schema_version` affect the key) collapse to the
//! struct's `PartialEq` derive — included below as one consolidated
//! sensitivity sweep. The `chunking_fingerprint must affect key`
//! assertion has moved to `adapter_token.rs` reframed as a sink-token
//! stability property (the runtime no longer owns a chunking
//! fingerprint).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use opendata_ingest_runtime::identity::{CommitIdentity, SchemaVersion, SequenceRange};
use opendata_ingest_runtime::sink::SinkId;
use opendata_ingest_runtime::source::SourceId;

fn make(source: &str, sink: &str, low: u64, high: u64, schema_version: u32) -> CommitIdentity {
    CommitIdentity {
        source: SourceId::from(source),
        sink: SinkId::from(sink),
        range: SequenceRange::new(low, high),
        schema_version: SchemaVersion(schema_version),
    }
}

fn hash(id: &CommitIdentity) -> u64 {
    let mut h = DefaultHasher::new();
    id.hash(&mut h);
    h.finish()
}

/// Property: building `CommitIdentity` twice from the same inputs
/// produces byte-identical results across independent constructions
/// — the runtime's stability-across-replay guarantee. Sweeps several
/// input shapes generatively so a future refactor that introduces
/// nondeterminism (e.g. time-based fields) trips at least one case.
#[test]
fn identity_is_stable_across_independent_constructions() {
    let inputs: &[(&str, &str, u64, u64, u32)] = &[
        ("buffer", "clickhouse_logs", 0, 0, 1),
        ("buffer", "clickhouse_logs", 7, 9, 1),
        ("source-a", "sink-a", 0, u64::MAX, 1),
        ("multi:source:colons", "sink:with:colons", 42, 42, 7),
        ("buffer", "clickhouse_logs", 0, 0, u32::MAX),
    ];
    for (source, sink, low, high, schema_version) in inputs {
        let a = make(source, sink, *low, *high, *schema_version);
        let b = make(source, sink, *low, *high, *schema_version);
        assert_eq!(a, b, "byte-identical struct across two builds");
        assert_eq!(
            a.to_string(),
            b.to_string(),
            "byte-identical Display projection across two builds",
        );
        assert_eq!(hash(&a), hash(&b), "identical hash across two builds");
    }
}

/// Property: every component of the identity affects equality. Pins
/// the `(source, sink, range, schema_version)` tuple as the complete
/// set of inputs — any new field added to `CommitIdentity` that
/// participates in `==` would surface as a new failure case here.
#[test]
fn identity_changes_when_any_input_changes() {
    let baseline = make("buffer", "clickhouse_logs", 7, 9, 1);

    let cases: &[(&str, CommitIdentity)] = &[
        ("source", make("other", "clickhouse_logs", 7, 9, 1)),
        ("sink", make("buffer", "other_sink", 7, 9, 1)),
        ("low", make("buffer", "clickhouse_logs", 8, 9, 1)),
        ("high", make("buffer", "clickhouse_logs", 7, 10, 1)),
        ("schema_version", make("buffer", "clickhouse_logs", 7, 9, 2)),
    ];
    for (field, other) in cases {
        assert_ne!(
            baseline, *other,
            "{field} must affect CommitIdentity equality",
        );
        assert_ne!(
            baseline.to_string(),
            other.to_string(),
            "{field} must affect Display projection",
        );
    }
}

/// Property: `Display` matches the canonical
/// `{source}:{sink}:{low}-{high}:{schema_version}` shape. The runtime
/// logs and metric labels use this string; pinning it here forces a
/// downstream consumer change to be explicit.
#[test]
fn identity_display_projection_is_canonical() {
    let id = make("buffer", "clickhouse_logs", 7, 9, 1);
    assert_eq!(id.to_string(), "buffer:clickhouse_logs:7-9:1");

    let edge = make("source", "sink", 0, u64::MAX, u32::MAX);
    assert_eq!(
        edge.to_string(),
        format!("source:sink:0-{}:{}", u64::MAX, u32::MAX),
    );
}

/// Multi-entry batch identity: a single Buffer batch sequence `S`
/// carries multiple OTel entries × records. The runtime's
/// `CommitIdentity.range` is `S..=S` regardless of how many rows the
/// batch holds — row-level coordinates
/// (`buffer_sequence`, `entry_index`, `record_index`) belong on the
/// row, not on the runtime commit identity. The runtime side of this
/// property is just the struct projection; this test pins the
/// architectural assumption so concurrency cannot quietly shift
/// `range` to a per-row notion.
#[test]
fn multi_entry_batch_collapses_to_single_sequence_range() {
    // Three entries × two records per entry, all under buffer
    // sequence S=42. The runtime's CommitIdentity is one struct
    // with range = 42..=42 — not three structs, not six structs.
    let s: u64 = 42;
    let id = make("buffer", "clickhouse_logs", s, s, 1);
    assert_eq!(id.range, SequenceRange::new(s, s));
    assert_eq!(id.to_string(), format!("buffer:clickhouse_logs:{s}-{s}:1"),);

    // The row-level (buffer_sequence, entry_index, record_index)
    // triples for the six rows would be:
    //   (42, 0, 0), (42, 0, 1),
    //   (42, 1, 0), (42, 1, 1),
    //   (42, 2, 0), (42, 2, 1)
    // All six rows share buffer_sequence=42; the runtime identity
    // carries the buffer-sequence axis only.
    let row_coords: Vec<(u64, u32, u32)> = (0..3u32)
        .flat_map(|entry| (0..2u32).map(move |rec| (s, entry, rec)))
        .collect();
    assert_eq!(row_coords.len(), 6);
    for (buffer_sequence, _entry, _rec) in &row_coords {
        assert_eq!(*buffer_sequence, s);
        assert_eq!(id.range.low, *buffer_sequence);
        assert_eq!(id.range.high, *buffer_sequence);
    }
}
