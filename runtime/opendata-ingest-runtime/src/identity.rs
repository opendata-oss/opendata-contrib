//! Runtime logical commit identity (RFC 0002 §Runtime/Sink Boundary).
//!
//! Three identities cross the runtime/sink boundary; this module hosts
//! the middle one:
//!
//! 1. Buffer/source progress identity — `last_acked_sequence` on the
//!    Buffer `Consumer`. Owned by the source.
//! 2. **Runtime logical commit identity** — [`CommitIdentity`]. A
//!    deterministic projection of
//!    `(source, sink, range, schema_version)`; byte-identical across
//!    replay. Owned by the runtime; consumed by the sink.
//! 3. Sink physical write/dedupe identity — physical tokens, file
//!    paths, manifest entries. Owned by the sink; each sink derives
//!    them from `CommitIdentity` plus its own adapter configuration.
//!    The runtime never inspects sink-physical tokens.

use std::fmt;

use crate::sink::SinkId;
use crate::source::SourceId;

/// Schema version carried with every decoded batch and with every
/// runtime commit identity. v1 carries an opaque counter; Phase 7's
/// schema/mapping document fleshes this out alongside the columnar
/// migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SchemaVersion(pub u32);

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Inclusive range over Buffer batch sequences. Single-batch ranges
/// have `low == high` — the runtime never coalesces source ranges in
/// Phase 6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SequenceRange {
    pub low: u64,
    pub high: u64,
}

impl SequenceRange {
    pub fn new(low: u64, high: u64) -> Self {
        Self { low, high }
    }
}

impl fmt::Display for SequenceRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.low, self.high)
    }
}

/// Deterministic projection of a single source-range commit. The
/// runtime hands this to the sink on every [`Sink::write`] and
/// [`Sink::check_committed`] call; replay of the same source range
/// under the same configuration produces a byte-identical struct
/// (all four fields are total — no hashing, no fingerprinting, no
/// sink-specific data).
///
/// The [`Display`] impl produces the canonical
/// `{source}:{sink}:{low}-{high}:{schema_version}` string the
/// runtime uses for logs and metric labels.
///
/// [`Sink::write`]: crate::sink::Sink::write
/// [`Sink::check_committed`]: crate::sink::Sink::check_committed
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommitIdentity {
    pub source: SourceId,
    pub sink: SinkId,
    pub range: SequenceRange,
    pub schema_version: SchemaVersion,
}

impl fmt::Display for CommitIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.source, self.sink, self.range, self.schema_version,
        )
    }
}
