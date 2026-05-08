//! Idempotency key construction (RFC 0002 rev 5 §`IdempotencyContract`).
//!
//! The runtime-level key identifies a single (source range, route)
//! commit. Sinks that internally chunk (ClickHouse insert chunks,
//! Iceberg Parquet files) construct their own per-chunk identifiers
//! by appending a sink-internal index — that suffix is the sink's
//! concern, not the runtime's.

use std::fmt;

use crate::router::RouteId;
use crate::source::SourceId;

/// Schema version handed to sinks for idempotency-key construction
/// and per-row materialization. v1 carries the value through; the
/// schema/mapping document layer (Phase 7) sources it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SchemaVersion(pub u32);

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub String);

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IdempotencyScope<'a> {
    pub source: &'a SourceId,
    pub route: &'a RouteId,
    pub low_sequence: u64,
    pub high_sequence: u64,
    pub schema_version: SchemaVersion,
    /// Pure-function hash of every input that affects how the sink
    /// internally chunks and orders this commit (commit-group
    /// thresholds, ordering rule id, sink-specific config). Two
    /// runtime configurations producing the same internal
    /// chunk/file boundaries share a fingerprint; any change that
    /// could move a record produces a different fingerprint.
    pub chunking_fingerprint: u64,
}

pub trait IdempotencyContract: Send + Sync {
    fn key(&self, scope: IdempotencyScope<'_>) -> IdempotencyKey;
}

/// RFC 0002 rev 5 default key shape:
/// `{source}:{route}:{low}-{high}:{schema_version}:{chunking_fingerprint:016x}`.
pub struct DefaultIdempotencyContract;

impl IdempotencyContract for DefaultIdempotencyContract {
    fn key(&self, scope: IdempotencyScope<'_>) -> IdempotencyKey {
        IdempotencyKey(format!(
            "{}:{}:{}-{}:{}:{:016x}",
            scope.source,
            scope.route,
            scope.low_sequence,
            scope.high_sequence,
            scope.schema_version,
            scope.chunking_fingerprint,
        ))
    }
}
