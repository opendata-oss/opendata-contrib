//! Decoder trait (RFC 0002 §`Decoder`).
//!
//! The runtime treats each entry's per-entry metadata as an opaque
//! byte payload and never interprets it. The decoder owns the metadata
//! format: it decides via [`Decoder::accepts`] whether it handles a
//! given metadata payload, and validates each entry inside
//! [`Decoder::decode`], returning `Err` on an unexpected or
//! inconsistent payload — which the runtime treats as fatal.
//!
//! The runtime drives one decoder per source: it calls `accepts` on the
//! first entry's metadata as a fail-fast and then `decode` per source
//! batch. Per-entry routing across decoders is accommodated by the
//! trait shape but not implemented today.

use crate::decoded_batch::DecodedBatch;
use crate::error::RuntimeResult;
use crate::source::{SourceBatch, SourceId};

pub trait Decoder: Send + Sync + 'static {
    /// Whether this decoder handles entries carrying the given opaque
    /// per-entry metadata bytes. The runtime passes the bytes through
    /// without interpreting them; the decoder is free to parse them.
    fn accepts(&self, raw_metadata: &[u8]) -> bool;

    /// Consume an entire source batch and produce **at least one
    /// [`DecodedBatch`]**. The decoder owns interpretation and
    /// validation of each entry's opaque metadata; an unexpected or
    /// inconsistent metadata payload must be returned as `Err`, which
    /// the runtime treats as fatal (it never acks the offending range).
    ///
    /// Today a decoder returns exactly one `DecodedBatch`; the `Vec`
    /// return leaves room for a future decoder to split one source
    /// batch into several, each covering a distinct sub-range of the
    /// input sequence span, admitted in order.
    ///
    /// Returning an empty `Vec` is a contract violation:
    /// `Runtime::handle_source_batch` rejects it with
    /// `RuntimeError::Decoder(_)` so descriptors stay admitted in
    /// contiguous source-sequence order at the [`AckCoordinator`].
    /// A decoder that has nothing to emit for an
    /// input batch should still produce one
    /// zero-record `DecodedBatch` covering the input
    /// sequence, so the source range stays accounted for and
    /// the Buffer ack frontier can advance.
    ///
    /// # Hard-abort cancellation
    ///
    /// `decode` is **synchronous**; the `hard_abort_token`
    /// cannot preempt a `decode` call mid-execution. The decode
    /// worker wraps the call in `tokio::select!` against the abort
    /// token, but the synchronous future runs to its first
    /// suspension point before the abort branch can fire — for a
    /// pure CPU-bound decoder, that means it runs to completion.
    /// Implementations are expected to be **fast** (the runtime's
    /// per-stage latency budget assumes microsecond-scale decode);
    /// a decoder that needs to do slow CPU work (e.g. complex
    /// transforms, regex compilation) should pre-compute and cache
    /// at construction. Long-running CPU work belongs in a future
    /// async-decode contract (likely paired with `spawn_blocking`
    /// at the runtime layer).
    fn decode(&self, batch: SourceBatch) -> RuntimeResult<Vec<DecodedBatch>>;
}

/// Source-level context the runtime constructs once per source and
/// hands to decoder plumbing for diagnostics and metric labels. The
/// `Decoder` trait does not consume it directly today, but the type
/// lives here so future per-source routing has a stable carrier.
#[derive(Debug, Clone)]
pub struct DecodeContext {
    pub source: SourceId,
}
