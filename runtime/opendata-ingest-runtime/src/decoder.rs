//! Decoder trait (RFC 0002 rev 5 §`Decoder`).
//!
//! v1 contract: one decoder per source. The runtime calls
//! `accepts(envelope)` once with the source's configured envelope at
//! startup or first non-empty batch, then drives `decode` per
//! source batch. Per-entry routing across decoders is supported by
//! the trait shape but not implemented in v1; v1 fails closed on
//! mixed envelopes within a single source, mirroring RFC 0001.

use crate::decoded_batch::DecodedBatch;
use crate::envelope::MetadataEnvelope;
use crate::error::RuntimeResult;
use crate::source::{SourceBatch, SourceId};

pub trait Decoder: Send + Sync + 'static {
    fn accepts(&self, envelope: &MetadataEnvelope) -> bool;

    /// Consume an entire source batch and produce **at least one
    /// [`DecodedBatch`]**. v1 returns exactly one (RFC 0002 rev 5);
    /// a future v2 may return multiple, but each one must occupy
    /// a distinct sub-range of the input sequence span and the
    /// runtime admits them in order.
    ///
    /// Returning an empty `Vec` is a contract violation:
    /// `Runtime::handle_source_batch` rejects it with
    /// `RuntimeError::Decoder(_)` to preserve
    /// **INV-ADMISSION-CONTIGUOUS** at the [`AckCoordinator`]
    /// (see `plans/odb-high-throughput/phase05-ack-correctness-design.md`
    /// rev 7). A decoder that has nothing to emit for an
    /// input batch should still produce one
    /// zero-record `DecodedBatch` covering the input
    /// sequence, so the source range stays accounted for and
    /// the Buffer ack frontier can advance.
    ///
    /// # Hard-abort cancellation
    ///
    /// `decode` is **synchronous**; Phase 6's `hard_abort_token`
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
    /// at the runtime layer) — see phase06 design §Open Questions
    /// for the deferral.
    fn decode(&self, batch: SourceBatch) -> RuntimeResult<Vec<DecodedBatch>>;
}

/// Source-level context the runtime constructs once per source and
/// hands to decoder plumbing for diagnostics, metric labels, and
/// (Phase 6+) per-source schema-version overrides. The `Decoder`
/// trait does not consume it directly in v1 — the v1 trait shape
/// matches RFC 0002 rev 5 verbatim — but the type lives here so
/// future-runtime routing has a stable carrier.
#[derive(Debug, Clone)]
pub struct DecodeContext {
    pub source: SourceId,
    pub configured_envelope: MetadataEnvelope,
}
