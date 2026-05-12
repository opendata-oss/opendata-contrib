//! Per-source ack coordinator (RFC 0002 rev 6 §Per-Source Ack
//! Coordinator; Phase 5.0 design rev 6 §Public API Surface >
//! `ack_coordinator.rs`).
//!
//! One [`AckCoordinator`] per source tracks pending source-sequence
//! ranges and a contiguous `acked_frontier`. The state machine
//! ensures the durable Buffer ack frontier (advanced by the runtime
//! via `BufferSource::ack_through(frontier)`) never crosses a hole
//! — i.e., a sequence whose configured sink has not committed.
//!
//! Pinned invariants (see design rev 6 §Invariants for the full
//! `INV-*` taxonomy):
//!
//! - INV-FRONTIER-NEVER-OVER-HOLE — `frontier()` never returns a
//!   value that skips an uncommitted pending range.
//! - INV-ADMISSION-CONTIGUOUS — `register_pending(low, _)` is
//!   strictly contiguous per source: the first call satisfies
//!   the baseline floor (`low == initial_frontier_baseline + 1`
//!   when supplied, else any `low` is absorbed); every
//!   subsequent call satisfies `low == last_registered_high +
//!   1`. Stronger than mere monotonicity: catches admission
//!   gaps that would silently stall `advance_frontier` at the
//!   hole forever (e.g., a decoder that returns an empty
//!   `Vec<DecodedBatch>` for a source batch and silently drops
//!   it from coordinator tracking).
//! - INV-FRONTIER-BASELINE-FLOOR — the coordinator's
//!   `effective_baseline` is fixed once, by the constructor's
//!   `initial_frontier_baseline` (when known durably) or by the
//!   first `register_pending(low, _)` absorbing `Some(low - 1)`.
//!   Under the constructor-baseline path, the first range must
//!   start at exactly `baseline + 1`.
//! - INV-IDEMPOTENT-COMMIT — repeat `mark_committed` on the same
//!   range is a no-op (supports retry-then-restart sequences).
//! - INV-PENDING-DISJOINT — overlapping ranges are rejected
//!   (decoder produces source-disjoint ranges by construction).
//! - INV-PENDING-BOUNDED — `advance_frontier` drops committed
//!   ranges from the pending map; under steady state the map
//!   reflects the uncommitted tail only.
//! - INV-MULTISOURCE-ISOLATION — distinct coordinators (one per
//!   `SourceId`) never share state.
//!
//! `mark_committed` and `advance_frontier` are separated so tests
//! can drive the state machine in fine-grained steps; the runtime
//! always pairs them.

use std::collections::{BTreeMap, HashMap};

use crate::error::{RuntimeError, RuntimeResult};
use crate::source::SourceId;

/// One pending sequence range waiting for the configured sink to
/// commit. RFC 0002 rev 6 §Per-Source Ack Coordinator > State
/// Machine: a single sink-commit bit per range, no per-chunk
/// tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingRange {
    pub low: u64,
    pub high: u64,
    pub sink_committed: bool,
}

/// Per-source ack coordinator. One instance per `SourceId`; the
/// runtime owns the registry of instances via [`AckCoordinators`].
///
/// All methods are `&mut self`. The runtime serializes calls per
/// coordinator: in Phase 5 the orchestration loop is single-threaded
/// per source. Phase 6's pipelined runtime will keep
/// `register_pending` in the synchronous descriptor-admission task
/// while running `mark_committed` / `advance_frontier` from the
/// parallel post-write stage behind a per-source mutex.
pub struct AckCoordinator {
    source: SourceId,
    /// Optional durable resume cursor known at build time. When
    /// `Some(b)`, `register_pending`'s first call must have
    /// `low == b + 1`. When `None`, the first `register_pending`
    /// absorbs `low - 1` (or `None` when `low == 0`) as the
    /// effective baseline. Today's binary wires
    /// `BufferSource::last_acked_sequence: None`, so the absorption
    /// path is the operative one in production.
    initial_frontier_baseline: Option<u64>,
    /// The active floor used by `register_pending` /
    /// `advance_frontier`. Fixed once (see `baseline_fixed`);
    /// never changes after.
    effective_baseline: Option<u64>,
    /// `true` once `effective_baseline` has been fixed.
    /// Distinguishes pre-fix from "fixed to `None`".
    baseline_fixed: bool,
    /// Highest sequence the coordinator has committed since
    /// construction. `None` until the first commit lands.
    acked_frontier: Option<u64>,
    /// Highest `high` value passed to a successful
    /// `register_pending`. Used to enforce
    /// INV-ADMISSION-CONTIGUOUS: each subsequent register call
    /// must have `low == last_registered_high + 1`. Stronger
    /// than the original monotonicity check (`low >
    /// last_registered_low`): admission gaps would silently
    /// stall `advance_frontier` at the hole. The strict-
    /// contiguity check catches a decoder that drops a source
    /// batch (returns empty `Vec<DecodedBatch>`) at the next
    /// admission attempt instead of letting the frontier stall
    /// forever.
    last_registered_high: Option<u64>,
    /// Ranges that have entered the pipeline. Keyed by `low`
    /// (unique by INV-PENDING-DISJOINT).
    pending: BTreeMap<u64, PendingRange>,
}

impl AckCoordinator {
    /// Build a fresh coordinator. `initial_frontier_baseline` is
    /// the durable resume cursor the runtime knows at build time:
    /// `Some(b)` pins `effective_baseline = Some(b)` eagerly;
    /// `None` defers the baseline fix until the first
    /// `register_pending` call, which absorbs `low - 1` (or
    /// `None` when `low == 0`). The deferred path is what
    /// today's binary wiring exercises — `BufferSource::new(..,
    /// last_acked_sequence: None, ..)` plus the source's
    /// `first_seen` field handles the Buffer ack side, and the
    /// coordinator's absorption mirrors it on the in-memory side.
    pub fn new(source: SourceId, initial_frontier_baseline: Option<u64>) -> Self {
        Self {
            source,
            initial_frontier_baseline,
            effective_baseline: initial_frontier_baseline,
            baseline_fixed: initial_frontier_baseline.is_some(),
            acked_frontier: None,
            last_registered_high: None,
            pending: BTreeMap::new(),
        }
    }

    pub fn source(&self) -> &SourceId {
        &self.source
    }

    pub fn initial_frontier_baseline(&self) -> Option<u64> {
        self.initial_frontier_baseline
    }

    /// Returns the `effective_baseline` once fixed, else the
    /// pre-fix state. Use [`baseline_fixed`](Self::baseline_fixed)
    /// to distinguish "fixed to `None`" from "not yet fixed".
    pub fn effective_baseline(&self) -> Option<u64> {
        self.effective_baseline
    }

    pub fn baseline_fixed(&self) -> bool {
        self.baseline_fixed
    }

    /// INV-FRONTIER-MONOTONIC, INV-FRONTIER-NEVER-OVER-HOLE,
    /// INV-FRONTIER-BASELINE-FLOOR. Highest contiguous committed
    /// sequence since construction; `None` until the first commit
    /// lands.
    pub fn frontier(&self) -> Option<u64> {
        self.acked_frontier
    }

    /// Number of pending (uncommitted) ranges. Exposed for the
    /// `runtime_pending_ranges{source}` gauge and tests.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Register a `[low, high]` source-sequence range that has
    /// entered the pipeline. On the first call when
    /// `initial_frontier_baseline` was `None` at construction,
    /// fixes `effective_baseline = Some(low - 1)` (or `None` when
    /// `low == 0`) and sets `baseline_fixed = true`.
    ///
    /// Returns `Err(RuntimeError::Ack)` when any of:
    /// - `low > high` (caller bug),
    /// - INV-ADMISSION-CONTIGUOUS: `low != last_registered_high + 1`
    ///   after the first registration. Stronger than mere
    ///   monotonicity: admission gaps would silently stall
    ///   `advance_frontier` at the hole. Catches a decoder that
    ///   drops a source batch (returns empty
    ///   `Vec<DecodedBatch>`) at the next admission attempt.
    /// - INV-FRONTIER-BASELINE-FLOOR against a constructor-supplied
    ///   baseline: `low != b + 1` on the first range when
    ///   `initial_frontier_baseline == Some(b)`,
    /// - INV-PENDING-DISJOINT: overlap with an existing pending,
    /// - `low <= self.acked_frontier`: range already acked.
    pub fn register_pending(&mut self, low: u64, high: u64) -> RuntimeResult<()> {
        if low > high {
            return Err(RuntimeError::Ack(format!(
                "invalid range: low={low} > high={high}"
            )));
        }

        // INV-FRONTIER-BASELINE-FLOOR (constructor-supplied
        // baseline path; strict equality).
        if self.last_registered_high.is_none()
            && let Some(b) = self.initial_frontier_baseline
            && low != b.saturating_add(1)
        {
            return Err(RuntimeError::Ack(format!(
                "first range low={low} != baseline+1={}",
                b.saturating_add(1)
            )));
        }

        // INV-ADMISSION-CONTIGUOUS: strict equality on the next
        // expected low. Stronger than the rev-6 monotonicity
        // check; catches the "decoder dropped a source batch"
        // liveness gap by failing loud at the next admission.
        if let Some(prev_high) = self.last_registered_high {
            let expected = prev_high.saturating_add(1);
            if low != expected {
                return Err(RuntimeError::Ack(format!(
                    "non-contiguous admission on source {}: last_high={prev_high}, expected low={expected}, got low={low}",
                    self.source,
                )));
            }
        }

        // INV-NO-ACK-BEFORE-COMMIT defense in depth.
        if let Some(f) = self.acked_frontier
            && low <= f
        {
            return Err(RuntimeError::Ack(format!(
                "range low={low} already covered by frontier={f} on source {}",
                self.source,
            )));
        }

        // INV-PENDING-DISJOINT. Check the neighbor immediately
        // below `high` (if any) and the neighbor at-or-above
        // `low` (if any).
        if let Some((_, neighbor)) = self.pending.range(..=high).next_back()
            && neighbor.high >= low
        {
            return Err(RuntimeError::Ack(format!(
                "range {low}..={high} overlaps pending {}..={}",
                neighbor.low, neighbor.high,
            )));
        }
        if let Some((&next_low, _)) = self.pending.range(low..).next()
            && next_low <= high
        {
            return Err(RuntimeError::Ack(format!(
                "range {low}..={high} overlaps pending starting at {next_low}",
            )));
        }

        // Absorb baseline if the constructor did not supply one.
        if !self.baseline_fixed {
            self.effective_baseline = if low == 0 { None } else { Some(low - 1) };
            self.baseline_fixed = true;
        }

        self.pending.insert(
            low,
            PendingRange {
                low,
                high,
                sink_committed: false,
            },
        );
        self.last_registered_high = Some(high);
        Ok(())
    }

    /// Mark the range whose `low_sequence == low` and
    /// `high_sequence == high` as sink-committed.
    ///
    /// INV-IDEMPOTENT-COMMIT: a second call on an already-
    /// committed range is a no-op. Returns
    /// `Err(RuntimeError::Ack)` if no pending range with the
    /// given `low` exists or if its stored `high` disagrees with
    /// the caller's.
    pub fn mark_committed(&mut self, low: u64, high: u64) -> RuntimeResult<()> {
        let entry = self.pending.get_mut(&low).ok_or_else(|| {
            RuntimeError::Ack(format!(
                "no pending range with low={low} on source {}",
                self.source,
            ))
        })?;
        if entry.high != high {
            return Err(RuntimeError::Ack(format!(
                "range {low}..={high} disagrees with pending {low}..={} on source {}",
                entry.high, self.source,
            )));
        }
        entry.sink_committed = true;
        Ok(())
    }

    /// Drive frontier advance + pending-map cleanup. The runtime
    /// calls this after every `mark_committed`; tests may also
    /// call it directly. INV-FRONTIER-NEVER-OVER-HOLE,
    /// INV-FRONTIER-BASELINE-FLOOR, INV-PENDING-BOUNDED.
    ///
    /// Pops every committed pending range whose `low` equals the
    /// next-expected sequence; stops at the first uncommitted
    /// range or the first non-contiguous committed range (the
    /// latter is unreachable under INV-ADMISSION-CONTIGUOUS,
    /// which guarantees the pending map is gap-free).
    pub fn advance_frontier(&mut self) {
        if !self.baseline_fixed {
            // No `register_pending` has run yet, so the pending
            // map is empty by construction. Bail out cheaply.
            return;
        }
        loop {
            let next_low_high = match self.pending.iter().next() {
                Some((_, range)) if range.sink_committed => (range.low, range.high),
                _ => return,
            };
            let (next_low, next_high) = next_low_high;
            let expected_low = match self.acked_frontier {
                Some(f) => f.saturating_add(1),
                None => match self.effective_baseline {
                    Some(b) => b.saturating_add(1),
                    None => 0,
                },
            };
            if next_low != expected_low {
                // Defense in depth: under INV-ADMISSION-CONTIGUOUS
                // every pending range slots into the next-expected
                // sequence, so this branch is unreachable in
                // well-formed input. Bail out without advancing.
                return;
            }
            self.pending.pop_first();
            self.acked_frontier = Some(next_high);
        }
    }
}

/// Registry of per-source [`AckCoordinator`]s. Phase 5 uses N=1;
/// Phase 6 wires N>1. The registry never iterates across sources
/// during a state transition (INV-MULTISOURCE-ISOLATION).
pub struct AckCoordinators {
    coordinators: HashMap<SourceId, AckCoordinator>,
}

impl AckCoordinators {
    pub fn new() -> Self {
        Self {
            coordinators: HashMap::new(),
        }
    }

    /// Insert a fresh coordinator for `source`, seeded with the
    /// durable resume cursor `initial_frontier_baseline` (the
    /// value `BufferSource::last_acked_sequence()` returns at
    /// build time). Returns `Err(RuntimeError::Ack)` if a
    /// coordinator already exists for this source.
    pub fn register_source(
        &mut self,
        source: SourceId,
        initial_frontier_baseline: Option<u64>,
    ) -> RuntimeResult<()> {
        if self.coordinators.contains_key(&source) {
            return Err(RuntimeError::Ack(format!(
                "coordinator already registered for source {source}"
            )));
        }
        self.coordinators.insert(
            source.clone(),
            AckCoordinator::new(source, initial_frontier_baseline),
        );
        Ok(())
    }

    pub fn get(&self, source: &SourceId) -> Option<&AckCoordinator> {
        self.coordinators.get(source)
    }

    pub fn get_mut(&mut self, source: &SourceId) -> Option<&mut AckCoordinator> {
        self.coordinators.get_mut(source)
    }

    /// Iterator over `(SourceId, frontier)` for metric emission /
    /// dry-run progress reporting.
    pub fn frontiers(&self) -> impl Iterator<Item = (&SourceId, Option<u64>)> {
        self.coordinators.iter().map(|(id, c)| (id, c.frontier()))
    }

    /// Total uncommitted ranges across all coordinators. Backs
    /// the `progress.pending_ranges_total` field that the test
    /// harness reads without a metrics provider.
    pub fn pending_total(&self) -> usize {
        self.coordinators.values().map(|c| c.pending_count()).sum()
    }
}

impl Default for AckCoordinators {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord(baseline: Option<u64>) -> AckCoordinator {
        AckCoordinator::new(SourceId::from("test"), baseline)
    }

    // --- Initial state ---

    #[test]
    fn frontier_starts_at_none_and_pending_is_empty() {
        let c = coord(None);
        assert_eq!(c.frontier(), None);
        assert_eq!(c.pending_count(), 0);
        assert!(!c.baseline_fixed());
        assert_eq!(c.effective_baseline(), None);
        assert_eq!(c.initial_frontier_baseline(), None);
    }

    #[test]
    fn new_with_constructor_baseline_fixes_eagerly() {
        let c = coord(Some(5));
        assert!(c.baseline_fixed());
        assert_eq!(c.effective_baseline(), Some(5));
        assert_eq!(c.initial_frontier_baseline(), Some(5));
        assert_eq!(c.frontier(), None);
    }

    // --- register_pending input validation ---

    #[test]
    fn register_pending_rejects_low_greater_than_high() {
        let mut c = coord(None);
        let err = c.register_pending(5, 4).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    #[test]
    fn register_pending_rejects_overlap_with_existing() {
        let mut c = coord(None);
        c.register_pending(0, 4).unwrap();
        // Overlapping range below
        let err = c.register_pending(3, 5).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    #[test]
    fn register_pending_rejects_non_monotonic_admission() {
        let mut c = coord(None);
        c.register_pending(0, 0).unwrap();
        c.register_pending(1, 1).unwrap();
        // Same low (or lower) — fails INV-ADMISSION-CONTIGUOUS
        // (the strict-equality successor check subsumes the
        // monotonicity check).
        let err = c.register_pending(1, 1).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    #[test]
    fn register_pending_rejects_admission_gap() {
        // INV-ADMISSION-CONTIGUOUS. A decoder that drops a
        // source batch (returns empty Vec<DecodedBatch>) would
        // skip its sequence in admission. The next attempt
        // (jumping from N to N+2) must be rejected loud rather
        // than silently stalling advance_frontier at the hole.
        let mut c = coord(None);
        c.register_pending(0, 0).unwrap();
        // Gap: expected 1, got 2.
        let err = c.register_pending(2, 2).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
        let msg = format!("{err}");
        assert!(
            msg.contains("non-contiguous admission"),
            "expected contiguity-error message, got: {msg}"
        );
        // The reject is non-destructive: pending count stays
        // 1, and the next correct admission (low=1) succeeds.
        assert_eq!(c.pending_count(), 1);
        c.register_pending(1, 1).unwrap();
        assert_eq!(c.pending_count(), 2);
    }

    #[test]
    fn register_pending_rejects_gap_when_high_spans_multiple_sequences() {
        // INV-ADMISSION-CONTIGUOUS, multi-sequence variant. A
        // hypothetical v2 decoder could emit a DecodedBatch
        // spanning low..=high; the next admission must start
        // at high + 1 exactly.
        let mut c = coord(None);
        c.register_pending(0, 4).unwrap();
        let err = c.register_pending(6, 6).unwrap_err(); // expected 5
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
        c.register_pending(5, 5).unwrap();
    }

    // --- INV-FRONTIER-BASELINE-FLOOR ---

    #[test]
    fn register_pending_rejects_first_range_below_baseline() {
        let mut c = coord(Some(5));
        // Baseline says next sequence is 6; admitting 4 is below.
        let err = c.register_pending(4, 4).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    #[test]
    fn register_pending_rejects_first_range_above_baseline_plus_one() {
        let mut c = coord(Some(5));
        // Baseline says next sequence is 6; admitting 7 skips a
        // sequence and would silently stall advance_frontier.
        let err = c.register_pending(7, 7).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    #[test]
    fn register_pending_first_range_at_baseline_plus_one_succeeds() {
        let mut c = coord(Some(5));
        c.register_pending(6, 6).unwrap();
        assert_eq!(c.pending_count(), 1);
        c.mark_committed(6, 6).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(6));
    }

    #[test]
    fn register_pending_absorbed_baseline_accepts_any_first_low() {
        // Constructor None → absorption path; first low can be
        // any value (Buffer may have GC'd or producer may have
        // started writing at sequence N > 0).
        let mut c = coord(None);
        c.register_pending(42, 42).unwrap();
        assert!(c.baseline_fixed());
        assert_eq!(c.effective_baseline(), Some(41));
        c.mark_committed(42, 42).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(42));
    }

    #[test]
    fn register_pending_absorbed_baseline_zero_stays_none() {
        let mut c = coord(None);
        c.register_pending(0, 0).unwrap();
        assert!(c.baseline_fixed());
        assert_eq!(c.effective_baseline(), None);
        c.mark_committed(0, 0).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(0));
    }

    #[test]
    fn register_pending_rejects_range_already_acked() {
        let mut c = coord(None);
        c.register_pending(0, 0).unwrap();
        c.mark_committed(0, 0).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(0));
        // Admitting a range whose low is at-or-below the frontier
        // is structurally wrong (replay should come through a
        // fresh coordinator).
        let err = c.register_pending(0, 0).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    // --- mark_committed ---

    #[test]
    fn mark_committed_unknown_range_errors() {
        let mut c = coord(None);
        let err = c.mark_committed(0, 0).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    #[test]
    fn mark_committed_twice_is_idempotent() {
        let mut c = coord(None);
        c.register_pending(0, 0).unwrap();
        c.mark_committed(0, 0).unwrap();
        // Second call is a no-op (INV-IDEMPOTENT-COMMIT).
        c.mark_committed(0, 0).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(0));
    }

    #[test]
    fn mark_committed_rejects_mismatched_high() {
        let mut c = coord(None);
        c.register_pending(0, 3).unwrap();
        let err = c.mark_committed(0, 4).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    // --- advance_frontier ---

    #[test]
    fn frontier_advances_through_contiguous_committed_ranges() {
        let mut c = coord(None);
        c.register_pending(0, 0).unwrap();
        c.register_pending(1, 2).unwrap();
        c.register_pending(3, 3).unwrap();
        c.mark_committed(0, 0).unwrap();
        c.mark_committed(1, 2).unwrap();
        c.mark_committed(3, 3).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(3));
        assert_eq!(c.pending_count(), 0);
    }

    #[test]
    fn frontier_advance_drops_completed_ranges_from_map() {
        let mut c = coord(None);
        c.register_pending(0, 0).unwrap();
        c.register_pending(1, 1).unwrap();
        c.mark_committed(0, 0).unwrap();
        c.mark_committed(1, 1).unwrap();
        assert_eq!(c.pending_count(), 2);
        c.advance_frontier();
        assert_eq!(c.pending_count(), 0, "INV-PENDING-BOUNDED");
        assert_eq!(c.frontier(), Some(1));
    }

    #[test]
    fn frontier_monotonic_across_advance_calls() {
        let mut c = coord(None);
        for i in 0..=2 {
            c.register_pending(i, i).unwrap();
        }
        c.mark_committed(0, 0).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(0));
        c.mark_committed(1, 1).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(1));
        c.mark_committed(2, 2).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(2));
    }

    #[test]
    fn frontier_anchors_at_baseline_plus_one_not_first_observed_range() {
        // Constructor-baseline = 5 means the first frontier
        // advance lands on the high of a range whose low is 6.
        // It does NOT treat an arbitrary later range as the
        // natural starting point — register_pending strictly
        // rejects low != 6 on the first call, so the only path
        // through is the b+1 sequence.
        let mut c = coord(Some(5));
        c.register_pending(6, 6).unwrap();
        c.register_pending(7, 7).unwrap();
        c.mark_committed(7, 7).unwrap();
        // Range 7 is committed but 6 is not; frontier must NOT
        // skip 6.
        c.advance_frontier();
        assert_eq!(c.frontier(), None);
        c.mark_committed(6, 6).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(7));
    }

    // --- Out-of-order completion (state-machine layer; Phase 5
    //     serial runtime can't express this end-to-end). ---

    #[test]
    fn frontier_stops_at_first_hole_when_commits_arrive_out_of_order() {
        let mut c = coord(None);
        // Register 0, 1, 2 in order (INV-ADMISSION-ORDER).
        c.register_pending(0, 0).unwrap();
        c.register_pending(1, 1).unwrap();
        c.register_pending(2, 2).unwrap();
        // Commit 1 and 2 — but NOT 0.
        c.mark_committed(1, 1).unwrap();
        c.mark_committed(2, 2).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), None, "INV-FRONTIER-NEVER-OVER-HOLE");
        assert_eq!(c.pending_count(), 3);
    }

    #[test]
    fn frontier_advances_after_hole_fills() {
        let mut c = coord(None);
        c.register_pending(0, 0).unwrap();
        c.register_pending(1, 1).unwrap();
        c.register_pending(2, 2).unwrap();
        c.mark_committed(1, 1).unwrap();
        c.mark_committed(2, 2).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), None);
        // Now fill the hole.
        c.mark_committed(0, 0).unwrap();
        c.advance_frontier();
        assert_eq!(c.frontier(), Some(2));
        assert_eq!(c.pending_count(), 0);
    }

    // --- Multi-source isolation ---

    #[test]
    fn multi_source_isolation_two_coordinators() {
        let mut a = AckCoordinator::new(SourceId::from("a"), None);
        let mut b = AckCoordinator::new(SourceId::from("b"), None);
        a.register_pending(0, 0).unwrap();
        b.register_pending(100, 100).unwrap();
        a.mark_committed(0, 0).unwrap();
        a.advance_frontier();
        // A's commit must not move B's frontier.
        assert_eq!(a.frontier(), Some(0));
        assert_eq!(b.frontier(), None);
        b.mark_committed(100, 100).unwrap();
        b.advance_frontier();
        assert_eq!(b.frontier(), Some(100));
        assert_eq!(a.frontier(), Some(0));
    }

    // --- AckCoordinators registry ---

    #[test]
    fn registry_rejects_duplicate_source() {
        let mut reg = AckCoordinators::new();
        reg.register_source(SourceId::from("a"), None).unwrap();
        let err = reg.register_source(SourceId::from("a"), None).unwrap_err();
        assert!(matches!(err, RuntimeError::Ack(_)), "got {err:?}");
    }

    #[test]
    fn registry_seeds_each_coordinator_with_its_own_baseline() {
        let mut reg = AckCoordinators::new();
        reg.register_source(SourceId::from("a"), Some(10)).unwrap();
        reg.register_source(SourceId::from("b"), None).unwrap();
        assert_eq!(
            reg.get(&SourceId::from("a"))
                .unwrap()
                .initial_frontier_baseline(),
            Some(10),
        );
        assert_eq!(
            reg.get(&SourceId::from("b"))
                .unwrap()
                .initial_frontier_baseline(),
            None,
        );
    }

    #[test]
    fn registry_pending_total_sums_across_coordinators() {
        let mut reg = AckCoordinators::new();
        reg.register_source(SourceId::from("a"), None).unwrap();
        reg.register_source(SourceId::from("b"), None).unwrap();
        reg.get_mut(&SourceId::from("a"))
            .unwrap()
            .register_pending(0, 0)
            .unwrap();
        reg.get_mut(&SourceId::from("b"))
            .unwrap()
            .register_pending(0, 1)
            .unwrap();
        reg.get_mut(&SourceId::from("b"))
            .unwrap()
            .register_pending(2, 2)
            .unwrap();
        assert_eq!(reg.pending_total(), 3);
    }
}
