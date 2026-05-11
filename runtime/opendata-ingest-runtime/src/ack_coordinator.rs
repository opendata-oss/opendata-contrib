//! Per-source ack coordinator skeleton.
//!
//! Phase 4.4c-0 aligns this to the RFC 0002 rev 6 single-sink
//! surface: `register_pending(range)` / `mark_committed(range)` /
//! `frontier()`. The Phase 4 implementation tracks a single
//! contiguous frontier (matching the current ClickHouse logs flow,
//! which writes ranges in order). Phase 5 expands the coordinator
//! into the full pending-ranges + out-of-order completion +
//! multi-source isolation state machine RFC 0002 rev 6 §Per-Source
//! Ack Coordinator describes.

use crate::error::{RuntimeError, RuntimeResult};
use crate::source::SourceId;

pub struct AckCoordinator {
    source: SourceId,
    /// Highest sequence whose configured sink has committed. `None`
    /// until the first commit lands. Phase 5 replaces this with the
    /// per-range pending map RFC 0002 rev 6 describes.
    acked_frontier: Option<u64>,
}

impl AckCoordinator {
    pub fn new(source: SourceId) -> Self {
        Self {
            source,
            acked_frontier: None,
        }
    }

    pub fn source(&self) -> &SourceId {
        &self.source
    }

    pub fn frontier(&self) -> Option<u64> {
        self.acked_frontier
    }

    /// Register a sequence range that has entered the pipeline.
    /// Phase 4 treats register/commit as a single ordered pair — the
    /// pipeline is serial, so ranges complete in the order they
    /// register. Phase 5 replaces this with a pending-range map that
    /// tolerates out-of-order completion. v1 callers can call this
    /// for documentation purposes; the Phase 4 frontier advance
    /// happens entirely inside `mark_committed`.
    pub fn register_pending(
        &mut self,
        _low_sequence: u64,
        _high_sequence: u64,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    /// Mark the configured sink as having committed the given
    /// sequence range. Phase 4 requires calls to arrive in monotonic
    /// `high_sequence` order (the serial pipeline guarantees this);
    /// Phase 5 tolerates out-of-order completion and only advances
    /// the contiguous frontier.
    pub fn mark_committed(&mut self, _low_sequence: u64, high_sequence: u64) -> RuntimeResult<()> {
        if let Some(prev) = self.acked_frontier
            && high_sequence <= prev
        {
            return Err(RuntimeError::Ack(format!(
                "non-monotonic frontier advance on source {}: have {prev}, got {high_sequence}",
                self.source,
            )));
        }
        self.acked_frontier = Some(high_sequence);
        Ok(())
    }
}
