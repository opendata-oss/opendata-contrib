//! Per-source ack coordinator skeleton.
//!
//! Phase 4.2 ships a single-route skeleton with `mark_route_committed`
//! only — fanout count of exactly 1 (one route per source, mirroring
//! the current ClickHouse logs path). Phase 5 expands this into the
//! full state machine (RFC 0002 rev 5 §Per-Source Ack Coordinator):
//! pending-range tracking, multi-route fanout, replay-on-restart via
//! `check_committed`, the documented flush policy.

use crate::error::{RuntimeError, RuntimeResult};
use crate::router::RouteId;
use crate::source::SourceId;

pub struct AckCoordinator {
    source: SourceId,
    /// Highest sequence whose required routes have all committed.
    /// `None` until the first route commit lands. Phase 5 replaces
    /// this with the per-range, per-route map RFC 0002 rev 5
    /// describes.
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

    /// Mark a route committed for the given sequence range. Phase
    /// 4.2 supports exactly one route per source; multi-route
    /// fanout (RFC 0002 rev 5 fanout invariant) lands in Phase 5.
    /// Calls must arrive in monotonic-high order.
    pub fn mark_route_committed(
        &mut self,
        _route: &RouteId,
        _low_sequence: u64,
        high_sequence: u64,
    ) -> RuntimeResult<()> {
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
