//! Runtime stage metric name skeleton (RFC 0002 rev 6
//! §Backpressure Model > required metrics).
//!
//! Phase 4.2 ships only the canonical metric names plus the label
//! enums so plugin and runtime code share a single source of truth.
//! Phase 6 wires concrete instrument constructors against the chosen
//! metrics provider.

pub const STAGE_QUEUE_DEPTH: &str = "runtime_stage_queue_depth";
pub const STAGE_INFLIGHT_BYTES: &str = "runtime_stage_inflight_bytes";
pub const STAGE_LATENCY_SECONDS: &str = "runtime_stage_latency_seconds";
pub const ACK_FRONTIER: &str = "runtime_ack_frontier";
pub const PENDING_RANGES: &str = "runtime_pending_ranges";
pub const BACKPRESSURE_REASON: &str = "runtime_backpressure_reason";
pub const SINK_COMMITS_TOTAL: &str = "runtime_sink_commits_total";
pub const SINK_QUEUE_DEPTH: &str = "runtime_sink_queue_depth";
pub const SINK_INFLIGHT_BYTES: &str = "runtime_sink_inflight_bytes";
pub const DESCRIPTORS_HANDED_OUT_TOTAL: &str = "runtime_descriptors_handed_out_total";
pub const ACK_LAG_SECONDS: &str = "runtime_ack_lag_seconds";
/// `head_sequence − last_acked_sequence`, as observed by the consumer
/// at the last manifest read/write. Surfaced as a gauge labelled
/// `source` so the Phase 8 cell-bench bottleneck classifier can detect
/// when ingestor work falls behind manifest growth.
pub const BUFFER_CONSUMER_SEQUENCE_LAG: &str = "buffer_consumer_sequence_lag";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackpressureReason {
    SourceBudget,
    DecodeBudget,
    SinkBudget,
    Retrying,
    FatalError,
}

impl BackpressureReason {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::SourceBudget => "source_budget",
            Self::DecodeBudget => "decode_budget",
            Self::SinkBudget => "sink_budget",
            Self::Retrying => "retrying",
            Self::FatalError => "fatal_error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkCommitOutcome {
    Committed,
    VerifiedAlreadyCommitted,
    FailedRetryable,
    FailedFatal,
}

impl SinkCommitOutcome {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::VerifiedAlreadyCommitted => "verified_already_committed",
            Self::FailedRetryable => "failed_retryable",
            Self::FailedFatal => "failed_fatal",
        }
    }
}
