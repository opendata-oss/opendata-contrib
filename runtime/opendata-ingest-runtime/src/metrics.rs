//! Runtime metric surface — typed `prometheus-client` `Family<_, _>`
//! for every metric the runtime emits, plus the per-metric label
//! structs.
//!
//! Pre-Stage-1 this module exposed string constants consumed by
//! `metrics::counter!(...)`-style macros (RFC 0002 rev 6
//! §Backpressure Model > required metrics). The Stage-1 migration
//! (2026-05-19) replaced every emission site with typed
//! `Family<L, Counter|Gauge|Histogram>` access via
//! [`RuntimeMetrics`]; the bin constructs `Arc<RuntimeMetrics>`,
//! registers it against the shared `Registry`, and threads it
//! through `RuntimeBuilder::with_runtime_metrics`. The legacy
//! string constants were deleted in C4 of that migration.

use std::sync::Arc;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

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

// =========================================================================
// Stage 1 — typed `prometheus-client` Family<_, _> emission surface.
//
// One Labels struct per distinct labelset used by the runtime, plus
// [`RuntimeMetrics`] which owns one Family per metric. The bin
// constructs `Arc<RuntimeMetrics>`, registers it against the global
// `Registry`, and passes it through `RuntimeBuilder::with_runtime_metrics`
// into the workers. Each emission site replaces
// `metrics::counter!(NAME, "k" => v).increment(N)` with
// `metrics.<field>.get_or_create(&Labels { ... }).inc_by(N)`.
// =========================================================================

/// Histogram bucket boundaries for `*_seconds` metrics (1 ms → 30 s).
/// Mirrors the legacy `SECONDS_HISTOGRAM_BUCKETS` from the
/// `metrics_recorder` module so cell-bench's
/// `histogram_quantile(rate(*_bucket[1m]))` queries see the same `le`
/// labels post-migration.
pub const SECONDS_HISTOGRAM_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

fn seconds_histogram() -> Histogram {
    Histogram::new(SECONDS_HISTOGRAM_BUCKETS.iter().copied())
}

/// `{source}` — used by ack-lag histogram + all `_total` runtime
/// counters that key off the source name.
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct SourceLabels {
    pub source: String,
}

/// `{source, reason}` — only `BACKPRESSURE_REASON` uses this set.
/// `reason` values come from [`BackpressureReason::as_label`].
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct SourceReasonLabels {
    pub source: String,
    pub reason: String,
}

/// `{stage, source}` — shared by every per-stage metric
/// (`STAGE_QUEUE_DEPTH`, `STAGE_INFLIGHT_BYTES`,
/// `STAGE_LATENCY_SECONDS`). `stage` values are
/// `"source" | "fetch" | "decode" | "sink_dispatch"`.
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct StageLabels {
    pub stage: String,
    pub source: String,
}

/// `{sink}` — `SINK_QUEUE_DEPTH` + `SINK_INFLIGHT_BYTES`.
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct SinkLabels {
    pub sink: String,
}

/// `{source, sink, result}` — only `SINK_COMMITS_TOTAL` uses this set.
/// `result` values come from [`SinkCommitOutcome::as_label`].
#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct SourceSinkResultLabels {
    pub source: String,
    pub sink: String,
    pub result: String,
}

/// Typed runtime metric surface. Each field corresponds 1:1 to one of
/// the string constants above; the C2 migration commit deletes those
/// constants and routes every emission through this struct.
pub struct RuntimeMetrics {
    pub backpressure_reason: Family<SourceReasonLabels, Counter>,
    pub descriptors_handed_out: Family<SourceLabels, Counter>,
    pub bytes_fetched: Family<SourceLabels, Counter>,
    pub records_decoded: Family<SourceLabels, Counter>,
    pub sink_commits: Family<SourceSinkResultLabels, Counter>,

    pub ack_frontier: Family<SourceLabels, Gauge>,
    pub pending_ranges: Family<SourceLabels, Gauge>,
    pub buffer_consumer_seq_lag: Family<SourceLabels, Gauge>,
    pub stage_queue_depth: Family<StageLabels, Gauge>,
    pub stage_inflight_bytes: Family<StageLabels, Gauge>,
    pub sink_queue_depth: Family<SinkLabels, Gauge>,
    pub sink_inflight_bytes: Family<SinkLabels, Gauge>,

    pub ack_lag_seconds: Family<SourceLabels, Histogram>,
    pub stage_latency_seconds: Family<StageLabels, Histogram>,
}

impl RuntimeMetrics {
    /// Construct with every Family empty. Histograms carry
    /// [`SECONDS_HISTOGRAM_BUCKETS`] as their bucket set.
    pub fn new() -> Self {
        Self {
            backpressure_reason: Family::<SourceReasonLabels, Counter>::default(),
            descriptors_handed_out: Family::<SourceLabels, Counter>::default(),
            bytes_fetched: Family::<SourceLabels, Counter>::default(),
            records_decoded: Family::<SourceLabels, Counter>::default(),
            sink_commits: Family::<SourceSinkResultLabels, Counter>::default(),

            ack_frontier: Family::<SourceLabels, Gauge>::default(),
            pending_ranges: Family::<SourceLabels, Gauge>::default(),
            buffer_consumer_seq_lag: Family::<SourceLabels, Gauge>::default(),
            stage_queue_depth: Family::<StageLabels, Gauge>::default(),
            stage_inflight_bytes: Family::<StageLabels, Gauge>::default(),
            sink_queue_depth: Family::<SinkLabels, Gauge>::default(),
            sink_inflight_bytes: Family::<SinkLabels, Gauge>::default(),

            ack_lag_seconds: Family::<SourceLabels, Histogram>::new_with_constructor(
                seconds_histogram,
            ),
            stage_latency_seconds: Family::<StageLabels, Histogram>::new_with_constructor(
                seconds_histogram,
            ),
        }
    }

    /// Register every Family on `registry` under the same metric names
    /// the legacy metrics-rs emission used. `Counter`s register without
    /// the `_total` suffix — `prometheus-client` appends it during
    /// encode — so we strip it from the registered name to keep the
    /// rendered output stable.
    pub fn register(self: &Arc<Self>, registry: &mut Registry) {
        registry.register(
            "runtime_backpressure_reason",
            "Backpressure events emitted by the runtime, labeled by source and reason.",
            self.backpressure_reason.clone(),
        );
        registry.register(
            "runtime_descriptors_handed_out",
            "Source descriptors admitted into the runtime pipeline.",
            self.descriptors_handed_out.clone(),
        );
        registry.register(
            "ingestor_bytes_fetched",
            "Bytes pulled from the buffer source per fetched batch.",
            self.bytes_fetched.clone(),
        );
        registry.register(
            "ingestor_records_decoded",
            "Records produced by the decoder stage.",
            self.records_decoded.clone(),
        );
        registry.register(
            "runtime_sink_commits",
            "Sink commit attempts labeled by source, sink, and result.",
            self.sink_commits.clone(),
        );

        registry.register(
            "runtime_ack_frontier",
            "Highest ack-through sequence the runtime has surfaced for a source.",
            self.ack_frontier.clone(),
        );
        registry.register(
            "runtime_pending_ranges",
            "Number of pending commit ranges tracked by the coordinator.",
            self.pending_ranges.clone(),
        );
        registry.register(
            "buffer_consumer_sequence_lag",
            "head_sequence − last_acked_sequence as observed at last manifest read/write.",
            self.buffer_consumer_seq_lag.clone(),
        );
        registry.register(
            "runtime_stage_queue_depth",
            "Pending items in each per-stage channel, labeled by stage and source.",
            self.stage_queue_depth.clone(),
        );
        registry.register(
            "runtime_stage_inflight_bytes",
            "In-flight bytes per stage, labeled by stage and source.",
            self.stage_inflight_bytes.clone(),
        );
        registry.register(
            "runtime_sink_queue_depth",
            "Pending sink commits in the dispatch queue, labeled by sink.",
            self.sink_queue_depth.clone(),
        );
        registry.register(
            "runtime_sink_inflight_bytes",
            "In-flight bytes pinned by concurrent sink commits, labeled by sink.",
            self.sink_inflight_bytes.clone(),
        );

        registry.register(
            "runtime_ack_lag_seconds",
            "Time spent waiting on ack-through coordination.",
            self.ack_lag_seconds.clone(),
        );
        registry.register(
            "runtime_stage_latency_seconds",
            "Per-batch latency at each stage, labeled by stage and source.",
            self.stage_latency_seconds.clone(),
        );
    }
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus_client::encoding::text::encode;

    #[test]
    fn register_and_encode_round_trip() {
        let metrics = Arc::new(RuntimeMetrics::new());
        let mut registry = Registry::default();
        metrics.register(&mut registry);

        metrics
            .descriptors_handed_out
            .get_or_create(&SourceLabels {
                source: "buffer".into(),
            })
            .inc_by(7);
        metrics
            .sink_commits
            .get_or_create(&SourceSinkResultLabels {
                source: "buffer".into(),
                sink: "clickhouse_logs".into(),
                result: SinkCommitOutcome::Committed.as_label().into(),
            })
            .inc();
        metrics
            .stage_inflight_bytes
            .get_or_create(&StageLabels {
                stage: "fetch".into(),
                source: "buffer".into(),
            })
            .set(4096);
        metrics
            .stage_latency_seconds
            .get_or_create(&StageLabels {
                stage: "fetch".into(),
                source: "buffer".into(),
            })
            .observe(0.042);

        let mut buf = String::new();
        encode(&mut buf, &registry).expect("encode");

        assert!(
            buf.contains("runtime_descriptors_handed_out_total{source=\"buffer\"} 7"),
            "missing descriptors counter line. Output was:\n{buf}",
        );
        assert!(
            buf.contains(
                "runtime_sink_commits_total{source=\"buffer\",sink=\"clickhouse_logs\",result=\"committed\"} 1"
            ),
            "missing sink-commits counter line. Output was:\n{buf}",
        );
        assert!(
            buf.contains("runtime_stage_inflight_bytes{stage=\"fetch\",source=\"buffer\"} 4096"),
            "missing inflight-bytes gauge line. Output was:\n{buf}",
        );
        assert!(
            buf.contains(
                "runtime_stage_latency_seconds_bucket{le=\"0.05\",stage=\"fetch\",source=\"buffer\"} 1"
            ),
            "missing stage-latency histogram +0.05s bucket count. Output was:\n{buf}",
        );
        assert!(
            buf.contains(
                "runtime_stage_latency_seconds_sum{stage=\"fetch\",source=\"buffer\"} 0.042"
            ),
            "missing stage-latency histogram sum. Output was:\n{buf}",
        );
    }
}
