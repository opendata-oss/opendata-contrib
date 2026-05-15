//! Prometheus recorder construction — extracted so the bucket config
//! has unit-test coverage that fails when someone reverts the
//! histogram-shape fix.
//!
//! metrics-exporter-prometheus's default rendering for
//! `metrics::histogram!` is a Prometheus *summary* (base series +
//! `_count` + `_sum` + per-quantile labels). The Phase 8 cell-bench
//! classifier needs real histograms because it runs
//! `histogram_quantile(sum by (le)(rate(*_bucket[1m])))` to aggregate
//! across cells — summaries can't be aggregated. We configure
//! explicit buckets for every `*_seconds` metric so the recorder
//! renders them as histograms with `_bucket{le=...}` series.

use anyhow::Result;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusRecorder};

/// Bucket boundaries for `*_seconds` histograms (in seconds). Spans
/// 1 ms → 30 s. ClickHouse INSERT p99 on a tuned cell sits around
/// 30–200 ms; a stuck-ingestor p99 can reach seconds. The 30 s top
/// bucket is wider than the runtime's `request_timeout_secs` default
/// (30 s), so a runaway INSERT lands in `+Inf` instead of
/// masquerading as a 30 s sample.
pub const SECONDS_HISTOGRAM_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Build the ingestor's Prometheus recorder with histogram-bucket
/// boundaries configured on every `*_seconds` metric. Called from the
/// binary entrypoint before `metrics::set_global_recorder`.
pub fn build_recorder() -> Result<PrometheusRecorder> {
    Ok(PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Suffix("_seconds".to_string()),
            SECONDS_HISTOGRAM_BUCKETS,
        )
        .map_err(|e| anyhow::anyhow!("configure histogram buckets: {e}"))?
        .build_recorder())
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::{histogram, with_local_recorder};

    /// Regression test for the row-8.4-prep HIGH finding: if the
    /// `set_buckets_for_metric` call is ever removed, the recorder
    /// would silently revert to summary rendering. cell-bench's
    /// `histogram_quantile(*_bucket)` queries would then return
    /// nothing (or NaN) at runtime — too late to catch. This test
    /// records a `_seconds` histogram value, renders the recorder,
    /// and asserts the output contains `_bucket{le=…}` series. A
    /// summary-shape regression breaks the assertion immediately.
    #[test]
    fn seconds_histograms_render_with_buckets() {
        let recorder = build_recorder().expect("build recorder");
        let handle = recorder.handle();
        with_local_recorder(&recorder, || {
            // One ~50 ms sample lands in the 0.05 bucket.
            histogram!("ingestor_test_seconds").record(0.0420f64);
        });
        let rendered = handle.render();
        assert!(
            rendered.contains("ingestor_test_seconds_bucket{le=\"0.05\""),
            "expected real histogram `_bucket` series; recorder may have \
             reverted to summary rendering. Output was:\n{rendered}"
        );
        // The +Inf bucket is the histogram-shape's load-bearing
        // signature — summaries don't have it.
        assert!(
            rendered.contains("ingestor_test_seconds_bucket{le=\"+Inf\""),
            "expected `_bucket{{le=\"+Inf\"}}` (histogram shape); got\n{rendered}"
        );
        // Summary-shape series have `quantile="…"` labels on the base
        // metric name; histograms don't. Confirm the recorder ISN'T
        // emitting summary quantiles for the `_seconds` family.
        assert!(
            !rendered.contains("ingestor_test_seconds{quantile=\""),
            "expected no `{{quantile=...}}` series for `_seconds` family \
             (that's the summary shape we're trying to avoid); got\n{rendered}"
        );
    }

    /// Counter metrics aren't affected by the `_seconds` matcher.
    /// This guards against a refactor accidentally widening the
    /// matcher to all metrics.
    #[test]
    fn counters_unaffected_by_seconds_bucket_config() {
        let recorder = build_recorder().expect("build recorder");
        let handle = recorder.handle();
        with_local_recorder(&recorder, || {
            metrics::counter!("ingestor_test_total").increment(7);
        });
        let rendered = handle.render();
        assert!(
            rendered.contains("ingestor_test_total 7"),
            "expected plain counter rendering; got\n{rendered}"
        );
        assert!(
            !rendered.contains("ingestor_test_total_bucket"),
            "counter should not render with buckets; got\n{rendered}"
        );
    }
}
