//! Stage-1 metrics-migration plumbing: the bin owns one
//! `prometheus_client::registry::Registry` and one
//! `metrics_exporter_prometheus::PrometheusHandle`, registers the
//! runtime + plugin metric structs against the Registry, and serves
//! both backends from `/metrics`.
//!
//! See `plans/odb-high-throughput/stage1-metrics-migration-plan.md`
//! §"Dual-backend render path". The Registry owns the typed
//! `RuntimeMetrics` + `ClickHouseMetrics` families; the
//! `PrometheusHandle` continues to render the buffer crate's
//! `metrics-rs` emissions (which counted correctly during the Stage 0
//! diagnostic and aren't part of the migration scope).
//!
//! After C2 + C3 land, every runtime + plugin emission routes through
//! `Arc<RuntimeMetrics>` / `Arc<ClickHouseMetrics>` and only the
//! buffer crate's counters flow through the `metrics-rs` path.

use std::sync::Arc;

use anyhow::Result;
use metrics_exporter_prometheus::PrometheusHandle;
use opendata_ingest_clickhouse::metrics::ClickHouseMetrics;
use opendata_ingest_runtime::metrics::RuntimeMetrics;
use prometheus_client::registry::Registry;

/// Owns the prometheus-client Registry plus the per-component metric
/// structs and the legacy `metrics-rs` PrometheusHandle. Constructed
/// once in `main` after `set_global_recorder`; passed by value to
/// `metrics_server::serve` and shared with the runtime + sink
/// constructors via the Arc fields.
pub struct MetricsRegistry {
    pub registry: Registry,
    pub runtime: Arc<RuntimeMetrics>,
    pub clickhouse: Arc<ClickHouseMetrics>,
    pub prom_handle: PrometheusHandle,
}

impl MetricsRegistry {
    /// Build a fresh Registry, register both metric structs, and pair
    /// them with the supplied legacy `PrometheusHandle` (which is what
    /// the buffer-crate's `metrics-rs` emission renders through).
    pub fn new(prom_handle: PrometheusHandle) -> Result<Self> {
        let mut registry = Registry::default();
        let runtime = Arc::new(RuntimeMetrics::new());
        let clickhouse = Arc::new(ClickHouseMetrics::new());
        runtime.register(&mut registry);
        clickhouse.register(&mut registry);
        Ok(Self {
            registry,
            runtime,
            clickhouse,
            prom_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use prometheus_client::encoding::text::encode;

    #[test]
    fn registry_renders_runtime_and_clickhouse_metric_names() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let r = MetricsRegistry::new(handle).expect("build registry");

        // Touch one metric from each backend so encode emits a series
        // (TYPE lines render even on empty Families, but a real series
        // lets us assert the round trip is wired correctly).
        r.runtime
            .descriptors_handed_out
            .get_or_create(&opendata_ingest_runtime::metrics::SourceLabels {
                source: "buffer".into(),
            })
            .inc();
        r.clickhouse.commit_bytes.inc_by(1024);

        let mut buf = String::new();
        encode(&mut buf, &r.registry).expect("encode");
        assert!(
            buf.contains("runtime_descriptors_handed_out_total{source=\"buffer\"} 1"),
            "expected runtime metric in registry output:\n{buf}",
        );
        assert!(
            buf.contains("ingestor_commit_bytes_total 1024"),
            "expected clickhouse metric in registry output:\n{buf}",
        );
    }
}
