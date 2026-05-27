//! HTTP server that exposes `/metrics` (Prometheus text format) and
//! `/-/healthy` for the ingestor binary.
//!
//! Dual-backend rendering: the response concatenates the
//! `prometheus-client` Registry encode (the typed runtime +
//! clickhouse metric structs) and the `metrics-rs`
//! `PrometheusHandle::render()` output (load-bearing for the buffer
//! crate's emissions). Both halves are valid Prometheus text format;
//! the scrape consumer sees one unified document.
//!
//! Lives in the lib (rather than inline in `bin/`) so tests can drive
//! the same routes the binary serves.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus_client::encoding::text::encode;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::metrics_registry::MetricsRegistry;

/// Shared state for the `/metrics` route. Wraps the registry in a
/// `Mutex` because `encode` takes `&Registry`, but axum's `State`
/// requires `Clone`; an `Arc<Mutex<_>>` makes that ergonomic without
/// sharing the registry across threads at write time (`encode` is
/// read-only — `Mutex` just allows interior mutability for the Arc
/// share, never contended in practice).
#[derive(Clone)]
struct MetricsState {
    inner: Arc<Mutex<MetricsRegistry>>,
}

/// Build the axum router that serves `/metrics` and `/-/healthy`.
///
/// Factored out so tests can drive the same routes the binary serves.
pub fn router(registry: MetricsRegistry) -> Router {
    let state = MetricsState {
        inner: Arc::new(Mutex::new(registry)),
    };
    Router::new()
        .route("/metrics", get(render_metrics))
        .route("/-/healthy", get(|| async { (StatusCode::OK, "OK") }))
        .with_state(state)
}

async fn render_metrics(State(state): State<MetricsState>) -> impl IntoResponse {
    let guard = match state.inner.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut body = String::new();
    if let Err(e) = encode(&mut body, &guard.registry) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode error: {e}"),
        )
            .into_response();
    }
    body.push_str(&guard.prom_handle.render());
    body.into_response()
}

/// Bind on `addr` and serve until the shutdown token is cancelled.
pub async fn serve(
    registry: MetricsRegistry,
    addr: SocketAddr,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding metrics server to {addr}"))?;
    info!(%addr, "metrics server listening");
    axum::serve(listener, router(registry))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .context("metrics server error")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use axum::http::StatusCode;
    use metrics::counter;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use opendata_ingest_clickhouse::metrics::ClickHouseMetrics;
    use opendata_ingest_runtime::metrics::{RuntimeMetrics, SourceLabels};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::*;

    /// Both backends render. The handler concatenates the typed
    /// Registry encode (runtime + clickhouse counters) with the
    /// `metrics-rs` `PrometheusHandle::render()` output (which the
    /// buffer crate still emits through). One scrape, both halves
    /// visible.
    #[tokio::test]
    async fn serves_dual_backend_metrics_and_health_over_http() {
        // metrics-rs half — the buffer crate's emission path.
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            counter!("ingestor_test_buffer_side_total").increment(11);
        });

        // prometheus-client half — typed RuntimeMetrics + ClickHouseMetrics.
        let registry = MetricsRegistry::new(handle).expect("build registry");
        registry
            .runtime
            .descriptors_handed_out
            .get_or_create(&SourceLabels {
                source: "buffer".into(),
            })
            .inc_by(3);
        registry.clickhouse.commit_bytes.inc_by(2048);

        // Bind ephemerally so test runs don't conflict.
        let probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let server = tokio::spawn(async move { serve(registry, addr, serve_shutdown).await });

        let client = reqwest::Client::new();
        let body = timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(resp) = client.get(format!("http://{addr}/metrics")).send().await
                    && resp.status().is_success()
                {
                    return resp.text().await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("metrics endpoint reachable within 2s");

        assert!(
            body.contains("ingestor_test_buffer_side_total 11"),
            "metrics-rs half missing from response body:\n{body}",
        );
        assert!(
            body.contains("runtime_descriptors_handed_out_total{source=\"buffer\"} 3"),
            "prometheus-client runtime metric missing from response body:\n{body}",
        );
        assert!(
            body.contains("ingestor_commit_bytes_total 2048"),
            "prometheus-client clickhouse metric missing from response body:\n{body}",
        );

        let health = client
            .get(format!("http://{addr}/-/healthy"))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(health.text().await.unwrap(), "OK");

        shutdown.cancel();
        let _ = timeout(Duration::from_secs(2), server).await;
    }

    /// Sanity: a fresh-registry round trip via the handler picks up
    /// new emissions made after construction.
    #[tokio::test]
    async fn handler_sees_post_construction_writes() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let registry = MetricsRegistry::new(handle).expect("build registry");
        let rt: Arc<RuntimeMetrics> = Arc::clone(&registry.runtime);
        let _ch: Arc<ClickHouseMetrics> = Arc::clone(&registry.clickhouse);

        let probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let server = tokio::spawn(async move { serve(registry, addr, serve_shutdown).await });

        // Increment AFTER the registry was moved into the server.
        rt.descriptors_handed_out
            .get_or_create(&SourceLabels {
                source: "buffer".into(),
            })
            .inc_by(7);

        let client = reqwest::Client::new();
        let body = timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(resp) = client.get(format!("http://{addr}/metrics")).send().await
                    && resp.status().is_success()
                {
                    return resp.text().await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("metrics endpoint reachable within 2s");

        assert!(
            body.contains("runtime_descriptors_handed_out_total{source=\"buffer\"} 7"),
            "post-construction increment not reflected in response body:\n{body}",
        );

        shutdown.cancel();
        let _ = timeout(Duration::from_secs(2), server).await;
    }
}
