//! Binary entry point.
//!
//! Loads YAML config (with `INGESTOR__` env overrides), constructs the
//! object store, builds a `buffer::Consumer`, wires the OTLP logs
//! decoder + ClickHouse sink (or a no-op `DryRunSink`) into a generic
//! [`Runtime`] via [`Runtime::builder`], and runs until
//! `SIGINT`/`SIGTERM`.
//!
//! Also serves `/metrics` (Prometheus text) and `/-/healthy` on a
//! dedicated HTTP port so the OTel collector can scrape runtime metrics.
//!
//! Phase 4.4d rewires construction from the legacy
//! `BufferConsumerRuntime` (which still lives in
//! `clickhouse-ingestor::runtime`, used by `tests/in_memory_runtime.rs`)
//! onto the new generic runtime. Phase 4.4e retires the legacy types.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::Parser;
use clickhouse_ingestor::metrics_recorder;
use clickhouse_ingestor::metrics_server;
use clickhouse_ingestor::{ClickHouseWriter, IngestorConfig, OtlpLogsClickHouseAdapter};
use opendata_ingest_clickhouse::ClickHouseSink;
use opendata_ingest_otel::logs::OtlpLogsDecoder;
use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::error::{RuntimeError, RuntimeResult};
use opendata_ingest_runtime::identity::CommitIdentity;
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, Runtime, RuntimeOptions, SinkPoolOptions, SourceBackpressureOptions,
};
use opendata_ingest_runtime::sink::{
    CommitStatus, Sink, SinkBudget, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId,
};
use opendata_ingest_runtime::source::BufferSource;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "clickhouse-ingestor",
    about = "OpenData Buffer to ClickHouse ingestor"
)]
struct Cli {
    /// Path to the ingestor YAML config file.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
}

/// A `Sink` placeholder used in dry-run mode. The `Runtime`'s
/// `dry_run` flag short-circuits the write path before `write` is
/// called, so this only needs to satisfy the trait. Carrying a real
/// `SinkId` keeps idempotency-key logs and metrics stable across
/// dry-run vs live runs.
struct DryRunSink {
    id: SinkId,
}

#[async_trait]
impl Sink for DryRunSink {
    fn id(&self) -> &SinkId {
        &self.id
    }
    fn write_budget(&self) -> SinkBudget {
        SinkBudget::default()
    }
    async fn write(&self, _commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        Err(SinkCommitFailure::Fatal(
            "DryRunSink::write called; runtime should have short-circuited via dry_run=true"
                .to_string()
                .into(),
        ))
    }
    async fn check_committed(&self, _identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
        Ok::<CommitStatus, RuntimeError>(CommitStatus::Unknown)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let cfg = IngestorConfig::load(&cli.config)
        .with_context(|| format!("loading ingestor config from {}", cli.config.display()))?;

    // Dump the full effective IngestorConfig (post-figment YAML+env
    // merge) so Phase 8 row 8.4 tuning runs can grep one log line
    // for exactly what knobs took effect. A partial-field log
    // previously hid silently-dropped knobs (serialization_format,
    // http_client_mode were declared in YAML but ..Default::default()'d
    // by writer_config until row 8.4).
    info!(config = ?cfg, "configuration loaded");

    // Install the metrics-rs recorder before any code that records or
    // describes metrics runs. The recorder is built in
    // `metrics_recorder::build_recorder` so the histogram-bucket
    // configuration has unit-test coverage that fails on regressions
    // (see `metrics_recorder::tests`).
    let recorder = metrics_recorder::build_recorder()?;
    let metrics_handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|e| anyhow::anyhow!("install global metrics recorder: {e}"))?;

    let object_store = common::create_object_store(&cfg.buffer.object_store)
        .context("constructing object store")?;
    let consumer_config = buffer::ConsumerConfig {
        object_store: cfg.buffer.object_store.clone(),
        manifest_path: cfg.buffer.manifest_path.clone(),
        data_path_prefix: cfg.buffer.data_prefix.clone(),
        gc_interval: std::time::Duration::from_secs(5 * 60),
        gc_grace_period: std::time::Duration::from_secs(10 * 60),
    };
    let consumer =
        buffer::Consumer::with_object_store(consumer_config, Arc::clone(&object_store), None)
            .await
            .context("initializing buffer consumer")?;

    let source = BufferSource::new(consumer, "buffer", cfg.buffer.manifest_path.clone(), None);
    let decoder = OtlpLogsDecoder::new();

    // ClickHouseSink in live mode; DryRunSink (never invoked) in
    // dry-run mode. The runtime's `dry_run` flag is the actual
    // gate — see Runtime::handle_source_batch.
    let runtime_options = RuntimeOptions {
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        ack_flush_policy: cfg.ack_flush_policy(),
        dry_run: cfg.runtime.dry_run,
        poll_interval: std::time::Duration::from_millis(cfg.runtime.poll_interval_ms),
        max_descriptors_per_poll: 1,
        max_retry_attempts: cfg.runtime.retry_max_attempts,
        retry_backoff: std::time::Duration::from_millis(cfg.runtime.retry_initial_backoff_ms),
        // Phase 6 per-source backpressure knobs threaded from
        // `IngestorConfig.runtime.*`. Defaults reproduce the
        // library's pipelined profile (64/8/4 in-flight/fetch/decode);
        // operators override via `INGESTOR__RUNTIME__*` env or YAML.
        source_defaults: SourceBackpressureOptions {
            max_inflight_batches: cfg.runtime.max_inflight_batches,
            max_inflight_bytes: cfg.runtime.max_inflight_bytes,
            estimated_max_batch_bytes: cfg.runtime.estimated_max_batch_bytes,
            fetch_concurrency: cfg.runtime.fetch_concurrency,
            decode_concurrency: cfg.runtime.decode_concurrency,
            oversize_fault_multiplier: cfg.runtime.oversize_fault_multiplier,
        },
        source_overrides: Default::default(),
        sink: SinkPoolOptions {
            max_concurrent_commits: cfg.sink.max_concurrent_commits,
            // The sink-namespaced retry knobs shadow the legacy
            // top-level fields per `RuntimeOptions::effective_retry_*`;
            // mirror the operator's `retry_*` settings here so the
            // resolved attempt count + backoff match what the YAML
            // describes.
            retry_max_attempts: cfg.runtime.retry_max_attempts,
            retry_initial_backoff_ms: cfg.runtime.retry_initial_backoff_ms,
        },
    };
    // Compile-time sanity check that the policy translation didn't
    // drift; not strictly necessary at runtime.
    let _: AckFlushPolicy = runtime_options.ack_flush_policy;

    let sink_id = "clickhouse_logs";
    let mut builder = Runtime::builder()
        .add_source(source)
        .add_decoder(decoder)
        .with_options(runtime_options);
    builder = if cfg.runtime.dry_run {
        builder.set_sink(DryRunSink {
            id: SinkId::from(sink_id),
        })
    } else {
        let adapter = Arc::new(OtlpLogsClickHouseAdapter::new(cfg.logs_adapter_config()));
        let writer = Arc::new(ClickHouseWriter::new(cfg.writer_config()));
        builder.set_sink(ClickHouseSink::new(sink_id, adapter, writer))
    };
    let runtime = builder
        .build()
        .map_err(|e| anyhow::anyhow!("building runtime: {e}"))?;

    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("received shutdown signal");
        signal_shutdown.cancel();
    });

    let metrics_addr: SocketAddr = cfg.metrics_server.bind_addr.parse().with_context(|| {
        format!(
            "parsing metrics_server.bind_addr={}",
            cfg.metrics_server.bind_addr
        )
    })?;
    let metrics_shutdown = shutdown.clone();
    let metrics_task = tokio::spawn(async move {
        if let Err(e) = metrics_server::serve(metrics_handle, metrics_addr, metrics_shutdown).await
        {
            error!(error = %e, "metrics server exited with error");
        }
    });

    let runtime_result = runtime.run(shutdown.clone()).await;
    shutdown.cancel();
    if let Err(e) = metrics_task.await {
        error!(error = %e, "metrics server task join failed");
    }
    runtime_result.map_err(|e| anyhow::anyhow!("runtime: {e}"))?;
    Ok(())
}
