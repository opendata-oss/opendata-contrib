//! Phase 1.4 baseline: ClickHouse ingestor stage latencies.
//!
//! Drives the runtime through a pre-populated Buffer in dry-run mode
//! (no real ClickHouse). Captures end-to-end pipeline timing plus the
//! metrics the ingestor already emits (commit_group_size,
//! ack_flush_latency_seconds, etc.).
//!
//! Stages required by benchmarks.md are reported best-effort:
//!
//!   * `next_batch`               — observed via the buffer crate's
//!                                  `buffer.fetch_duration_seconds`
//!                                  histogram (same as Phase 1.3).
//!   * `decode_envelope`,
//!     `decode_signal`,
//!     `commit_group_append`,
//!     `adapter_plan`             — NOT instrumented in v1; recorded
//!                                  as a single coarse stage
//!                                  `decode_through_plan`. Phase 6
//!                                  of the impl plan adds the runtime
//!                                  stage histograms (RFC 0002) that
//!                                  will let a re-run split this.
//!   * `writer_insert`            — `n/a` in dry-run (no writer).
//!                                  Real-CH bench is gated on Docker;
//!                                  Phase 8 owns that path.
//!   * `ack_through`              — `n/a` in dry-run (acks skipped).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata as MetricsMeta, Recorder, SharedString, Unit};
use serde_json::json;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use tokio_util::sync::CancellationToken;

use buffer::{CompressionType, Producer, ProducerConfig};
use clickhouse_ingestor::{
    AckFlushPolicy, BufferConsumerRuntime, CommitGroupThresholds, ConfiguredEnvelope,
    LogsAdapterConfig, OtlpLogsClickHouseAdapter, OtlpLogsDecoder, PayloadEncoding,
    RuntimeOptions, SignalType,
};
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 100)]
    log_records_per_payload: usize,
    #[arg(long, default_value_t = 50)]
    payloads: usize,
    #[arg(long, default_value_t = 5)]
    iterations: usize,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value = "1.4")]
    unit_id: String,
    #[arg(long, default_value = "baseline")]
    change_slug: String,
    #[arg(long, default_value = "")]
    notes: String,
}

// =============================================================================
// Capturing metrics recorder.
// =============================================================================

#[derive(Default, Clone)]
struct Captured {
    histograms: Arc<Mutex<HashMap<String, Vec<f64>>>>,
    counters: Arc<Mutex<HashMap<String, u64>>>,
}

impl Captured {
    fn reset(&self) {
        self.histograms.lock().unwrap().clear();
        self.counters.lock().unwrap().clear();
    }
    fn drain_histograms(&self) -> HashMap<String, Vec<f64>> {
        std::mem::take(&mut *self.histograms.lock().unwrap())
    }
    fn drain_counters(&self) -> HashMap<String, u64> {
        std::mem::take(&mut *self.counters.lock().unwrap())
    }
}

struct CapRecorder { captured: Captured }
impl Recorder for CapRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &MetricsMeta<'_>) -> Counter {
        Counter::from_arc(Arc::new(CFn {
            name: key.name().to_string(),
            counters: Arc::clone(&self.captured.counters),
        }))
    }
    fn register_gauge(&self, _: &Key, _: &MetricsMeta<'_>) -> Gauge { Gauge::noop() }
    fn register_histogram(&self, key: &Key, _: &MetricsMeta<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(HFn {
            name: key.name().to_string(),
            histograms: Arc::clone(&self.captured.histograms),
        }))
    }
}
struct CFn { name: String, counters: Arc<Mutex<HashMap<String, u64>>> }
impl metrics::CounterFn for CFn {
    fn increment(&self, value: u64) {
        let mut g = self.counters.lock().unwrap();
        *g.entry(self.name.clone()).or_insert(0) += value;
    }
    fn absolute(&self, value: u64) {
        self.counters.lock().unwrap().insert(self.name.clone(), value);
    }
}
struct HFn { name: String, histograms: Arc<Mutex<HashMap<String, Vec<f64>>>> }
impl metrics::HistogramFn for HFn {
    fn record(&self, value: f64) {
        self.histograms.lock().unwrap()
            .entry(self.name.clone()).or_default().push(value);
    }
}

// =============================================================================
// OTLP payload synthesis.
// =============================================================================

fn make_logs(service: &str, batch_idx: usize, record_count: usize) -> Vec<u8> {
    let log_records = (0..record_count)
        .map(|i| LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000 + (batch_idx * 1_000_000 + i) as u64,
            observed_time_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue { value: Some(Value::StringValue(format!("body-{batch_idx}-{i}"))) }),
            attributes: vec![KeyValue {
                key: "i".into(),
                value: Some(AnyValue { value: Some(Value::IntValue(i as i64)) }),
            }],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        })
        .collect();
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue { value: Some(Value::StringValue(service.to_string())) }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut buf = Vec::with_capacity(req.encoded_len());
    req.encode(&mut buf).unwrap();
    buf
}

fn logs_envelope() -> Bytes {
    Bytes::from(vec![1u8, 2 /*Logs*/, 1 /*OtlpProtobuf*/, 0])
}

// =============================================================================
// Single iteration.
// =============================================================================

#[derive(Clone)]
struct IterResult {
    iteration: usize,
    started_unix_ms: u64,
    elapsed_seconds: f64,
    log_records_processed: u64,
    payload_bytes_processed: u64,
    fetch_duration_seconds: Vec<f64>,
    commit_group_size_rows: Vec<f64>,
    commit_group_size_bytes: Vec<f64>,
    counters: HashMap<String, u64>,
}

async fn run_iteration(iter: usize, captured: Captured, args: &Args) -> Result<IterResult> {
    captured.reset();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = format!("bench-1.4/iter-{iter}/manifest");
    let data_prefix = format!("bench-1.4/iter-{iter}/data");

    // Pre-populate the buffer with N OTLP-logs payloads.
    let pcfg = ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.clone(),
        manifest_path: manifest_path.clone(),
        flush_interval: Duration::from_millis(50),
        flush_size_bytes: 1, // force per-produce flush
        max_buffered_inputs: 1024,
        batch_compression: CompressionType::None,
    };
    let producer = Producer::with_object_store(pcfg, Arc::clone(&store), Arc::new(SystemClock))
        .context("Producer::with_object_store")?;
    let mut total_payload_bytes = 0u64;
    for batch_idx in 0..args.payloads {
        let payload = make_logs("svc-bench", batch_idx, args.log_records_per_payload);
        total_payload_bytes += payload.len() as u64;
        let h = producer
            .produce(vec![Bytes::from(payload)], logs_envelope())
            .await
            .context("produce")?;
        std::mem::forget(h); // we'll await durability via flush below
    }
    producer.flush().await.context("producer flush")?;
    producer.close().await.context("producer close")?;

    // Spin up the runtime in dry-run mode (writer = None).
    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.clone(),
        data_path_prefix: data_prefix.clone(),
        gc_interval: Duration::from_secs(60 * 60),
        gc_grace_period: Duration::from_secs(60 * 60),
    };
    let consumer = buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None)
        .await
        .context("Consumer::with_object_store")?;

    let options = RuntimeOptions {
        manifest_path: manifest_path.clone(),
        data_path_prefix: data_prefix.clone(),
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        commit_group: CommitGroupThresholds {
            max_rows: 100_000,
            max_bytes: 32 * 1024 * 1024,
            max_age: Duration::from_millis(50),
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: true,
        poll_interval: Duration::from_millis(5),
    };
    let runtime = BufferConsumerRuntime::new(
        consumer,
        OtlpLogsDecoder::new(),
        OtlpLogsClickHouseAdapter::new(LogsAdapterConfig::default()),
        None, // writer
        options,
    );
    let mut progress = runtime.progress();
    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();

    let started_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let timed_start = Instant::now();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    // Wait for the runtime to decode all payloads.
    let target_seq = args.payloads as u64 - 1;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(()) = progress.changed().await {
            let p = *progress.borrow();
            if let Some(s) = p.last_decoded_sequence
                && s >= target_seq
            {
                break;
            }
        } else {
            anyhow::bail!("progress channel closed unexpectedly");
        }
        if Instant::now() >= deadline {
            anyhow::bail!("runtime did not finish within 60s");
        }
    }

    // Trigger graceful shutdown and wait for the runtime to drain its
    // open commit group. This includes the final flush_group +
    // adapter_plan + ack_through pass — work that must be inside the
    // timed window for `iteration_elapsed_seconds` to reflect the
    // full pipeline cost. Stopping the timer at last_decoded_sequence
    // would exclude the drain.
    shutdown.cancel();
    let _ = handle.await;
    let elapsed = timed_start.elapsed().as_secs_f64();

    let mut h = captured.drain_histograms();
    let counters = captured.drain_counters();
    let fetch_duration_seconds = h.remove("buffer.fetch_duration_seconds").unwrap_or_default();
    let commit_group_size_rows = h.remove("ingestor_commit_group_size_rows").unwrap_or_default();
    let commit_group_size_bytes = h.remove("ingestor_commit_group_size_bytes").unwrap_or_default();

    let log_records_processed = (args.payloads as u64) * (args.log_records_per_payload as u64);

    Ok(IterResult {
        iteration: iter,
        started_unix_ms,
        elapsed_seconds: elapsed,
        log_records_processed,
        payload_bytes_processed: total_payload_bytes,
        fetch_duration_seconds,
        commit_group_size_rows,
        commit_group_size_bytes,
        counters,
    })
}

// =============================================================================
// Aggregation + schema-v2 emission.
// =============================================================================

fn agg(samples: &[f64]) -> serde_json::Value {
    if samples.is_empty() {
        return json!({"median": 0.0, "p10": 0.0, "p90": 0.0, "n": 0});
    }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |frac: f64| -> f64 {
        let idx = ((s.len() as f64 - 1.0) * frac) as usize;
        s[idx]
    };
    json!({"median": q(0.5), "p10": q(0.1), "p90": q(0.9), "n": s.len()})
}

fn med(samples: &[f64]) -> f64 {
    if samples.is_empty() { return 0.0; }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn pct(samples: &[f64], frac: f64) -> f64 {
    if samples.is_empty() { return 0.0; }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((s.len() as f64 - 1.0) * frac) as usize;
    s[idx]
}

fn ts_now_iso() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let (y, mo, d, h, mi, se) = epoch_parts(secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z")
}

fn epoch_parts(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let mut days = secs.div_euclid(86_400);
    let mut sod = secs.rem_euclid(86_400);
    let h = (sod / 3600) as u32;
    sod %= 3600;
    let mi = (sod / 60) as u32;
    let se = (sod % 60) as u32;
    let mut y: i64 = 1970;
    let leap = |y: i64| (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    loop { let dy = if leap(y) { 366 } else { 365 }; if days >= dy { days -= dy; y += 1; } else { break; } }
    let dim = |y: i64, m: u32| match m { 1|3|5|7|8|10|12 => 31, 4|6|9|11 => 30, 2 => if leap(y) {29} else {28}, _ => 0 };
    let mut m: u32 = 1;
    loop { let d = dim(y, m) as i64; if days >= d { days -= d; m += 1; } else { break; } }
    (y, m, (days + 1) as u32, h, mi, se)
}

fn run_id_slug(slug: &str) -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let (y, mo, d, h, mi, _) = epoch_parts(secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}{mi:02}-{slug}")
}

fn git_info(repo: &str) -> serde_json::Value {
    let rev = std::process::Command::new("git")
        .args(["-C", repo, "rev-parse", "HEAD"]).output().ok()
        .filter(|o| o.status.success()).and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim()[..7.min(s.trim().len())].to_string()).unwrap_or_default();
    let branch = std::process::Command::new("git")
        .args(["-C", repo, "rev-parse", "--abbrev-ref", "HEAD"]).output().ok()
        .filter(|o| o.status.success()).and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string()).unwrap_or_default();
    let dirty = std::process::Command::new("git")
        .args(["-C", repo, "status", "--porcelain"]).output().ok()
        .filter(|o| o.status.success()).map(|o| !o.stdout.is_empty()).unwrap_or(false);
    json!({"rev": rev, "branch": branch, "dirty": dirty})
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let captured = Captured::default();
    metrics::set_global_recorder(CapRecorder { captured: captured.clone() })
        .map_err(|e| anyhow::anyhow!("set_global_recorder: {e}"))?;

    let run_dir = args.output_dir.join(run_id_slug(&args.change_slug));
    let raw_dir = run_dir.join("raw");
    std::fs::create_dir_all(&raw_dir).context("mkdir -p run_dir/raw")?;

    let started_iso = ts_now_iso();
    let mut iters: Vec<IterResult> = Vec::with_capacity(args.iterations);
    for i in 1..=args.iterations {
        eprintln!("iteration {i}/{}: {} payloads × {} log records",
                  args.iterations, args.payloads, args.log_records_per_payload);
        let r = run_iteration(i, captured.clone(), &args).await?;
        let raw_path = raw_dir.join(format!("run-{i}.metrics.jsonl"));
        let mut buf = String::new();
        for (idx, secs) in r.fetch_duration_seconds.iter().enumerate() {
            let line = json!({
                "ts_unix_ms": r.started_unix_ms + idx as u64,
                "metric": "buffer.fetch_duration_seconds",
                "value": secs,
                "labels": { "iteration": i, "obs_index": idx }
            });
            buf.push_str(&serde_json::to_string(&line)?);
            buf.push('\n');
        }
        for (idx, v) in r.commit_group_size_rows.iter().enumerate() {
            let line = json!({
                "ts_unix_ms": r.started_unix_ms + idx as u64,
                "metric": "ingestor_commit_group_size_rows",
                "value": v, "labels": { "iteration": i, "obs_index": idx }
            });
            buf.push_str(&serde_json::to_string(&line)?); buf.push('\n');
        }
        for (idx, v) in r.commit_group_size_bytes.iter().enumerate() {
            let line = json!({
                "ts_unix_ms": r.started_unix_ms + idx as u64,
                "metric": "ingestor_commit_group_size_bytes",
                "value": v, "labels": { "iteration": i, "obs_index": idx }
            });
            buf.push_str(&serde_json::to_string(&line)?); buf.push('\n');
        }
        std::fs::write(&raw_path, buf)?;
        iters.push(r);
    }
    let ended_iso = ts_now_iso();

    // Aggregate.
    let throughput_records: Vec<f64> = iters.iter()
        .map(|r| r.log_records_processed as f64 / r.elapsed_seconds.max(1e-9)).collect();
    let throughput_bytes: Vec<f64> = iters.iter()
        .map(|r| r.payload_bytes_processed as f64 / r.elapsed_seconds.max(1e-9)).collect();
    let elapsed: Vec<f64> = iters.iter().map(|r| r.elapsed_seconds).collect();

    let next_batch_all: Vec<f64> = iters.iter().flat_map(|r| r.fetch_duration_seconds.clone()).collect();
    let cg_rows_all: Vec<f64> = iters.iter().flat_map(|r| r.commit_group_size_rows.clone()).collect();
    let cg_bytes_all: Vec<f64> = iters.iter().flat_map(|r| r.commit_group_size_bytes.clone()).collect();

    let scalars = json!({
        "total_throughput_records_per_sec": agg(&throughput_records),
        "total_throughput_bytes_per_sec":   agg(&throughput_bytes),
        "iteration_elapsed_seconds":        agg(&elapsed),
        "median_commit_group_rows":         agg(&cg_rows_all),
        "median_commit_group_bytes":        agg(&cg_bytes_all),
        "p95_insert_latency_ms":            json!({"median": null, "p10": null, "p90": null, "n": 0, "note": "n/a in dry-run"}),
        "time_since_last_successful_ack_seconds": json!({"median": null, "p10": null, "p90": null, "n": 0, "note": "n/a in dry-run"}),
    });

    let stages = json!([
        {
            "name": "next_batch",
            "median_ms_per_op": med(&next_batch_all) * 1000.0,
            "p10": pct(&next_batch_all, 0.1) * 1000.0,
            "p90": pct(&next_batch_all, 0.9) * 1000.0,
            "ops_per_sec": next_batch_all.len() as f64 / elapsed.iter().sum::<f64>().max(1e-9),
            "notes": "Observed via buffer.fetch_duration_seconds histogram. Same instrumentation as Phase 1.3."
        },
        {
            "name": "decode_through_plan",
            "median_ms_per_op": serde_json::Value::Null,
            "p10": serde_json::Value::Null,
            "p90": serde_json::Value::Null,
            "ops_per_sec": serde_json::Value::Null,
            "notes": "Coarse stage covering decode_envelope + decode_signal + commit_group_append + adapter_plan. Per-stage timing not instrumented in v1; Phase 6 adds the runtime stage histograms (RFC 0002) that will let a re-run split this. End-to-end work is captured in iteration_elapsed_seconds minus next_batch."
        },
        {
            "name": "writer_insert",
            "median_ms_per_op": serde_json::Value::Null,
            "p10": serde_json::Value::Null,
            "p90": serde_json::Value::Null,
            "ops_per_sec": serde_json::Value::Null,
            "notes": "n/a in dry-run; the runtime is constructed with writer=None. Phase 8 (Docker + testcontainers ClickHouse) covers the real-CH baseline."
        },
        {
            "name": "ack_through",
            "median_ms_per_op": serde_json::Value::Null,
            "p10": serde_json::Value::Null,
            "p90": serde_json::Value::Null,
            "ops_per_sec": serde_json::Value::Null,
            "notes": "n/a in dry-run; acks are skipped per AckController.dry_run."
        },
    ]);

    let per_iter: Vec<serde_json::Value> = iters.iter().enumerate().map(|(idx, r)| {
        json!({
            "iteration": r.iteration,
            "scalars": {
                "iteration_elapsed_seconds": r.elapsed_seconds,
                "iteration_throughput_records_per_sec": throughput_records[idx],
                "iteration_throughput_bytes_per_sec":   throughput_bytes[idx],
                "iteration_records_processed": r.log_records_processed as f64,
            },
            "stages": [
                { "name": "next_batch", "median_ms_per_op": med(&r.fetch_duration_seconds) * 1000.0 },
                { "name": "decode_through_plan", "median_ms_per_op": serde_json::Value::Null },
            ],
            "histograms": {
                "commit_group_size_rows": {
                    "buckets": [10.0, 100.0, 1000.0, 10000.0, 100000.0],
                    "counts": bucket_counts(&r.commit_group_size_rows, &[10.0, 100.0, 1000.0, 10000.0, 100000.0])
                },
                "commit_group_size_bytes": {
                    "buckets": [1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0],
                    "counts": bucket_counts(&r.commit_group_size_bytes, &[1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0])
                }
            },
            "raw_log":     format!("raw/run-{}.metrics.jsonl", r.iteration),
            "raw_metrics": format!("raw/run-{}.metrics.jsonl", r.iteration),
        })
    }).collect();

    let results = json!({
        "schema_version": 2,
        "scalars": scalars,
        "stages":  stages,
        "histograms": {
            "commit_group_size_rows_aggregate": {
                "buckets": [10.0, 100.0, 1000.0, 10000.0, 100000.0],
                "counts": bucket_counts(&cg_rows_all, &[10.0, 100.0, 1000.0, 10000.0, 100000.0])
            }
        },
        "iterations": per_iter,
    });
    std::fs::write(run_dir.join("results.json"), serde_json::to_string_pretty(&results)?)?;

    // timeseries.json — schema v2 required for unit 1.4.
    let series_fetch = build_aggregated_series(
        "buffer.fetch_duration_seconds",
        &iters.iter().map(|r| r.fetch_duration_seconds.clone()).collect::<Vec<_>>(),
    );
    let series_cg_rows = build_aggregated_series(
        "ingestor_commit_group_size_rows",
        &iters.iter().map(|r| r.commit_group_size_rows.clone()).collect::<Vec<_>>(),
    );
    let series_cg_bytes = build_aggregated_series(
        "ingestor_commit_group_size_bytes",
        &iters.iter().map(|r| r.commit_group_size_bytes.clone()).collect::<Vec<_>>(),
    );
    let iteration_starts: Vec<u64> = iters.iter().map(|r| r.started_unix_ms).collect();
    let timeseries = json!({
        "schema_version": 2,
        "window_seconds": 1.0,
        "iterations_aggregated": iters.len(),
        "iteration_starts_unix_ms": iteration_starts,
        "series": [series_fetch, series_cg_rows, series_cg_bytes],
    });
    std::fs::write(run_dir.join("timeseries.json"), serde_json::to_string_pretty(&timeseries)?)?;

    let workload_canonical = json!({
        "kind": "ch-ingestor-otlp-logs",
        "log_records_per_payload": args.log_records_per_payload,
        "payloads": args.payloads,
        "schema": "otel-logs-protobuf",
        "generator": "ch-ingestor-bench-stage-latencies",
    });
    std::fs::write(raw_dir.join("workload.canonical.json"),
                   serde_json::to_string_pretty(&workload_canonical)?)?;
    let canon = serde_json::to_string(&workload_canonical)?;

    let opendata_contrib = std::env::var("OPENDATA_CONTRIB_REPO_PATH").unwrap_or_else(|_| ".".into());
    let opendata = std::env::var("OPENDATA_REPO_PATH").ok();
    let metadata = json!({
        "schema_version": 2,
        "phase": "phase01-baseline",
        "unit_id": args.unit_id,
        "unit_title": "Benchmark current ClickHouse ingestor stage latencies (dry-run)",
        "owner": "ClickHouse Implementor",
        "started_at": started_iso,
        "ended_at": ended_iso,
        "experiment": {
            "kind": "ab",
            "dimensions": [],
            "fixed_controls": {
                "log_records_per_payload": args.log_records_per_payload,
                "payloads": args.payloads,
                "dry_run": true,
                "writer": null,
            },
            "matrix_file": null,
        },
        "varied_param": null,
        "git": {
            "opendata":         opendata.as_deref().map(git_info).unwrap_or(serde_json::Value::Null),
            "opendata-go":      serde_json::Value::Null,
            "opendata-contrib": git_info(&opendata_contrib),
        },
        "host": {
            "machine": std::env::var("HOSTNAME").unwrap_or_default(),
            "os": std::env::consts::OS.to_string() + " " + std::env::consts::ARCH,
            "cpu_model": std::env::var("CPU_MODEL").unwrap_or_else(|_| "unknown".into()),
            "cpu_count_logical": std::thread::available_parallelism().map(|n| n.get() as i64).unwrap_or(1),
            "memory_gb": -1,
            "container": serde_json::Value::Null,
            "low_perturbation": true,
        },
        "binary": {
            "name": "ch-ingestor-bench-stage-latencies",
            "build_command": "cargo build --release -p clickhouse-ingestor-bench --bin ch-ingestor-bench-stage-latencies",
            "build_rev": "see git.opendata-contrib.rev",
            "build_flags": "release"
        },
        "config": {
            "runtime_yaml_path": serde_json::Value::Null,
            "effective_yaml_hash": "n/a",
            "effective_yaml": {
                "buffer.object_store": "InMemory",
                "buffer.batch_compression": "none",
                "runtime.dry_run": true,
                "runtime.writer": null,
                "runtime.commit_group.max_rows": 100_000,
                "runtime.commit_group.max_bytes": 32 * 1024 * 1024,
                "runtime.ack_flush_policy": "EveryCommitGroup",
            }
        },
        "services": {
            "clickhouse":   serde_json::Value::Null,
            "object_store": { "kind": "in-memory", "endpoint": null, "region": null, "bucket": null, "container_image": null },
            "iceberg":      serde_json::Value::Null,
        },
        "workload": {
            "generator": "ch-ingestor-bench-stage-latencies",
            "generator_rev": "see git.opendata-contrib.rev",
            "seed": 42,
            "schema": "otel-logs-protobuf",
            "schema_hash": format!("sha256:{}", sha256_hex(b"otel-logs-protobuf")),
            "fingerprint": format!(
                "ch-ingestor-otlp-logs-{}records-per-payload-{}payloads-dry-run",
                args.log_records_per_payload, args.payloads),
            "fingerprint_hash": format!("sha256:{}", sha256_hex(canon.as_bytes())),
            "canonical_path": "raw/workload.canonical.json",
            "records_total": (args.log_records_per_payload * args.payloads) as i64,
            "batches_total": args.payloads as i64,
            "records_per_batch": args.log_records_per_payload as i64,
            "approx_bytes_per_record": -1,
            "encoding": "otlp_protobuf",
            "compression": "none",
            "attribute_cardinality": null,
        },
        "iterations": args.iterations,
        "baseline_run": null,
        "notes": format!("{} | Stages decode_envelope, decode_signal, commit_group_append, adapter_plan, writer_insert, ack_through are not instrumented in v1; Phase 6 (RFC 0002 runtime stage histograms) and Phase 8 (real-CH bench) will populate them.", args.notes),
    });
    std::fs::write(run_dir.join("metadata.json"), serde_json::to_string_pretty(&metadata)?)?;

    println!("Wrote artifacts to {}", run_dir.display());
    Ok(())
}

/// Aggregate per-iteration series into a schema-v2 timeseries entry,
/// aligned on sample index (one observation per Buffer batch / commit
/// group event). See `plans/odb-high-throughput/benchmarks.md` rev 5:
/// each sample carries `sample_index` (not `t_offset_ms`) and the
/// series envelope declares `labels.alignment = "sample_index"`.
fn build_aggregated_series(metric: &str, per_iter: &[Vec<f64>]) -> serde_json::Value {
    let max_len = per_iter.iter().map(|v| v.len()).max().unwrap_or(0);
    let mut samples: Vec<serde_json::Value> = Vec::with_capacity(max_len);
    for j in 0..max_len {
        let mut values: Vec<f64> = Vec::with_capacity(per_iter.len());
        for it in per_iter {
            if let Some(v) = it.get(j).copied() {
                values.push(v);
            }
        }
        if values.is_empty() { continue; }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = values[values.len() / 2];
        let p90_idx = ((values.len() as f64 - 1.0) * 0.9) as usize;
        let p90 = values[p90_idx];
        let max = *values.last().unwrap();
        samples.push(json!({
            "sample_index": j as i64,
            "median": med,
            "p90":    p90,
            "max":    max,
            "n":      values.len(),
        }));
    }
    json!({
        "metric": metric,
        "labels": { "alignment": "sample_index" },
        "samples": samples,
    })
}

fn bucket_counts(samples: &[f64], buckets: &[f64]) -> Vec<u64> {
    let mut counts = vec![0u64; buckets.len()];
    for v in samples {
        for (i, b) in buckets.iter().enumerate() {
            if *v < *b { counts[i] += 1; break; }
        }
    }
    counts
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut s = String::with_capacity(64);
    for b in out {
        use std::fmt::Write;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}
