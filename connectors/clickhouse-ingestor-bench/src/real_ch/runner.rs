//! §Iteration Protocol implementation for the Phase 7.2 real-CH
//! bench. Drives the production `Runtime::builder` against a live
//! ClickHouse table via the production `ClickHouseSink<OtlpLogsClickHouseAdapter>`
//! and `OtlpLogsDecoder`. The Iteration Protocol matches the design
//! pseudocode directly (prefill → drain warmup → snapshot baseline →
//! drain timed → snapshot final → shutdown → correctness queries)
//! because real ClickHouse insert latency makes the warmup boundary
//! observable; the in-memory dry-run port's two-phase producer
//! workaround (`stage_latencies::iteration`) is not needed here.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow, bail};
use clickhouse_ingestor::writer::{ClickHouseWriter, HttpClientMode, WriterConfig};
use opendata_ingest_clickhouse::adapter::logs::OtlpLogsClickHouseAdapter;
use opendata_ingest_clickhouse::serializer::SerializationFormat;
use opendata_ingest_clickhouse::sink::ClickHouseSink;
use opendata_ingest_otel::logs::OtlpLogsDecoder;
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, Runtime, RuntimeOptions, SinkPoolOptions, SourceBackpressureOptions,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::fixture::RealClickHouseFixture;
use super::workload::{LogWorkload, LogWorkloadConfig, WorkloadEnv, build_env};
use crate::metrics_recorder::init_metrics_recorder;
use crate::stage_latencies::iteration::{
    ClickHouseSamples, Stage, StageSamples, StageWorkerCounts, collect_bench_samples,
};

#[derive(Debug, Clone)]
pub struct RealChConfig {
    /// Workload knob block.
    pub workload: LogWorkloadConfig,
    /// Iteration count (default 1 for first cut).
    pub iterations: usize,
    /// Runtime options (defaults are pipelined per Phase 6).
    pub runtime_options: RuntimeOptions,
    /// ClickHouse serialization format for the sink writer.
    pub serialization_format: SerializationFormat,
    /// HTTP client mode for the sink writer.
    pub http_client_mode: HttpClientMode,
    /// Warmup-drain deadline.
    pub warmup_deadline: Duration,
    /// Timed-window drain deadline.
    pub timed_deadline: Duration,
    /// Minimum timed-window duration. The design's 20.0 s floor
    /// applies to real-CH runs; CI smoke can lower this to e.g.
    /// 0.5 s to keep the test fast.
    pub min_timed_window_seconds: f64,
    /// Output directory (the run sub-dir
    /// `<UTC-stamp>-<change-slug>/` is created under this).
    pub output_dir: PathBuf,
    /// Change slug appended to the UTC stamp.
    pub change_slug: String,
    /// Free-form notes captured in `metadata.notes`.
    pub notes: String,
}

impl Default for RealChConfig {
    fn default() -> Self {
        Self {
            workload: LogWorkloadConfig::default(),
            iterations: 1,
            runtime_options: RuntimeOptions {
                ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
                dry_run: false,
                poll_interval: Duration::from_millis(5),
                source_defaults: SourceBackpressureOptions::default(),
                sink: SinkPoolOptions::default(),
                ..Default::default()
            },
            serialization_format: SerializationFormat::JsonEachRow,
            http_client_mode: HttpClientMode::PerCall,
            warmup_deadline: Duration::from_secs(120),
            timed_deadline: Duration::from_secs(600),
            min_timed_window_seconds: 0.0,
            output_dir: PathBuf::from("bench-results/phase07/7.2-real-ch-smoke"),
            change_slug: "baseline".to_string(),
            notes: String::new(),
        }
    }
}

/// One real-CH iteration's measurements.
#[derive(Debug, Clone)]
pub struct RealChIterationReport {
    pub iteration: usize,
    pub timed_started_unix_ms: u64,
    pub elapsed_seconds: f64,
    /// Records the runtime acked during the **timed window only**
    /// (final cumulative count minus the baseline captured at
    /// warmup boundary). Drives all timed-report scalars.
    pub records_processed: u64,
    /// Cumulative records the runtime acked from start through
    /// shutdown — equals `records_visible` against the live CH
    /// in the happy path. Kept for the per-iteration correctness
    /// gate, which compares against `count_visible` (which sees
    /// warmup + timed because the bench TRUNCATEs once per
    /// iteration, not at the warmup boundary).
    pub records_processed_cumulative: u64,
    pub stage_samples: StageSamples,
    pub clickhouse_samples: ClickHouseSamples,
    pub worker_counts: StageWorkerCounts,
    pub records_visible: u64,
    pub records_raw: u64,
    pub pre_dedupe_dupes: u64,
    pub post_dedupe_dupes: u64,
}

impl RealChIterationReport {
    pub fn worker_utilization(&self, stage: Stage) -> f64 {
        let stage_seconds = self.stage_samples.sum(stage);
        let workers = self.worker_counts.get(stage) as f64;
        let denom = self.elapsed_seconds * workers;
        if denom <= 0.0 {
            0.0
        } else {
            stage_seconds / denom
        }
    }

    pub fn per_record_service_time(&self, stage: Stage) -> f64 {
        let records = self.records_processed as f64;
        if records <= 0.0 {
            0.0
        } else {
            self.stage_samples.sum(stage) / records
        }
    }

    /// `Σ clickhouse_serialization_duration_seconds`.
    pub fn serialize_seconds_total(&self) -> f64 {
        self.clickhouse_samples
            .serialize_duration_seconds
            .iter()
            .sum()
    }

    /// `Σ clickhouse_insert_duration_seconds` (one sample per HTTP
    /// INSERT attempt). Includes serialize time since the attempt
    /// timer wraps the whole `execute_once` body.
    pub fn insert_seconds_total(&self) -> f64 {
        self.clickhouse_samples.insert_duration_seconds.iter().sum()
    }

    /// `Σ clickhouse_serialized_bytes`.
    pub fn serialized_bytes_total(&self) -> u64 {
        self.clickhouse_samples
            .serialized_bytes
            .iter()
            .map(|v| *v as u64)
            .sum()
    }

    /// `serialize_fraction_of_insert ∈ [0, 1]`. Per §Bottleneck
    /// Attribution Methodology.
    pub fn serialize_fraction_of_insert(&self) -> f64 {
        let ins = self.insert_seconds_total();
        if ins <= 0.0 {
            0.0
        } else {
            (self.serialize_seconds_total() / ins).clamp(0.0, 1.0)
        }
    }

    /// `serialize_seconds_per_record`.
    pub fn serialize_seconds_per_record(&self) -> f64 {
        let records = self.records_processed as f64;
        if records <= 0.0 {
            0.0
        } else {
            self.serialize_seconds_total() / records
        }
    }

    /// `serialization_bytes_per_record`.
    pub fn serialization_bytes_per_record(&self) -> f64 {
        let records = self.records_processed as f64;
        if records <= 0.0 {
            0.0
        } else {
            self.serialized_bytes_total() as f64 / records
        }
    }

    /// `end_to_end_seconds_per_record`.
    pub fn end_to_end_seconds_per_record(&self) -> f64 {
        let records = self.records_processed as f64;
        if records <= 0.0 {
            0.0
        } else {
            self.elapsed_seconds / records
        }
    }
}

/// Bundle returned by `run_real_ch`: the run directory + the
/// JSON the bench wrote there.
#[derive(Debug)]
pub struct RealChRunArtifacts {
    pub run_dir: PathBuf,
    pub reports: Vec<RealChIterationReport>,
    pub correctness_passed: bool,
}

/// Top-level entrypoint: drive N iterations of the real-CH §Iteration
/// Protocol against the fixture's live ClickHouse, then emit the
/// benchmarks.md v2 artifact bundle + a counts-based correctness.json
/// (ack-invariant scenarios land alongside the `TestObservableSink`
/// trait refactor in a follow-up).
pub async fn run_real_ch(
    cfg: RealChConfig,
    fixture: &RealClickHouseFixture,
) -> Result<RealChRunArtifacts> {
    let snapshotter = init_metrics_recorder();

    let mut reports: Vec<RealChIterationReport> = Vec::with_capacity(cfg.iterations);
    for iter_idx in 1..=cfg.iterations {
        // Fresh manifest + data prefix per iteration so the
        // in-memory ObjectStore stays isolated.
        let workload_cfg = LogWorkloadConfig {
            manifest_path: format!("phase07/7.2/iter-{iter_idx}/manifest"),
            data_prefix: format!("phase07/7.2/iter-{iter_idx}/data"),
            ..cfg.workload.clone()
        };

        // Reset the fixture's table for this iteration. Counts
        // resetting means `count_visible` after shutdown reflects
        // ONLY this iteration's writes — same property the design's
        // pseudocode names.
        truncate_table(&fixture.writer, &fixture.database, &fixture.table).await?;

        // Drain residue from earlier iterations.
        let _residue = snapshotter.snapshot();

        let env = build_env(&workload_cfg).await?;
        let handle = LogWorkload::handle(&workload_cfg);
        // Destructure so the producer lives outside the runtime's
        // builder consumption — phases A/B feed it while the
        // runtime is running off `env.source`.
        let WorkloadEnv {
            producer,
            source,
            source_id,
            ..
        } = env;

        let adapter = Arc::new(OtlpLogsClickHouseAdapter::new(
            fixture.adapter_config.clone(),
        ));
        // Build a fresh `ClickHouseWriter` per iteration so the
        // matrix sweep can flip `serialization_format` and
        // `http_client_mode` without rebuilding the fixture (which
        // owns the testcontainers handle). The fixture's writer
        // stays in use for `TRUNCATE TABLE` + SELECT-style queries.
        let writer_config = WriterConfig {
            endpoint: fixture.endpoint.clone(),
            user: fixture.writer.config().user.clone(),
            password: fixture.writer.config().password.clone(),
            request_timeout: fixture.writer.config().request_timeout,
            max_attempts: fixture.writer.config().max_attempts,
            initial_backoff: fixture.writer.config().initial_backoff,
            serialization_format: cfg.serialization_format,
            http_client_mode: cfg.http_client_mode.clone(),
        };
        let sink_writer = Arc::new(ClickHouseWriter::new(writer_config));
        let sink = ClickHouseSink::new("phase07-real-ch", adapter, sink_writer);
        let runtime = Runtime::builder()
            .add_source(source)
            .add_decoder(OtlpLogsDecoder::new())
            .set_sink(sink)
            .with_options(cfg.runtime_options.clone())
            .build()
            .map_err(|e| anyhow!("Runtime::build: {e}"))?;
        let bp = cfg.runtime_options.backpressure_for(&source_id);
        let worker_counts = StageWorkerCounts {
            source: 1,
            fetch: bp.fetch_concurrency,
            decode: bp.decode_concurrency,
            sink_dispatch: cfg.runtime_options.sink.max_concurrent_commits,
        };

        let mut progress = runtime.progress();
        let shutdown = CancellationToken::new();
        let runtime_task = {
            let s = shutdown.clone();
            tokio::spawn(async move { runtime.run(s).await })
        };

        // ── Phase A: produce warmup payloads, drain to warmup boundary ──
        if cfg.workload.warmup_payloads > 0 {
            LogWorkload::produce(
                &workload_cfg,
                &producer,
                0,
                cfg.workload.warmup_payloads as u64,
            )
            .await?;
            drain_until(
                &mut progress,
                handle.warmup_highest_sequence,
                cfg.warmup_deadline,
                "warmup",
            )
            .await?;
        }

        // ── Snapshot baseline (drains warmup-phase metrics) ──
        //   Also capture the runtime's records_written counter at
        //   the warmup boundary so the timed report's
        //   `records_processed` is timed-only (`final - baseline`),
        //   not cumulative. The design's §Iteration Protocol
        //   pseudocode emits `timed_records_count(workload_cfg)`;
        //   `progress.records_written` is monotonic from runtime
        //   start, so the subtraction is the equivalent for an
        //   already-running pipeline.
        let baseline_records_written = progress.borrow().records_written;
        let _baseline = snapshotter.snapshot();
        let timed_start = Instant::now();
        let timed_started_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // ── Phase B: produce timed payloads, drain to total boundary ──
        if cfg.workload.timed_payloads > 0 {
            LogWorkload::produce(
                &workload_cfg,
                &producer,
                cfg.workload.warmup_payloads as u64,
                cfg.workload.timed_payloads as u64,
            )
            .await?;
        }
        producer
            .close()
            .await
            .map_err(|e| anyhow!("producer.close: {e}"))?;
        drain_until(
            &mut progress,
            handle.highest_sequence,
            cfg.timed_deadline,
            "timed",
        )
        .await?;

        let elapsed_seconds = timed_start.elapsed().as_secs_f64();
        if elapsed_seconds < cfg.min_timed_window_seconds {
            bail!(
                "timed window too short ({elapsed_seconds:.4}s; floor {:.4}s) — raise timed_payloads or lower min_timed_window_seconds",
                cfg.min_timed_window_seconds,
            );
        }

        let progress_final = *progress.borrow();
        shutdown.cancel();
        runtime_task
            .await
            .map_err(|e| anyhow!("runtime task join: {e}"))?
            .map_err(|e| anyhow!("runtime.run: {e}"))?;

        let final_snapshot = snapshotter.snapshot();
        let (stage_samples, clickhouse_samples) = collect_bench_samples(final_snapshot);

        // ── Correctness queries against the live CH ──
        let records_raw = fixture
            .count_raw()
            .await
            .map_err(|e| anyhow!("count_raw: {e}"))?;
        let records_visible = fixture
            .count_visible()
            .await
            .map_err(|e| anyhow!("count_visible: {e}"))?;
        let pre_dedupe_dupes = fixture
            .count_pre_dedupe_duplicates()
            .await
            .map_err(|e| anyhow!("count_pre_dedupe_duplicates: {e}"))?;
        let post_dedupe_dupes = fixture
            .count_post_dedupe_duplicates()
            .await
            .map_err(|e| anyhow!("count_post_dedupe_duplicates: {e}"))?;

        // Timed-only records: total since runtime start minus
        // what was already written when the timed window began.
        // Matches the design's `timed_records_count(workload_cfg)`.
        let records_processed_timed = progress_final
            .records_written
            .saturating_sub(baseline_records_written);
        reports.push(RealChIterationReport {
            iteration: iter_idx,
            timed_started_unix_ms,
            elapsed_seconds,
            records_processed: records_processed_timed,
            records_processed_cumulative: progress_final.records_written,
            stage_samples,
            clickhouse_samples,
            worker_counts,
            records_visible,
            records_raw,
            pre_dedupe_dupes,
            post_dedupe_dupes,
        });
    }

    let correctness_passed = reports.iter().all(|r| {
        // CH was TRUNCATE'd at iteration start, so `count_visible`
        // reflects warmup + timed. Compare against the cumulative
        // count, not the timed-only `records_processed`.
        r.records_visible == r.records_processed_cumulative
            && r.post_dedupe_dupes == 0
            && r.records_processed_cumulative > 0
    });

    let run_dir = write_artifacts(&reports, &cfg, correctness_passed)?;

    Ok(RealChRunArtifacts {
        run_dir,
        reports,
        correctness_passed,
    })
}

async fn truncate_table(writer: &ClickHouseWriter, database: &str, table: &str) -> Result<()> {
    writer
        .execute_statement(&format!("TRUNCATE TABLE IF EXISTS {database}.{table}"))
        .await
        .map_err(|e| anyhow!("TRUNCATE TABLE {database}.{table}: {e}"))?;
    Ok(())
}

async fn drain_until(
    progress: &mut tokio::sync::watch::Receiver<opendata_ingest_runtime::runtime::RuntimeProgress>,
    target: u64,
    deadline: Duration,
    phase: &str,
) -> Result<()> {
    let start = Instant::now();
    loop {
        {
            let p = *progress.borrow();
            if let Some(s) = p.last_acked_sequence
                && s >= target
            {
                return Ok(());
            }
        }
        if start.elapsed() > deadline {
            let last = progress.borrow().last_acked_sequence;
            bail!(
                "{phase} did not drain within {deadline:?} (last_acked={last:?}, target={target})"
            );
        }
        tokio::select! {
            res = progress.changed() => {
                res.map_err(|e| anyhow!("progress channel closed during {phase}: {e}"))?;
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

fn write_artifacts(
    reports: &[RealChIterationReport],
    cfg: &RealChConfig,
    correctness_passed: bool,
) -> Result<PathBuf> {
    let started_at = utc_iso(
        reports
            .first()
            .map(|r| r.timed_started_unix_ms)
            .unwrap_or(0),
    );
    let ended_at = utc_iso(now_unix_ms());
    let run_id = run_id_slug(&cfg.change_slug);
    let run_dir = cfg.output_dir.join(&run_id);
    let raw_dir = run_dir.join("raw");
    std::fs::create_dir_all(&raw_dir)?;

    let results = build_results(reports);
    let timeseries = build_timeseries(reports);
    let correctness = build_correctness(reports, correctness_passed);
    let metadata = build_metadata(reports, cfg, &started_at, &ended_at);

    std::fs::write(
        run_dir.join("results.json"),
        serde_json::to_string_pretty(&results)?,
    )?;
    std::fs::write(
        run_dir.join("timeseries.json"),
        serde_json::to_string_pretty(&timeseries)?,
    )?;
    std::fs::write(
        run_dir.join("correctness.json"),
        serde_json::to_string_pretty(&correctness)?,
    )?;
    std::fs::write(
        run_dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata)?,
    )?;
    for report in reports {
        let path = raw_dir.join(format!("run-{}.metrics.jsonl", report.iteration));
        std::fs::write(&path, render_raw_metrics_jsonl(report))?;
    }
    Ok(run_dir)
}

fn build_results(reports: &[RealChIterationReport]) -> Value {
    let throughputs: Vec<f64> = reports
        .iter()
        .map(|r| {
            if r.elapsed_seconds <= 0.0 {
                0.0
            } else {
                r.records_processed as f64 / r.elapsed_seconds
            }
        })
        .collect();
    let elapsed: Vec<f64> = reports.iter().map(|r| r.elapsed_seconds).collect();

    let mut scalars = serde_json::Map::new();
    scalars.insert(
        "total_throughput_records_per_sec".to_string(),
        agg(&throughputs),
    );
    scalars.insert("iteration_elapsed_seconds".to_string(), agg(&elapsed));

    // §Bottleneck Attribution Methodology — HTTP-side decomposition
    // per matrix point. One sample per iteration since we run a
    // single iteration per matrix point in the lightweight 7.6
    // sweep.
    let serialize_fractions: Vec<f64> = reports
        .iter()
        .map(|r| r.serialize_fraction_of_insert())
        .collect();
    let serialize_seconds_per_record: Vec<f64> = reports
        .iter()
        .map(|r| r.serialize_seconds_per_record())
        .collect();
    let serialization_bytes_per_record: Vec<f64> = reports
        .iter()
        .map(|r| r.serialization_bytes_per_record())
        .collect();
    let end_to_end_seconds_per_record: Vec<f64> = reports
        .iter()
        .map(|r| r.end_to_end_seconds_per_record())
        .collect();
    scalars.insert(
        "serialize_fraction_of_insert".to_string(),
        agg(&serialize_fractions),
    );
    scalars.insert(
        "serialize_seconds_per_record".to_string(),
        agg(&serialize_seconds_per_record),
    );
    scalars.insert(
        "serialization_bytes_per_record".to_string(),
        agg(&serialization_bytes_per_record),
    );
    scalars.insert(
        "end_to_end_seconds_per_record".to_string(),
        agg(&end_to_end_seconds_per_record),
    );

    let mut stages: Vec<Value> = Vec::with_capacity(4);
    for stage in Stage::ALL {
        let label = stage.as_label();
        let utilizations: Vec<f64> = reports
            .iter()
            .map(|r| r.worker_utilization(stage))
            .collect();
        let service_times: Vec<f64> = reports
            .iter()
            .map(|r| r.per_record_service_time(stage))
            .collect();
        let all_samples: Vec<f64> = reports
            .iter()
            .flat_map(|r| r.stage_samples.get(stage).iter().copied())
            .collect();
        let total_elapsed: f64 = elapsed.iter().sum();
        scalars.insert(format!("worker_utilization_{label}"), agg(&utilizations));
        scalars.insert(
            format!("per_record_service_time_{label}"),
            agg(&service_times),
        );
        stages.push(json!({
            "name": label,
            "median_ms_per_op": median_of(&all_samples) * 1000.0,
            "p10": quantile(&all_samples, 0.10) * 1000.0,
            "p90": quantile(&all_samples, 0.90) * 1000.0,
            "ops_per_sec": if total_elapsed <= 0.0 { 0.0 } else { all_samples.len() as f64 / total_elapsed },
            "worker_count": reports.first().map(|r| r.worker_counts.get(stage)).unwrap_or(0),
            "samples": all_samples.len(),
        }));
    }

    let iterations: Vec<Value> = reports.iter().map(iteration_block).collect();
    json!({
        "schema_version": 2,
        "scalars": Value::Object(scalars),
        "stages": stages,
        "histograms": {},
        "iterations": iterations,
    })
}

fn iteration_block(r: &RealChIterationReport) -> Value {
    let throughput = if r.elapsed_seconds <= 0.0 {
        0.0
    } else {
        r.records_processed as f64 / r.elapsed_seconds
    };
    let mut stage_objs: Vec<Value> = Vec::with_capacity(4);
    for stage in Stage::ALL {
        let label = stage.as_label();
        let samples = r.stage_samples.get(stage);
        stage_objs.push(json!({
            "name": label,
            "median_ms_per_op": median_of(samples) * 1000.0,
            "p10": quantile(samples, 0.10) * 1000.0,
            "p90": quantile(samples, 0.90) * 1000.0,
            "ops_per_sec": if r.elapsed_seconds <= 0.0 { 0.0 } else { samples.len() as f64 / r.elapsed_seconds },
            "worker_count": r.worker_counts.get(stage),
            "samples": samples.len(),
            "worker_utilization": r.worker_utilization(stage),
            "per_record_service_time_seconds": r.per_record_service_time(stage),
        }));
    }
    json!({
        "iteration": r.iteration,
        "scalars": {
            "iteration_elapsed_seconds": r.elapsed_seconds,
            "iteration_throughput_records_per_sec": throughput,
            "iteration_records_processed":           r.records_processed as f64,
            "iteration_records_processed_cumulative": r.records_processed_cumulative as f64,
            "records_visible_in_clickhouse": r.records_visible as f64,
            "records_raw_in_clickhouse":     r.records_raw as f64,
            "pre_dedupe_duplicates": r.pre_dedupe_dupes as f64,
            "post_dedupe_duplicates": r.post_dedupe_dupes as f64,
        },
        "stages": stage_objs,
        "histograms": {},
        "raw_log":     format!("raw/run-{}.metrics.jsonl", r.iteration),
        "raw_metrics": format!("raw/run-{}.metrics.jsonl", r.iteration),
    })
}

fn build_timeseries(reports: &[RealChIterationReport]) -> Value {
    let iteration_starts: Vec<u64> = reports.iter().map(|r| r.timed_started_unix_ms).collect();
    let mut series: Vec<Value> = Vec::with_capacity(4);
    for stage in Stage::ALL {
        let label = stage.as_label();
        let per_iter: Vec<Vec<f64>> = reports
            .iter()
            .map(|r| r.stage_samples.get(stage).to_vec())
            .collect();
        series.push(sample_index_series(
            "runtime_stage_latency_seconds",
            json!({"alignment": "sample_index", "stage": label}),
            &per_iter,
        ));
    }
    json!({
        "schema_version": 2,
        "window_seconds": 1.0,
        "iterations_aggregated": reports.len() as i64,
        "iteration_starts_unix_ms": iteration_starts,
        "series": series,
    })
}

fn sample_index_series(metric: &str, labels: Value, per_iter: &[Vec<f64>]) -> Value {
    let max_len = per_iter.iter().map(|v| v.len()).max().unwrap_or(0);
    let mut samples = Vec::with_capacity(max_len);
    for j in 0..max_len {
        let mut values: Vec<f64> = per_iter
            .iter()
            .filter_map(|it| it.get(j).copied())
            .collect();
        if values.is_empty() {
            continue;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let med = values[values.len() / 2];
        let p90_idx = ((values.len() as f64 - 1.0) * 0.90) as usize;
        let p90 = values[p90_idx];
        let max = *values.last().unwrap_or(&0.0);
        samples.push(json!({
            "sample_index": j as i64,
            "median": med,
            "p90": p90,
            "max": max,
            "n": values.len() as i64,
        }));
    }
    json!({
        "metric": metric,
        "labels": labels,
        "samples": samples,
    })
}

fn build_correctness(reports: &[RealChIterationReport], passed: bool) -> Value {
    let per_iter: Vec<Value> = reports
        .iter()
        .map(|r| {
            // Correctness compares the cumulative count (warmup +
            // timed) against `count_visible` because the table was
            // TRUNCATE'd once at iteration start, so CH sees both
            // phases. Throughput uses timed-only — those are
            // semantically different.
            json!({
                "iteration": r.iteration,
                "records_processed_timed":      r.records_processed,
                "records_processed_cumulative": r.records_processed_cumulative,
                "records_raw_in_clickhouse":    r.records_raw,
                "records_visible_in_clickhouse": r.records_visible,
                "pre_dedupe_duplicates":  r.pre_dedupe_dupes,
                "post_dedupe_duplicates": r.post_dedupe_dupes,
                "records_missing_from_sink":
                    r.records_processed_cumulative.saturating_sub(r.records_visible),
                "passed": r.records_visible == r.records_processed_cumulative
                    && r.post_dedupe_dupes == 0
                    && r.records_processed_cumulative > 0,
            })
        })
        .collect();
    json!({
        "schema_version": 2,
        "summary": {
            "passed": passed,
            "iterations": reports.len() as i64,
            "required_sinks": ["clickhouse"],
            "ack_invariant_checks_reported": 0,
            "notes": "Row 7.2 first cut: counts-based correctness only. Full ack_invariant_checks land alongside the TestObservableSink trait refactor.",
        },
        "iterations": per_iter,
    })
}

fn build_metadata(
    reports: &[RealChIterationReport],
    cfg: &RealChConfig,
    started_at: &str,
    ended_at: &str,
) -> Value {
    let fingerprint = format!(
        "phase07-real-ch-{}records-per-payload-{}warmup-{}timed",
        cfg.workload.records_per_source_range,
        cfg.workload.warmup_payloads,
        cfg.workload.timed_payloads
    );
    let fingerprint_canonical = json!({
        "kind": "phase07-real-ch-smoke",
        "records_per_source_range": cfg.workload.records_per_source_range,
        "warmup_payloads": cfg.workload.warmup_payloads,
        "timed_payloads": cfg.workload.timed_payloads,
        "service_name": cfg.workload.service_name,
        "schema": "otel_logs_v1",
        "sink": "ClickHouseSink",
        "decoder": "OtlpLogsDecoder",
    });
    let fingerprint_canonical_str =
        serde_json::to_string(&fingerprint_canonical).unwrap_or_default();
    let bp = reports
        .first()
        .map(|r| r.worker_counts)
        .unwrap_or(StageWorkerCounts {
            source: 1,
            fetch: 1,
            decode: 1,
            sink_dispatch: 1,
        });

    json!({
        "schema_version": 2,
        "phase": "phase07-clickhouse-throughput",
        "unit_id": "7.2",
        "unit_title": "Stand up real-ClickHouse bench harness end-to-end (testcontainers-rs path; docker-compose path also lands; generalize correctness checks)",
        "owner": "Benchmark/Perf Implementor",
        "started_at": started_at,
        "ended_at": ended_at,
        "experiment": {
            "kind": "ab",
            "dimensions": [],
            "fixed_controls": {
                "records_per_source_range": cfg.workload.records_per_source_range,
                "warmup_payloads": cfg.workload.warmup_payloads,
                "timed_payloads": cfg.workload.timed_payloads,
                "source.fetch_concurrency": bp.fetch,
                "source.decode_concurrency": bp.decode,
                "sink.max_concurrent_commits": bp.sink_dispatch,
                "sink.kind": "clickhouse",
                "serialization_format": cfg.serialization_format.as_label(),
                "http_client_mode": cfg.http_client_mode.as_label(),
            },
            "matrix_file": Value::Null,
        },
        "varied_param": Value::Null,
        "git": {
            "opendata":         Value::Null,
            "opendata-go":      Value::Null,
            "opendata-contrib": git_info("."),
        },
        "host": {
            "machine": std::env::var("HOSTNAME").unwrap_or_default(),
            "os": format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
            "cpu_model": std::env::var("CPU_MODEL").unwrap_or_else(|_| "unknown".into()),
            "cpu_count_logical": std::thread::available_parallelism().map(|n| n.get() as i64).unwrap_or(1),
            "memory_gb": -1,
            "container": Value::Null,
            "low_perturbation": true,
        },
        "binary": {
            "name": "phase07-real-ch-smoke",
            "build_command": "cargo build --release -p clickhouse-ingestor-bench --features real-ch --bin phase07-real-ch-smoke",
            "build_rev": "see git.opendata-contrib.rev",
            "build_flags": "release",
        },
        "config": {
            "runtime_yaml_path": Value::Null,
            "effective_yaml_hash": "n/a",
            "effective_yaml": {
                "buffer.object_store": "InMemory",
                "buffer.batch_compression": "none",
                "runtime.dry_run": false,
                "runtime.sink.max_concurrent_commits": bp.sink_dispatch,
                "runtime.source.fetch_concurrency": bp.fetch,
                "runtime.source.decode_concurrency": bp.decode,
                "runtime.ack_flush_policy": "EveryCommitGroup",
                "sink.kind": "clickhouse",
                "sink.serialization_format": cfg.serialization_format.as_label(),
                "sink.http_client_mode": cfg.http_client_mode.as_label(),
            },
        },
        "services": {
            "clickhouse": {
                "version": "testcontainers-default",
                "engine": "ReplacingMergeTree",
                "engine_version_column": "_adapter_version",
                "table_ddl_path": Value::Null,
                "table_ddl_hash": Value::Null,
                "order_by": Value::Null,
                "settings_overrides": {},
                "container_image": "testcontainers-modules/clickhouse@default",
            },
            "object_store": {
                "kind": "in-memory",
                "endpoint": Value::Null,
                "region": Value::Null,
                "bucket": Value::Null,
                "container_image": Value::Null,
            },
            "iceberg": Value::Null,
        },
        "workload": {
            "generator": "phase07-real-ch-otlp-logs-synth",
            "generator_rev": "see git.opendata-contrib.rev",
            "seed": 0,
            "schema": "otel_logs_v1",
            "schema_hash": format!("sha256:{}", sha256_hex(b"otel_logs_v1")),
            "fingerprint": fingerprint,
            "fingerprint_hash": format!("sha256:{}", sha256_hex(fingerprint_canonical_str.as_bytes())),
            "canonical_path": Value::Null,
            "records_total": (cfg.workload.records_per_source_range as i64)
                * (cfg.workload.warmup_payloads as i64 + cfg.workload.timed_payloads as i64),
            "batches_total": (cfg.workload.warmup_payloads + cfg.workload.timed_payloads) as i64,
            "records_per_batch": cfg.workload.records_per_source_range as i64,
            "approx_bytes_per_record": 64,
            "encoding": "otlp_protobuf",
            "compression": "none",
            "attribute_cardinality": Value::Null,
        },
        "iterations": cfg.iterations as i64,
        "baseline_run": Value::Null,
        "notes": cfg.notes,
    })
}

fn render_raw_metrics_jsonl(r: &RealChIterationReport) -> String {
    let mut buf = String::new();
    for stage in Stage::ALL {
        let label = stage.as_label();
        for (idx, value) in r.stage_samples.get(stage).iter().enumerate() {
            let line = json!({
                "ts_unix_ms": r.timed_started_unix_ms + idx as u64,
                "metric": "runtime_stage_latency_seconds",
                "value": value,
                "labels": {
                    "iteration": r.iteration,
                    "stage": label,
                    "sample_index": idx,
                },
            });
            buf.push_str(&line.to_string());
            buf.push('\n');
        }
    }
    buf
}

fn agg(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({"median": 0.0, "p10": 0.0, "p90": 0.0, "n": 0});
    }
    json!({
        "median": median_of(samples),
        "p10": quantile(samples, 0.10),
        "p90": quantile(samples, 0.90),
        "n": samples.len() as i64,
    })
}

fn median_of(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    s[s.len() / 2]
}

fn quantile(samples: &[f64], frac: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((s.len() as f64 - 1.0) * frac) as usize;
    s[idx]
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn utc_iso(unix_ms: u64) -> String {
    let secs = (unix_ms / 1000) as i64;
    let (y, mo, d, h, mi, se) = epoch_parts(secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z")
}

fn run_id_slug(slug: &str) -> String {
    let secs = (now_unix_ms() / 1000) as i64;
    let (y, mo, d, h, mi, _) = epoch_parts(secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}{mi:02}-{slug}")
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
    loop {
        let dy = if leap(y) { 366 } else { 365 };
        if days >= dy {
            days -= dy;
            y += 1;
        } else {
            break;
        }
    }
    let dim = |y: i64, m: u32| match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    };
    let mut m: u32 = 1;
    loop {
        let d = dim(y, m) as i64;
        if days >= d {
            days -= d;
            m += 1;
        } else {
            break;
        }
    }
    (y, m, (days + 1) as u32, h, mi, se)
}

fn git_info(repo: &str) -> Value {
    let rev = std::process::Command::new("git")
        .args(["-C", repo, "rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim()[..7.min(s.trim().len())].to_string())
        .unwrap_or_default();
    let branch = std::process::Command::new("git")
        .args(["-C", repo, "rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    // Dirty check ignores `bench-results/` (the bench's own output
    // tree) so a previous run's artifacts sitting in the worktree
    // don't poison the source-code cleanliness signal. Trusts that
    // `bench-results/` is generated and tracked by convention; the
    // source-code path is what determines whether the binary built
    // matches a commit.
    let dirty = std::process::Command::new("git")
        .args([
            "-C",
            repo,
            "status",
            "--porcelain",
            "--",
            ".",
            ":!bench-results",
        ])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    json!({"rev": rev, "branch": branch, "dirty": dirty})
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
