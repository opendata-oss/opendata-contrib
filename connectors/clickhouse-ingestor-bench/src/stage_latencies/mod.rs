//! Phase 7.1: stage-latency bench port (Migration Plan step 1).
//!
//! Re-port of the Phase 1.4 stage-latency bench onto the patched
//! `Runtime::builder` + Phase 6 stage histograms. Drives the
//! pipelined runtime against an in-memory `BufferSource` +
//! `BenchSink`, captures the four stage labels
//! (`source | fetch | decode | sink_dispatch`) via the process-
//! global `metrics-util::DebuggingRecorder`, and writes
//! `metadata.json` / `results.json` / `timeseries.json` per
//! `plans/odb-high-throughput/benchmarks.md` v2 schema.
//!
//! Row 7.1 has no `correctness.json` (in-memory dry-run is the wrong
//! layer for a correctness gate; row 7.2 owns that against real
//! ClickHouse). Row 7.2 will extract the iteration protocol into a
//! generic `iteration.rs` driver parameterized over a fixture trait
//! and share the scaffolding here.

pub mod iteration;
pub mod output;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

pub use iteration::{
    IterationParams, IterationReport, Stage, StageSamples, StageWorkerCounts,
    default_runtime_options, install_recorder, run_iteration,
};
pub use output::{RunArtifacts, RunInputs, write_run};

/// Outer configuration for the bench's `run_stage_latencies`
/// driver. Defaults are sized for in-memory `cargo test` runs; the
/// CLI binary can override every field.
#[derive(Debug, Clone)]
pub struct StageLatenciesConfig {
    pub records_per_source_range: usize,
    pub warmup_payloads: u64,
    pub timed_payloads: u64,
    pub iterations: usize,
    pub min_timed_window_seconds: f64,
    pub warmup_deadline: Duration,
    pub timed_deadline: Duration,
    pub runtime_options: opendata_ingest_runtime::runtime::RuntimeOptions,
    pub output_dir: PathBuf,
    pub change_slug: String,
    pub notes: String,
}

impl Default for StageLatenciesConfig {
    fn default() -> Self {
        Self {
            records_per_source_range: 1,
            warmup_payloads: 4,
            timed_payloads: 32,
            iterations: 3,
            // Row 7.1 in-memory: 0.0 disables the floor entirely.
            // Real-CH rows (7.2+) raise this to 20.0 per the design.
            min_timed_window_seconds: 0.0,
            warmup_deadline: Duration::from_secs(60),
            timed_deadline: Duration::from_secs(600),
            runtime_options: default_runtime_options(),
            output_dir: PathBuf::from("bench-results/phase07/7.1-port-baseline"),
            change_slug: "baseline".to_string(),
            notes: String::new(),
        }
    }
}

/// Top-level driver: run N iterations of the protocol, then emit
/// the artifact bundle. Returns the run directory the artifacts
/// were written to.
pub async fn run_stage_latencies(cfg: StageLatenciesConfig) -> Result<RunArtifacts> {
    let snapshotter = install_recorder();
    let params = IterationParams {
        warmup_payloads: cfg.warmup_payloads,
        timed_payloads: cfg.timed_payloads,
        records_per_source_range: cfg.records_per_source_range,
        runtime_options: cfg.runtime_options.clone(),
        warmup_deadline: cfg.warmup_deadline,
        timed_deadline: cfg.timed_deadline,
        min_timed_window_seconds: cfg.min_timed_window_seconds,
    };

    let mut reports: Vec<IterationReport> = Vec::with_capacity(cfg.iterations);
    for idx in 1..=cfg.iterations {
        let report = run_iteration(idx, &params, &snapshotter).await?;
        reports.push(report);
    }

    let inputs = RunInputs {
        output_dir: &cfg.output_dir,
        change_slug: &cfg.change_slug,
        unit_id: "7.1",
        unit_title: "Port Phase 1.4 stage-latency bench onto patched Runtime::builder + Phase 6 stage histograms",
        phase: "phase07-clickhouse-throughput",
        owner: "Benchmark/Perf Implementor",
        records_per_source_range: cfg.records_per_source_range,
        warmup_payloads: cfg.warmup_payloads,
        timed_payloads: cfg.timed_payloads,
        iterations: cfg.iterations,
        notes: &cfg.notes,
    };
    write_run(&reports, &inputs)
}
