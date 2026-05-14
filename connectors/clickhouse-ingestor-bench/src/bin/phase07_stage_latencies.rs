//! Phase 7.1 stage-latency bench binary. Drives
//! `clickhouse_ingestor_bench::stage_latencies::run_stage_latencies`
//! against an in-memory `BufferSource` + `BenchSink` and writes the
//! benchmarks.md v2 artifact bundle to the requested output dir.
//!
//! Defaults are sized for an operator-managed run; the integration
//! tests use a much smaller workload.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use clickhouse_ingestor_bench::stage_latencies::{StageLatenciesConfig, run_stage_latencies};

#[derive(Parser, Debug)]
#[command(
    name = "phase07-stage-latencies",
    about = "Phase 7.1: stage-latency bench port (Runtime::builder + Phase 6 stage histograms)."
)]
struct Args {
    /// Records emitted per source range (one Buffer payload =
    /// one source range = one entry in the synthetic workload).
    /// Defaults to the smoke value used by `cargo test`; operator
    /// runs raise this to drive realistic stage utilization.
    #[arg(long, default_value_t = 1)]
    records_per_source_range: usize,

    /// Number of warmup payloads (drained before the timed window
    /// starts).
    #[arg(long, default_value_t = 200)]
    warmup_payloads: u64,

    /// Number of timed payloads (the window the bench measures).
    #[arg(long, default_value_t = 2_000)]
    timed_payloads: u64,

    /// Number of iterations to run + aggregate.
    #[arg(long, default_value_t = 5)]
    iterations: usize,

    /// Minimum timed-window duration (seconds). 0.0 disables the
    /// floor entirely; the design's `MIN_TIMED_WINDOW_SECONDS = 20.0`
    /// is the real-CH default that row 7.2+ adopt.
    #[arg(long, default_value_t = 0.0)]
    min_timed_window_seconds: f64,

    /// Warmup-drain deadline (seconds).
    #[arg(long, default_value_t = 60)]
    warmup_deadline_secs: u64,

    /// Timed-window drain deadline (seconds).
    #[arg(long, default_value_t = 600)]
    timed_deadline_secs: u64,

    /// Output directory (the run sub-dir
    /// `<UTC-stamp>-<change-slug>/` is created under this path).
    #[arg(long, default_value = "bench-results/phase07/7.1-port-baseline")]
    output_dir: PathBuf,

    /// Sub-directory slug appended to the UTC stamp.
    #[arg(long, default_value = "baseline")]
    change_slug: String,

    /// Free-form notes captured in `metadata.notes`.
    #[arg(long, default_value = "")]
    notes: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,phase07=info")),
        )
        .with_target(false)
        .compact()
        .try_init()
        .ok();

    let args = Args::parse();
    let cfg = StageLatenciesConfig {
        records_per_source_range: args.records_per_source_range,
        warmup_payloads: args.warmup_payloads,
        timed_payloads: args.timed_payloads,
        iterations: args.iterations,
        min_timed_window_seconds: args.min_timed_window_seconds,
        warmup_deadline: Duration::from_secs(args.warmup_deadline_secs),
        timed_deadline: Duration::from_secs(args.timed_deadline_secs),
        output_dir: args.output_dir,
        change_slug: args.change_slug,
        notes: args.notes,
        ..Default::default()
    };

    let run = run_stage_latencies(cfg).await?;
    println!("wrote {}", run.run_dir.display());
    Ok(())
}
