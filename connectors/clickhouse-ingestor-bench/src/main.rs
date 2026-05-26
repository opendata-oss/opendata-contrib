//! `clickhouse-ingestor-bench` binary. Runs the in-memory
//! correctness smoke and writes `correctness.json` + witness JSONL
//! files under
//! `<bench-results-root>/correctness-smoke/<UTC>-smoke/`.
//!
//! Real perf-test runs against ClickHouse + S3 are operator work;
//! the binary here is wired for the smoke run.

use std::path::PathBuf;

use clap::Parser;
use clickhouse_ingestor_bench::correctness::{run_dir_timestamp, run_smoke};

#[derive(Debug, Parser)]
#[command(
    name = "clickhouse-ingestor-bench",
    about = "Correctness smoke for the OpenData → ClickHouse runtime"
)]
struct Args {
    /// Root directory for bench output. Defaults to `bench-results/`
    /// relative to the current working directory.
    #[arg(long, default_value = "bench-results")]
    bench_results_root: PathBuf,

    /// Optional override for the run directory name. Useful for
    /// CI to pin a stable name across runs; otherwise the bench
    /// uses `<UTC>-smoke`.
    #[arg(long)]
    run_name: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,opendata_ingest_runtime=info".into()),
        )
        .init();

    let args = Args::parse();

    let run_name = args
        .run_name
        .unwrap_or_else(|| format!("{}-smoke", run_dir_timestamp()));
    let run_dir = args
        .bench_results_root
        .join("correctness-smoke")
        .join(&run_name);
    std::fs::create_dir_all(&run_dir)?;

    eprintln!(
        "clickhouse-ingestor-bench: writing to {}",
        run_dir.display()
    );

    let report = run_smoke(&run_dir).await?;

    let pass_count = report
        .ack_invariant_checks
        .iter()
        .filter(|c| c.passed)
        .count();
    let total = report.ack_invariant_checks.len();
    eprintln!(
        "clickhouse-ingestor-bench: {pass_count}/{total} checks passed; \
         correctness.json at {}",
        run_dir.join("correctness.json").display()
    );

    let gate = pass_count == total
        && report.summary.duplicate_records_post_dedupe == 0
        && report
            .sources
            .values()
            .all(|s| s.records_missing_from_sink == 0);
    if !gate {
        eprintln!("clickhouse-ingestor-bench: gate FAILED");
        std::process::exit(1);
    }
    eprintln!("clickhouse-ingestor-bench: gate PASSED");
    Ok(())
}
