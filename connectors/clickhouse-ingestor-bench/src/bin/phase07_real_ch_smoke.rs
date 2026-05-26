//! Real-ClickHouse bench binary.
//!
//! Spins up a `testcontainers` ClickHouse, runs the production
//! `Runtime::builder` against it end-to-end via the Iteration
//! Protocol, and writes the benchmarks.md v2 artifact bundle plus a
//! counts-based `correctness.json` to the requested output dir.
//!
//! Requires Docker. Use `cargo build -p clickhouse-ingestor-bench
//! --features real-ch --bin phase07-real-ch-smoke` to build.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use clickhouse_ingestor_bench::real_ch::{RealChConfig, RealClickHouseFixture, run_real_ch};
use opendata_ingest_clickhouse::adapter::logs::LogsAdapterConfig;
use opendata_ingest_runtime::source::SourceId;

#[derive(Parser, Debug)]
#[command(
    name = "phase07-real-ch-smoke",
    about = "Real-ClickHouse bench (testcontainers)."
)]
struct Args {
    #[arg(long, default_value_t = 100)]
    records_per_source_range: usize,

    #[arg(long, default_value_t = 5)]
    warmup_payloads: usize,

    #[arg(long, default_value_t = 50)]
    timed_payloads: usize,

    #[arg(long, default_value_t = 1)]
    iterations: usize,

    /// Minimum timed-window duration (seconds). 0.0 disables. The
    /// design pins 20.0 s for production runs; the smoke binary
    /// defaults to 0.0 so a tiny workload doesn't error out.
    #[arg(long, default_value_t = 0.0)]
    min_timed_window_seconds: f64,

    #[arg(long, default_value_t = 120)]
    warmup_deadline_secs: u64,

    #[arg(long, default_value_t = 600)]
    timed_deadline_secs: u64,

    /// `max_chunk_rows` for the adapter. The adapter chunks each
    /// SinkCommit's records into HTTP INSERTs of at most this many
    /// rows.
    #[arg(long, default_value_t = 100_000)]
    adapter_max_chunk_rows: usize,

    /// `max_chunk_bytes` for the adapter.
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    adapter_max_chunk_bytes: usize,

    #[arg(long, default_value = "bench-results/phase07/7.2-real-ch-smoke")]
    output_dir: PathBuf,

    #[arg(long, default_value = "baseline")]
    change_slug: String,

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

    let adapter_cfg = LogsAdapterConfig {
        database: "phase07_bench".into(),
        table: "logs".into(),
        adapter_version: 1,
        max_chunk_rows: args.adapter_max_chunk_rows,
        max_chunk_bytes: args.adapter_max_chunk_bytes,
        insert_quorum: None,
        apply_deduplication_token: false,
    };
    let fixture = RealClickHouseFixture::setup_testcontainers(
        adapter_cfg.database.clone(),
        adapter_cfg.table.clone(),
        adapter_cfg,
    )
    .await?;
    eprintln!("clickhouse endpoint: {}", fixture.endpoint);

    let mut cfg = RealChConfig {
        iterations: args.iterations,
        min_timed_window_seconds: args.min_timed_window_seconds,
        warmup_deadline: Duration::from_secs(args.warmup_deadline_secs),
        timed_deadline: Duration::from_secs(args.timed_deadline_secs),
        output_dir: args.output_dir,
        change_slug: args.change_slug,
        notes: args.notes,
        ..Default::default()
    };
    cfg.workload.source_id = SourceId::from("phase07-real-ch");
    cfg.workload.records_per_source_range = args.records_per_source_range;
    cfg.workload.warmup_payloads = args.warmup_payloads;
    cfg.workload.timed_payloads = args.timed_payloads;

    let run = run_real_ch(cfg, &fixture).await?;
    println!("wrote {}", run.run_dir.display());
    if !run.correctness_passed {
        eprintln!("WARN: correctness gate did not pass — see correctness.json");
        std::process::exit(2);
    }
    Ok(())
}
