//! Phase 7.6 matrix sweep binary. Spins up a `testcontainers`
//! ClickHouse, runs the lightweight 4-point matrix
//! (JSONEachRow/RowBinary × PerCall/Pooled), and writes
//! matrix.json + per-point artifact bundles for row 7.7's
//! bottleneck readout.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use clickhouse_ingestor_bench::real_ch::{
    LogWorkloadConfig, MatrixConfig, RealClickHouseFixture, run_matrix,
};
use opendata_ingest_clickhouse::adapter::logs::LogsAdapterConfig;
use opendata_ingest_runtime::source::SourceId;

#[derive(Parser, Debug)]
#[command(
    name = "phase07-matrix",
    about = "Phase 7.6: format × http_mode matrix sweep (testcontainers)."
)]
struct Args {
    #[arg(long, default_value_t = 1000)]
    records_per_source_range: usize,

    #[arg(long, default_value_t = 10)]
    warmup_payloads: usize,

    #[arg(long, default_value_t = 100)]
    timed_payloads: usize,

    #[arg(long, default_value_t = 1)]
    iterations: usize,

    #[arg(long, default_value_t = 0.0)]
    min_timed_window_seconds: f64,

    #[arg(long, default_value_t = 180)]
    warmup_deadline_secs: u64,

    #[arg(long, default_value_t = 600)]
    timed_deadline_secs: u64,

    #[arg(long, default_value_t = 100_000)]
    adapter_max_chunk_rows: usize,

    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    adapter_max_chunk_bytes: usize,

    #[arg(
        long,
        default_value = "bench-results/phase07/7.6-chunk-and-concurrency-sweep"
    )]
    output_dir: PathBuf,

    #[arg(long, default_value = "format-x-http")]
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
        database: "phase07_matrix".into(),
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

    let mut cfg = MatrixConfig {
        workload: LogWorkloadConfig {
            source_id: SourceId::from("phase07-matrix"),
            records_per_source_range: args.records_per_source_range,
            warmup_payloads: args.warmup_payloads,
            timed_payloads: args.timed_payloads,
            ..Default::default()
        },
        iterations: args.iterations,
        warmup_deadline: Duration::from_secs(args.warmup_deadline_secs),
        timed_deadline: Duration::from_secs(args.timed_deadline_secs),
        min_timed_window_seconds: args.min_timed_window_seconds,
        output_dir: args.output_dir,
        change_slug: args.change_slug,
        notes: args.notes,
        ..Default::default()
    };
    // Re-emit default points so the source_id update on workload
    // is consistent.
    cfg.points = clickhouse_ingestor_bench::real_ch::matrix::default_format_x_http_points();

    let run = run_matrix(cfg, &fixture).await?;
    println!("wrote {}", run.run_dir.display());
    Ok(())
}
