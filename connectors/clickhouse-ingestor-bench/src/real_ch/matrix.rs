//! Phase 7.6 matrix runner (lightweight). Sweeps a small product
//! of `(SerializationFormat, HttpClientMode)` against a shared
//! testcontainers ClickHouse. The full 6-axis grid the design
//! specifies (workload × concurrency × chunk_rows × chunk_bytes ×
//! format × http_mode) is deferred; the 4-point format × http_mode
//! sweep is enough for row 7.7's bottleneck classification to
//! disambiguate `serialize_cpu_bound` from `http_io_bound` vs
//! `balanced` at the same workload.
//!
//! Each matrix point reuses the §Iteration Protocol via
//! `runner::run_real_ch` and emits its own results.json /
//! correctness.json bundle, plus a parent `matrix.json` index.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clickhouse_ingestor::writer::HttpClientMode;
use opendata_ingest_clickhouse::serializer::SerializationFormat;
use serde_json::{Value, json};

use super::fixture::RealClickHouseFixture;
use super::runner::{RealChConfig, RealChIterationReport, run_real_ch};
use super::workload::LogWorkloadConfig;

/// One axis-value combination the matrix runner walks.
#[derive(Debug, Clone)]
pub struct MatrixPoint {
    pub id: String,
    pub serialization_format: SerializationFormat,
    pub http_client_mode: HttpClientMode,
}

impl MatrixPoint {
    fn coordinates(&self) -> Value {
        json!({
            "serialization_format": self.serialization_format.as_label(),
            "http_client_mode":     self.http_client_mode.as_label(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct MatrixConfig {
    pub workload: LogWorkloadConfig,
    pub iterations: usize,
    pub warmup_deadline: Duration,
    pub timed_deadline: Duration,
    pub min_timed_window_seconds: f64,
    pub output_dir: PathBuf,
    pub change_slug: String,
    pub notes: String,
    /// Realized matrix points. Defaults to the 4-point format ×
    /// http_mode sweep; callers can plug in a different list.
    pub points: Vec<MatrixPoint>,
}

impl Default for MatrixConfig {
    fn default() -> Self {
        Self {
            workload: LogWorkloadConfig::default(),
            iterations: 1,
            warmup_deadline: Duration::from_secs(120),
            timed_deadline: Duration::from_secs(600),
            min_timed_window_seconds: 0.0,
            output_dir: PathBuf::from("bench-results/phase07/7.6-chunk-and-concurrency-sweep"),
            change_slug: "format-x-http".to_string(),
            notes: String::new(),
            points: default_format_x_http_points(),
        }
    }
}

/// 4 points: {JSONEachRow, RowBinary} × {PerCall, Pooled}.
pub fn default_format_x_http_points() -> Vec<MatrixPoint> {
    let pool = HttpClientMode::Pooled {
        pool_max_idle_per_host: 16,
        pool_idle_timeout_ms: 30_000,
    };
    vec![
        MatrixPoint {
            id: "point-001".to_string(),
            serialization_format: SerializationFormat::JsonEachRow,
            http_client_mode: HttpClientMode::PerCall,
        },
        MatrixPoint {
            id: "point-002".to_string(),
            serialization_format: SerializationFormat::JsonEachRow,
            http_client_mode: pool.clone(),
        },
        MatrixPoint {
            id: "point-003".to_string(),
            serialization_format: SerializationFormat::RowBinary,
            http_client_mode: HttpClientMode::PerCall,
        },
        MatrixPoint {
            id: "point-004".to_string(),
            serialization_format: SerializationFormat::RowBinary,
            http_client_mode: pool,
        },
    ]
}

#[derive(Debug)]
pub struct MatrixRunArtifacts {
    pub run_dir: PathBuf,
    pub points: Vec<MatrixPointResult>,
}

#[derive(Debug, Clone)]
pub struct MatrixPointResult {
    pub point: MatrixPoint,
    pub correctness_passed: bool,
    pub reports: Vec<RealChIterationReport>,
}

/// Drive every point in `cfg.points` against the shared fixture.
/// Writes one matrix.json index + per-point subdirectories.
pub async fn run_matrix(
    cfg: MatrixConfig,
    fixture: &RealClickHouseFixture,
) -> Result<MatrixRunArtifacts> {
    if cfg.points.is_empty() {
        bail!(
            "matrix has zero points to run — check `--only-points` filter against the registered point IDs"
        );
    }
    let run_id = run_id_slug(&cfg.change_slug);
    let run_dir = cfg.output_dir.join(&run_id);
    std::fs::create_dir_all(&run_dir).with_context(|| format!("mkdir -p {}", run_dir.display()))?;

    let mut points_out: Vec<MatrixPointResult> = Vec::with_capacity(cfg.points.len());
    let started_unix_ms = now_unix_ms();

    for point in &cfg.points {
        eprintln!(
            "[matrix] running {} (format={}, http_mode={})",
            point.id,
            point.serialization_format.as_label(),
            point.http_client_mode.as_label(),
        );
        let point_dir = run_dir.join(&point.id);
        let run_cfg = RealChConfig {
            workload: cfg.workload.clone(),
            iterations: cfg.iterations,
            runtime_options: opendata_ingest_runtime::runtime::RuntimeOptions {
                dry_run: false,
                ack_flush_policy:
                    opendata_ingest_runtime::runtime::AckFlushPolicy::EveryCommitGroup,
                poll_interval: Duration::from_millis(5),
                ..Default::default()
            },
            serialization_format: point.serialization_format,
            http_client_mode: point.http_client_mode.clone(),
            warmup_deadline: cfg.warmup_deadline,
            timed_deadline: cfg.timed_deadline,
            min_timed_window_seconds: cfg.min_timed_window_seconds,
            output_dir: point_dir.clone(),
            change_slug: point.id.clone(),
            notes: format!(
                "Phase 7.6 matrix point {}; format={}, http_mode={}",
                point.id,
                point.serialization_format.as_label(),
                point.http_client_mode.as_label(),
            ),
        };
        let run = run_real_ch(run_cfg, fixture).await?;
        points_out.push(MatrixPointResult {
            point: point.clone(),
            correctness_passed: run.correctness_passed,
            reports: run.reports,
        });
    }

    let ended_unix_ms = now_unix_ms();
    let any_failed = points_out.iter().any(|p| !p.correctness_passed);
    if any_failed {
        bail!(
            "at least one matrix point failed its correctness gate; see correctness.json under each point's subdir"
        );
    }
    write_matrix_index(&run_dir, &cfg, &points_out, started_unix_ms, ended_unix_ms)?;

    Ok(MatrixRunArtifacts {
        run_dir,
        points: points_out,
    })
}

fn median_of_iter<I: Iterator<Item = f64>>(it: I) -> f64 {
    let mut v: Vec<f64> = it.collect();
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v[v.len() / 2]
}

fn write_matrix_index(
    run_dir: &Path,
    cfg: &MatrixConfig,
    points: &[MatrixPointResult],
    started_unix_ms: u64,
    ended_unix_ms: u64,
) -> Result<()> {
    let mut points_json: Vec<Value> = Vec::with_capacity(points.len());
    for p in points {
        let throughput: f64 = p
            .reports
            .iter()
            .map(|r| {
                if r.elapsed_seconds <= 0.0 {
                    0.0
                } else {
                    r.records_processed as f64 / r.elapsed_seconds
                }
            })
            .sum::<f64>()
            / (p.reports.len() as f64).max(1.0);
        let stage_utils: BTreeMap<&str, f64> = ["source", "fetch", "decode", "sink_dispatch"]
            .into_iter()
            .map(|s| {
                let stage = crate::stage_latencies::Stage::parse(s).expect("static stage label");
                let med = if p.reports.is_empty() {
                    0.0
                } else {
                    let mut vs: Vec<f64> = p
                        .reports
                        .iter()
                        .map(|r| r.worker_utilization(stage))
                        .collect();
                    vs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    vs[vs.len() / 2]
                };
                (s, med)
            })
            .collect();
        let elapsed: Vec<f64> = p.reports.iter().map(|r| r.elapsed_seconds).collect();
        let elapsed_median = if elapsed.is_empty() {
            0.0
        } else {
            let mut e = elapsed.clone();
            e.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            e[e.len() / 2]
        };
        let serialize_fraction_median =
            median_of_iter(p.reports.iter().map(|r| r.serialize_fraction_of_insert()));
        let serialize_seconds_per_record =
            median_of_iter(p.reports.iter().map(|r| r.serialize_seconds_per_record()));
        let serialization_bytes_per_record =
            median_of_iter(p.reports.iter().map(|r| r.serialization_bytes_per_record()));
        let end_to_end_seconds_per_record =
            median_of_iter(p.reports.iter().map(|r| r.end_to_end_seconds_per_record()));
        points_json.push(json!({
            "id":                 p.point.id,
            "coordinates":        p.point.coordinates(),
            "correctness_passed": p.correctness_passed,
            "subdir":             p.point.id,
            "scalars": {
                "total_throughput_records_per_sec": throughput,
                "iteration_elapsed_seconds_median": elapsed_median,
                "worker_utilization": stage_utils,
                "serialize_fraction_of_insert":     serialize_fraction_median,
                "serialize_seconds_per_record":     serialize_seconds_per_record,
                "serialization_bytes_per_record":   serialization_bytes_per_record,
                "end_to_end_seconds_per_record":    end_to_end_seconds_per_record,
                "iterations": p.reports.len() as i64,
            },
        }));
    }

    let metadata = json!({
        "schema_version": 2,
        "phase": "phase07-clickhouse-throughput",
        "unit_id": "7.6",
        "unit_title": "Sweep workload × sink × format × http_mode (matrix experiment per benchmarks.md)",
        "owner": "Benchmark/Perf Implementor",
        "started_at": utc_iso(started_unix_ms),
        "ended_at":   utc_iso(ended_unix_ms),
        "experiment": {
            "kind": "matrix",
            "dimensions": ["serialization_format", "http_client_mode"],
            "fixed_controls": {
                "records_per_source_range": cfg.workload.records_per_source_range,
                "warmup_payloads": cfg.workload.warmup_payloads,
                "timed_payloads":  cfg.workload.timed_payloads,
            },
            "matrix_file": "matrix.json",
        },
        "varied_param": Value::Null,
        "notes": cfg.notes.clone(),
    });
    let matrix = json!({
        "schema_version": 2,
        "dimensions": ["serialization_format", "http_client_mode"],
        "fixed_controls": {
            "records_per_source_range": cfg.workload.records_per_source_range,
            "warmup_payloads": cfg.workload.warmup_payloads,
            "timed_payloads":  cfg.workload.timed_payloads,
        },
        "points": points_json,
        "skip_summary": {
            "rule_1_workload_axis_correctness_check_inert_pairs": 0,
            "notes": "Row 7.6 lightweight: only format × http_mode dimensions; the workload-axis correctness check from the design's full 6-axis sweep doesn't apply here.",
        },
    });

    std::fs::write(
        run_dir.join("matrix.json"),
        serde_json::to_string_pretty(&matrix)?,
    )?;
    std::fs::write(
        run_dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata)?,
    )?;
    Ok(())
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
