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

/// Filter a point list by comma-separated id. Empty input returns
/// every point; unknown ids return an error so a typo doesn't
/// silently produce an empty matrix.
pub fn filter_points_by_id(all_points: Vec<MatrixPoint>, only: &str) -> Result<Vec<MatrixPoint>> {
    let trimmed = only.trim();
    if trimmed.is_empty() {
        return Ok(all_points);
    }
    let wanted: std::collections::HashSet<&str> = trimmed.split(',').map(str::trim).collect();
    let known: std::collections::HashSet<String> =
        all_points.iter().map(|p| p.id.clone()).collect();
    let unknown: Vec<&str> = wanted
        .iter()
        .filter(|id| !known.contains(**id))
        .copied()
        .collect();
    if !unknown.is_empty() {
        bail!(
            "--only-points: unknown point id(s) {:?}; known ids = {:?}",
            unknown,
            known,
        );
    }
    let filtered: Vec<MatrixPoint> = all_points
        .into_iter()
        .filter(|p| wanted.contains(p.id.as_str()))
        .collect();
    if filtered.is_empty() {
        bail!("--only-points produced an empty point set — pass at least one valid id");
    }
    Ok(filtered)
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

    let results = build_matrix_results(points);
    let timeseries = build_matrix_timeseries(points);
    let correctness = build_matrix_correctness(points);

    std::fs::write(
        run_dir.join("matrix.json"),
        serde_json::to_string_pretty(&matrix)?,
    )?;
    std::fs::write(
        run_dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata)?,
    )?;
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
    Ok(())
}

/// Parent `results.json` per benchmarks.md `matrix.json` Schema:
/// a `points` map keyed by point id, each entry carrying the
/// design's per-point scalars (`total_throughput_records_per_sec`,
/// `serialize_fraction_of_insert`, `serialization_bytes_per_record`,
/// `worker_utilization[*]`, `end_to_end_seconds_per_record`).
pub fn build_matrix_results(points: &[MatrixPointResult]) -> Value {
    let mut points_map = serde_json::Map::with_capacity(points.len());
    for p in points {
        let throughput = if p.reports.is_empty() {
            0.0
        } else {
            p.reports
                .iter()
                .map(|r| {
                    if r.elapsed_seconds <= 0.0 {
                        0.0
                    } else {
                        r.records_processed as f64 / r.elapsed_seconds
                    }
                })
                .sum::<f64>()
                / p.reports.len() as f64
        };
        let elapsed_median = median_of_iter(p.reports.iter().map(|r| r.elapsed_seconds));
        let mut stage_utils = serde_json::Map::with_capacity(4);
        for s in ["source", "fetch", "decode", "sink_dispatch"] {
            let stage = crate::stage_latencies::Stage::parse(s).expect("static stage label");
            stage_utils.insert(
                s.to_string(),
                Value::from(median_of_iter(
                    p.reports.iter().map(|r| r.worker_utilization(stage)),
                )),
            );
        }
        points_map.insert(
            p.point.id.clone(),
            json!({
                "coordinates": p.point.coordinates(),
                "subdir":      p.point.id,
                "correctness_passed": p.correctness_passed,
                "iterations": p.reports.len() as i64,
                "scalars": {
                    "total_throughput_records_per_sec": throughput,
                    "iteration_elapsed_seconds_median": elapsed_median,
                    "serialize_fraction_of_insert":
                        median_of_iter(p.reports.iter().map(|r| r.serialize_fraction_of_insert())),
                    "serialize_seconds_per_record":
                        median_of_iter(p.reports.iter().map(|r| r.serialize_seconds_per_record())),
                    "serialization_bytes_per_record":
                        median_of_iter(p.reports.iter().map(|r| r.serialization_bytes_per_record())),
                    "end_to_end_seconds_per_record":
                        median_of_iter(p.reports.iter().map(|r| r.end_to_end_seconds_per_record())),
                    "per_record_service_time": {
                        "source":        median_of_iter(p.reports.iter().map(|r| r.per_record_service_time(crate::stage_latencies::Stage::Source))),
                        "fetch":         median_of_iter(p.reports.iter().map(|r| r.per_record_service_time(crate::stage_latencies::Stage::Fetch))),
                        "decode":        median_of_iter(p.reports.iter().map(|r| r.per_record_service_time(crate::stage_latencies::Stage::Decode))),
                        "sink_dispatch": median_of_iter(p.reports.iter().map(|r| r.per_record_service_time(crate::stage_latencies::Stage::SinkDispatch))),
                    },
                    "worker_utilization": Value::Object(stage_utils),
                },
            }),
        );
    }
    json!({
        "schema_version": 2,
        "experiment_kind": "matrix",
        "points": Value::Object(points_map),
        // Empty top-level `scalars` / `stages` / `iterations` — the
        // matrix form is `points`-shaped per the design (one row per
        // matrix point), not the A/B form's aggregated scalars.
        "scalars": {},
        "stages": [],
        "histograms": {},
        "iterations": [],
    })
}

/// Parent `timeseries.json`. The matrix form keeps each point's
/// per-stage histograms tagged with `labels.point_id` so a single
/// downstream parser can iterate every series across the run.
pub fn build_matrix_timeseries(points: &[MatrixPointResult]) -> Value {
    let mut series: Vec<Value> = Vec::new();
    let mut iteration_starts: Vec<u64> = Vec::new();
    for p in points {
        for stage in crate::stage_latencies::Stage::ALL {
            let per_iter: Vec<Vec<f64>> = p
                .reports
                .iter()
                .map(|r| r.stage_samples.get(stage).to_vec())
                .collect();
            series.push(sample_index_series(
                "runtime_stage_latency_seconds",
                json!({
                    "alignment": "sample_index",
                    "stage": stage.as_label(),
                    "point_id": p.point.id,
                    "format":   p.point.serialization_format.as_label(),
                    "http_mode": p.point.http_client_mode.as_label(),
                }),
                &per_iter,
            ));
        }
        for r in &p.reports {
            iteration_starts.push(r.timed_started_unix_ms);
        }
    }
    json!({
        "schema_version": 2,
        "window_seconds": 1.0,
        "iterations_aggregated": iteration_starts.len() as i64,
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

/// Parent `correctness.json`. The matrix run's union shape:
/// `summary.passed` reflects every-point-passing; `points`
/// surfaces each point's counts gate + the per-iteration
/// records_processed_timed / records_processed_cumulative /
/// records_visible_in_clickhouse / dedupe counts so a reviewer can
/// audit the gate without diving into each per-point subdir.
pub fn build_matrix_correctness(points: &[MatrixPointResult]) -> Value {
    let all_passed = points.iter().all(|p| p.correctness_passed);
    let mut points_map = serde_json::Map::with_capacity(points.len());
    for p in points {
        let iters: Vec<Value> = p
            .reports
            .iter()
            .map(|r| {
                json!({
                    "iteration":                  r.iteration,
                    "records_processed_timed":    r.records_processed,
                    "records_processed_cumulative": r.records_processed_cumulative,
                    "records_raw_in_clickhouse":  r.records_raw,
                    "records_visible_in_clickhouse": r.records_visible,
                    "pre_dedupe_duplicates":      r.pre_dedupe_dupes,
                    "post_dedupe_duplicates":     r.post_dedupe_dupes,
                    "records_missing_from_sink":
                        r.records_processed_cumulative.saturating_sub(r.records_visible),
                })
            })
            .collect();
        points_map.insert(
            p.point.id.clone(),
            json!({
                "coordinates": p.point.coordinates(),
                "subdir":      p.point.id,
                "passed":      p.correctness_passed,
                "iterations":  iters,
            }),
        );
    }
    json!({
        "schema_version": 2,
        "summary": {
            "passed": all_passed,
            "matrix_points": points.len() as i64,
            "required_sinks": ["clickhouse"],
            "ack_invariant_checks_reported": 0,
            "notes": "Phase 7.6 lightweight: counts-only correctness across every point. Full `ack_invariant_checks` against the production ClickHouseSink land alongside the TestObservableSink trait refactor.",
        },
        "points": Value::Object(points_map),
    })
}

/// Read the per-point `results.json` / `correctness.json` for an
/// already-completed matrix run dir and emit the parent
/// `results.json` / `timeseries.json` / `correctness.json` per the
/// design's benchmarks.md contract. Used to backfill canonical
/// parent artifacts for runs that completed before the
/// `write_matrix_index` extension landed (rev-2 series of the row
/// 7.6 sweep). Read-only against the per-point bundles; only writes
/// the three parent files at `run_dir`.
pub fn backfill_parent_aggregates(run_dir: &Path) -> Result<()> {
    let matrix_path = run_dir.join("matrix.json");
    let raw = std::fs::read_to_string(&matrix_path)
        .with_context(|| format!("read {}", matrix_path.display()))?;
    let matrix: Value =
        serde_json::from_str(&raw).with_context(|| format!("parse {}", matrix_path.display()))?;
    let points_arr = matrix["points"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("matrix.json: missing `points` array"))?;

    let mut results_points = serde_json::Map::with_capacity(points_arr.len());
    let mut correctness_points = serde_json::Map::with_capacity(points_arr.len());
    let mut series: Vec<Value> = Vec::new();
    let mut iteration_starts: Vec<u64> = Vec::new();
    let mut all_passed = true;

    for p in points_arr {
        let id = p["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("matrix.json point missing id"))?;
        let subdir_top = run_dir.join(id);
        let point_run_dir = first_subdir(&subdir_top)?;

        // Per-point results.json — extract scalars, build parent
        // results.points[id] from them.
        let pr: Value = read_json(&point_run_dir.join("results.json"))?;
        let pc: Value = read_json(&point_run_dir.join("correctness.json"))?;
        let pt: Value = read_json(&point_run_dir.join("timeseries.json"))?;

        let scalars = &pr["scalars"];
        let pt_passed = pc["iterations"]
            .as_array()
            .and_then(|it| it.first())
            .and_then(|i| i["passed"].as_bool())
            .unwrap_or(false);
        all_passed &= pt_passed;

        let coord = &p["coordinates"];
        results_points.insert(
            id.to_string(),
            json!({
                "coordinates": coord.clone(),
                "subdir":      id,
                "correctness_passed": pt_passed,
                "iterations":  pr["iterations"].as_array().map(|a| a.len() as i64).unwrap_or(0),
                "scalars": {
                    "total_throughput_records_per_sec": scalars["total_throughput_records_per_sec"]["median"].clone(),
                    "iteration_elapsed_seconds_median":  scalars["iteration_elapsed_seconds"]["median"].clone(),
                    "serialize_fraction_of_insert":      scalars["serialize_fraction_of_insert"]["median"].clone(),
                    "serialize_seconds_per_record":      scalars["serialize_seconds_per_record"]["median"].clone(),
                    "serialization_bytes_per_record":    scalars["serialization_bytes_per_record"]["median"].clone(),
                    "end_to_end_seconds_per_record":     scalars["end_to_end_seconds_per_record"]["median"].clone(),
                    "per_record_service_time": {
                        "source":        scalars["per_record_service_time_source"]["median"].clone(),
                        "fetch":         scalars["per_record_service_time_fetch"]["median"].clone(),
                        "decode":        scalars["per_record_service_time_decode"]["median"].clone(),
                        "sink_dispatch": scalars["per_record_service_time_sink_dispatch"]["median"].clone(),
                    },
                    "worker_utilization": {
                        "source":        scalars["worker_utilization_source"]["median"].clone(),
                        "fetch":         scalars["worker_utilization_fetch"]["median"].clone(),
                        "decode":        scalars["worker_utilization_decode"]["median"].clone(),
                        "sink_dispatch": scalars["worker_utilization_sink_dispatch"]["median"].clone(),
                    },
                },
            }),
        );

        // Tag every per-point timeseries series with the point id +
        // coordinates so the parent timeseries.json is one
        // self-describing stream.
        if let Some(point_series) = pt["series"].as_array() {
            for s in point_series {
                let mut s = s.clone();
                if let Some(labels) = s.get_mut("labels").and_then(|l| l.as_object_mut()) {
                    labels.insert("point_id".into(), Value::String(id.to_string()));
                    if let Some(fmt) = coord["serialization_format"].as_str() {
                        labels.insert("format".into(), Value::String(fmt.to_string()));
                    }
                    if let Some(http) = coord["http_client_mode"].as_str() {
                        labels.insert("http_mode".into(), Value::String(http.to_string()));
                    }
                }
                series.push(s);
            }
        }
        if let Some(starts) = pt["iteration_starts_unix_ms"].as_array() {
            for s in starts {
                if let Some(n) = s.as_u64() {
                    iteration_starts.push(n);
                }
            }
        }

        // Union correctness — carry every per-point iteration's
        // counts straight through, keep the same field names the
        // build-from-reports path emits so consumers don't have to
        // branch on which writer ran.
        let mut iters_out: Vec<Value> = Vec::new();
        if let Some(its) = pc["iterations"].as_array() {
            for i in its {
                iters_out.push(json!({
                    "iteration":                  i["iteration"].clone(),
                    "records_processed_timed":    i["records_processed_timed"].clone(),
                    "records_processed_cumulative": i["records_processed_cumulative"].clone(),
                    "records_raw_in_clickhouse":  i["records_raw_in_clickhouse"].clone(),
                    "records_visible_in_clickhouse": i["records_visible_in_clickhouse"].clone(),
                    "pre_dedupe_duplicates":      i["pre_dedupe_duplicates"].clone(),
                    "post_dedupe_duplicates":     i["post_dedupe_duplicates"].clone(),
                    "records_missing_from_sink":  i["records_missing_from_sink"].clone(),
                }));
            }
        }
        correctness_points.insert(
            id.to_string(),
            json!({
                "coordinates": coord.clone(),
                "subdir":      id,
                "passed":      pt_passed,
                "iterations":  iters_out,
            }),
        );
    }

    let results = json!({
        "schema_version": 2,
        "experiment_kind": "matrix",
        "points": Value::Object(results_points),
        "scalars": {},
        "stages": [],
        "histograms": {},
        "iterations": [],
    });
    let timeseries = json!({
        "schema_version": 2,
        "window_seconds": 1.0,
        "iterations_aggregated": iteration_starts.len() as i64,
        "iteration_starts_unix_ms": iteration_starts,
        "series": series,
    });
    let correctness = json!({
        "schema_version": 2,
        "summary": {
            "passed": all_passed,
            "matrix_points": points_arr.len() as i64,
            "required_sinks": ["clickhouse"],
            "ack_invariant_checks_reported": 0,
            "notes": "Phase 7.6 lightweight: counts-only correctness across every point. Full `ack_invariant_checks` against the production ClickHouseSink land alongside the TestObservableSink trait refactor.",
        },
        "points": Value::Object(correctness_points),
    });

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
    Ok(())
}

fn first_subdir(parent: &Path) -> Result<PathBuf> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(parent)
        .with_context(|| format!("read_dir {}", parent.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    entries.sort();
    entries
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no per-iteration subdir under {}", parent.display()))
}

fn read_json(path: &Path) -> Result<Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))
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

// ===========================================================================
// Tests — pure in-memory, no Docker required.
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage_latencies::iteration::{ClickHouseSamples, StageSamples, StageWorkerCounts};

    fn sample_report(
        iteration: usize,
        ts_ms: u64,
        elapsed: f64,
        records: u64,
    ) -> RealChIterationReport {
        RealChIterationReport {
            iteration,
            timed_started_unix_ms: ts_ms,
            elapsed_seconds: elapsed,
            records_processed: records,
            records_processed_cumulative: records + 200, // 200-payload warmup
            stage_samples: StageSamples {
                source: vec![1.0, 1.1, 1.2],
                fetch: vec![0.01, 0.02],
                decode: vec![0.5, 0.6, 0.7],
                sink_dispatch: vec![0.001, 0.002],
            },
            clickhouse_samples: ClickHouseSamples {
                serialize_duration_seconds: vec![0.1],
                insert_duration_seconds: vec![1.0],
                serialized_bytes: vec![201_000.0],
                chunk_rows: vec![1000.0],
            },
            worker_counts: StageWorkerCounts {
                source: 1,
                fetch: 8,
                decode: 4,
                sink_dispatch: 4,
            },
            records_visible: records + 200,
            records_raw: records + 200,
            pre_dedupe_dupes: 0,
            post_dedupe_dupes: 0,
        }
    }

    fn sample_point_result(
        id: &str,
        format: SerializationFormat,
        http: HttpClientMode,
    ) -> MatrixPointResult {
        MatrixPointResult {
            point: MatrixPoint {
                id: id.to_string(),
                serialization_format: format,
                http_client_mode: http,
            },
            correctness_passed: true,
            reports: vec![sample_report(1, 1_700_000_000_000, 30.0, 100_000)],
        }
    }

    #[test]
    fn filter_points_by_id_empty_returns_all() {
        let all = default_format_x_http_points();
        let got = filter_points_by_id(all.clone(), "").expect("empty returns all");
        assert_eq!(got.len(), 4);
    }

    #[test]
    fn filter_points_by_id_picks_named_subset() {
        let all = default_format_x_http_points();
        let got = filter_points_by_id(all, "point-004").expect("known id");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].id, "point-004");
        assert_eq!(got[0].serialization_format, SerializationFormat::RowBinary);
    }

    #[test]
    fn filter_points_by_id_accepts_multiple_ids_comma_separated() {
        let all = default_format_x_http_points();
        let got = filter_points_by_id(all, "point-001,point-003").expect("two known ids");
        let mut ids: Vec<&str> = got.iter().map(|p| p.id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["point-001", "point-003"]);
    }

    #[test]
    fn filter_points_by_id_rejects_unknown_id() {
        let all = default_format_x_http_points();
        let err = filter_points_by_id(all, "point-999").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("point-999"),
            "expected unknown id in msg: {msg}"
        );
        assert!(
            msg.contains("known ids"),
            "expected known list in msg: {msg}"
        );
    }

    #[test]
    fn filter_points_by_id_rejects_partial_unknown() {
        // Even if some ids are valid, any unknown id fails fast so
        // typos don't silently drop expected points.
        let all = default_format_x_http_points();
        let err = filter_points_by_id(all, "point-001,point-typo").unwrap_err();
        assert!(format!("{err}").contains("point-typo"));
    }

    #[test]
    fn build_matrix_results_emits_points_map_with_design_scalars() {
        let points = vec![
            sample_point_result(
                "point-001",
                SerializationFormat::JsonEachRow,
                HttpClientMode::PerCall,
            ),
            sample_point_result(
                "point-004",
                SerializationFormat::RowBinary,
                HttpClientMode::Pooled {
                    pool_max_idle_per_host: 16,
                    pool_idle_timeout_ms: 30_000,
                },
            ),
        ];
        let results = build_matrix_results(&points);
        assert_eq!(results["schema_version"], 2);
        assert_eq!(results["experiment_kind"], "matrix");
        // `points` is a map keyed by point id, not an array.
        let points_obj = results["points"].as_object().expect("points map");
        assert!(points_obj.contains_key("point-001"));
        assert!(points_obj.contains_key("point-004"));

        let p1 = &points_obj["point-001"];
        assert_eq!(p1["coordinates"]["serialization_format"], "json_each_row");
        assert_eq!(p1["coordinates"]["http_client_mode"], "per_call");
        let p1s = &p1["scalars"];
        // Throughput: 100k records / 30s ≈ 3333 rec/s.
        let throughput = p1s["total_throughput_records_per_sec"].as_f64().unwrap();
        assert!(
            throughput > 3000.0 && throughput < 4000.0,
            "throughput {throughput}"
        );
        // serialize_fraction_of_insert = 0.1 / 1.0 = 0.1.
        assert!((p1s["serialize_fraction_of_insert"].as_f64().unwrap() - 0.1).abs() < 1e-9);
        // 201_000 bytes / 100_000 records = 2.01.
        assert!((p1s["serialization_bytes_per_record"].as_f64().unwrap() - 2.01).abs() < 1e-9);
        // Worker utilization map must carry all four stages.
        let wu = p1s["worker_utilization"].as_object().expect("wu map");
        for stage in ["source", "fetch", "decode", "sink_dispatch"] {
            assert!(wu.contains_key(stage), "missing wu key {stage}");
        }
    }

    #[test]
    fn build_matrix_timeseries_tags_series_with_point_coordinates() {
        let points = vec![sample_point_result(
            "point-002",
            SerializationFormat::JsonEachRow,
            HttpClientMode::Pooled {
                pool_max_idle_per_host: 16,
                pool_idle_timeout_ms: 30_000,
            },
        )];
        let ts = build_matrix_timeseries(&points);
        assert_eq!(ts["schema_version"], 2);
        let series = ts["series"].as_array().expect("series array");
        // 1 point × 4 stages = 4 series.
        assert_eq!(series.len(), 4);
        for s in series {
            let labels = s["labels"].as_object().expect("labels");
            assert_eq!(labels["point_id"], "point-002");
            assert_eq!(labels["format"], "json_each_row");
            assert_eq!(labels["http_mode"], "pooled");
            assert!(labels.contains_key("stage"));
            assert_eq!(labels["alignment"], "sample_index");
        }
    }

    #[test]
    fn build_matrix_correctness_union_passed_when_all_passed() {
        let mut points = vec![
            sample_point_result(
                "point-001",
                SerializationFormat::JsonEachRow,
                HttpClientMode::PerCall,
            ),
            sample_point_result(
                "point-002",
                SerializationFormat::JsonEachRow,
                HttpClientMode::PerCall,
            ),
        ];
        points[0].correctness_passed = true;
        points[1].correctness_passed = true;
        let correctness = build_matrix_correctness(&points);
        assert_eq!(correctness["schema_version"], 2);
        assert_eq!(correctness["summary"]["passed"], true);
        assert_eq!(correctness["summary"]["matrix_points"], 2);
        let pts = correctness["points"].as_object().expect("points map");
        assert!(pts.contains_key("point-001"));
        assert!(pts.contains_key("point-002"));
        assert_eq!(pts["point-001"]["passed"], true);
    }

    #[test]
    fn build_matrix_correctness_union_fails_when_any_point_fails() {
        let mut points = vec![
            sample_point_result(
                "point-001",
                SerializationFormat::JsonEachRow,
                HttpClientMode::PerCall,
            ),
            sample_point_result(
                "point-002",
                SerializationFormat::RowBinary,
                HttpClientMode::PerCall,
            ),
        ];
        points[0].correctness_passed = true;
        points[1].correctness_passed = false;
        let correctness = build_matrix_correctness(&points);
        assert_eq!(correctness["summary"]["passed"], false);
        assert_eq!(correctness["points"]["point-001"]["passed"], true);
        assert_eq!(correctness["points"]["point-002"]["passed"], false);
    }

    #[test]
    fn write_matrix_index_emits_all_four_parent_artifacts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = MatrixConfig {
            workload: LogWorkloadConfig::default(),
            iterations: 1,
            warmup_deadline: Duration::from_secs(60),
            timed_deadline: Duration::from_secs(300),
            min_timed_window_seconds: 0.0,
            output_dir: dir.path().to_path_buf(),
            change_slug: "test".to_string(),
            notes: "matrix-artifact-writer smoke test".to_string(),
            points: default_format_x_http_points(),
        };
        let points = vec![
            sample_point_result(
                "point-001",
                SerializationFormat::JsonEachRow,
                HttpClientMode::PerCall,
            ),
            sample_point_result(
                "point-004",
                SerializationFormat::RowBinary,
                HttpClientMode::Pooled {
                    pool_max_idle_per_host: 16,
                    pool_idle_timeout_ms: 30_000,
                },
            ),
        ];
        write_matrix_index(
            dir.path(),
            &cfg,
            &points,
            1_700_000_000_000,
            1_700_000_030_000,
        )
        .expect("write_matrix_index");
        for name in [
            "matrix.json",
            "metadata.json",
            "results.json",
            "timeseries.json",
            "correctness.json",
        ] {
            let p = dir.path().join(name);
            assert!(p.exists(), "expected {name} at {}", p.display());
            let body = std::fs::read_to_string(&p).expect("read");
            let _v: Value = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("invalid JSON in {name}: {e}"));
        }
        // matrix.json contains skip_summary (design contract).
        let matrix: Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("matrix.json")).expect("read matrix.json"),
        )
        .expect("parse matrix.json");
        assert!(
            matrix["skip_summary"].is_object(),
            "matrix.json missing skip_summary"
        );
        let results: Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("results.json")).expect("read results.json"),
        )
        .expect("parse results.json");
        assert!(results["points"]["point-001"].is_object());
        assert!(results["points"]["point-004"].is_object());
    }
}
