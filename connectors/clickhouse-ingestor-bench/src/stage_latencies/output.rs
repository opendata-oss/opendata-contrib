//! Artifact emission for the Phase 7.1 stage-latency bench. Writes
//! `metadata.json`, `results.json`, `timeseries.json`, and the
//! per-iteration `raw/run-N.metrics.jsonl` files per
//! `plans/odb-high-throughput/benchmarks.md` v2 schemas.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::iteration::{IterationReport, Stage};

/// What the bench-doc surface presents as a "run". One outer driver
/// invocation produces one run dir with N iterations under it.
#[derive(Debug)]
pub struct RunArtifacts {
    pub run_dir: PathBuf,
    pub metadata: Value,
    pub results: Value,
    pub timeseries: Value,
}

#[derive(Debug, Clone)]
pub struct RunInputs<'a> {
    /// E.g. `bench-results/phase07/7.1-port-baseline/`.
    pub output_dir: &'a Path,
    /// Sub-directory slug appended to the UTC stamp. `baseline` →
    /// `2026-05-13T1530-baseline/`.
    pub change_slug: &'a str,
    pub unit_id: &'a str,
    pub unit_title: &'a str,
    pub phase: &'a str,
    pub owner: &'a str,
    pub records_per_source_range: usize,
    pub warmup_payloads: u64,
    pub timed_payloads: u64,
    pub iterations: usize,
    pub notes: &'a str,
}

pub fn write_run(reports: &[IterationReport], inputs: &RunInputs<'_>) -> Result<RunArtifacts> {
    let started_at = utc_iso(
        reports
            .first()
            .map(|r| r.timed_started_unix_ms)
            .unwrap_or(0),
    );
    let ended_unix_ms = now_unix_ms();
    let ended_at = utc_iso(ended_unix_ms);

    let run_id = run_id_slug(inputs.change_slug);
    let run_dir = inputs.output_dir.join(&run_id);
    let raw_dir = run_dir.join("raw");
    std::fs::create_dir_all(&raw_dir).with_context(|| format!("mkdir -p {}", raw_dir.display()))?;

    let results = build_results(reports);
    let timeseries = build_timeseries(reports);
    let metadata = build_metadata(reports, inputs, &started_at, &ended_at);

    std::fs::write(
        run_dir.join("results.json"),
        serde_json::to_string_pretty(&results)?,
    )?;
    std::fs::write(
        run_dir.join("timeseries.json"),
        serde_json::to_string_pretty(&timeseries)?,
    )?;
    std::fs::write(
        run_dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata)?,
    )?;
    for report in reports {
        let path = raw_dir.join(format!("run-{}.metrics.jsonl", report.iteration));
        std::fs::write(&path, render_raw_metrics_jsonl(report))?;
    }

    Ok(RunArtifacts {
        run_dir,
        metadata,
        results,
        timeseries,
    })
}

fn build_results(reports: &[IterationReport]) -> Value {
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
        let median_sec_per_op = median_of(
            &reports
                .iter()
                .flat_map(|r| r.stage_samples.get(stage).iter().copied())
                .collect::<Vec<_>>(),
        );
        let p10_sec_per_op = quantile(
            &reports
                .iter()
                .flat_map(|r| r.stage_samples.get(stage).iter().copied())
                .collect::<Vec<_>>(),
            0.10,
        );
        let p90_sec_per_op = quantile(
            &reports
                .iter()
                .flat_map(|r| r.stage_samples.get(stage).iter().copied())
                .collect::<Vec<_>>(),
            0.90,
        );
        let total_samples: usize = reports
            .iter()
            .map(|r| r.stage_samples.get(stage).len())
            .sum();
        let total_elapsed: f64 = elapsed.iter().sum();
        let ops_per_sec = if total_elapsed <= 0.0 {
            0.0
        } else {
            total_samples as f64 / total_elapsed
        };
        scalars.insert(format!("worker_utilization_{label}"), agg(&utilizations));
        scalars.insert(
            format!("per_record_service_time_{label}"),
            agg(&service_times),
        );
        stages.push(json!({
            "name": label,
            "median_ms_per_op": median_sec_per_op * 1000.0,
            "p10": p10_sec_per_op * 1000.0,
            "p90": p90_sec_per_op * 1000.0,
            "ops_per_sec": ops_per_sec,
            "worker_count": reports.first().map(|r| r.worker_counts.get(stage)).unwrap_or(0),
            "samples": total_samples,
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

fn iteration_block(report: &IterationReport) -> Value {
    let throughput = if report.elapsed_seconds <= 0.0 {
        0.0
    } else {
        report.records_processed as f64 / report.elapsed_seconds
    };
    let mut stage_objs: Vec<Value> = Vec::with_capacity(4);
    for stage in Stage::ALL {
        let label = stage.as_label();
        let samples = report.stage_samples.get(stage);
        stage_objs.push(json!({
            "name": label,
            "median_ms_per_op": median_of(samples) * 1000.0,
            "p10": quantile(samples, 0.10) * 1000.0,
            "p90": quantile(samples, 0.90) * 1000.0,
            "ops_per_sec": if report.elapsed_seconds <= 0.0 { 0.0 } else { samples.len() as f64 / report.elapsed_seconds },
            "worker_count": report.worker_counts.get(stage),
            "samples": samples.len(),
            "worker_utilization": report.worker_utilization(stage),
            "per_record_service_time_seconds": report.per_record_service_time(stage),
        }));
    }
    json!({
        "iteration": report.iteration,
        "scalars": {
            "iteration_elapsed_seconds": report.elapsed_seconds,
            "iteration_throughput_records_per_sec": throughput,
            "iteration_records_processed": report.records_processed as f64,
        },
        "stages": stage_objs,
        "histograms": {},
        "raw_log":     format!("raw/run-{}.metrics.jsonl", report.iteration),
        "raw_metrics": format!("raw/run-{}.metrics.jsonl", report.iteration),
    })
}

fn build_timeseries(reports: &[IterationReport]) -> Value {
    let iteration_starts: Vec<u64> = reports.iter().map(|r| r.timed_started_unix_ms).collect();
    let mut series: Vec<Value> = Vec::with_capacity(4);
    for stage in Stage::ALL {
        let label = stage.as_label();
        let per_iter: Vec<Vec<f64>> = reports
            .iter()
            .map(|r| r.stage_samples.get(stage).to_vec())
            .collect();
        series.push(build_sample_index_series(
            "runtime_stage_latency_seconds",
            json!({
                "alignment": "sample_index",
                "stage": label,
            }),
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

fn build_sample_index_series(metric: &str, labels: Value, per_iter: &[Vec<f64>]) -> Value {
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

fn build_metadata(
    reports: &[IterationReport],
    inputs: &RunInputs<'_>,
    started_at: &str,
    ended_at: &str,
) -> Value {
    let fingerprint = format!(
        "phase07-stage-latencies-{}records-per-payload-{}warmup-{}timed",
        inputs.records_per_source_range, inputs.warmup_payloads, inputs.timed_payloads
    );
    let fingerprint_canonical = json!({
        "kind": "phase07-stage-latencies",
        "records_per_source_range": inputs.records_per_source_range,
        "warmup_payloads": inputs.warmup_payloads,
        "timed_payloads": inputs.timed_payloads,
        "generator": "phase07-stage-latencies-bench",
        "decoder": "FakeDecoder",
        "sink": "BenchSink",
    });
    let fingerprint_canonical_str =
        serde_json::to_string(&fingerprint_canonical).unwrap_or_default();
    let bp =
        reports
            .first()
            .map(|r| r.worker_counts)
            .unwrap_or(super::iteration::StageWorkerCounts {
                source: 1,
                fetch: 1,
                decode: 1,
                sink_dispatch: 1,
            });

    json!({
        "schema_version": 2,
        "phase": inputs.phase,
        "unit_id": inputs.unit_id,
        "unit_title": inputs.unit_title,
        "owner": inputs.owner,
        "started_at": started_at,
        "ended_at": ended_at,
        "experiment": {
            "kind": "ab",
            "dimensions": [],
            "fixed_controls": {
                "records_per_source_range": inputs.records_per_source_range,
                "warmup_payloads": inputs.warmup_payloads,
                "timed_payloads": inputs.timed_payloads,
                "source.fetch_concurrency": bp.fetch,
                "source.decode_concurrency": bp.decode,
                "sink.max_concurrent_commits": bp.sink_dispatch,
                "runtime.dry_run": false,
                "sink.kind": "bench-sink",
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
            "name": "phase07-stage-latencies",
            "build_command": "cargo build --release -p clickhouse-ingestor-bench --bin phase07-stage-latencies",
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
                "sink.kind": "bench-sink",
            },
        },
        "services": {
            "clickhouse":   Value::Null,
            "object_store": {
                "kind": "in-memory",
                "endpoint": Value::Null,
                "region": Value::Null,
                "bucket": Value::Null,
                "container_image": Value::Null,
            },
            "iceberg":      Value::Null,
        },
        "workload": {
            "generator": "phase07-stage-latencies-bench",
            "generator_rev": "see git.opendata-contrib.rev",
            "seed": 0,
            "schema": "bench.fake.v1",
            "schema_hash": format!("sha256:{}", sha256_hex(b"bench.fake.v1")),
            "fingerprint": fingerprint,
            "fingerprint_hash": format!("sha256:{}", sha256_hex(fingerprint_canonical_str.as_bytes())),
            "canonical_path": "raw/workload.canonical.json",
            "records_total": (inputs.records_per_source_range as i64) * (inputs.warmup_payloads as i64 + inputs.timed_payloads as i64),
            "batches_total": (inputs.warmup_payloads as i64 + inputs.timed_payloads as i64),
            "records_per_batch": inputs.records_per_source_range as i64,
            "approx_bytes_per_record": 16,
            "encoding": "synthetic-payload-strings",
            "compression": "none",
            "attribute_cardinality": Value::Null,
        },
        "iterations": inputs.iterations as i64,
        "baseline_run": Value::Null,
        "notes": inputs.notes,
    })
}

fn render_raw_metrics_jsonl(report: &IterationReport) -> String {
    let mut buf = String::new();
    for stage in Stage::ALL {
        let label = stage.as_label();
        for (idx, value) in report.stage_samples.get(stage).iter().enumerate() {
            let line = json!({
                "ts_unix_ms": report.timed_started_unix_ms + idx as u64,
                "metric": "runtime_stage_latency_seconds",
                "value": value,
                "labels": {
                    "iteration": report.iteration,
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

// =========================================================================
// helpers — agg / quantile / time / git
// =========================================================================

fn agg(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({"median": 0.0, "p10": 0.0, "p90": 0.0, "n": 0});
    }
    let med = median_of(samples);
    let p10 = quantile(samples, 0.10);
    let p90 = quantile(samples, 0.90);
    json!({
        "median": med,
        "p10": p10,
        "p90": p90,
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
    // Dirty check excludes `bench-results/` — see real_ch::runner
    // for the rationale (bench-output tracking doesn't poison the
    // source-code cleanliness signal).
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
