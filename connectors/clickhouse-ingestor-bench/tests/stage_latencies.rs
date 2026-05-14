//! Integration tests for the Phase 7.1 stage-latency bench.
//!
//! Acceptance gates per `plans/odb-high-throughput/phase07-clickhouse-throughput-design.md`
//! §Migration Plan step 1:
//!
//! - `cargo test -p clickhouse-ingestor-bench` exercises the
//!   harness end-to-end (this file).
//! - Artifacts validate against benchmarks.md v2 schema.
//! - All four stage labels emit ≥ 1 non-zero sample.
//! - `worker_utilization[*] ∈ [0, 1]` (the units-correctness gate
//!   from the rev-1 review fix).

use std::time::Duration;

use clickhouse_ingestor_bench::stage_latencies::{
    Stage, StageLatenciesConfig, run_stage_latencies,
};
use serde_json::Value;
use tempfile::tempdir;

#[tokio::test]
async fn phase07_stage_latencies_smoke_emits_required_artifacts() {
    // Tight workload: keeps the test fast while still producing
    // ≥ 1 non-zero sample per stage. The minimum-window floor is
    // disabled (0.0) — row 7.1 in-memory dry-run doesn't need the
    // real-CH 20 s requirement, and CI shouldn't pay that cost.
    let out_dir = tempdir().expect("tempdir");
    let cfg = StageLatenciesConfig {
        records_per_source_range: 1,
        warmup_payloads: 4,
        timed_payloads: 24,
        iterations: 2,
        min_timed_window_seconds: 0.0,
        warmup_deadline: Duration::from_secs(30),
        timed_deadline: Duration::from_secs(60),
        output_dir: out_dir.path().to_path_buf(),
        change_slug: "smoke".to_string(),
        notes: "phase07-stage-latencies smoke".to_string(),
        ..Default::default()
    };

    let run = run_stage_latencies(cfg).await.expect("run_stage_latencies");

    let metadata: Value = serde_json::from_slice(
        &std::fs::read(run.run_dir.join("metadata.json")).expect("read metadata.json"),
    )
    .expect("parse metadata.json");
    let results: Value = serde_json::from_slice(
        &std::fs::read(run.run_dir.join("results.json")).expect("read results.json"),
    )
    .expect("parse results.json");
    let timeseries: Value = serde_json::from_slice(
        &std::fs::read(run.run_dir.join("timeseries.json")).expect("read timeseries.json"),
    )
    .expect("parse timeseries.json");

    // ── Schema-v2 envelope checks ──
    assert_eq!(metadata["schema_version"], 2, "metadata.schema_version");
    assert_eq!(results["schema_version"], 2, "results.schema_version");
    assert_eq!(timeseries["schema_version"], 2, "timeseries.schema_version");
    assert_eq!(
        metadata["phase"], "phase07-clickhouse-throughput",
        "metadata.phase"
    );
    assert_eq!(metadata["unit_id"], "7.1", "metadata.unit_id");
    assert_eq!(metadata["experiment"]["kind"], "ab", "experiment.kind");
    assert!(
        metadata["varied_param"].is_null(),
        "varied_param is null for ab smoke",
    );

    // ── Iterations populated ──
    let iterations = results["iterations"]
        .as_array()
        .expect("results.iterations");
    assert_eq!(iterations.len(), 2, "iterations count");
    for it in iterations {
        let scalars = &it["scalars"];
        assert!(
            scalars["iteration_elapsed_seconds"].as_f64().unwrap() > 0.0,
            "iteration_elapsed_seconds positive",
        );
        // The runtime is live (not dry-run); BenchSink reports
        // rows_written = 1 per call, and we admitted N payloads.
        assert!(
            scalars["iteration_records_processed"].as_f64().unwrap() > 0.0,
            "iteration_records_processed positive",
        );
    }

    // ── All four stage labels emit ≥ 1 non-zero sample ──
    // (Migration Plan step 1 acceptance criterion.)
    let stages = results["stages"].as_array().expect("results.stages");
    let labels: Vec<&str> = stages.iter().map(|s| s["name"].as_str().unwrap()).collect();
    for stage in Stage::ALL {
        assert!(
            labels.contains(&stage.as_label()),
            "results.stages missing label {}: saw {labels:?}",
            stage.as_label(),
        );
    }
    for stage_obj in stages {
        let name = stage_obj["name"].as_str().unwrap();
        let samples = stage_obj["samples"].as_i64().expect("stage.samples");
        assert!(
            samples >= 1,
            "stage {name} expected ≥ 1 sample, got {samples}",
        );
        let median_ms = stage_obj["median_ms_per_op"].as_f64().unwrap();
        assert!(
            median_ms > 0.0,
            "stage {name} median_ms_per_op should be > 0 with non-zero samples (got {median_ms})",
        );
    }

    // ── worker_utilization[stage] ∈ [0, 1] ──
    // (rev-1 review fix: clean units gate.)
    for stage in Stage::ALL {
        let label = stage.as_label();
        let key = format!("worker_utilization_{label}");
        let scalar = results["scalars"]
            .get(&key)
            .unwrap_or_else(|| panic!("missing scalar {key}"));
        let median = scalar["median"].as_f64().unwrap();
        assert!(
            (0.0..=1.0).contains(&median),
            "worker_utilization[{label}] median {median} not in [0, 1]",
        );
        let p10 = scalar["p10"].as_f64().unwrap();
        let p90 = scalar["p90"].as_f64().unwrap();
        assert!(
            (0.0..=1.0).contains(&p10),
            "worker_utilization[{label}] p10 {p10} not in [0, 1]",
        );
        assert!(
            (0.0..=1.0).contains(&p90),
            "worker_utilization[{label}] p90 {p90} not in [0, 1]",
        );
    }

    // ── timeseries.json has four sample_index-aligned series, one
    //    per stage label, each with at least one sample ──
    let series = timeseries["series"].as_array().expect("timeseries.series");
    let mut found_stages: std::collections::HashSet<String> = Default::default();
    for s in series {
        assert_eq!(
            s["labels"]["alignment"], "sample_index",
            "timeseries series alignment",
        );
        let stage_label = s["labels"]["stage"].as_str().unwrap();
        found_stages.insert(stage_label.to_string());
        let samples = s["samples"].as_array().unwrap();
        assert!(
            !samples.is_empty(),
            "timeseries series stage={stage_label} has zero samples",
        );
        for sample in samples {
            assert!(
                sample["sample_index"].is_i64(),
                "every sample must carry sample_index",
            );
        }
    }
    for stage in Stage::ALL {
        assert!(
            found_stages.contains(stage.as_label()),
            "timeseries.series missing stage={}",
            stage.as_label(),
        );
    }

    // ── raw/run-N.metrics.jsonl exists per iteration ──
    for i in 1..=2 {
        let path = run
            .run_dir
            .join("raw")
            .join(format!("run-{i}.metrics.jsonl"));
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert!(
            !body.is_empty(),
            "raw/run-{i}.metrics.jsonl should not be empty",
        );
        // Every line must be parseable as JSON with the required
        // fields (ts_unix_ms, metric, value, labels).
        for line in body.lines() {
            let v: Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSONL: {e}: {line}"));
            assert!(v["ts_unix_ms"].is_u64());
            assert!(v["metric"].is_string());
            assert!(v["value"].is_f64() || v["value"].is_i64());
            assert!(v["labels"].is_object());
        }
    }
}
