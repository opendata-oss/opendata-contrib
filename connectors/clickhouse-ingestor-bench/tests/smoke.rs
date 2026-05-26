//! Smoke test that drives `run_smoke` end to end against a
//! tmpdir. Pins the gate criteria:
//!
//! - every `ack_invariant_checks[*].passed == true`
//! - `summary.duplicate_records_post_dedupe == 0`
//! - every `sources[s].records_missing_from_sink == 0`
//!
//! Also pins the on-disk shape: `correctness.json`,
//! `metadata.json`, and one witness JSONL per check.

use std::path::PathBuf;

use clickhouse_ingestor_bench::correctness::run_smoke;

fn tmp_run_dir() -> PathBuf {
    let base = std::env::temp_dir();
    let unique = format!(
        "clickhouse-ingestor-bench-smoke-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    base.join(unique)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn correctness_smoke_passes_every_check_and_writes_outputs() {
    let run_dir = tmp_run_dir();
    std::fs::create_dir_all(&run_dir).expect("create run_dir");

    let report = run_smoke(&run_dir).await.expect("run_smoke");

    // Gate predicates.
    assert_eq!(report.schema_version, 2);
    assert_eq!(report.summary.duplicate_records_post_dedupe, 0);
    assert_eq!(report.summary.ack_invariant_violations, 0);
    for (name, src) in &report.sources {
        assert_eq!(
            src.records_missing_from_sink, 0,
            "source {name} should have zero missing records",
        );
        assert_eq!(src.duplicate_records_post_dedupe, 0);
    }
    for check in &report.ack_invariant_checks {
        assert!(
            check.passed,
            "check {:?} must pass; evidence={:?}",
            check.name, check.evidence,
        );
    }

    // Required output files.
    assert!(run_dir.join("correctness.json").exists());
    assert!(run_dir.join("metadata.json").exists());
    for check in &report.ack_invariant_checks {
        let witness_path = run_dir.join(&check.evidence);
        assert!(
            witness_path.exists(),
            "witness {} should exist at {:?}",
            check.name,
            witness_path,
        );
    }

    // Cleanup (best-effort).
    let _ = std::fs::remove_dir_all(&run_dir);
}
