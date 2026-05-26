//! `correctness.json` and `metadata.json` schema types + writers,
//! aligned with `plans/odb-high-throughput/benchmarks.md`
//! §`correctness.json` Schema and §`metadata.json` Schema.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;

/// `correctness.json` schema v2 — single-service shape (one
/// `summary` + one `sources` map; an alternative
/// `services_under_test[]` shape would apply to a duplicated-queue
/// multi-service scenario).
#[derive(Debug, Serialize)]
pub struct CorrectnessReport {
    pub schema_version: u32,
    pub summary: SummarySection,
    pub sources: BTreeMap<String, PerSourceCorrectness>,
    pub ack_invariant_checks: Vec<AckInvariantCheck>,
}

#[derive(Debug, Serialize)]
pub struct SummarySection {
    pub records_generated: u64,
    pub highest_generated_sequence: u64,
    pub duplicate_records_post_dedupe: u64,
    pub ack_invariant_violations: u64,
    pub required_sink: String,
}

#[derive(Debug, Serialize)]
pub struct PerSourceCorrectness {
    pub highest_generated_sequence: u64,
    pub highest_acked_sequence: u64,
    pub records_visible_in_sink: u64,
    pub records_missing_from_sink: u64,
    pub duplicate_records_pre_dedupe: u64,
    pub duplicate_records_post_dedupe: u64,
    pub notes: String,
}

#[derive(Debug, Serialize)]
pub struct AckInvariantCheck {
    pub name: String,
    pub passed: bool,
    pub evidence: String,
}

/// `metadata.json` schema v2 — minimal smoke shape. The
/// full schema includes hardware / dataset / git-sha fields the
/// in-memory smoke run doesn't have anything meaningful to put
/// in; the bench writes a small subset and leaves the others to
/// the perf-test runner that drives a real cluster.
#[derive(Debug, Serialize)]
pub struct RunMetadata {
    pub schema_version: u32,
    pub phase: String,
    pub unit_id: String,
    pub unit_title: String,
    pub owner: String,
    pub started_at: String,
    pub ended_at: String,
    pub experiment: ExperimentMeta,
    pub run_kind: String,
    pub notes: String,
}

#[derive(Debug, Serialize)]
pub struct ExperimentMeta {
    pub kind: String,
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(value)?;
    std::fs::write(path, text)?;
    Ok(())
}
