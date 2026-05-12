//! Boundary-leak guard for the runtime/sink interface cleanup
//! (impl-plan row 5.9).
//!
//! After row 5.9 the runtime crate exposes
//! [`opendata_ingest_runtime::identity::CommitIdentity`] as the sole
//! logical commit identity. Plugin and connector crates derive their
//! own sink-physical tokens locally; they must not pull
//! `IdempotencyKey` / `CommitGroupBatch` / `RecordSize` (all removed)
//! back from the runtime crate.
//!
//! This test grep-scans every `.rs` file under the contrib workspace
//! and fails if any one of those names appears as a **path-qualified
//! reference** (e.g. `use opendata_ingest_runtime::commit_group::…`
//! or `runtime::idempotency::IdempotencyKey`). The patterns target
//! import / type-use forms only; bare names in doc-comment prose are
//! allowed so historical context can stay readable. The check is
//! intentionally textual — a compile-adjacent CI guard, not a static
//! analysis pass.

use std::fs;
use std::path::{Path, PathBuf};

/// Path-qualified references the removed runtime modules / types
/// would produce. Prose mentions of bare names (e.g.
/// "the old `IdempotencyKey`") do not match.
const FORBIDDEN_PATTERNS: &[&str] = &[
    "opendata_ingest_runtime::commit_group",
    "opendata_ingest_runtime::idempotency",
    "::IdempotencyKey",
    "::IdempotencyContract",
    "::DefaultIdempotencyContract",
    "::IdempotencyScope",
    "::CommitGroupBatch",
    "::CommitGroupThresholds",
    "::CommitGroup ",
    "::CommitGroup,",
    "::CommitGroup<",
    "::RecordSize",
];

/// Test files that are intentionally allowed to *mention* a forbidden
/// name (e.g. this guard's own pattern list, or a doc-comment that
/// references the historical name). Keys are relative paths from the
/// workspace root.
fn allowlisted(path: &Path) -> bool {
    let s = path.to_string_lossy();
    s.ends_with("runtime/opendata-ingest-runtime/tests/boundary_leak_guard.rs")
        // The traces round-trip test is `#![cfg(any())]`-gated; it
        // never compiles and is a tracked Phase 4 D3 follow-up. The
        // guard ignores it since the cfg makes the contents
        // unreachable. See plans/odb-high-throughput/next-session.md
        // §Tracked follow-ups.
        || s.ends_with("connectors/clickhouse-ingestor/tests/clickhouse_round_trip_traces.rs")
}

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR for this crate is
    //   <workspace>/runtime/opendata-ingest-runtime
    // The workspace root is two levels up.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root above runtime crate")
        .to_path_buf()
}

fn collect_rust_files(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if path.is_dir() {
            // Skip target/, .git/, and the worktrees/ tree if we are
            // somehow rooted at the responsive repo top level.
            if matches!(name, "target" | ".git" | "worktrees" | "node_modules") {
                continue;
            }
            collect_rust_files(&path, out);
        } else if name.ends_with(".rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_plugin_or_connector_references_removed_runtime_types() {
    let root = workspace_root();
    let scan_dirs = [
        root.join("runtime"),
        root.join("plugins"),
        root.join("connectors"),
    ];

    let mut files = Vec::new();
    for dir in &scan_dirs {
        collect_rust_files(dir, &mut files);
    }
    assert!(
        !files.is_empty(),
        "boundary guard found no .rs files under {scan_dirs:?} — check the path",
    );

    let mut leaks: Vec<String> = Vec::new();
    for file in &files {
        if allowlisted(file) {
            continue;
        }
        let content = match fs::read_to_string(file) {
            Ok(c) => c,
            Err(_) => continue,
        };
        for pattern in FORBIDDEN_PATTERNS {
            if content.contains(pattern) {
                leaks.push(format!(
                    "{} contains forbidden pattern `{}`",
                    file.strip_prefix(&root).unwrap_or(file).display(),
                    pattern,
                ));
            }
        }
    }

    assert!(
        leaks.is_empty(),
        "runtime/sink boundary leak — plugin/connector code still references removed runtime types:\n{}",
        leaks.join("\n"),
    );
}
