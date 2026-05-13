//! Process-level metrics recorder for the Phase 6.x §1.2 closeout.
//!
//! `metrics::set_global_recorder` (the API
//! [`metrics_util::debugging::DebuggingRecorder::install`] calls
//! internally) can be invoked at most once per process. This
//! module wraps the install in a `OnceLock` so multiple tests
//! within the same integration-test binary share one global
//! recorder + one detached `Snapshotter`. Each test snapshots the
//! shared state after driving a pipeline and asserts the named
//! series are present.
//!
//! Why this lives at the bench crate level (not in
//! `runtime/opendata-ingest-runtime/tests/`): the runtime crate's
//! integration tests spawn workers via `tokio::spawn`, and
//! `metrics::with_local_recorder` is thread-local — workers
//! emitting from spawned tasks would not write to a recorder
//! installed in the test's task. The global-recorder path
//! sidesteps that entirely.

use std::sync::OnceLock;

use metrics_util::debugging::{DebuggingRecorder, Snapshotter};

/// Storage for the install-once global snapshotter. Returned by
/// [`init_metrics_recorder`].
static GLOBAL_SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();

/// Install the [`DebuggingRecorder`] as the process global
/// recorder on first call; return the detached `Snapshotter` on
/// every call. Idempotent — a second call returns the same
/// snapshotter the first call stored.
///
/// Tests call this at the top of `#[test]` to ensure the
/// recorder is live before the runtime emits any metrics. Each
/// test gets the same snapshotter and reads the cumulative state
/// across earlier tests within the same binary — single-test
/// scopes can subtract a baseline snapshot to isolate, but the
/// §1.2 assertion ("every named series has at least one sample")
/// only requires monotone accumulation.
pub fn init_metrics_recorder() -> &'static Snapshotter {
    GLOBAL_SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // `install()` returns `Err(SetRecorderError)` only if a
        // recorder is already installed. We're behind a OnceLock
        // so first-and-only caller; expect success.
        recorder.install().expect("global recorder install");
        snapshotter
    })
}
