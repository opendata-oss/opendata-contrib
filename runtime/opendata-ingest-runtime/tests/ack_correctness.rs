//! Phase 5 ack-correctness harness. Each test pins one named
//! `INV-*` invariant from
//! `plans/odb-high-throughput/phase05-ack-correctness-design.md`
//! rev 6.
//!
//! See `tests/support/mod.rs` for the shared fixtures
//! (`FakeDecoder`, `ProgrammableSink`, `BlockToken`,
//! `in_memory_buffer_source`, etc.).

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::error::RuntimeError;
use opendata_ingest_runtime::runtime::{AckFlushPolicy, Runtime, RuntimeOptions};
use opendata_ingest_runtime::sink::{CommitStatus, SinkId};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use support::{
    FakeDecoder, ProgrammableSink, ScriptedWrite, in_memory_buffer_source, logs_envelope,
};

fn live_options() -> RuntimeOptions {
    RuntimeOptions {
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: false,
        poll_interval: Duration::from_millis(10),
        max_descriptors_per_poll: 1,
        max_retry_attempts: 3,
        retry_backoff: Duration::from_millis(0),
    }
}

/// INV-NO-ACK-BEFORE-COMMIT (NotCommitted path). Runtime must
/// retry until `Ok` and only advance the ack frontier on the
/// successful write. Script: [NotCommitted, NotCommitted, Ok]
/// with `max_retry_attempts = 3`. Assertions:
///   - `progress.last_acked_sequence` reaches `Some(0)` only
///     after the `Ok` lands.
///   - The sink saw exactly 3 write calls for sequence 0
///     (proves the retry path ran).
///   - The sink saw 0 check_committed calls (NotCommitted does
///     not trigger the precheck path).
///   - All 3 write calls share the same idempotency key
///     (INV-SINK-RETRY-IDEMPOTENT; the runtime re-issues with
///     the same SinkCommit on NotCommitted).
#[tokio::test]
async fn runtime_does_not_ack_until_sink_returns_ok() {
    let fx = in_memory_buffer_source(
        "ingest/test/no-ack-until-ok/manifest",
        "ingest/test/no-ack-until-ok/data",
    )
    .await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![
            ScriptedWrite::NotCommitted {
                message: "transient".into(),
            },
            ScriptedWrite::NotCommitted {
                message: "transient".into(),
            },
            ScriptedWrite::Ok { rows_written: 1 },
        ],
        CommitStatus::Unknown,
    );
    let write_calls = Arc::clone(&sink.write_calls);
    let check_calls = Arc::clone(&sink.check_committed_calls);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(live_options())
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let p = timeout(Duration::from_secs(5), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.last_acked_sequence == Some(0) {
                return p;
            }
        }
    })
    .await
    .expect("ack frontier never advanced");

    assert_eq!(p.last_acked_sequence, Some(0));
    assert_eq!(p.records_written, 1);

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let writes = write_calls.lock().unwrap().clone();
    let checks = check_calls.lock().unwrap().clone();
    assert_eq!(
        writes.len(),
        3,
        "runtime must retry twice before Ok; got writes={writes:?}"
    );
    assert!(
        writes.iter().all(|w| w.high_sequence == 0),
        "all retries must target the same range; got writes={writes:?}"
    );
    assert!(
        checks.is_empty(),
        "NotCommitted does not trigger check_committed; got checks={checks:?}"
    );
    let key = &writes[0].idempotency_key;
    assert!(
        writes.iter().all(|w| &w.idempotency_key == key),
        "INV-SINK-RETRY-IDEMPOTENT: all retries share the same key; got writes={writes:?}"
    );

    fx.producer.close().await.expect("close producer");
}

/// INV-NO-ACK-BEFORE-COMMIT (Fatal path) + halt behavior. A
/// Fatal sink failure must:
///   - cause `Runtime::run` to return `Err(RuntimeError::Sink(_))`,
///   - leave the ack frontier at `None` (the failing range was
///     never committed),
///   - exhibit exactly one write call (Fatal is not retried).
#[tokio::test]
async fn runtime_does_not_ack_when_sink_returns_fatal() {
    let fx = in_memory_buffer_source("ingest/test/fatal/manifest", "ingest/test/fatal/data").await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![ScriptedWrite::Fatal {
            message: "permissions denied".into(),
        }],
        CommitStatus::Unknown,
    );
    let write_calls = Arc::clone(&sink.write_calls);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(live_options())
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let join = timeout(Duration::from_secs(5), handle)
        .await
        .expect("runtime should exit promptly on Fatal")
        .expect("runtime task join");
    let err = join.expect_err("Fatal must surface as RuntimeError");
    assert!(
        matches!(err, RuntimeError::Sink(_)),
        "expected RuntimeError::Sink, got {err:?}"
    );

    // The final progress snapshot still has last_acked_sequence
    // == None — Fatal exits before mark_committed runs.
    let final_progress = *progress_rx.borrow_and_update();
    assert_eq!(
        final_progress.last_acked_sequence, None,
        "ack frontier must not advance on Fatal"
    );

    let writes = write_calls.lock().unwrap().clone();
    assert_eq!(
        writes.len(),
        1,
        "Fatal is not retried; got writes={writes:?}"
    );

    let _ = shutdown;
    fx.producer.close().await.expect("close producer");
}

/// INV-MAYBE-COMMITTED-RESOLVES (Committed branch). Moved from
/// `tests/runtime_compat.rs` where it landed in Phase 4 review
/// round 2 (commit `6d28604`); the harness now lives alongside
/// the other ack-correctness tests.
///
/// `Sink::write` returns `MaybeCommitted`; `check_committed`
/// reports `Committed`. The runtime must mark the range
/// committed without re-issuing `write`.
#[tokio::test]
async fn runtime_treats_maybe_committed_then_committed_as_success() {
    let fx = in_memory_buffer_source(
        "ingest/test/maybe-committed-then-committed/manifest",
        "ingest/test/maybe-committed-then-committed/data",
    )
    .await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![ScriptedWrite::MaybeCommitted {
            message: "ambiguous insert; prior attempt may have committed".into(),
        }],
        CommitStatus::Committed,
    );
    let write_calls = Arc::clone(&sink.write_calls);
    let check_calls = Arc::clone(&sink.check_committed_calls);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(live_options())
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let p = timeout(Duration::from_secs(5), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.last_acked_sequence == Some(0) {
                return p;
            }
        }
    })
    .await
    .expect("ack frontier never advanced");

    assert_eq!(p.last_acked_sequence, Some(0));

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let writes = write_calls.lock().unwrap().clone();
    let checks = check_calls.lock().unwrap().clone();
    assert_eq!(
        writes.len(),
        1,
        "runtime must NOT retry write after Committed; got writes={writes:?}"
    );
    assert_eq!(
        checks.len(),
        1,
        "runtime must call check_committed exactly once; got checks={checks:?}"
    );

    fx.producer.close().await.expect("close producer");
}

/// INV-MAYBE-COMMITTED-RESOLVES (Unknown branch). Moved from
/// `tests/runtime_compat.rs` (Phase 4 review round 2 commit
/// `6d28604`).
///
/// `Sink::write` returns `MaybeCommitted`; `check_committed`
/// reports `Unknown` (the ClickHouse default — the short-window
/// dedupe token has expired by the time the runtime asks).
/// Runtime treats `Unknown` like `NotCommitted` for the retry
/// decision and re-issues `write`; on `Ok`, ack frontier
/// advances.
#[tokio::test]
async fn runtime_retries_write_after_maybe_committed_when_check_returns_unknown() {
    let fx = in_memory_buffer_source(
        "ingest/test/maybe-committed-then-unknown/manifest",
        "ingest/test/maybe-committed-then-unknown/data",
    )
    .await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![
            ScriptedWrite::MaybeCommitted {
                message: "ambiguous insert".into(),
            },
            ScriptedWrite::Ok { rows_written: 1 },
        ],
        CommitStatus::Unknown,
    );
    let write_calls = Arc::clone(&sink.write_calls);
    let check_calls = Arc::clone(&sink.check_committed_calls);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(live_options())
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let p = timeout(Duration::from_secs(5), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.last_acked_sequence == Some(0) {
                return p;
            }
        }
    })
    .await
    .expect("ack frontier never advanced");

    assert_eq!(p.last_acked_sequence, Some(0));
    assert_eq!(p.records_written, 1);

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let writes = write_calls.lock().unwrap().clone();
    let checks = check_calls.lock().unwrap().clone();
    assert_eq!(
        writes.len(),
        2,
        "runtime must retry write once after Unknown; got writes={writes:?}"
    );
    assert_eq!(
        checks.len(),
        1,
        "runtime must call check_committed exactly once; got checks={checks:?}"
    );

    fx.producer.close().await.expect("close producer");
}

/// INV-RETRY-BUDGET-HALTS. When `max_retry_attempts` is
/// exhausted on a `NotCommitted` path, the runtime returns
/// `RuntimeError::Sink(_)` and the ack frontier does not
/// advance through the failing range.
///
/// Script: 4 NotCommitted responses with `max_retry_attempts =
/// 2`. The runtime issues the first write, then retries up to
/// 2 more times (3 writes total — initial + 2 retries), then
/// bubbles the error.
#[tokio::test]
async fn runtime_retry_budget_exhaustion_does_not_ack_failing_range() {
    let fx = in_memory_buffer_source(
        "ingest/test/retry-budget-exhaustion/manifest",
        "ingest/test/retry-budget-exhaustion/data",
    )
    .await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![
            ScriptedWrite::NotCommitted {
                message: "1".into(),
            },
            ScriptedWrite::NotCommitted {
                message: "2".into(),
            },
            ScriptedWrite::NotCommitted {
                message: "3".into(),
            },
            ScriptedWrite::NotCommitted {
                message: "4".into(),
            },
        ],
        CommitStatus::Unknown,
    );
    let write_calls = Arc::clone(&sink.write_calls);

    let mut opts = live_options();
    opts.max_retry_attempts = 2;
    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let join = timeout(Duration::from_secs(5), handle)
        .await
        .expect("runtime should exit on retry-budget exhaustion")
        .expect("runtime task join");
    let err = join.expect_err("retry-budget exhaustion must surface as RuntimeError");
    assert!(
        matches!(err, RuntimeError::Sink(_)),
        "expected RuntimeError::Sink, got {err:?}"
    );

    // Ack frontier never advanced.
    let final_progress = *progress_rx.borrow_and_update();
    assert_eq!(
        final_progress.last_acked_sequence, None,
        "ack frontier must not advance when retries exhaust"
    );

    // Initial attempt + 2 retries = 3 writes total.
    let writes = write_calls.lock().unwrap().clone();
    assert_eq!(
        writes.len(),
        3,
        "initial write + max_retry_attempts=2 retries = 3 writes; got writes={writes:?}"
    );
    // All target the same sequence with the same idempotency
    // key (INV-SINK-RETRY-IDEMPOTENT).
    assert!(writes.iter().all(|w| w.high_sequence == 0));
    let key = &writes[0].idempotency_key;
    assert!(writes.iter().all(|w| &w.idempotency_key == key));

    let _ = shutdown;
    fx.producer.close().await.expect("close producer");
}
