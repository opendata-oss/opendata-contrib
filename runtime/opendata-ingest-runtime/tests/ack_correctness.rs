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
    FakeDecoder, ProgrammableSink, ScriptedWrite, buffer_source_on_store, in_memory_buffer_source,
    logs_envelope,
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

/// INV-FENCE-ABORTS-DURABLE-ACK. When the underlying
/// `buffer::Consumer` returns `buffer::Error::Fenced` from a
/// manifest-modifying call (`ack`, `flush`, `next_batch`),
/// `BufferSource` surfaces it as `RuntimeError::Source(_)`,
/// the runtime task exits, and **no durable Buffer manifest
/// update is advanced by the fenced runtime**. Replay through
/// the new (un-fenced) consumer is idempotent via
/// INV-SINK-RETRY-IDEMPOTENT.
///
/// Choreography (rev 6 §Test Plan > Runtime integration tests
/// row for this test). Every `Consumer::with_object_store` calls
/// `initialize()` which fences any prior consumer, so we use a
/// SINGLE replacement consumer B that both bumps the epoch
/// (fencing A) AND drives replay. The durable-state proof is in
/// the replay runtime's own behavior — `sink_b.write_calls[0]`
/// for sequence 0 + matching `IdempotencyKey` across A's and B's
/// commits. No out-of-band `next_descriptors` call on
/// `BufferSource(B)`: that would dequeue sequence 0 from B's
/// cursor and the replay runtime would never see it.
#[tokio::test]
async fn runtime_fence_aborts_durable_ack_with_source_error() {
    let manifest_path = "ingest/test/fence-abort/manifest";
    let data_prefix = "ingest/test/fence-abort/data";

    // Set up the in-memory store + producer + Consumer A's
    // BufferSource. The store stays alive for B's BufferSource.
    let fx_a = in_memory_buffer_source(manifest_path, data_prefix).await;
    let store = Arc::clone(&fx_a.store);
    fx_a.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx_a.producer.flush().await.expect("flush");

    // Gate A's first Sink::write. The gate stays engaged
    // until `block` is dropped.
    let sink_a = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![ScriptedWrite::Ok { rows_written: 1 }],
        CommitStatus::Unknown,
    );
    let writes_a = Arc::clone(&sink_a.write_calls);
    let block = sink_a.block_until_released(true);

    let runtime_a = Runtime::builder()
        .add_source(fx_a.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink_a)
        .with_options(live_options())
        .build()
        .expect("build runtime A");

    let shutdown_a = CancellationToken::new();
    let shutdown_a_run = shutdown_a.clone();
    let handle_a = tokio::spawn(async move { runtime_a.run(shutdown_a_run).await });

    // Wait until A is parked at the sink gate. The sink fires
    // `entered.notify_one()` AFTER incrementing entered_count,
    // so the count is guaranteed ≥ 1 once this resolves.
    timeout(Duration::from_secs(5), block.wait_for_entry())
        .await
        .expect("A never parked at sink gate");
    assert!(block.entered_count() >= 1);

    // While A is parked, construct B against the same manifest.
    // B's `Consumer::initialize()` bumps the epoch and silently
    // fences A (A doesn't learn until its next manifest call).
    let source_b =
        buffer_source_on_store(Arc::clone(&store), manifest_path, data_prefix, None).await;
    // Non-consuming in-memory wrapper read — does not touch the
    // consumer cursor.
    assert_eq!(source_b.last_acked_sequence(), None);

    // Release A. A's Sink::write returns Ok; the runtime calls
    // mark_committed, advance_frontier, then ack_through(0)
    // which trips on Error::Fenced.
    drop(block);
    let join_a = timeout(Duration::from_secs(5), handle_a)
        .await
        .expect("A never exited")
        .expect("A task join");
    let err_a = join_a.expect_err("fence must surface as RuntimeError");
    assert!(
        matches!(err_a, RuntimeError::Source(_)),
        "expected RuntimeError::Source on fence, got {err_a:?}"
    );
    let _ = shutdown_a;

    // A's sink saw exactly one write (the gated one) targeting
    // sequence 0. Capture its idempotency key for the replay
    // comparison.
    let writes_a_snapshot = writes_a.lock().unwrap().clone();
    assert_eq!(writes_a_snapshot.len(), 1);
    assert_eq!(writes_a_snapshot[0].high_sequence, 0);
    let key_a = writes_a_snapshot[0].idempotency_key.clone();

    // Build the replay runtime on B with a fresh ProgrammableSink
    // scripted Ok. The replay runtime's first next_descriptors
    // call hits Consumer B's `next_batch`, which dequeues
    // sequence 0 from the durable manifest. If the manifest had
    // advanced past 0 due to A's fenced ack, B would get None
    // and the replay would hang on the poll loop.
    let sink_b = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![ScriptedWrite::Ok { rows_written: 1 }],
        CommitStatus::Unknown,
    );
    let writes_b = Arc::clone(&sink_b.write_calls);

    let runtime_b = Runtime::builder()
        .add_source(source_b)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink_b)
        .with_options(live_options())
        .build()
        .expect("build runtime B");
    let mut progress_b = runtime_b.progress();

    let shutdown_b = CancellationToken::new();
    let shutdown_b_run = shutdown_b.clone();
    let handle_b = tokio::spawn(async move { runtime_b.run(shutdown_b_run).await });

    let p = timeout(Duration::from_secs(5), async {
        loop {
            progress_b
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_b.borrow();
            if p.last_acked_sequence == Some(0) {
                return p;
            }
        }
    })
    .await
    .expect("B never advanced ack frontier — durable manifest may have been wrongly advanced by fenced A");

    assert_eq!(p.last_acked_sequence, Some(0));

    shutdown_b.cancel();
    handle_b
        .await
        .expect("B task join")
        .expect("B exited cleanly");

    let writes_b_snapshot = writes_b.lock().unwrap().clone();
    assert_eq!(
        writes_b_snapshot.len(),
        1,
        "B must replay sequence 0 exactly once; got writes={writes_b_snapshot:?}"
    );
    assert_eq!(
        writes_b_snapshot[0].high_sequence, 0,
        "B must replay sequence 0 (manifest unadvanced); got {writes_b_snapshot:?}"
    );
    // INV-SINK-RETRY-IDEMPOTENT: B's commit shares A's key.
    assert_eq!(
        writes_b_snapshot[0].idempotency_key, key_a,
        "B's replay must reuse A's IdempotencyKey for INV-SINK-RETRY-IDEMPOTENT"
    );

    fx_a.producer.close().await.expect("close producer");
}
