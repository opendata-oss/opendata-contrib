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
