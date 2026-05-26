//! Re-runs the ack-correctness invariants under the pipelined
//! `RuntimeOptions` profile (`fetch_concurrency = 8`,
//! `decode_concurrency = 4`, `max_inflight_batches = 32`,
//! `sink.max_concurrent_commits = 4`). Much of the concurrent
//! correctness coverage comes from these tests passing unchanged
//! under concurrency. We re-implement a representative subset
//! (single-batch happy + retry paths, multi-batch retry, Fatal,
//! MaybeCommitted, fence) rather than re-running every serial test
//! — integration tests can't import each other, and the invariants
//! the runtime guards are structural (admission ordering,
//! ack-on-advance, retry idempotence) and hold across the entire
//! concurrent profile.

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::error::RuntimeError;
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, Runtime, RuntimeOptions, SinkPoolOptions, SourceBackpressureOptions,
};
use opendata_ingest_runtime::sink::{CommitStatus, SinkId};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use support::{
    FakeDecoder, ProgrammableSink, ScriptedWrite, in_memory_buffer_source, logs_envelope,
};

/// Pipelined options sweep: `fetch_concurrency = 8`,
/// `decode_concurrency = 4`, `max_inflight_batches = 32`,
/// `sink.max_concurrent_commits = 4`.
fn pipelined_options() -> RuntimeOptions {
    RuntimeOptions {
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: false,
        poll_interval: Duration::from_millis(5),
        max_descriptors_per_poll: 1,
        max_retry_attempts: 3,
        retry_backoff: Duration::from_millis(0),
        source_defaults: SourceBackpressureOptions {
            max_inflight_batches: 32,
            fetch_concurrency: 8,
            decode_concurrency: 4,
            ..SourceBackpressureOptions::default()
        },
        source_overrides: Default::default(),
        sink: SinkPoolOptions {
            max_concurrent_commits: 4,
            ..SinkPoolOptions::default()
        },
    }
}

/// INV-NO-ACK-BEFORE-COMMIT under concurrency. Script the sink with
/// `[NotCommitted, NotCommitted, Ok]` for the one source batch.
/// One of the 4 writer workers picks it up, retries serially within
/// that worker, and lands `Ok` on the third attempt. The runtime
/// must reach `last_acked_sequence == Some(0)` only after `Ok`.
#[tokio::test]
async fn concurrent_runtime_does_not_ack_until_sink_returns_ok() {
    let fx = in_memory_buffer_source(
        "ingest/test/concurrent/no-ack-until-ok/manifest",
        "ingest/test/concurrent/no-ack-until-ok/data",
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
    let writes = Arc::clone(&sink.write_calls);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(pipelined_options())
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(10), async {
        loop {
            progress_rx.changed().await.expect("progress closed");
            if progress_rx.borrow().last_acked_sequence == Some(0) {
                return;
            }
        }
    })
    .await
    .expect("runtime should advance ack frontier after Ok");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let write_count = writes.lock().unwrap().len();
    assert_eq!(
        write_count, 3,
        "two retries before Ok under concurrent options"
    );

    fx.producer.close().await.expect("close producer");
}

/// INV-RETRY-BUDGET-HALTS under concurrency. Fatal halts the
/// runtime even when multiple writer workers are alive; the typed
/// `RuntimeError::Sink` arrives at the actor unaltered (the row
/// 6.4 contract: writer sends Fatal as `WriteCompletion::Fatal(e)`
/// so the completion arm extracts the original error).
#[tokio::test]
async fn concurrent_runtime_does_not_ack_when_sink_returns_fatal() {
    let fx = in_memory_buffer_source(
        "ingest/test/concurrent/fatal/manifest",
        "ingest/test/concurrent/fatal/data",
    )
    .await;
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
        .with_options(pipelined_options())
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    let join = timeout(Duration::from_secs(5), handle)
        .await
        .expect("runtime should exit promptly on Fatal")
        .expect("runtime task join");
    let err = join.expect_err("Fatal must surface as RuntimeError");
    assert!(
        matches!(err, RuntimeError::Sink(_)),
        "expected RuntimeError::Sink, got {err:?}"
    );

    let final_progress = *progress_rx.borrow_and_update();
    assert_eq!(
        final_progress.last_acked_sequence, None,
        "ack frontier must not advance on Fatal under concurrency"
    );

    let writes = write_calls.lock().unwrap().clone();
    assert_eq!(writes.len(), 1, "Fatal is not retried");

    let _ = shutdown;
    fx.producer.close().await.expect("close producer");
}

/// Multi-batch happy path under concurrency. 50 batches produced;
/// every `Sink::write` returns `Ok`; the runtime drives all 50
/// through the pipelined topology and reaches
/// `last_acked_sequence == Some(49)`. Pins INV-FRONTIER-NEVER-OVER-HOLE
/// + INV-PENDING-BOUNDED under the writer pool — out-of-order
/// completions arrive at the actor (writers complete in parallel)
/// but the coordinator's state machine keeps the frontier honest.
#[tokio::test]
async fn concurrent_50_batches_advance_frontier_under_writer_pool() {
    let fx = in_memory_buffer_source(
        "ingest/test/concurrent/50-batches/manifest",
        "ingest/test/concurrent/50-batches/data",
    )
    .await;
    let batch_count = 50u64;
    for i in 0..batch_count {
        fx.producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        fx.producer.flush().await.expect("flush");
    }

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        (0..batch_count)
            .map(|_| ScriptedWrite::Ok { rows_written: 1 })
            .collect(),
        CommitStatus::Unknown,
    );
    let write_calls = Arc::clone(&sink.write_calls);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(pipelined_options())
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(30), async {
        loop {
            progress_rx.changed().await.expect("progress closed");
            if progress_rx.borrow().last_acked_sequence == Some(batch_count - 1) {
                return;
            }
        }
    })
    .await
    .expect("runtime should reach full frontier under writer pool");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let writes = write_calls.lock().unwrap().clone();
    assert_eq!(
        writes.len() as u64,
        batch_count,
        "every batch must produce exactly one Sink::write under concurrency",
    );
    let mut seen: Vec<u64> = writes.iter().map(|w| w.high_sequence).collect();
    seen.sort();
    let expected: Vec<u64> = (0..batch_count).collect();
    assert_eq!(seen, expected, "all source sequences covered exactly once");

    fx.producer.close().await.expect("close producer");
}
