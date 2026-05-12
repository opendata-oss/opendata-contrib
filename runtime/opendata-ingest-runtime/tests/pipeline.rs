//! Pipeline-specific integration tests landed across rows 6.2 – 6.6.
//!
//! Each test pins one named invariant from
//! `plans/odb-high-throughput/phase06-pipelined-runtime-design.md`
//! rev 8 §Test Plan > Pipeline correctness tests. Row 6.2 lands the
//! first test (admission ordering under parallel fetch); the file
//! grows with each subsequent row.

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, AdmissionRecorder, Runtime, RuntimeOptions, SinkPoolOptions,
    SourceBackpressureOptions,
};
use opendata_ingest_runtime::source::SourceId;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use support::{FakeDecoder, FakeSink, in_memory_buffer_source, logs_envelope};

fn options_with_fetch_concurrency(fetch_concurrency: u32) -> RuntimeOptions {
    RuntimeOptions {
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: false,
        poll_interval: Duration::from_millis(2),
        max_descriptors_per_poll: 1,
        max_retry_attempts: 0,
        retry_backoff: Duration::from_millis(0),
        source_defaults: SourceBackpressureOptions {
            fetch_concurrency,
            // Row 6.1's `serial()` profile is too tight under
            // parallelism: max_inflight_batches = 1 forces admission
            // to wait for each commit before issuing the next one.
            // Bump up so the actor can queue ahead of the fetch
            // pool and exercise the structural admission ordering.
            max_inflight_batches: 16,
            ..SourceBackpressureOptions::default()
        },
        source_overrides: Default::default(),
        sink: SinkPoolOptions::default(),
    }
}

/// INV-ADMISSION-CONTIGUOUS under parallel fetch.
///
/// Run 100 source batches through the runtime with
/// `fetch_concurrency = 4` and a synthetic fetch-delay function
/// that injects alternating fast / slow latencies. Even sequences
/// fetch at ~1 ms; odd sequences at ~10 ms. The 10× spread forces
/// fetch completion order to drift relative to admission order
/// (slow-fetching workers will fall behind), but the actor's
/// admission arm runs synchronously between `next_descriptors`
/// and `descriptor_tx.send`, so `register_pending` is called in
/// admission order regardless of downstream completion ordering.
///
/// Assertion: the admission recorder observes `(buffer, 0)`,
/// `(buffer, 1)`, …, `(buffer, 99)` in strict order.
#[tokio::test]
async fn pipeline_register_pending_called_in_admission_order() {
    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/admission-order/manifest",
        "ingest/test/pipeline/admission-order/data",
    )
    .await;
    let batch_count = 100u64;
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

    let sink = FakeSink::new("fake-sink");
    let captured = Arc::clone(&sink.captured);

    let recorder: AdmissionRecorder = Arc::new(Mutex::new(Vec::new()));
    let recorder_runtime = Arc::clone(&recorder);

    // Uneven fetch latency: alternating fast/slow by sequence
    // parity. The spread is large enough that parallel fetch
    // workers reorder completion relative to admission, but the
    // admission arm is structurally serial.
    let delay_fn: Arc<dyn Fn(u64) -> Duration + Send + Sync> = Arc::new(|seq: u64| {
        if seq.is_multiple_of(2) {
            Duration::from_millis(1)
        } else {
            Duration::from_millis(10)
        }
    });

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(options_with_fetch_concurrency(4))
        .with_admission_recorder(recorder_runtime)
        .with_test_fetch_delay(delay_fn)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(30), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.source_ranges_committed >= batch_count {
                return;
            }
        }
    })
    .await
    .expect("runtime did not commit all batches in time");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let events = recorder.lock().unwrap().clone();
    assert_eq!(
        events.len(),
        batch_count as usize,
        "recorder should observe one (source, seq) per source batch",
    );
    let expected: Vec<(SourceId, u64)> = (0..batch_count)
        .map(|i| (SourceId::from("buffer"), i))
        .collect();
    assert_eq!(
        events, expected,
        "INV-ADMISSION-CONTIGUOUS: admission arm must call \
         register_pending in source-sequence order regardless of \
         per-worker fetch latency"
    );

    // Sanity: every batch reached the sink exactly once with the
    // expected range. Captures arrive in fetch-completion order
    // (not source-sequence order) under parallel fetch, so sort
    // before checking range coverage.
    let mut committed = captured.lock().unwrap().clone();
    committed.sort_by_key(|c| c.low_sequence);
    assert_eq!(committed.len(), batch_count as usize);
    for (i, c) in committed.iter().enumerate() {
        assert_eq!(c.low_sequence, i as u64);
        assert_eq!(c.high_sequence, i as u64);
    }

    fx.producer.close().await.expect("close producer");
}

/// Phase 5 ack-correctness invariant under
/// `fetch_concurrency = 8`. The 6.2 acceptance demands every
/// Phase 5 test pass under parallel fetch; this is the smoke
/// version — three batches, NotCommitted/NotCommitted/Ok script,
/// 8 fetch workers. Acks land only after the Ok lands; identity
/// stays byte-identical across retries.
#[tokio::test]
async fn pipeline_ack_correctness_under_fetch_concurrency_8() {
    use opendata_ingest_runtime::sink::CommitStatus;
    use opendata_ingest_runtime::sink::SinkId;
    use support::{ProgrammableSink, ScriptedWrite};

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/concurrent-ack/manifest",
        "ingest/test/pipeline/concurrent-ack/data",
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

    let mut opts = options_with_fetch_concurrency(8);
    opts.max_retry_attempts = 3;

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(10), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            if progress_rx.borrow().last_acked_sequence == Some(0) {
                return;
            }
        }
    })
    .await
    .expect("runtime did not advance ack frontier");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let (write_count, identity_count) = {
        let write_calls = writes.lock().unwrap();
        let count = write_calls.len();
        let identities: std::collections::HashSet<_> =
            write_calls.iter().map(|w| w.identity.clone()).collect();
        (count, identities.len())
    };
    assert_eq!(write_count, 3, "two retries before Ok");
    assert_eq!(
        identity_count, 1,
        "INV-SINK-RETRY-IDEMPOTENT: identity stable across retries",
    );

    fx.producer.close().await.expect("close producer");
}
