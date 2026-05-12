//! End-to-end runtime smoke tests: real Producer + Consumer +
//! `BufferSource` + `Runtime`, with the fakes from
//! `tests/support/mod.rs`. Validates the Phase 4 compat surface
//! (descriptor flow, dry-run mode, live mode + ack frontier,
//! HIGH-1 resume regression, MED-5 decoder-accepts gate, the
//! `MaybeCommitted → check_committed → retry` protocol).
//!
//! The fixtures live in `tests/support/mod.rs` and are included
//! here via `#[path = "support/mod.rs"]`. Each `tests/*.rs` is a
//! separate Cargo binary, so the support module is linked
//! per-binary at compile time (the standard Cargo workaround
//! for integration-test fixture sharing — a library-side
//! `cfg(feature = "test-support")` module does not compose with
//! the default `cargo test --workspace` invocation).

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, Runtime, RuntimeOptions, SinkPoolOptions, SourceBackpressureOptions,
};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use support::{
    FakeDecoder, FakeSink, buffer_source_on_store, in_memory_buffer_source, logs_envelope,
};

fn options(dry_run: bool) -> RuntimeOptions {
    RuntimeOptions {
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run,
        poll_interval: Duration::from_millis(10),
        max_descriptors_per_poll: 1,
        max_retry_attempts: 0,
        retry_backoff: Duration::from_millis(0),
        source_defaults: SourceBackpressureOptions::serial(),
        source_overrides: Default::default(),
        sink: SinkPoolOptions::default(),
    }
}

/// Phase 4 review HIGH-1 regression. A `BufferSource` constructed
/// with `last_acked_sequence: None` and a producer that has already
/// advanced past sequence 0 should NOT replay `Consumer::ack(0)..
/// =Consumer::ack(N)` on the first `ack_through(N)` call. Anchor the
/// ack range at the first sequence the source actually handed out.
#[tokio::test]
async fn ack_through_anchors_at_first_seen_after_resume() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/resume-ack/manifest";
    let data_prefix = "ingest/test/resume-ack/data";

    let fixture = support::buffer_source_on_store_with_producer(
        Arc::clone(&store),
        manifest_path,
        data_prefix,
        None,
    )
    .await;
    let producer = fixture.producer;
    let mut source1 = fixture.source;

    for i in 0..3 {
        producer
            .produce(
                vec![Bytes::from(format!("batch-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        producer.flush().await.expect("flush");
    }

    for expected in 0..3u64 {
        let descs = source1
            .next_descriptors(1, Default::default())
            .await
            .expect("next_descriptors");
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0].sequence, expected);
        let _ = source1
            .fetch_handle()
            .fetch(descs[0].clone())
            .await
            .expect("fetch");
        source1
            .ack_through(descs[0].sequence)
            .await
            .expect("ack_through");
    }
    source1.flush_acks().await.expect("flush_acks");
    assert_eq!(source1.last_acked_sequence(), Some(2));
    drop(source1);

    producer
        .produce(vec![Bytes::from_static(b"after-resume")], logs_envelope())
        .await
        .expect("produce 4");
    producer.flush().await.expect("flush 4");

    // Construct a fresh BufferSource with last_acked: None — the
    // point of HIGH-1 is that this case must anchor at the first
    // sequence actually observed (3) rather than fabricating acks
    // for 0..2.
    let mut source2 =
        buffer_source_on_store(Arc::clone(&store), manifest_path, data_prefix, None).await;
    assert_eq!(source2.last_acked_sequence(), None);

    let descs = source2
        .next_descriptors(1, Default::default())
        .await
        .expect("next_descriptors after resume");
    assert_eq!(descs.len(), 1);
    assert_eq!(
        descs[0].sequence, 3,
        "fresh consumer resumes at 3 because the prior consumer acked through 2",
    );
    let _ = source2
        .fetch_handle()
        .fetch(descs[0].clone())
        .await
        .expect("fetch after resume");

    source2
        .ack_through(3)
        .await
        .expect("ack_through must not fabricate acks for unseen sequences");
    source2.flush_acks().await.expect("flush after resume");
    assert_eq!(source2.last_acked_sequence(), Some(3));

    producer.close().await.expect("close producer");
}

/// Phase 4 review MED-5 regression. Runtime must call
/// `Decoder::accepts(envelope)` and fail closed when it returns
/// false.
#[tokio::test]
async fn runtime_fails_closed_when_decoder_rejects_envelope() {
    let fx = in_memory_buffer_source(
        "ingest/test/decoder-rejects/manifest",
        "ingest/test/decoder-rejects/data",
    )
    .await;

    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = FakeSink::new("fake");
    let captured = Arc::clone(&sink.captured);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::rejecting())
        .set_sink(sink)
        .with_options(options(false))
        .build()
        .expect("build");

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let join = timeout(Duration::from_secs(5), handle)
        .await
        .expect("runtime should exit after rejecting the envelope")
        .expect("runtime task join");
    let err = join.expect_err("decoder rejection must surface as RuntimeError");
    let msg = format!("{err}");
    assert!(
        msg.contains("decoder rejected configured envelope"),
        "unexpected error: {msg}"
    );
    let _ = shutdown;

    // Sink must never be called.
    assert!(captured.lock().unwrap().is_empty());

    fx.producer.close().await.expect("close producer");
}

#[tokio::test]
async fn buffer_source_returns_descriptors_after_producer_flush() {
    let fx = in_memory_buffer_source(
        "ingest/test/source-smoke/manifest",
        "ingest/test/source-smoke/data",
    )
    .await;
    let producer = fx.producer;
    let mut source = fx.source;

    producer
        .produce(vec![Bytes::from_static(b"a")], logs_envelope())
        .await
        .expect("produce");
    producer.flush().await.expect("flush");

    let mut descriptors = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let batch = source
            .next_descriptors(1, Default::default())
            .await
            .expect("next");
        if !batch.is_empty() {
            descriptors.extend(batch);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(descriptors.len(), 1, "expected one descriptor after flush");
    assert_eq!(descriptors[0].sequence, 0);

    let handle = source.fetch_handle();
    let fetched = handle
        .fetch(descriptors.into_iter().next().unwrap())
        .await
        .expect("fetch");
    assert_eq!(fetched.entries.len(), 1);
    assert_eq!(fetched.entries[0].raw_bytes.as_ref(), b"a");

    producer.close().await.expect("close producer");
}

#[tokio::test]
async fn dry_run_advances_progress_and_skips_sink() {
    let fx = in_memory_buffer_source(
        "ingest/test/runtime-dryrun/manifest",
        "ingest/test/runtime-dryrun/data",
    )
    .await;

    fx.producer
        .produce(vec![Bytes::from_static(b"a")], logs_envelope())
        .await
        .expect("produce a");
    fx.producer.flush().await.expect("flush a");
    fx.producer
        .produce(vec![Bytes::from_static(b"b")], logs_envelope())
        .await
        .expect("produce b");
    fx.producer.flush().await.expect("flush b");

    let sink = FakeSink::new("fake");
    let captured = Arc::clone(&sink.captured);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(options(true))
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
            if p.batches_read >= 2 {
                return p;
            }
        }
    })
    .await
    .expect("timeout waiting for progress");

    assert_eq!(p.batches_read, 2);
    assert_eq!(p.last_decoded_sequence, Some(1));
    assert!(
        p.last_acked_sequence.is_none(),
        "dry-run must not ack the buffer"
    );

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    assert!(
        captured.lock().unwrap().is_empty(),
        "dry-run must not call Sink::write"
    );

    fx.producer.close().await.expect("close producer");
}

#[tokio::test]
async fn live_mode_writes_to_sink_and_advances_ack_frontier() {
    let fx = in_memory_buffer_source(
        "ingest/test/runtime-live/manifest",
        "ingest/test/runtime-live/data",
    )
    .await;

    for i in 0..3 {
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

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(options(false))
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
            if p.source_ranges_committed >= 3 {
                return p;
            }
        }
    })
    .await
    .expect("timeout waiting for progress");

    assert_eq!(p.batches_read, 3);
    assert_eq!(p.source_ranges_committed, 3);
    assert_eq!(p.last_decoded_sequence, Some(2));
    assert_eq!(p.last_acked_sequence, Some(2));
    assert_eq!(p.records_written, 3);

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let snapshot = captured.lock().unwrap().clone();
    assert_eq!(snapshot.len(), 3, "one SinkCommit per source range");
    for (i, c) in snapshot.iter().enumerate() {
        assert_eq!(c.source, "buffer");
        assert_eq!(c.sink, "fake-sink");
        assert_eq!(c.low_sequence, i as u64);
        assert_eq!(c.high_sequence, i as u64);
        assert_eq!(c.record_count, 1);
        // CommitIdentity Display shape (RFC 0002 §Runtime/Sink
        // Boundary): {source}:{sink}:{low}-{high}:{schema_version}.
        let expected_identity = format!("buffer:fake-sink:{i}-{i}:1");
        assert_eq!(c.identity, expected_identity);
    }

    fx.producer.close().await.expect("close producer");
}

// The two `MaybeCommitted` round-trip tests
// (`runtime_treats_maybe_committed_then_committed_as_success` +
// `runtime_retries_write_after_maybe_committed_when_check_returns_unknown`)
// previously lived here from Phase 4 review round 2 (commit
// 6d28604). Phase 5.4 moves them to `tests/ack_correctness.rs`
// alongside the rest of the INV-MAYBE-COMMITTED-RESOLVES tests.
