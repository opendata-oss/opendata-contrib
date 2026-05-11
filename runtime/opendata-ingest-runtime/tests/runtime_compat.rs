//! End-to-end runtime smoke test: real Producer + Consumer +
//! `BufferSource` + `Runtime`, with a fake decoder and a fake sink
//! that captures `SinkCommit`s. Validates that the serial loop
//! reads from the buffer, runs the decoder, writes to the sink (in
//! live mode), and advances the ack frontier.
//!
//! Does not depend on `opendata-ingest-clickhouse` or
//! `opendata-ingest-otel`; the runtime crate's compat surface stands
//! on its own.

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use opendata_ingest_runtime::decoded_batch::{
    BatchStats, DecodedBatch, DecodedRecords, SourceCoordinateColumns, TypedRecords, TypedSchema,
};
use opendata_ingest_runtime::decoder::Decoder;
use opendata_ingest_runtime::envelope::{
    ConfiguredEnvelope, MetadataEnvelope, PayloadEncoding, SignalType,
};
use opendata_ingest_runtime::error::{RuntimeError, RuntimeResult};
use opendata_ingest_runtime::idempotency::{IdempotencyKey, SchemaVersion};
use opendata_ingest_runtime::runtime::{AckFlushPolicy, Runtime, RuntimeOptions};
use opendata_ingest_runtime::sink::{
    CommitStatus, Sink, SinkBudget, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId,
};
use opendata_ingest_runtime::source::{BufferSource, SourceBatch};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// 4-byte metadata envelope matching the runtime's configured shape:
/// version=1, signal=Logs, encoding=OtlpProtobuf. The fake decoder
/// accepts any envelope, but the runtime's `validate_consistent`
/// gate runs first and would reject a mismatch.
fn logs_envelope() -> Bytes {
    Bytes::from_static(&[1, 2, 1, 0])
}

#[derive(Debug)]
struct FakeRecords {
    schema: TypedSchema,
    count: usize,
}

impl FakeRecords {
    fn new(count: usize) -> Self {
        Self {
            schema: TypedSchema {
                name: "fake.test.v1".into(),
                version: SchemaVersion(1),
            },
            count,
        }
    }
}

impl TypedRecords for FakeRecords {
    fn record_count(&self) -> usize {
        self.count
    }
    fn estimated_bytes(&self) -> usize {
        self.count * 16
    }
    fn schema(&self) -> &TypedSchema {
        &self.schema
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Fake decoder: produces one `DecodedBatch` per `SourceBatch` whose
/// `record_count == entry_count`. Doesn't actually parse the
/// payload; just shapes a batch the runtime can carry to the sink.
/// `accepts_anything` lets a test toggle the `Decoder::accepts` gate
/// without rebuilding the whole struct.
struct FakeDecoder {
    accepts_anything: bool,
}

impl FakeDecoder {
    fn permissive() -> Self {
        Self {
            accepts_anything: true,
        }
    }
    fn rejecting() -> Self {
        Self {
            accepts_anything: false,
        }
    }
}

impl Decoder for FakeDecoder {
    fn accepts(&self, _envelope: &MetadataEnvelope) -> bool {
        self.accepts_anything
    }

    fn decode(&self, batch: SourceBatch) -> RuntimeResult<Vec<DecodedBatch>> {
        let entry_count = batch.entries.len();
        let source = batch.source.clone();
        let sequence = batch.sequence;
        let source_columns = SourceCoordinateColumns {
            manifest_path: batch.manifest_path.clone(),
            data_path: batch.data_object_path.clone(),
            sequences: vec![sequence; entry_count],
            entry_indices: (0..entry_count as u32).collect(),
            record_indices: vec![0; entry_count],
            ingestion_time_ms: batch.entries.iter().map(|e| e.ingestion_time_ms).collect(),
        };
        Ok(vec![DecodedBatch {
            source,
            low_sequence: sequence,
            high_sequence: sequence,
            source_entry_count: entry_count as u32,
            records: DecodedRecords::Typed(Arc::new(FakeRecords::new(entry_count))),
            source_columns,
            stats: BatchStats {
                source_byte_count: 0,
                decoded_byte_estimate: (entry_count * 16) as u64,
            },
            schema_version: SchemaVersion(1),
        }])
    }
}

#[derive(Debug, Clone)]
struct CapturedCommit {
    source: String,
    sink: String,
    low_sequence: u64,
    high_sequence: u64,
    idempotency_key: String,
    record_count: usize,
}

#[derive(Clone)]
struct FakeSink {
    id: SinkId,
    captured: Arc<Mutex<Vec<CapturedCommit>>>,
}

#[async_trait]
impl Sink for FakeSink {
    fn id(&self) -> &SinkId {
        &self.id
    }
    fn write_budget(&self) -> SinkBudget {
        SinkBudget::default()
    }
    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        let record_count = match &commit.batch.records {
            DecodedRecords::Typed(t) => t.record_count(),
        };
        self.captured.lock().unwrap().push(CapturedCommit {
            source: commit.source.to_string(),
            sink: commit.sink.to_string(),
            low_sequence: commit.low_sequence,
            high_sequence: commit.high_sequence,
            idempotency_key: commit.idempotency_key.to_string(),
            record_count,
        });
        Ok(SinkCommitResult {
            bytes_written: 0,
            rows_written: record_count as u64,
        })
    }
    async fn check_committed(&self, _key: &IdempotencyKey) -> RuntimeResult<CommitStatus> {
        Ok::<CommitStatus, RuntimeError>(CommitStatus::Unknown)
    }
}

struct Fixture {
    producer: buffer::Producer,
    source: BufferSource,
}

async fn fixture(store: Arc<dyn ObjectStore>, manifest_path: &str, data_prefix: &str) -> Fixture {
    let producer_config = buffer::ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.into(),
        manifest_path: manifest_path.into(),
        flush_interval: Duration::from_secs(24 * 3600),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1000,
        batch_compression: buffer::CompressionType::None,
    };
    let producer = buffer::Producer::with_object_store(
        producer_config,
        Arc::clone(&store),
        Arc::new(SystemClock),
    )
    .expect("producer");

    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer = buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None)
        .await
        .expect("consumer");
    let source = BufferSource::new(consumer, "buffer", manifest_path, None);

    Fixture { producer, source }
}

/// Scripted response from `ProgrammableSink::write`. The test pushes a
/// queue of these and the sink pops one per `write` call. The full
/// variant set is exposed so future tests (Phase 5 correctness
/// harness) can script every `SinkCommitFailure` shape.
#[derive(Clone)]
#[allow(dead_code)]
enum ScriptedWrite {
    Ok { rows_written: u64 },
    MaybeCommitted { message: String },
    NotCommitted { message: String },
    Fatal { message: String },
}

/// Sink that consumes a scripted sequence of `write` responses and
/// returns a configured `check_committed` answer. Records every
/// `write` call and every `check_committed` call so a test can
/// assert the runtime's `MaybeCommitted → check_committed → retry`
/// protocol.
#[derive(Clone)]
struct ProgrammableSink {
    id: SinkId,
    write_script: Arc<Mutex<std::collections::VecDeque<ScriptedWrite>>>,
    write_calls: Arc<Mutex<Vec<u64>>>,
    check_committed_response: CommitStatus,
    check_committed_calls: Arc<Mutex<Vec<String>>>,
}

impl ProgrammableSink {
    fn new(
        id: impl Into<SinkId>,
        script: Vec<ScriptedWrite>,
        check_committed_response: CommitStatus,
    ) -> Self {
        Self {
            id: id.into(),
            write_script: Arc::new(Mutex::new(script.into())),
            write_calls: Arc::new(Mutex::new(Vec::new())),
            check_committed_response,
            check_committed_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Sink for ProgrammableSink {
    fn id(&self) -> &SinkId {
        &self.id
    }
    fn write_budget(&self) -> SinkBudget {
        SinkBudget::default()
    }
    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        self.write_calls.lock().unwrap().push(commit.high_sequence);
        let next = self
            .write_script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ScriptedWrite::Fatal {
                message: "ProgrammableSink: script exhausted".into(),
            });
        match next {
            ScriptedWrite::Ok { rows_written } => Ok(SinkCommitResult {
                bytes_written: 0,
                rows_written,
            }),
            ScriptedWrite::MaybeCommitted { message } => {
                Err(SinkCommitFailure::MaybeCommitted(message.into()))
            }
            ScriptedWrite::NotCommitted { message } => {
                Err(SinkCommitFailure::NotCommitted(message.into()))
            }
            ScriptedWrite::Fatal { message } => Err(SinkCommitFailure::Fatal(message.into())),
        }
    }
    async fn check_committed(&self, key: &IdempotencyKey) -> RuntimeResult<CommitStatus> {
        self.check_committed_calls
            .lock()
            .unwrap()
            .push(key.to_string());
        Ok::<CommitStatus, RuntimeError>(self.check_committed_response)
    }
}

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
    }
}

/// Phase 4 review HIGH-1 regression. A `BufferSource` constructed
/// with `last_acked_sequence: None` and a producer that has already
/// advanced past sequence 0 should NOT replay `Consumer::ack(0)..
/// =Consumer::ack(N)` on the first `ack_through(N)` call. Anchor the
/// ack range at the first sequence the source actually handed out.
///
/// The test drains 3 batches with one `BufferSource`, drops it, then
/// produces one more batch (sequence 3) and constructs a fresh
/// `BufferSource` with `last_acked: None`. The fresh source's first
/// `next_descriptors` returns sequence 3; the fixed `ack_through(3)`
/// calls `Consumer::ack(3)` exactly once. The bug would have called
/// `ack(0), ack(1), ack(2), ack(3)` against a consumer that already
/// dequeued 0..2 — silently advancing through fabricated sequences.
#[tokio::test]
async fn ack_through_anchors_at_first_seen_after_resume() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/resume-ack/manifest";
    let data_prefix = "ingest/test/resume-ack/data";

    let producer_config = buffer::ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.into(),
        manifest_path: manifest_path.into(),
        flush_interval: Duration::from_secs(24 * 3600),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1000,
        batch_compression: buffer::CompressionType::None,
    };
    let producer = buffer::Producer::with_object_store(
        producer_config,
        Arc::clone(&store),
        Arc::new(SystemClock),
    )
    .expect("producer");

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

    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer1 =
        buffer::Consumer::with_object_store(consumer_config.clone(), Arc::clone(&store), None)
            .await
            .expect("consumer1");
    let mut source1 = BufferSource::new(consumer1, "buffer", manifest_path, None);

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

    let consumer2 =
        buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), Some(2))
            .await
            .expect("consumer2");
    // The point of HIGH-1 is the `last_acked_sequence: None` case.
    // Even though the consumer above is constructed with `Some(2)`
    // (a real restart would persist that durably), the
    // BufferSource is intentionally constructed with `None` to
    // simulate the binary's current main.rs wiring.
    let mut source2 = BufferSource::new(consumer2, "buffer", manifest_path, None);
    assert_eq!(source2.last_acked_sequence(), None);

    let descs = source2
        .next_descriptors(1, Default::default())
        .await
        .expect("next_descriptors after resume");
    assert_eq!(descs.len(), 1);
    assert_eq!(
        descs[0].sequence, 3,
        "fresh consumer with last_acked=Some(2) resumes at 3"
    );
    let _ = source2
        .fetch_handle()
        .fetch(descs[0].clone())
        .await
        .expect("fetch after resume");

    // Before the HIGH-1 fix, this called Consumer::ack(0), ack(1),
    // ack(2), ack(3) — Consumer::ack(0) is accepted by buffer
    // v0.2.0's first-ack-is-unrestricted rule, then ack(1) and
    // ack(2) succeed (sequential), but they're fabricated acks
    // for sequences this source never observed. The fix anchors
    // at first_seen=3 and issues a single ack(3).
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
/// false. Per RFC 0002 rev 6 §`Decoder`, the runtime enforces the
/// plugin boundary by gating decode on accepts.
#[tokio::test]
async fn runtime_fails_closed_when_decoder_rejects_envelope() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fixture = fixture(
        Arc::clone(&store),
        "ingest/test/decoder-rejects/manifest",
        "ingest/test/decoder-rejects/data",
    )
    .await;
    let Fixture { producer, source } = fixture;

    producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    producer.flush().await.expect("flush");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = FakeSink {
        id: SinkId::from("fake"),
        captured: Arc::clone(&captured),
    };

    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(FakeDecoder::rejecting())
        .set_sink(sink)
        .with_options(options(false))
        .build()
        .expect("build");

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    // The runtime should error out within a poll interval as soon as
    // it reads the first batch.
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

    producer.close().await.expect("close producer");
}

#[tokio::test]
async fn buffer_source_returns_descriptors_after_producer_flush() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fixture = fixture(
        Arc::clone(&store),
        "ingest/test/source-smoke/manifest",
        "ingest/test/source-smoke/data",
    )
    .await;
    let Fixture {
        producer,
        mut source,
    } = fixture;

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
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fixture = fixture(
        Arc::clone(&store),
        "ingest/test/runtime-dryrun/manifest",
        "ingest/test/runtime-dryrun/data",
    )
    .await;
    let Fixture { producer, source } = fixture;

    producer
        .produce(vec![Bytes::from_static(b"a")], logs_envelope())
        .await
        .expect("produce a");
    producer.flush().await.expect("flush a");
    producer
        .produce(vec![Bytes::from_static(b"b")], logs_envelope())
        .await
        .expect("produce b");
    producer.flush().await.expect("flush b");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = FakeSink {
        id: SinkId::from("fake"),
        captured: Arc::clone(&captured),
    };

    let runtime = Runtime::builder()
        .add_source(source)
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

    producer.close().await.expect("close producer");
}

#[tokio::test]
async fn live_mode_writes_to_sink_and_advances_ack_frontier() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fixture = fixture(
        Arc::clone(&store),
        "ingest/test/runtime-live/manifest",
        "ingest/test/runtime-live/data",
    )
    .await;
    let Fixture { producer, source } = fixture;

    for i in 0..3 {
        producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        producer.flush().await.expect("flush");
    }

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = FakeSink {
        id: SinkId::from("fake-sink"),
        captured: Arc::clone(&captured),
    };

    let runtime = Runtime::builder()
        .add_source(source)
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

    let snapshot: Vec<CapturedCommit> = captured.lock().unwrap().clone();
    assert_eq!(snapshot.len(), 3, "one SinkCommit per source range");
    for (i, c) in snapshot.iter().enumerate() {
        assert_eq!(c.source, "buffer");
        assert_eq!(c.sink, "fake-sink");
        assert_eq!(c.low_sequence, i as u64);
        assert_eq!(c.high_sequence, i as u64);
        assert_eq!(c.record_count, 1);
        // Default idempotency key shape: {source}:{sink}:{low}-{high}:{schema_version}:{chunking_fingerprint:016x}
        let expected_key = format!("buffer:fake-sink:{i}-{i}:1:0000000000000000");
        assert_eq!(c.idempotency_key, expected_key);
    }

    producer.close().await.expect("close producer");
}

/// Phase 4 review (round 2) coverage gap. The runtime branches on
/// `SinkCommitFailure::MaybeCommitted` by calling
/// `Sink::check_committed`; if the sink reports `Committed`, the
/// runtime must NOT issue a second `write` and the ack frontier
/// must still advance. Verifies the RFC 0002 rev 6 §`Sink` rule
/// that an ambiguous prior write whose commit is later confirmed
/// is treated as a successful commit.
#[tokio::test]
async fn runtime_treats_maybe_committed_then_committed_as_success() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fixture = fixture(
        Arc::clone(&store),
        "ingest/test/maybe-committed-then-committed/manifest",
        "ingest/test/maybe-committed-then-committed/data",
    )
    .await;
    let Fixture { producer, source } = fixture;

    producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        "programmable",
        vec![ScriptedWrite::MaybeCommitted {
            message: "ambiguous insert; prior attempt may have committed".into(),
        }],
        CommitStatus::Committed,
    );
    let write_calls = Arc::clone(&sink.write_calls);
    let check_calls = Arc::clone(&sink.check_committed_calls);

    let mut opts = options(false);
    // Allow at most one retry so the test surfaces a failure if the
    // runtime ignores the Committed signal and retries the write.
    opts.max_retry_attempts = 3;
    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
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

    assert_eq!(
        p.last_acked_sequence,
        Some(0),
        "ack frontier must advance once check_committed → Committed"
    );

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
        "runtime must call check_committed exactly once for the MaybeCommitted; got checks={checks:?}"
    );

    producer.close().await.expect("close producer");
}

/// Phase 4 review (round 2) coverage gap. The other half of the
/// MaybeCommitted protocol: when `check_committed` returns
/// `Unknown` (the ClickHouse default — its short-window dedupe
/// token has expired by the time the runtime asks), the runtime
/// treats Unknown like NotCommitted and retries the write. RFC
/// 0002 rev 6 §`Sink::SinkCommitFailure`: "treats Unknown like
/// NotCommitted for retry; relies on table-level dedupe".
#[tokio::test]
async fn runtime_retries_write_after_maybe_committed_when_check_returns_unknown() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fixture = fixture(
        Arc::clone(&store),
        "ingest/test/maybe-committed-then-unknown/manifest",
        "ingest/test/maybe-committed-then-unknown/data",
    )
    .await;
    let Fixture { producer, source } = fixture;

    producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        "programmable",
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

    let mut opts = options(false);
    opts.max_retry_attempts = 3;
    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
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
    // The Ok response carried rows_written=1; runtime should pick it
    // up from the SinkCommitResult after the retry, not from the
    // decoded count.
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
        "runtime must call check_committed exactly once for the MaybeCommitted; got checks={checks:?}"
    );

    producer.close().await.expect("close producer");
}
