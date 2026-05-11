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
struct FakeDecoder;

impl Decoder for FakeDecoder {
    fn accepts(&self, _envelope: &MetadataEnvelope) -> bool {
        true
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
        .add_decoder(FakeDecoder)
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
        .add_decoder(FakeDecoder)
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
