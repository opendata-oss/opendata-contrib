//! End-to-end in-memory test: real Producer + Consumer + Runtime in
//! dry-run mode. Validates that the layered pipeline wires together,
//! that per-entry envelope decoding survives multiple metadata ranges
//! per Buffer batch, and that the recording-sink pattern captures the
//! adapter's planned `InsertChunk`s under a real pipeline.
//!
//! Does not require Docker. The testcontainers-gated test in
//! `tests/clickhouse_round_trip.rs` covers the real-ClickHouse path.
//!
//! Phase 4.4e rewrote this test against `Runtime::builder` and the
//! rev-6 `Sink` trait. The legacy "DroppingAdapter contract guard"
//! test was dropped — the runtime-level chunk-row-count guard moved
//! out with `BufferConsumerRuntime`; reintroducing it at the sink
//! layer is a Phase-5 correctness-harness concern.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use clickhouse_ingestor::{
    Adapter, ClickHouseAdapterBatch, DecodedLogRecord, InsertChunk, LogsAdapterConfig,
    OtlpLogsClickHouseAdapter, OtlpLogsDecoder,
};
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use opendata_ingest_otel::logs::TypedDecodedLogs;
use opendata_ingest_runtime::decoded_batch::DecodedRecords;
use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::error::{RuntimeError, RuntimeResult};
use opendata_ingest_runtime::identity::CommitIdentity;
use opendata_ingest_runtime::runtime::{AckFlushPolicy, Runtime, RuntimeOptions};
use opendata_ingest_runtime::sink::{
    CommitStatus, Sink, SinkBudget, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId,
};
use opendata_ingest_runtime::source::BufferSource;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Build an OTLP ExportLogsServiceRequest with a single resource_log
/// containing `record_count` log records, each tagged so the test
/// can match them back.
fn make_logs(service: &str, record_count: usize) -> Vec<u8> {
    let log_records = (0..record_count)
        .map(|i| LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
            observed_time_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!("body-{i}"))),
            }),
            attributes: vec![KeyValue {
                key: "i".into(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(i as i64)),
                }),
            }],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        })
        .collect();
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service.to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

/// Per-entry envelope: version=1, signal=Logs, encoding=OtlpProtobuf.
fn logs_envelope() -> Bytes {
    Bytes::from_static(&[1, 2, 1, 0])
}

/// Sink that satisfies the trait but is never invoked — Runtime's
/// `dry_run=true` short-circuits the write path before `write` is
/// called. Carrying a real `SinkId` keeps logs and metric labels
/// stable across dry-run vs live runs.
struct DryRunSink {
    id: SinkId,
}

#[async_trait]
impl Sink for DryRunSink {
    fn id(&self) -> &SinkId {
        &self.id
    }
    fn write_budget(&self) -> SinkBudget {
        SinkBudget::default()
    }
    async fn write(&self, _commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        Err(SinkCommitFailure::Fatal(
            "DryRunSink::write called; runtime should have short-circuited via dry_run=true"
                .to_string()
                .into(),
        ))
    }
    async fn check_committed(&self, _identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
        Ok::<CommitStatus, RuntimeError>(CommitStatus::Unknown)
    }
}

/// Sink that records the planned `InsertChunk`s the adapter would
/// emit, then returns Ok without making any HTTP calls. Equivalent
/// of the legacy `RecordingAdapter` decorator under the new sink
/// trait — gives the test access to chunk-token shapes without a
/// real ClickHouse instance.
#[derive(Clone)]
struct RecordingSink {
    id: SinkId,
    adapter: Arc<OtlpLogsClickHouseAdapter>,
    captured: Arc<Mutex<Vec<InsertChunk>>>,
}

#[async_trait]
impl Sink for RecordingSink {
    fn id(&self) -> &SinkId {
        &self.id
    }
    fn write_budget(&self) -> SinkBudget {
        SinkBudget::default()
    }
    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        let SinkCommit { identity, batch } = commit;
        let DecodedRecords::Typed(records) = batch.records;
        let logs = records
            .as_any()
            .downcast_ref::<TypedDecodedLogs>()
            .ok_or_else(|| {
                SinkCommitFailure::Fatal(
                    "RecordingSink expects TypedDecodedLogs".to_string().into(),
                )
            })?;
        let selected: Vec<DecodedLogRecord> = logs.records().to_vec();
        let bytes: usize = selected.iter().map(|r| r.approx_size_bytes()).sum();
        let group = ClickHouseAdapterBatch {
            identity,
            records: selected,
            bytes,
        };
        let chunks = self
            .adapter
            .plan(group)
            .map_err(|e| SinkCommitFailure::Fatal(Box::new(e)))?;
        let bytes_written: u64 = chunks
            .iter()
            .map(|c| c.rows.iter().map(|r| r.len() as u64).sum::<u64>())
            .sum();
        let rows_written: u64 = chunks.iter().map(|c| c.rows_count() as u64).sum();
        self.captured.lock().unwrap().extend(chunks.iter().cloned());
        Ok(SinkCommitResult {
            bytes_written,
            rows_written,
        })
    }
    async fn check_committed(&self, _identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
        Ok::<CommitStatus, RuntimeError>(CommitStatus::Unknown)
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

#[tokio::test]
async fn dry_run_decodes_and_advances_progress_through_real_buffer() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/in-memory-runtime/manifest";
    let data_prefix = "ingest/test/in-memory-runtime/data";

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

    // Push two batches. Each Producer::produce becomes one entry; the
    // producer flush coalesces them into one Buffer batch with two
    // metadata ranges.
    producer
        .produce(vec![Bytes::from(make_logs("svc-a", 2))], logs_envelope())
        .await
        .expect("produce a");
    producer
        .produce(vec![Bytes::from(make_logs("svc-b", 3))], logs_envelope())
        .await
        .expect("produce b");
    producer.flush().await.expect("flush");

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

    let sink = DryRunSink {
        id: SinkId::from("clickhouse_logs"),
    };
    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(OtlpLogsDecoder::new())
        .set_sink(sink)
        .with_options(options(true))
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let timed = timeout(Duration::from_secs(5), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.records_written >= 5 && p.last_decoded_sequence.is_some() {
                return p;
            }
        }
    })
    .await
    .expect("timeout waiting for progress");

    assert!(timed.last_decoded_sequence.is_some());
    assert!(
        timed.last_acked_sequence.is_none(),
        "dry-run must not ack the buffer"
    );
    assert_eq!(timed.records_written, 5);
    assert!(timed.source_ranges_committed >= 1);

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    producer.close().await.expect("close producer");
}

#[tokio::test]
async fn adapter_chunks_carry_tokens_under_real_pipeline() {
    // Threads a real Producer + Consumer + Runtime with a RecordingSink
    // that captures `InsertChunk`s from the adapter's `plan` call, so
    // we can inspect token shape without an HTTP round-trip. Live
    // mode (dry_run=false) so the sink actually runs.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/recording/manifest";
    let data_prefix = "ingest/test/recording/data";

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

    producer
        .produce(vec![Bytes::from(make_logs("svc", 4))], logs_envelope())
        .await
        .expect("produce");
    producer.flush().await.expect("flush");

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

    let adapter = Arc::new(OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
        max_chunk_rows: 2,
        ..LogsAdapterConfig::default()
    }));
    let captured = Arc::new(Mutex::new(Vec::<InsertChunk>::new()));
    let sink = RecordingSink {
        id: SinkId::from("clickhouse_logs"),
        adapter: Arc::clone(&adapter),
        captured: Arc::clone(&captured),
    };

    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(OtlpLogsDecoder::new())
        .set_sink(sink)
        .with_options(options(false))
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let _ = timeout(Duration::from_secs(5), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.source_ranges_committed >= 1 {
                return p;
            }
        }
    })
    .await
    .expect("progress timeout");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let chunks: Vec<InsertChunk> = captured.lock().unwrap().clone();
    // 4 records / max_chunk_rows=2 → 2 chunks for the (single) source range.
    assert_eq!(chunks.len(), 2, "expected 2 chunks, got {chunks:?}");
    let tokens: Vec<&str> = chunks
        .iter()
        .map(|c| c.idempotency_token.as_str())
        .collect();
    // Chunk 0 and chunk 1 must differ (the per-chunk index varies).
    assert_ne!(tokens[0], tokens[1]);
    for chunk in &chunks {
        assert_eq!(chunk.database, "responsive");
        assert_eq!(chunk.table, "logs");
        let token = &chunk.idempotency_token;
        // Token shape is {manifest}:{db}.{table}:{low}-{high}:{ver}:{fp}:{idx}
        let manifest_prefix = format!("{manifest_path}:responsive.logs:");
        assert!(
            token.starts_with(&manifest_prefix),
            "token must start with {manifest_prefix}, got {token}"
        );
    }
    producer.close().await.expect("close producer");
}
