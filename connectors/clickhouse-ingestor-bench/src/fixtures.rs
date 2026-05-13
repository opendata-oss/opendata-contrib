//! In-memory test fixtures: `BenchSink` + `FakeDecoder` +
//! `in_memory_buffer_source`. Mirrors the relevant pieces of
//! `runtime/opendata-ingest-runtime/tests/support/mod.rs` but lives
//! here so the bench crate stays self-contained — the runtime
//! crate's `tests/support/` module is only reachable from
//! integration tests under `runtime/.../tests/`, not from other
//! workspace crates.

use std::any::Any;
use std::collections::VecDeque;
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
use opendata_ingest_runtime::envelope::MetadataEnvelope;
use opendata_ingest_runtime::error::{RuntimeError, RuntimeResult};
use opendata_ingest_runtime::identity::{CommitIdentity, SchemaVersion};
use opendata_ingest_runtime::sink::{
    CommitStatus, Sink, SinkBudget, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId,
};
use opendata_ingest_runtime::source::BufferSource;
use slatedb::object_store::ObjectStore;

/// 4-byte metadata envelope matching the runtime's configured
/// shape: version=1, signal=Logs, encoding=OtlpProtobuf.
pub fn logs_envelope() -> Bytes {
    Bytes::from_static(&[1, 2, 1, 0])
}

#[derive(Debug)]
pub struct FakeRecords {
    schema: TypedSchema,
    count: usize,
}

impl FakeRecords {
    pub fn new(count: usize) -> Self {
        Self {
            schema: TypedSchema {
                name: "bench.fake.v1".into(),
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

/// Permissive decoder that emits one `DecodedBatch` per
/// `SourceBatch`. The bench scenarios drive small synthetic
/// payloads; correctness checks don't depend on schema shape.
pub struct FakeDecoder;

impl Decoder for FakeDecoder {
    fn accepts(&self, _envelope: &MetadataEnvelope) -> bool {
        true
    }

    fn decode(
        &self,
        batch: opendata_ingest_runtime::source::SourceBatch,
    ) -> RuntimeResult<Vec<DecodedBatch>> {
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

/// Scripted sink response. The bench's smoke run uses `Ok` for
/// every-batch scenarios and `MaybeCommitted` for the
/// `maybe_committed_replay_idempotent` scenario.
#[derive(Clone, Debug)]
pub enum ScriptedWrite {
    Ok { rows_written: u64 },
    MaybeCommitted { message: String },
}

#[derive(Debug, Clone)]
pub struct CapturedWrite {
    pub identity: CommitIdentity,
    pub identity_string: String,
}

/// Per-sequence sink latency closure. Factored out for clippy's
/// `type_complexity` lint.
pub type LatencyFn = Arc<dyn Fn(u64) -> Option<Duration> + Send + Sync>;

/// Callback fired AFTER the sink resolves a write to `Ok(...)`.
/// Receives the committed sequence. Used by the bench harness's
/// `no_ack_before_sink_commit` scenario to push a SinkCommitOk
/// event onto the same shared ordered log the runtime's
/// `AckThroughObserver` pushes Ack events to.
pub type CommitObserver = Arc<dyn Fn(u64) + Send + Sync>;

/// Sink used by the bench's correctness scenarios. Records every
/// `write` call (so a scenario can assert byte-identical identity
/// across retries), supports per-sequence latency injection (so a
/// scenario can force out-of-order completion or a sink outage),
/// and supports a deterministic per-sequence script for crafting
/// `MaybeCommitted` retries on specific sequences.
///
/// Separate from the runtime crate's `ProgrammableSink` because
/// the bench crate cannot consume runtime tests' `tests/support/`
/// module — that module is not exported as a library item.
#[derive(Clone)]
pub struct BenchSink {
    id: SinkId,
    /// Default response when no per-sequence script entry exists.
    default_response: Arc<Mutex<ScriptedWrite>>,
    /// Per-sequence response. Each `Vec<ScriptedWrite>` is popped
    /// front-to-back on each call against that sequence; falls
    /// back to `default_response` when empty / not present.
    per_sequence_script: Arc<Mutex<std::collections::HashMap<u64, VecDeque<ScriptedWrite>>>>,
    /// Per-sequence latency. The sink sleeps `latency_fn(seq)`
    /// (if `Some(d)`) before consulting the script.
    latency_fn: Arc<Mutex<Option<LatencyFn>>>,
    /// Optional callback fired AFTER a write resolves to `Ok`. The
    /// scenarios that care about temporal ordering (`no_ack_before_sink_commit`)
    /// install one that pushes a SinkCommitOk event onto a shared
    /// ordered log.
    commit_observer: Arc<Mutex<Option<CommitObserver>>>,
    /// Every successful `write` call's identity, in call order.
    pub write_calls: Arc<Mutex<Vec<CapturedWrite>>>,
    check_committed_response: Arc<Mutex<CommitStatus>>,
}

impl BenchSink {
    pub fn new(id: impl Into<SinkId>) -> Self {
        Self {
            id: id.into(),
            default_response: Arc::new(Mutex::new(ScriptedWrite::Ok { rows_written: 1 })),
            per_sequence_script: Arc::new(Mutex::new(std::collections::HashMap::new())),
            latency_fn: Arc::new(Mutex::new(None)),
            commit_observer: Arc::new(Mutex::new(None)),
            write_calls: Arc::new(Mutex::new(Vec::new())),
            check_committed_response: Arc::new(Mutex::new(CommitStatus::Unknown)),
        }
    }

    pub fn set_default_response(&self, r: ScriptedWrite) {
        *self.default_response.lock().unwrap() = r;
    }

    pub fn set_per_sequence_script(&self, sequence: u64, script: Vec<ScriptedWrite>) {
        self.per_sequence_script
            .lock()
            .unwrap()
            .insert(sequence, script.into());
    }

    pub fn set_latency_fn(&self, f: LatencyFn) {
        *self.latency_fn.lock().unwrap() = Some(f);
    }

    /// Install a callback fired after each successful `write`
    /// (i.e. one that resolves to `Ok(SinkCommitResult)`). The
    /// callback fires synchronously with the sink's response
    /// before the writer worker emits `WriteCompletion::Committed`
    /// upstream, so events pushed here are temporally ordered
    /// before the matching ack — that's the property the
    /// `no_ack_before_sink_commit` scenario relies on.
    pub fn set_commit_observer(&self, f: CommitObserver) {
        *self.commit_observer.lock().unwrap() = Some(f);
    }

    pub fn set_check_committed_response(&self, r: CommitStatus) {
        *self.check_committed_response.lock().unwrap() = r;
    }

    fn lookup_latency(&self, sequence: u64) -> Option<Duration> {
        let guard = self.latency_fn.lock().unwrap();
        guard.as_ref().and_then(|f| f(sequence))
    }

    fn next_response(&self, sequence: u64) -> ScriptedWrite {
        let mut script_map = self.per_sequence_script.lock().unwrap();
        if let Some(queue) = script_map.get_mut(&sequence)
            && let Some(next) = queue.pop_front()
        {
            return next;
        }
        self.default_response.lock().unwrap().clone()
    }
}

#[async_trait]
impl Sink for BenchSink {
    fn id(&self) -> &SinkId {
        &self.id
    }
    fn write_budget(&self) -> SinkBudget {
        SinkBudget::default()
    }
    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        let seq = commit.identity.range.high;
        self.write_calls.lock().unwrap().push(CapturedWrite {
            identity: commit.identity.clone(),
            identity_string: commit.identity.to_string(),
        });
        if let Some(d) = self.lookup_latency(seq) {
            tokio::time::sleep(d).await;
        }
        match self.next_response(seq) {
            ScriptedWrite::Ok { rows_written } => {
                // Fire the post-Ok observer SYNCHRONOUSLY with the
                // sink's response so the event lands in the shared
                // log before the writer worker emits the
                // `WriteCompletion::Committed` (which in turn
                // triggers `ack_through`) — proving the temporal
                // ordering required by INV-NO-ACK-BEFORE-COMMIT.
                let observer = self.commit_observer.lock().unwrap().clone();
                if let Some(cb) = observer {
                    cb(seq);
                }
                Ok(SinkCommitResult {
                    bytes_written: 0,
                    rows_written,
                })
            }
            ScriptedWrite::MaybeCommitted { message } => {
                Err(SinkCommitFailure::MaybeCommitted(message.into()))
            }
        }
    }
    async fn check_committed(&self, _identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
        Ok::<CommitStatus, RuntimeError>(*self.check_committed_response.lock().unwrap())
    }
}

/// Reusable in-memory pipeline fixture: `ObjectStore` +
/// `Producer` + `BufferSource`. Each scenario builds a fresh one
/// so witness data doesn't cross-contaminate.
pub struct InMemoryFixture {
    pub store: Arc<dyn ObjectStore>,
    pub producer: buffer::Producer,
    pub source: BufferSource,
    pub manifest_path: String,
    pub data_prefix: String,
}

pub async fn in_memory_fixture(manifest_path: &str, data_prefix: &str) -> InMemoryFixture {
    let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());

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

    InMemoryFixture {
        store,
        producer,
        source,
        manifest_path: manifest_path.into(),
        data_prefix: data_prefix.into(),
    }
}

pub async fn produce_n_batches(producer: &buffer::Producer, n: u64) {
    for i in 0..n {
        producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        producer.flush().await.expect("flush");
    }
}
