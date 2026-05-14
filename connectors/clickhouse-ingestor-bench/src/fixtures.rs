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
use tokio_util::sync::CancellationToken;

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

/// `TypedRecords` impl that reports a configurable
/// `estimated_bytes`. Used by the
/// `sink_outage_backpressure_bounded` scenario to drive the
/// post-decode reservation up to (and at) `max_inflight_bytes`
/// so the bound assertion is meaningful — `FakeRecords`
/// reports ~16 bytes/record, which would shrink the reservation
/// far below the budget and never trigger backpressure.
#[derive(Debug)]
pub struct LargeRecords {
    schema: TypedSchema,
    count: usize,
    bytes: usize,
}

impl LargeRecords {
    pub fn new(count: usize, bytes: usize) -> Self {
        Self {
            schema: TypedSchema {
                name: "bench.large.v1".into(),
                version: SchemaVersion(1),
            },
            count,
            bytes,
        }
    }
}

impl TypedRecords for LargeRecords {
    fn record_count(&self) -> usize {
        self.count
    }
    fn estimated_bytes(&self) -> usize {
        self.bytes
    }
    fn schema(&self) -> &TypedSchema {
        &self.schema
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Decoder that emits one `DecodedBatch` whose
/// `estimated_bytes` is the configured value. Mirrors the
/// runtime test's `large_records::LargeDecoder`.
pub struct LargeDecoder {
    pub bytes_per_batch: usize,
}

impl Decoder for LargeDecoder {
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
            records: DecodedRecords::Typed(Arc::new(LargeRecords::new(
                entry_count,
                self.bytes_per_batch,
            ))),
            source_columns,
            stats: BatchStats {
                source_byte_count: 0,
                decoded_byte_estimate: self.bytes_per_batch as u64,
            },
            schema_version: SchemaVersion(1),
        }])
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

/// BenchSink's internal per-attempt outcome. The
/// [`crate::test_observable_sink::TestObservableSink`] trait
/// exposes a higher-level `ScriptedWrite` enum (e.g.
/// `MaybeCommittedThenOk`) that this internal type is the
/// per-attempt expansion of.
#[derive(Clone, Debug)]
pub(crate) enum BenchAttempt {
    Ok { rows_written: u64 },
    MaybeCommitted { message: String },
}

/// Per-sequence sink latency closure. Factored out for clippy's
/// `type_complexity` lint.
pub type LatencyFn = Arc<dyn Fn(u64) -> Option<Duration> + Send + Sync>;

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
    /// Per-sequence attempt queue. BenchSink pops the front entry
    /// per `Sink::write` call; falls back to `Ok { rows_written: 1 }`
    /// when empty / absent. The `TestObservableSink::set_per_sequence_forced_outcome`
    /// trait method translates the high-level
    /// `ScriptedWrite::MaybeCommittedThenOk` into
    /// `[MaybeCommitted, Ok]` here.
    per_sequence_script: Arc<Mutex<std::collections::HashMap<u64, VecDeque<BenchAttempt>>>>,
    /// Per-sequence latency. The sink sleeps `latency_fn(seq)`
    /// (if `Some(d)`) before consulting the script.
    latency_fn: Arc<Mutex<Option<LatencyFn>>>,
    /// `TestObservableSink::set_commit_observer` slot. Fires after
    /// each `Ok` write resolves.
    commit_observer: Arc<Mutex<Option<Arc<dyn crate::test_observable_sink::CommitObserver>>>>,
    /// Per-sequence block. `write` for a sequence with a
    /// registered `CancellationToken` parks on
    /// `token.cancelled().await` BEFORE consulting the script.
    /// Tests use this to deterministically hold a specific
    /// sequence's commit until they've made assertions about
    /// peer-sequence state; releasing via `token.cancel()`
    /// resolves any current / future waiter (lost-wakeup safe,
    /// unlike `Notify::notify_waiters`).
    per_sequence_block: Arc<Mutex<std::collections::HashMap<u64, CancellationToken>>>,
    /// Captured-writes log accumulated by every `write` call.
    /// Drained via `TestObservableSink::drain_captured_writes`.
    write_calls: Arc<Mutex<Vec<crate::test_observable_sink::CapturedWrite>>>,
    check_committed_response: Arc<Mutex<CommitStatus>>,
}

impl BenchSink {
    pub fn new(id: impl Into<SinkId>) -> Self {
        Self {
            id: id.into(),
            per_sequence_script: Arc::new(Mutex::new(std::collections::HashMap::new())),
            latency_fn: Arc::new(Mutex::new(None)),
            commit_observer: Arc::new(Mutex::new(None)),
            per_sequence_block: Arc::new(Mutex::new(std::collections::HashMap::new())),
            write_calls: Arc::new(Mutex::new(Vec::new())),
            check_committed_response: Arc::new(Mutex::new(CommitStatus::Unknown)),
        }
    }

    pub fn set_latency_fn(&self, f: LatencyFn) {
        *self.latency_fn.lock().unwrap() = Some(f);
    }

    /// Remove the block for `seq` so subsequent writes against
    /// it bypass the gate. Useful for retry paths.
    pub fn clear_per_sequence_block(&self, seq: u64) {
        self.per_sequence_block.lock().unwrap().remove(&seq);
    }

    async fn await_per_sequence_block(&self, seq: u64) {
        let token = self.per_sequence_block.lock().unwrap().get(&seq).cloned();
        if let Some(t) = token {
            t.cancelled().await;
        }
    }

    pub fn set_check_committed_response(&self, r: CommitStatus) {
        *self.check_committed_response.lock().unwrap() = r;
    }

    fn lookup_latency(&self, sequence: u64) -> Option<Duration> {
        let guard = self.latency_fn.lock().unwrap();
        guard.as_ref().and_then(|f| f(sequence))
    }

    fn next_attempt(&self, sequence: u64) -> BenchAttempt {
        let mut script_map = self.per_sequence_script.lock().unwrap();
        if let Some(queue) = script_map.get_mut(&sequence)
            && let Some(next) = queue.pop_front()
        {
            return next;
        }
        BenchAttempt::Ok { rows_written: 1 }
    }
}

#[async_trait]
impl crate::test_observable_sink::TestObservableSink for BenchSink {
    fn set_per_sequence_block(&self, seq: u64) -> CancellationToken {
        let token = CancellationToken::new();
        self.per_sequence_block
            .lock()
            .unwrap()
            .insert(seq, token.clone());
        token
    }

    fn set_per_sequence_forced_outcome(
        &self,
        seq: u64,
        outcome: crate::test_observable_sink::ScriptedWrite,
    ) {
        let script: Vec<BenchAttempt> = match outcome {
            crate::test_observable_sink::ScriptedWrite::Ok => {
                vec![BenchAttempt::Ok { rows_written: 1 }]
            }
            crate::test_observable_sink::ScriptedWrite::MaybeCommittedThenOk => vec![
                BenchAttempt::MaybeCommitted {
                    message: format!("scripted MaybeCommitted on seq={seq}"),
                },
                BenchAttempt::Ok { rows_written: 1 },
            ],
        };
        self.per_sequence_script
            .lock()
            .unwrap()
            .insert(seq, script.into());
    }

    fn set_commit_observer(&self, observer: Arc<dyn crate::test_observable_sink::CommitObserver>) {
        *self.commit_observer.lock().unwrap() = Some(observer);
    }

    fn drain_captured_writes(&self) -> Vec<crate::test_observable_sink::CapturedWrite> {
        std::mem::take(&mut *self.write_calls.lock().unwrap())
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
        if let Some(d) = self.lookup_latency(seq) {
            tokio::time::sleep(d).await;
        }
        self.await_per_sequence_block(seq).await;
        match self.next_attempt(seq) {
            BenchAttempt::Ok { rows_written } => {
                // Fire the post-Ok observer SYNCHRONOUSLY with the
                // sink's response so the event lands in the shared
                // log before the writer worker emits the
                // `WriteCompletion::Committed` (which in turn
                // triggers `ack_through`) — proving the temporal
                // ordering required by INV-NO-ACK-BEFORE-COMMIT.
                let observer = self.commit_observer.lock().unwrap().clone();
                if let Some(cb) = observer {
                    cb.record_commit(&commit.identity);
                }
                self.write_calls
                    .lock()
                    .unwrap()
                    .push(crate::test_observable_sink::CapturedWrite {
                        identity: commit.identity.clone(),
                        outcome: crate::test_observable_sink::WriteOutcome::Committed {
                            rows: rows_written,
                        },
                    });
                Ok(SinkCommitResult {
                    bytes_written: 0,
                    rows_written,
                })
            }
            BenchAttempt::MaybeCommitted { message } => {
                self.write_calls
                    .lock()
                    .unwrap()
                    .push(crate::test_observable_sink::CapturedWrite {
                        identity: commit.identity.clone(),
                        outcome: crate::test_observable_sink::WriteOutcome::Failure(
                            crate::test_observable_sink::SinkCommitFailureKind::MaybeCommitted,
                        ),
                    });
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
