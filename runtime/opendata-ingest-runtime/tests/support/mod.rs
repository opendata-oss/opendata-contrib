//! Shared test fixtures for the runtime crate's integration
//! tests. Included via `#[path = "support/mod.rs"] mod support;`
//! from each `tests/*.rs` (Cargo compiles each integration test
//! as its own binary; this module is duplicated into each at
//! compile time).
//!
//! Hosted as a `tests/support` module rather than a library-side
//! `#[cfg(feature = "test-support")] pub mod ...`, which does NOT
//! compose with the default `cargo test --workspace` invocation
//! because integration tests link against the library *as an
//! external dependency*, so `cfg(test)` is not set during that
//! compile. The shared `tests/support/mod.rs` pattern keeps
//! `cargo test --workspace` the canonical invocation.

#![allow(dead_code)] // Not every integration test uses every fixture.

pub mod counting_store;
pub use counting_store::CountingObjectStore;

use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use opendata_ingest_runtime::source::{BufferSource, SourceBatch};
use slatedb::object_store::ObjectStore;
use tokio::sync::Notify;

// =========================================================================
// Envelope helper
// =========================================================================

/// 4-byte metadata envelope matching the runtime's configured
/// shape: version=1, signal=Logs, encoding=OtlpProtobuf. The fake
/// decoder accepts any envelope, but the runtime's
/// `validate_consistent` gate runs first and would reject a
/// mismatch.
pub fn logs_envelope() -> Bytes {
    Bytes::from_static(&[1, 2, 1, 0])
}

// =========================================================================
// FakeRecords / FakeDecoder
// =========================================================================

#[derive(Debug)]
pub struct FakeRecords {
    schema: TypedSchema,
    count: usize,
}

impl FakeRecords {
    pub fn new(count: usize) -> Self {
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

/// Fake decoder: produces one `DecodedBatch` per `SourceBatch`
/// whose `record_count == entry_count`. Doesn't actually parse
/// the payload; just shapes a batch the runtime can carry to the
/// sink. `accepts_anything` lets a test toggle the
/// `Decoder::accepts` gate.
pub struct FakeDecoder {
    pub accepts_anything: bool,
}

impl FakeDecoder {
    pub fn permissive() -> Self {
        Self {
            accepts_anything: true,
        }
    }
    pub fn rejecting() -> Self {
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

// =========================================================================
// CapturedCommit + FakeSink
// =========================================================================

#[derive(Debug, Clone)]
pub struct CapturedCommit {
    pub source: String,
    pub sink: String,
    pub low_sequence: u64,
    pub high_sequence: u64,
    pub identity: String,
    pub record_count: usize,
}

/// Capturing sink that always returns `Ok` and stores every
/// commit it sees. Tests use this for the success path.
#[derive(Clone)]
pub struct FakeSink {
    pub id: SinkId,
    pub captured: Arc<Mutex<Vec<CapturedCommit>>>,
}

impl FakeSink {
    pub fn new(id: impl Into<SinkId>) -> Self {
        Self {
            id: id.into(),
            captured: Arc::new(Mutex::new(Vec::new())),
        }
    }
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
            source: commit.identity.source.to_string(),
            sink: commit.identity.sink.to_string(),
            low_sequence: commit.identity.range.low,
            high_sequence: commit.identity.range.high,
            identity: commit.identity.to_string(),
            record_count,
        });
        Ok(SinkCommitResult {
            bytes_written: 0,
            rows_written: record_count as u64,
        })
    }
    async fn check_committed(&self, _identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
        Ok::<CommitStatus, RuntimeError>(CommitStatus::Unknown)
    }
}

// =========================================================================
// ProgrammableSink + ScriptedWrite + BlockToken
// =========================================================================

/// Scripted response from `ProgrammableSink::write`. The test
/// pushes a queue of these and the sink pops one per `write`
/// call. The full variant set is exposed so a test can script
/// every `SinkCommitFailure` shape.
#[derive(Clone)]
pub enum ScriptedWrite {
    Ok { rows_written: u64 },
    MaybeCommitted { message: String },
    NotCommitted { message: String },
    Fatal { message: String },
}

#[derive(Debug, Clone)]
pub struct CapturedWrite {
    pub source: String,
    pub sink: String,
    pub low_sequence: u64,
    pub high_sequence: u64,
    pub identity: String,
    pub record_count: usize,
}

/// Gate token returned by [`ProgrammableSink::block_until_released`].
/// While alive, every gated sink call (always
/// `check_committed`; also `write` when `also_gate_write` was
/// `true`) blocks on `release.notified().await`. On drop, the
/// token notifies `release` so any parked call proceeds with the
/// configured response.
///
/// The sink increments `entered_count` via
/// `fetch_add(1, Ordering::SeqCst)` **before** firing
/// `entered.notify_one()`, so a test that awaits
/// [`wait_for_entry`](Self::wait_for_entry) and then reads
/// [`entered_count`](Self::entered_count) is guaranteed to
/// observe a count ≥ N after the Nth entry signal.
pub struct BlockToken {
    release: Arc<Notify>,
    entered: Arc<Notify>,
    entered_count: Arc<AtomicUsize>,
    /// Back-reference to the sink's `gate_active` flag so
    /// `Drop` can disengage the gate. Without this, a token's
    /// drop would wake current waiters but leave the gate
    /// armed; a later sink call (in the same test) would park
    /// forever.
    gate_active: Arc<Mutex<bool>>,
}

impl BlockToken {
    /// Resolves when the next gated sink call enters the gate.
    /// Each call to `wait_for_entry().await` resolves on the
    /// next gate entry; a test that wants to wait for N entries
    /// either awaits N times or polls
    /// `entered_count() >= N` after a single notify.
    pub async fn wait_for_entry(&self) {
        self.entered.notified().await;
    }

    pub fn entered_count(&self) -> usize {
        self.entered_count.load(Ordering::SeqCst)
    }
}

impl Drop for BlockToken {
    fn drop(&mut self) {
        // Disengage the gate FIRST so any sink call already
        // racing into `maybe_park` (or arriving later in the
        // same test) sees `gate_active == false` and proceeds
        // without parking. Then wake any waiters currently
        // parked.
        *self.gate_active.lock().unwrap() = false;
        // `Notify::notify_waiters` wakes all waiters present at
        // the time of the call (unlike `notify_one`, which only
        // signals one). Multi-entry scripts can leave more than
        // one waiter parked; wake them all.
        self.release.notify_waiters();
    }
}

/// Closure returning an optional per-sequence sink latency.
/// Returning `Some(d)` makes the sink sleep `d` before consulting
/// the write script for that sequence; returning `None` (or not
/// configuring one at all) leaves the write unsleeping. Used by
/// `pipeline_runtime_level_out_of_order_completion_no_frontier_hole`
/// and `pipeline_slow_sink_injection_caps_inflight_bytes_and_recovers`,
/// plus the bench harness's `sink_outage_backpressure_bounded` and
/// `out_of_order_completion_no_frontier_hole` invariant checks.
pub type SinkLatencyFn = Arc<dyn Fn(u64) -> Option<Duration> + Send + Sync>;

#[derive(Clone)]
pub struct ProgrammableSink {
    pub id: SinkId,
    pub write_script: Arc<Mutex<VecDeque<ScriptedWrite>>>,
    /// Captured `write` calls (one entry per call) — see
    /// [`CapturedWrite`]. Use this in preference to the bare
    /// `write_calls` Vec of high sequences when you need the
    /// idempotency key or record count.
    pub write_calls: Arc<Mutex<Vec<CapturedWrite>>>,
    pub check_committed_response: Arc<Mutex<CommitStatus>>,
    pub check_committed_calls: Arc<Mutex<Vec<String>>>,
    gate_release: Arc<Notify>,
    gate_entered: Arc<Notify>,
    gate_entered_count: Arc<AtomicUsize>,
    gate_active: Arc<Mutex<bool>>,
    gate_writes: Arc<Mutex<bool>>,
    per_sequence_latency: Arc<Mutex<Option<SinkLatencyFn>>>,
    /// Per-sequence block. `write` for a sequence with a registered
    /// `CancellationToken` parks on `token.cancelled().await`
    /// before consulting the script. Tests use this to
    /// deterministically hold a specific commit while assertions
    /// run on peer-sequence state. `CancellationToken` is
    /// lost-wakeup-safe — `cancel()` resolves all waiters
    /// (current and future), so releasing before the writer
    /// parks still bypasses the block.
    per_sequence_block:
        Arc<Mutex<std::collections::HashMap<u64, tokio_util::sync::CancellationToken>>>,
}

impl ProgrammableSink {
    pub fn new(
        id: impl Into<SinkId>,
        script: Vec<ScriptedWrite>,
        check_committed_response: CommitStatus,
    ) -> Self {
        Self {
            id: id.into(),
            write_script: Arc::new(Mutex::new(script.into())),
            write_calls: Arc::new(Mutex::new(Vec::new())),
            check_committed_response: Arc::new(Mutex::new(check_committed_response)),
            check_committed_calls: Arc::new(Mutex::new(Vec::new())),
            gate_release: Arc::new(Notify::new()),
            gate_entered: Arc::new(Notify::new()),
            gate_entered_count: Arc::new(AtomicUsize::new(0)),
            gate_active: Arc::new(Mutex::new(false)),
            gate_writes: Arc::new(Mutex::new(false)),
            per_sequence_latency: Arc::new(Mutex::new(None)),
            per_sequence_block: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Install a per-sequence block. Returns the
    /// `CancellationToken` controlling the gate — test releases
    /// the block by calling `token.cancel()`. `CancellationToken`
    /// is lost-wakeup-safe: cancelling before the writer reaches
    /// the await still resolves the future immediately.
    pub fn set_per_sequence_block(&self, seq: u64) -> tokio_util::sync::CancellationToken {
        let token = tokio_util::sync::CancellationToken::new();
        self.per_sequence_block
            .lock()
            .unwrap()
            .insert(seq, token.clone());
        token
    }

    /// Remove the block for `seq` so subsequent writes against
    /// it bypass the gate.
    pub fn clear_per_sequence_block(&self, seq: u64) {
        self.per_sequence_block.lock().unwrap().remove(&seq);
    }

    async fn await_per_sequence_block(&self, seq: u64) {
        let token = self.per_sequence_block.lock().unwrap().get(&seq).cloned();
        if let Some(t) = token {
            t.cancelled().await;
        }
    }

    /// Install a per-sequence latency closure. Each `write` call
    /// computes `latency(commit.identity.range.high)` and, when the
    /// returned `Option<Duration>` is `Some`, sleeps that long
    /// **before** the gate-park / write-script consultation. Tests
    /// that want different writes to complete in non-source order
    /// configure asymmetric latencies (e.g., seq=0 → 200 ms, every
    /// other seq → 0). The closure may be replaced (`set_…` is
    /// idempotent); `None` removes the latency entirely.
    pub fn set_per_sequence_latency(&self, latency: SinkLatencyFn) {
        *self.per_sequence_latency.lock().unwrap() = Some(latency);
    }

    fn lookup_latency(&self, sequence: u64) -> Option<Duration> {
        let guard = self.per_sequence_latency.lock().unwrap();
        guard.as_ref().and_then(|f| f(sequence))
    }

    /// Replace the script atomically so one sink instance can be
    /// reused across a pre-crash/post-crash test boundary
    /// without rebuilding fixtures.
    ///
    /// Gate state is **not** modified — `replace_script` only
    /// swaps the response queue. To re-engage a gate after a
    /// prior token dropped, call `block_until_released` again.
    pub fn replace_script(&self, new_script: Vec<ScriptedWrite>) {
        *self.write_script.lock().unwrap() = new_script.into();
    }

    /// Engage the gate. While the returned [`BlockToken`] is
    /// alive, every `check_committed` call (always) and every
    /// `write` call (when `also_gate_write` is true) blocks
    /// before returning the scripted response. Tests use this
    /// to park the runtime mid-await for crash-replay or fence
    /// choreography.
    ///
    /// The sink does `fetch_add(1, SeqCst)` on `entered_count`
    /// **before** `notify_one()` so a test that awaits
    /// `wait_for_entry().await` and then reads `entered_count()`
    /// is guaranteed to see a monotonic value ≥ 1.
    ///
    /// Only one active gate per sink at a time; constructing a
    /// second gate while the first is alive replaces the
    /// internal flags but the token's drop semantics still hold
    /// (the previous token's drop will wake any waiters and
    /// clear `gate_active`, which the new token's construction
    /// then resets to `true`).
    pub fn block_until_released(&self, also_gate_write: bool) -> BlockToken {
        *self.gate_active.lock().unwrap() = true;
        *self.gate_writes.lock().unwrap() = also_gate_write;
        // Reset the per-gate counter so a single test can engage
        // multiple gates in sequence and count entries per gate.
        self.gate_entered_count.store(0, Ordering::SeqCst);
        BlockToken {
            release: Arc::clone(&self.gate_release),
            entered: Arc::clone(&self.gate_entered),
            entered_count: Arc::clone(&self.gate_entered_count),
            gate_active: Arc::clone(&self.gate_active),
        }
    }

    /// Update the response `check_committed` returns. Useful
    /// when a test wants to flip the answer mid-run (e.g., first
    /// MaybeCommitted resolves to Unknown, then later to
    /// Committed).
    pub fn set_check_committed_response(&self, response: CommitStatus) {
        *self.check_committed_response.lock().unwrap() = response;
    }

    /// Park the current task at the gate if engaged. The
    /// counter is incremented BEFORE the entry notify fires so
    /// `wait_for_entry().await` + `entered_count()` is
    /// monotonic.
    async fn maybe_park(&self, for_write: bool) {
        let gate_active = *self.gate_active.lock().unwrap();
        if !gate_active {
            return;
        }
        if for_write && !*self.gate_writes.lock().unwrap() {
            return;
        }
        // ORDER: fetch_add BEFORE notify_one, so a waiter that wakes
        // on the notification always observes the incremented count.
        self.gate_entered_count.fetch_add(1, Ordering::SeqCst);
        self.gate_entered.notify_one();
        self.gate_release.notified().await;
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
        let record_count = match &commit.batch.records {
            DecodedRecords::Typed(t) => t.record_count(),
        };
        self.write_calls.lock().unwrap().push(CapturedWrite {
            source: commit.identity.source.to_string(),
            sink: commit.identity.sink.to_string(),
            low_sequence: commit.identity.range.low,
            high_sequence: commit.identity.range.high,
            identity: commit.identity.to_string(),
            record_count,
        });

        // Per-sequence latency runs BEFORE gate-park / script
        // consultation so a slow-sequence test can stall a single
        // write while peer writes flow through unaffected. The
        // closure runs once per call; no clone of `self` is held
        // across the await.
        if let Some(latency) = self.lookup_latency(commit.identity.range.high) {
            tokio::time::sleep(latency).await;
        }

        self.await_per_sequence_block(commit.identity.range.high)
            .await;

        self.maybe_park(true).await;

        let next = self
            .write_script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ScriptedWrite::Fatal {
                message: "ProgrammableSink: write script exhausted".into(),
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

    async fn check_committed(&self, identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
        self.check_committed_calls
            .lock()
            .unwrap()
            .push(identity.to_string());
        self.maybe_park(false).await;
        Ok::<CommitStatus, RuntimeError>(*self.check_committed_response.lock().unwrap())
    }
}

// =========================================================================
// in_memory_buffer_source helper
// =========================================================================

pub struct InMemoryBufferFixture {
    pub store: Arc<dyn ObjectStore>,
    pub producer: buffer::Producer,
    pub source: BufferSource,
    pub manifest_path: String,
    pub data_prefix: String,
}

/// Build an in-memory `ObjectStore` + `Producer` + `Consumer` +
/// `BufferSource`. The producer and source share the same
/// in-memory store; tests can produce entries via `producer`
/// and the source will dequeue them via `next_descriptors`.
/// The source is constructed with `last_acked_sequence: None`
/// (the same shape today's binary wires).
pub async fn in_memory_buffer_source(
    manifest_path: &str,
    data_prefix: &str,
) -> InMemoryBufferFixture {
    let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
    let fixture = build_buffer_fixture(Arc::clone(&store), manifest_path, data_prefix, None).await;
    InMemoryBufferFixture {
        store,
        producer: fixture.producer,
        source: fixture.source,
        manifest_path: manifest_path.into(),
        data_prefix: data_prefix.into(),
    }
}

/// Like [`in_memory_buffer_source`] but reuses a caller-supplied
/// `ObjectStore` (so a test can construct multiple
/// `BufferSource`s — e.g., for fence and crash-replay tests —
/// against the same durable manifest). Returns only the source;
/// the producer (which would re-initialize the manifest and
/// fence the original) is dropped.
pub async fn buffer_source_on_store(
    store: Arc<dyn ObjectStore>,
    manifest_path: &str,
    data_prefix: &str,
    last_acked_sequence: Option<u64>,
) -> BufferSource {
    let fixture =
        build_buffer_fixture(store, manifest_path, data_prefix, last_acked_sequence).await;
    fixture.source
}

/// Like [`buffer_source_on_store`] but also returns the
/// producer. Use when the test needs to interleave production
/// and consumption against a single store. Note that the
/// producer's construction does not bump the manifest epoch
/// (only consumer init does), so producing into a store that
/// already has a live consumer is safe.
pub async fn buffer_source_on_store_with_producer(
    store: Arc<dyn ObjectStore>,
    manifest_path: &str,
    data_prefix: &str,
    last_acked_sequence: Option<u64>,
) -> BufferFixtureParts {
    build_buffer_fixture(store, manifest_path, data_prefix, last_acked_sequence).await
}

pub struct CountingBufferFixture {
    pub store: Arc<CountingObjectStore>,
    pub producer: buffer::Producer,
    pub source: BufferSource,
    pub manifest_path: String,
    pub data_prefix: String,
    pub manifest_gets: Arc<std::sync::atomic::AtomicU64>,
    pub data_gets: Arc<std::sync::atomic::AtomicU64>,
}

/// Like [`in_memory_buffer_source`] but layers a path-filtered
/// `CountingObjectStore` over the in-memory store. The counters
/// reflect every GET against the underlying store after the wrapper
/// is built — producer-side activity will increment them during
/// fixture setup, so callers should `store(0, ...)` both counters
/// after producing test data and before exercising the runtime.
pub async fn counting_in_memory_buffer_source(
    manifest_path: &str,
    data_prefix: &str,
) -> CountingBufferFixture {
    let inner: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
    let counting = Arc::new(CountingObjectStore::new(
        Arc::clone(&inner),
        manifest_path.to_string(),
    ));
    let manifest_gets = counting.manifest_gets_counter();
    let data_gets = counting.data_gets_counter();
    let fixture = build_buffer_fixture(
        Arc::clone(&counting) as Arc<dyn ObjectStore>,
        manifest_path,
        data_prefix,
        None,
    )
    .await;
    CountingBufferFixture {
        store: counting,
        producer: fixture.producer,
        source: fixture.source,
        manifest_path: manifest_path.into(),
        data_prefix: data_prefix.into(),
        manifest_gets,
        data_gets,
    }
}

pub struct BufferFixtureParts {
    pub producer: buffer::Producer,
    pub source: BufferSource,
}

async fn build_buffer_fixture(
    store: Arc<dyn ObjectStore>,
    manifest_path: &str,
    data_prefix: &str,
    last_acked_sequence: Option<u64>,
) -> BufferFixtureParts {
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
    let consumer = buffer::Consumer::with_object_store(
        consumer_config,
        Arc::clone(&store),
        last_acked_sequence,
    )
    .await
    .expect("consumer");
    let source = BufferSource::new(consumer, "buffer", manifest_path, last_acked_sequence);
    BufferFixtureParts { producer, source }
}
