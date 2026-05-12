//! Pipelined runtime: per-source actor + bounded fetch/decode/sink workers.
//!
//! Row 6.1 stood up the actor + channel scaffolding with a single
//! internal worker. Row 6.2 splits the fetch stage into N parallel
//! workers driven by `source_defaults.fetch_concurrency`; the decode +
//! sink stages remain serial (row 6.3 fans out decode workers; row
//! 6.4 lifts the sink write into a shared pool).
//!
//! ```text
//!   per-source actor                  fetch pool (N tasks)        decode + sink worker
//!   ────────────────                  ────────────────────        ────────────────────
//!   admission arm  ─[Admitted]──▶ (async_channel: MPMC) ──▶ [Fetched] ──▶ decode + write_with_retry
//!   completion arm ◀──────────────[WriteCompletion]────────────────────
//! ```
//!
//! The actor owns `&mut BufferSource` and `&mut AckCoordinator`;
//! admission and completion both run as `select!` arms on the same
//! task, so `register_pending` (admission arm) and
//! `mark_committed` / `advance_frontier` / `ack_through` /
//! `flush_acks` (completion arm) need no `Arc<Mutex<_>>`. The
//! channels are 1-element-deep `async_channel::bounded` (descriptor
//! side) and `tokio::sync::mpsc` (completion side, single
//! consumer).
//!
//! Why two channels and a separate worker instead of an inline body?
//! 6.2 will replace the single internal worker with N fetch workers
//! that share a bounded MPMC descriptor channel; 6.3 layers M decode
//! workers; 6.4 lifts the sink write into a shared writer pool. The
//! actor's `select!` shape and its `register_pending`-before-send
//! contract carry through unchanged. Landing the scaffolding here
//! lets the Phase 5 test matrix flush out the topology before
//! parallelism arrives.
//!
//! Invariants pinned in this row:
//!
//! - INV-ADMISSION-CONTIGUOUS — the actor's admission arm is the
//!   only call site for `AckCoordinator::register_pending`. It
//!   runs synchronously inside the actor's task between
//!   `next_descriptors` and `descriptor_tx.send`.
//! - INV-FRONTIER-NEVER-OVER-HOLE — completion arm calls
//!   `mark_committed` + `advance_frontier` on each completion.
//! - INV-ACK-CALLED-ON-ADVANCE — `ack_through` only fires when
//!   the coordinator's `frontier()` strictly exceeds the actor's
//!   `last_ack_sent`.
//! - INV-DESCRIPTOR-LOSS-FATAL — admission's `descriptor_tx.send`
//!   returning `Err` halts the runtime with a `Pipeline` error.
//! - INV-SINK-RETRY-IDEMPOTENT — `write_with_retry` re-issues the
//!   same `SinkCommit` (with a byte-identical `CommitIdentity`)
//!   on `NotCommitted` / `MaybeCommitted`.
//! - INV-MAYBE-COMMITTED-RESOLVES — `MaybeCommitted` triggers
//!   `Sink::check_committed(&identity)` before retry.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::ack_coordinator::AckCoordinators;
use crate::decoded_batch::{DecodedBatch, DecodedRecords};
use crate::decoder::Decoder;
use crate::envelope::{
    ConfiguredEnvelope, PayloadEncoding, SignalType, decode_envelopes, validate_consistent,
};
use crate::error::{RuntimeError, RuntimeResult};
use crate::identity::{CommitIdentity, SequenceRange};
use crate::sink::{CommitStatus, Sink, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId};
use crate::source::{
    BufferSource, BufferSourceFetchHandle, SourceBatch, SourceBatchDescriptor, SourceBudget,
    SourceId,
};
use crate::source_budget::{ByteReservation, SourceByteBudget};

/// Test instrumentation: record each `(source_id, sequence)` pair the
/// per-source actor passes to
/// [`AckCoordinator::register_pending`](crate::ack_coordinator::AckCoordinator::register_pending)
/// in its admission arm, in admission order. Pins
/// INV-ADMISSION-CONTIGUOUS under parallel fetch (row 6.2). Plain
/// `pub` — gating it behind `#[cfg(test)]` would hide it from
/// integration tests under `tests/`, which link against the runtime
/// crate as a published library and do not see test-only items.
pub type AdmissionRecorder = Arc<std::sync::Mutex<Vec<(SourceId, u64)>>>;

/// Test instrumentation: inject artificial latency at the fetch
/// stage so a test can stress the actor's admission ordering under
/// uneven fetch completion times (row 6.2 §Test Plan
/// `pipeline_register_pending_called_in_admission_order`). The
/// closure receives the descriptor's `buffer_sequence` and returns
/// the `Duration` the fetch worker should sleep before invoking
/// `BufferSourceFetchHandle::fetch`. Not used in production.
pub type TestFetchDelayFn = Arc<dyn Fn(u64) -> Duration + Send + Sync>;

// =========================================================================
// Public configuration types
// =========================================================================

/// How often the runtime calls [`BufferSource::flush_acks`] after
/// acking a source range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum AckFlushPolicy {
    /// Flush after every committed source range. Default. Replay
    /// window after a crash is at most one source range.
    #[default]
    EveryCommitGroup,
    /// Flush after `n` committed source ranges. Operator opt-in for
    /// higher throughput at the cost of an explicit replay window.
    EveryN { n: u32 },
}

/// Per-source backpressure + pool sizing. Default values produce the
/// pipelined behavior Phase 6's bench gate expects (`64`/`8`/`4`); a
/// test that wants the Phase 5 serial shape can override
/// `max_inflight_batches`, `fetch_concurrency`, `decode_concurrency`
/// each to `1`.
///
/// Row 6.1 implements only the byte/slot bookkeeping; `fetch_concurrency`
/// and `decode_concurrency` are configured but the internal worker is
/// single-threaded until 6.2 / 6.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceBackpressureOptions {
    /// Max in-flight `SourceBatch`es per source across the fetch
    /// and decode stages. Bounds the descriptor channel capacity.
    pub max_inflight_batches: u32,
    /// Max in-flight bytes per source, measured as **post-decode
    /// retained memory** — `decoded.estimated_bytes() +
    /// source_coords_bytes` summed over every in-flight unit.
    /// Reserved pessimistically at admission, reconciled at decode
    /// (row 6.3), released at sink commit.
    pub max_inflight_bytes: u64,
    /// Pessimistic per-batch reservation at admission, sized for
    /// the post-decode footprint. Reconciled to actual post-decode
    /// bytes at decode completion (row 6.3).
    pub estimated_max_batch_bytes: u64,
    /// Number of fetch workers per source. Row 6.2 wires this; row
    /// 6.1 ignores the value (single internal worker).
    pub fetch_concurrency: u32,
    /// Number of decode workers per source. Row 6.3 wires this.
    pub decode_concurrency: u32,
    /// Cap on the post-decode byte size of any single batch,
    /// expressed as a multiple of `estimated_max_batch_bytes`. Row
    /// 6.3 wires this into the decode worker.
    pub oversize_fault_multiplier: u32,
}

impl Default for SourceBackpressureOptions {
    fn default() -> Self {
        Self {
            max_inflight_batches: 64,
            max_inflight_bytes: 256 * 1024 * 1024,
            estimated_max_batch_bytes: 4 * 1024 * 1024,
            fetch_concurrency: 8,
            decode_concurrency: 4,
            oversize_fault_multiplier: 4,
        }
    }
}

impl SourceBackpressureOptions {
    /// Phase 5 serial-equivalent profile (`max_inflight_batches = 1`,
    /// `fetch_concurrency = 1`, `decode_concurrency = 1`). Useful for
    /// tests that want to assert behavior under the Phase 6 supervisor
    /// without any parallelism.
    pub fn serial() -> Self {
        Self {
            max_inflight_batches: 1,
            max_inflight_bytes: 256 * 1024 * 1024,
            estimated_max_batch_bytes: 4 * 1024 * 1024,
            fetch_concurrency: 1,
            decode_concurrency: 1,
            oversize_fault_multiplier: 4,
        }
    }
}

/// Shared sink writer pool sizing + retry. The sink itself is
/// singular (RFC 0002 single-sink scope); fields here apply to the
/// one configured sink. Row 6.1 doesn't construct a writer pool —
/// the worker calls `Sink::write` inline — but the config shape is
/// in place for 6.4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkPoolOptions {
    /// Max concurrent `Sink::write` calls in flight across all
    /// sources. Row 6.4 wires this against the shared writer pool.
    pub max_concurrent_commits: u32,
    /// Per-`SinkCommit` retry budget. Shadows the legacy
    /// `RuntimeOptions.max_retry_attempts` when both are set.
    pub retry_max_attempts: u32,
    /// Initial retry backoff (exponential growth with jitter is
    /// future work; row 6.1 uses a constant backoff).
    pub retry_initial_backoff_ms: u64,
}

impl Default for SinkPoolOptions {
    fn default() -> Self {
        Self {
            max_concurrent_commits: 4,
            retry_max_attempts: 6,
            retry_initial_backoff_ms: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeOptions {
    /// Configured envelope every entry must match (validated per
    /// source batch before the decoder runs).
    pub configured_envelope: ConfiguredEnvelope,
    /// Ack flush cadence.
    pub ack_flush_policy: AckFlushPolicy,
    /// When true, the runtime runs the full decode pipeline but
    /// skips [`Sink::write`] and the Buffer ack/flush. Used by
    /// integration tests and by the binary's `--dry-run` mode.
    pub dry_run: bool,
    /// How long to sleep when [`BufferSource::next_descriptors`]
    /// returns an empty descriptor list (no more visible batches).
    pub poll_interval: Duration,
    /// Maximum number of descriptors to request per poll. Phase 6
    /// row 6.1 forces this to 1 inside the admission arm
    /// (`next_descriptors(K=1)`) so every descriptor is registered
    /// before it leaves the synchronous arm; the field is kept on
    /// the options struct for back-compat with Phase 5 callers.
    pub max_descriptors_per_poll: usize,
    /// Max retries per source range before a non-fatal sink failure
    /// is bubbled up as `RuntimeError::Sink`. Shadowed by
    /// `sink.retry_max_attempts` when the per-pool value is
    /// configured; the more-specific knob wins.
    pub max_retry_attempts: u32,
    /// Sleep between retry attempts.
    pub retry_backoff: Duration,
    /// Per-source backpressure + pool sizing. Applied identically
    /// to every source unless `source_overrides` is non-empty.
    pub source_defaults: SourceBackpressureOptions,
    /// Optional per-source overrides keyed by `SourceId`. Lookup
    /// falls back to `source_defaults` when no override exists for
    /// the source.
    pub source_overrides: HashMap<SourceId, SourceBackpressureOptions>,
    /// Shared sink writer pool sizing + retry.
    pub sink: SinkPoolOptions,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            configured_envelope: ConfiguredEnvelope {
                version: 1,
                signal_type: SignalType::Logs,
                encoding: PayloadEncoding::OtlpProtobuf,
            },
            ack_flush_policy: AckFlushPolicy::default(),
            dry_run: true,
            poll_interval: Duration::from_millis(250),
            max_descriptors_per_poll: 1,
            max_retry_attempts: 3,
            retry_backoff: Duration::from_millis(100),
            source_defaults: SourceBackpressureOptions::default(),
            source_overrides: HashMap::new(),
            sink: SinkPoolOptions::default(),
        }
    }
}

impl RuntimeOptions {
    /// Look up the [`SourceBackpressureOptions`] for `source`. Falls
    /// back to `source_defaults` if no override is registered.
    pub fn backpressure_for(&self, source: &SourceId) -> SourceBackpressureOptions {
        self.source_overrides
            .get(source)
            .copied()
            .unwrap_or(self.source_defaults)
    }
}

/// Counters published via the watch channel for tests/metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeProgress {
    pub batches_read: u64,
    pub source_ranges_committed: u64,
    pub records_written: u64,
    pub last_decoded_sequence: Option<u64>,
    pub last_acked_sequence: Option<u64>,
    /// Number of registered-but-not-yet-committed ranges across
    /// all sources hosted by this runtime.
    pub pending_ranges_total: usize,
}

// =========================================================================
// Internal channel envelopes
// =========================================================================

/// Output of the per-source actor's admission arm. Carries the
/// descriptor plus the byte reservation and batch-slot permit
/// that travel with the unit through every stage.
struct AdmittedDescriptor {
    descriptor: SourceBatchDescriptor,
    /// Bytes held against the source's budget. Reserved at
    /// admission (pessimistic). Released on Drop alongside the
    /// batch permit when the worker finishes handling the unit.
    reservation: ByteReservation,
    /// One slot from the per-source batch-count semaphore.
    /// Released on Drop.
    batch_permit: tokio::sync::OwnedSemaphorePermit,
}

/// Output of a fetch worker. The reservation + batch_permit travel
/// with the unit through decode + sink; they drop when the decode
/// worker emits a `WriteCompletion` (success or fatal).
struct FetchedBatch {
    source_batch: SourceBatch,
    reservation: ByteReservation,
    batch_permit: tokio::sync::OwnedSemaphorePermit,
    /// Original descriptor sequence — used by the decode worker to
    /// validate the decoded range matches what admission registered.
    admitted_sequence: u64,
}

/// Worker → actor completion message.
enum WriteCompletion {
    Committed(CommittedReport),
    Fatal(RuntimeError),
}

struct CommittedReport {
    /// Source-range covered by this commit. Must equal the range
    /// the actor registered on admission (Phase 6 contract: one
    /// SinkCommit per source range, low == high == descriptor.sequence).
    range: (u64, u64),
    /// Authoritative row count from the sink. In dry-run, the
    /// decoded record count.
    rows_written: u64,
}

// =========================================================================
// Runtime + RuntimeBuilder
// =========================================================================

pub struct Runtime {
    source: BufferSource,
    decoder: Arc<dyn Decoder>,
    sink: Arc<dyn Sink>,
    coordinators: AckCoordinators,
    options: RuntimeOptions,
    progress_tx: watch::Sender<RuntimeProgress>,
    progress_rx: watch::Receiver<RuntimeProgress>,
    admission_recorder: Option<AdmissionRecorder>,
    test_fetch_delay: Option<TestFetchDelayFn>,
}

pub struct RuntimeBuilder {
    source: Option<BufferSource>,
    decoder: Option<Arc<dyn Decoder>>,
    sink: Option<Arc<dyn Sink>>,
    options: RuntimeOptions,
    admission_recorder: Option<AdmissionRecorder>,
    test_fetch_delay: Option<TestFetchDelayFn>,
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder {
            source: None,
            decoder: None,
            sink: None,
            options: RuntimeOptions::default(),
            admission_recorder: None,
            test_fetch_delay: None,
        }
    }

    pub fn options(&self) -> &RuntimeOptions {
        &self.options
    }

    /// Subscribe to a stream of progress snapshots; tests use this to
    /// observe forward progress without poking at internals.
    pub fn progress(&self) -> watch::Receiver<RuntimeProgress> {
        self.progress_rx.clone()
    }

    /// Run until cancellation. On `shutdown.cancelled()` admission
    /// stops, the in-flight units drain, the durable ack frontier is
    /// flushed, and the function returns `Ok(())`. Any pipeline /
    /// sink / source error halts the runtime with the corresponding
    /// `RuntimeError`.
    pub async fn run(self, shutdown: CancellationToken) -> RuntimeResult<()> {
        let Runtime {
            source,
            decoder,
            sink,
            mut coordinators,
            options,
            progress_tx,
            progress_rx: _progress_rx,
            admission_recorder,
            test_fetch_delay,
        } = self;

        info!(
            source = %source.id(),
            sink = %sink.id(),
            dry_run = options.dry_run,
            "starting runtime"
        );

        let source_id = source.id().clone();
        let sink_id = sink.id().clone();
        let bp = options.backpressure_for(&source_id);
        let fetch_concurrency = bp.fetch_concurrency.max(1) as usize;
        // Channel depths follow the per-source batch-count axis so
        // backpressure stays purely on `batch_semaphore` (the
        // per-source budget actor + the slot semaphore are the
        // gates; the channels themselves are just hand-off
        // queues). Match Phase 6 design §Internal channel types.
        let channel_depth = bp.max_inflight_batches.max(1) as usize;

        // Per-source byte budget + batch-slot semaphore. Both feed
        // backpressure on the actor's admission arm.
        let budget = SourceByteBudget::new(source_id.clone(), bp.max_inflight_bytes);
        let batch_semaphore = Arc::new(Semaphore::new(bp.max_inflight_batches.max(1) as usize));

        let (descriptor_tx, descriptor_rx) =
            async_channel::bounded::<AdmittedDescriptor>(channel_depth);
        let (fetched_tx, fetched_rx) = async_channel::bounded::<FetchedBatch>(channel_depth);
        let (completion_tx, completion_rx) = mpsc::channel::<WriteCompletion>(channel_depth);

        let mut coordinator = coordinators.take(&source_id).ok_or_else(|| {
            RuntimeError::Ack(format!("no coordinator registered for source {source_id}",))
        })?;

        // Spawn N fetch workers. Each holds a clone of the
        // `BufferSourceFetchHandle` — RFC 0003 says `fetch(&self,
        // ...)` is safe to call from N tasks against distinct
        // descriptors. The shared `descriptor_rx` is an
        // async_channel receiver: cloning the receiver is the MPMC
        // pattern (each worker's clone reads from the same queue;
        // `recv` is wait-and-grab).
        let mut fetch_handles = Vec::with_capacity(fetch_concurrency);
        for worker_idx in 0..fetch_concurrency {
            let handle = source.fetch_handle();
            let rx = descriptor_rx.clone();
            let tx = fetched_tx.clone();
            let completion = completion_tx.clone();
            let src_id = source_id.clone();
            let delay = test_fetch_delay.clone();
            fetch_handles.push(tokio::spawn(fetch_worker(
                handle, rx, tx, completion, src_id, worker_idx, delay,
            )));
        }
        // Drop the local handles to descriptor_rx / fetched_tx so
        // worker exits close the receiver side cleanly on shutdown.
        drop(descriptor_rx);
        drop(fetched_tx);

        let decode_handle = tokio::spawn(decode_sink_worker(
            Arc::clone(&decoder),
            Arc::clone(&sink),
            options.clone(),
            sink_id.clone(),
            source_id.clone(),
            fetched_rx,
            completion_tx,
        ));

        let actor_result = per_source_actor(
            source,
            &mut coordinator,
            options.clone(),
            descriptor_tx,
            completion_rx,
            Arc::clone(&budget),
            batch_semaphore,
            sink_id,
            shutdown,
            progress_tx.clone(),
            admission_recorder,
        )
        .await;

        // Drain fetch workers (their descriptor_rx closes once the
        // actor drops descriptor_tx). Collect the first fatal — if
        // any — so it surfaces. Fetch workers that exit cleanly via
        // a closed channel return `Ok(())`.
        let mut fetch_result: RuntimeResult<()> = Ok(());
        for handle in fetch_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if fetch_result.is_ok() {
                        fetch_result = Err(e);
                    }
                }
                Err(join_err) => {
                    if fetch_result.is_ok() {
                        fetch_result = Err(RuntimeError::Pipeline(format!(
                            "fetch worker panicked: {join_err}"
                        )));
                    }
                }
            }
        }

        let decode_result = decode_handle.await.map_err(|join_err| {
            RuntimeError::Pipeline(format!("decode/sink worker panicked: {join_err}"))
        })?;

        // Surface the most informative error: actor errors take
        // precedence (they reflect coordinator / source state) but
        // a worker fatal that the actor never observed is still a
        // failure.
        match (actor_result, fetch_result, decode_result) {
            (Err(e), _, _) => Err(e),
            (Ok(()), Err(e), _) => Err(e),
            (Ok(()), Ok(()), Err(e)) => Err(e),
            (Ok(()), Ok(()), Ok(())) => {
                info!("runtime exited cleanly");
                Ok(())
            }
        }
    }
}

impl RuntimeBuilder {
    pub fn add_source(mut self, source: BufferSource) -> Self {
        self.source = Some(source);
        self
    }

    pub fn add_decoder<D>(mut self, decoder: D) -> Self
    where
        D: Decoder,
    {
        self.decoder = Some(Arc::new(decoder));
        self
    }

    pub fn set_sink<S>(mut self, sink: S) -> Self
    where
        S: Sink,
    {
        self.sink = Some(Arc::new(sink));
        self
    }

    pub fn with_options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
        self
    }

    /// Attach a test-only [`AdmissionRecorder`]. Each
    /// `(source_id, sequence)` pair the per-source actor passes to
    /// `register_pending` lands in the recorder's `Vec` in
    /// admission order. Pin INV-ADMISSION-CONTIGUOUS in integration
    /// tests where you can't observe the coordinator directly.
    pub fn with_admission_recorder(mut self, recorder: AdmissionRecorder) -> Self {
        self.admission_recorder = Some(recorder);
        self
    }

    /// Attach a test-only [`TestFetchDelayFn`]. Each fetch worker
    /// sleeps for `delay(descriptor.sequence)` before invoking the
    /// underlying `BufferSourceFetchHandle::fetch`. Used to stress
    /// admission ordering under uneven fetch completion times.
    pub fn with_test_fetch_delay(mut self, delay: TestFetchDelayFn) -> Self {
        self.test_fetch_delay = Some(delay);
        self
    }

    pub fn build(self) -> RuntimeResult<Runtime> {
        let source = self
            .source
            .ok_or_else(|| RuntimeError::Config("no source configured".into()))?;
        let decoder = self
            .decoder
            .ok_or_else(|| RuntimeError::Config("no decoder configured".into()))?;
        let sink = self
            .sink
            .ok_or_else(|| RuntimeError::Config("no sink configured".into()))?;
        let mut coordinators = AckCoordinators::new();
        coordinators.register_source(source.id().clone(), source.last_acked_sequence())?;
        let (progress_tx, progress_rx) = watch::channel(RuntimeProgress::default());
        Ok(Runtime {
            source,
            decoder,
            sink,
            coordinators,
            options: self.options,
            progress_tx,
            progress_rx,
            admission_recorder: self.admission_recorder,
            test_fetch_delay: self.test_fetch_delay,
        })
    }
}

// =========================================================================
// Per-source actor
// =========================================================================

/// Drives admission + completion for a single source. Owns
/// `&mut BufferSource` and `&mut AckCoordinator` for the duration of
/// the run; admission and completion share the actor's `&mut self`
/// across distinct `select!` arms.
#[allow(clippy::too_many_arguments)]
async fn per_source_actor(
    mut source: BufferSource,
    coordinator: &mut crate::ack_coordinator::AckCoordinator,
    options: RuntimeOptions,
    descriptor_tx: async_channel::Sender<AdmittedDescriptor>,
    mut completion_rx: mpsc::Receiver<WriteCompletion>,
    budget: Arc<SourceByteBudget>,
    batch_semaphore: Arc<Semaphore>,
    _sink_id: SinkId,
    shutdown: CancellationToken,
    progress_tx: watch::Sender<RuntimeProgress>,
    admission_recorder: Option<AdmissionRecorder>,
) -> RuntimeResult<()> {
    let source_id = source.id().clone();
    let bp = options.backpressure_for(&source_id);
    let dry_run = options.dry_run;

    let mut progress = RuntimeProgress::default();
    let mut last_ack_sent: Option<u64> = source.last_acked_sequence();
    let mut groups_since_flush: u32 = 0;
    let mut in_flight: u64 = 0;
    let mut admission_open = true;

    loop {
        if !admission_open && in_flight == 0 {
            break;
        }

        // Build the admission attempt only when admission is open
        // and the channel has space. Holding a guard-style
        // permit/reservation across the select! would either pin
        // them across the completion branch (wasteful) or risk
        // dropping them on a no-op poll. Pattern: do a *peek* —
        // try_acquire / try_recv — and on success commit by
        // performing the awaited operations in an immediate
        // dedicated arm. The shape below uses an `if admission_open`
        // guard plus an inner `if let Ok(permit) = semaphore.try_acquire_owned()`
        // gate to start the admission flow only when capacity is
        // there.
        tokio::select! {
            biased;

            // 1. External shutdown — close admission. The runtime
            //    continues draining completions until in_flight == 0.
            _ = shutdown.cancelled(), if admission_open => {
                debug!("admission token cancelled; draining in-flight commits");
                admission_open = false;
                // Closing the sender flushes the worker's
                // descriptor_rx after it drains.
                descriptor_tx.close();
            }

            // 2. Completion arm — drain writer completions promptly
            //    so the byte budget recovers and admission can
            //    park-then-resume cleanly.
            completion = completion_rx.recv(), if in_flight > 0 => {
                match completion {
                    Some(WriteCompletion::Committed(report)) => {
                        let (low, high) = report.range;
                        coordinator.mark_committed(low, high)?;
                        coordinator.advance_frontier();
                        in_flight -= 1;

                        progress.batches_read = progress.batches_read.saturating_add(1);
                        progress.last_decoded_sequence = Some(high);
                        progress.records_written = progress
                            .records_written
                            .saturating_add(report.rows_written);
                        progress.source_ranges_committed = progress
                            .source_ranges_committed
                            .saturating_add(1);

                        let frontier = coordinator.frontier();
                        if !dry_run
                            && let Some(f) = frontier
                        {
                            let should_ack = match last_ack_sent {
                                Some(prev) => f > prev,
                                None => true,
                            };
                            if should_ack {
                                source.ack_through(f).await?;
                                last_ack_sent = Some(f);
                                groups_since_flush = groups_since_flush.saturating_add(1);
                                let should_flush = match options.ack_flush_policy {
                                    AckFlushPolicy::EveryCommitGroup => true,
                                    AckFlushPolicy::EveryN { n } => {
                                        groups_since_flush >= n.max(1)
                                    }
                                };
                                if should_flush {
                                    source.flush_acks().await?;
                                    groups_since_flush = 0;
                                }
                                progress.last_acked_sequence = Some(f);
                            }
                        }
                        progress.pending_ranges_total = coordinator.pending_count();
                        let _ = progress_tx.send(progress);
                    }
                    Some(WriteCompletion::Fatal(e)) => {
                        return Err(e);
                    }
                    None => {
                        // Worker dropped its completion_tx while we
                        // still have in-flight units. That's a
                        // structural failure.
                        return Err(RuntimeError::Pipeline(format!(
                            "internal worker exited with {in_flight} in-flight unit(s) on source {source_id}",
                        )));
                    }
                }
            }

            // 3. Admission arm — only when admission is open AND
            //    backpressure permits.
            biased_arm = admission_attempt(
                &source_id,
                &budget,
                &batch_semaphore,
                in_flight,
                &bp,
            ), if admission_open => {
                let AdmissionGate { batch_permit, reservation } = biased_arm;

                // K=1: register every descriptor before it leaves
                // the synchronous arm.
                let descriptors = source
                    .next_descriptors(1, SourceBudget::default())
                    .await?;
                if descriptors.is_empty() {
                    drop(reservation);
                    drop(batch_permit);
                    tokio::time::sleep(options.poll_interval).await;
                    continue;
                }

                let descriptor = descriptors.into_iter().next().expect("len checked");
                let seq = descriptor.sequence;

                // INV-ADMISSION-CONTIGUOUS: synchronous register
                // before send. Recorder hook fires immediately
                // before register_pending so a test sees the
                // admission order even when fetch/decode/sink
                // stages complete out of order.
                if let Some(recorder) = admission_recorder.as_ref() {
                    recorder.lock().unwrap().push((source_id.clone(), seq));
                }
                coordinator.register_pending(seq, seq)?;
                in_flight = in_flight.saturating_add(1);

                if descriptor_tx
                    .send(AdmittedDescriptor {
                        descriptor,
                        reservation,
                        batch_permit,
                    })
                    .await
                    .is_err()
                {
                    return Err(RuntimeError::Pipeline(format!(
                        "descriptor lost: source={source_id} seq={seq} cause=worker-stage-closed",
                    )));
                }
            }
        }
    }

    if !dry_run {
        source.flush_acks().await?;
    }

    // Final progress sample on the way out.
    progress.pending_ranges_total = coordinator.pending_count();
    let _ = progress_tx.send(progress);
    Ok(())
}

/// Output of a successful admission attempt: the actor still has to
/// poll the buffer and `register_pending` synchronously, but the
/// per-source backpressure axes have already been claimed.
struct AdmissionGate {
    batch_permit: tokio::sync::OwnedSemaphorePermit,
    reservation: ByteReservation,
}

/// Park until the per-source budget + batch-slot semaphore both have
/// room for the next descriptor. Cancellation-safe under `select!`:
/// dropping the returned future before it resolves leaves the budget
/// at its pre-call value (no held bytes / no held permit).
async fn admission_attempt(
    _source: &SourceId,
    budget: &Arc<SourceByteBudget>,
    batch_semaphore: &Arc<Semaphore>,
    _in_flight: u64,
    bp: &SourceBackpressureOptions,
) -> AdmissionGate {
    let batch_permit = Arc::clone(batch_semaphore)
        .acquire_owned()
        .await
        .expect("batch semaphore not closed before actor exit");
    let reservation = budget.reserve(bp.estimated_max_batch_bytes).await;
    AdmissionGate {
        batch_permit,
        reservation,
    }
}

// =========================================================================
// Per-source fetch workers (row 6.2)
// =========================================================================

/// One of N fetch workers per source. Each holds a clone of the
/// source's `BufferSourceFetchHandle` (wrapping
/// `Arc<buffer::ConsumerFetchHandle>`, RFC 0003 — `fetch(&self, ...)`
/// is safe to call from concurrent tasks against distinct
/// descriptors). The shared `descriptor_rx` is an
/// `async_channel::Receiver` clone; recv is MPMC. Re-fetch safety
/// holds by RFC 0003 §Concurrency Model, though the actor's
/// admission arm sends each descriptor exactly once so re-fetch is
/// purely defense-in-depth.
///
/// The worker emits two metrics per fetch:
/// - `runtime_stage_queue_depth{stage=fetch,source}` — sampled
///   before the recv awaits.
/// - `runtime_stage_latency_seconds{stage=fetch,source}` — observed
///   per fetch invocation.
#[allow(clippy::too_many_arguments)]
async fn fetch_worker(
    fetch_handle: BufferSourceFetchHandle,
    descriptor_rx: async_channel::Receiver<AdmittedDescriptor>,
    fetched_tx: async_channel::Sender<FetchedBatch>,
    completion_tx: mpsc::Sender<WriteCompletion>,
    source_id: SourceId,
    worker_idx: usize,
    test_fetch_delay: Option<TestFetchDelayFn>,
) -> RuntimeResult<()> {
    let source_label = source_id.0.clone();
    loop {
        metrics::gauge!(
            crate::metrics::STAGE_QUEUE_DEPTH,
            "stage" => "fetch",
            "source" => source_label.clone(),
        )
        .set(descriptor_rx.len() as f64);

        let admitted = match descriptor_rx.recv().await {
            Ok(a) => a,
            Err(_) => return Ok(()), // descriptor channel closed; graceful exit
        };
        let AdmittedDescriptor {
            descriptor,
            reservation,
            batch_permit,
        } = admitted;
        let admitted_sequence = descriptor.sequence;

        if let Some(delay_fn) = test_fetch_delay.as_ref() {
            let delay = delay_fn(admitted_sequence);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }

        let stage_start = std::time::Instant::now();
        let source_batch = match fetch_handle.fetch(descriptor).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    error = %e,
                    source = %source_id,
                    worker = worker_idx,
                    "fetch worker fatal",
                );
                let _ = completion_tx.send(WriteCompletion::Fatal(e)).await;
                return Ok(());
            }
        };
        metrics::histogram!(
            crate::metrics::STAGE_LATENCY_SECONDS,
            "stage" => "fetch",
            "source" => source_label.clone(),
        )
        .record(stage_start.elapsed().as_secs_f64());

        if fetched_tx
            .send(FetchedBatch {
                source_batch,
                reservation,
                batch_permit,
                admitted_sequence,
            })
            .await
            .is_err()
        {
            // Decode stage closed unexpectedly. INV-DESCRIPTOR-LOSS-FATAL.
            let _ = completion_tx
                .send(WriteCompletion::Fatal(RuntimeError::Pipeline(format!(
                    "fetched-batch lost: source={source_id} seq={admitted_sequence} cause=decode-stage-closed",
                ))))
                .await;
            return Ok(());
        }
    }
}

// =========================================================================
// Decode + sink worker (single task in row 6.2; row 6.3 fans out
// decode workers; row 6.4 lifts the sink write into a shared pool).
// =========================================================================

#[allow(clippy::too_many_arguments)]
async fn decode_sink_worker(
    decoder: Arc<dyn Decoder>,
    sink: Arc<dyn Sink>,
    options: RuntimeOptions,
    sink_id: SinkId,
    source_id: SourceId,
    fetched_rx: async_channel::Receiver<FetchedBatch>,
    completion_tx: mpsc::Sender<WriteCompletion>,
) -> RuntimeResult<()> {
    while let Ok(fetched) = fetched_rx.recv().await {
        let FetchedBatch {
            source_batch,
            reservation,
            batch_permit,
            admitted_sequence,
        } = fetched;

        let outcome = process_fetched_batch(
            &decoder,
            &sink,
            &options,
            &sink_id,
            source_batch,
            admitted_sequence,
        )
        .await;

        let message = match outcome {
            Ok(report) => WriteCompletion::Committed(report),
            Err(e) => {
                warn!(error = %e, source = %source_id, "decode/sink worker fatal");
                let _ = completion_tx.send(WriteCompletion::Fatal(e)).await;
                return Ok(());
            }
        };

        if completion_tx.send(message).await.is_err() {
            return Ok(()); // actor exited
        }

        drop(reservation);
        drop(batch_permit);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_fetched_batch(
    decoder: &Arc<dyn Decoder>,
    sink: &Arc<dyn Sink>,
    options: &RuntimeOptions,
    sink_id: &SinkId,
    source_batch: SourceBatch,
    admitted_sequence: u64,
) -> RuntimeResult<CommittedReport> {
    // Per-entry envelope validation. Envelope failures route as
    // RuntimeError::Decoder so the boundary stays the same as Phase
    // 4/5.
    let envelopes =
        decode_envelopes(&source_batch).map_err(|e| RuntimeError::Decoder(Box::new(e)))?;
    validate_consistent(&envelopes, &options.configured_envelope)
        .map_err(|e| RuntimeError::Decoder(Box::new(e)))?;

    if let Some(envelope) = envelopes.first()
        && !decoder.accepts(envelope)
    {
        return Err(RuntimeError::Decoder(
            format!(
                "decoder rejected configured envelope: version={} signal_type={:?} encoding={:?}",
                envelope.version, envelope.signal_type, envelope.encoding,
            )
            .into(),
        ));
    }

    let decoded_batches = decoder.decode(source_batch)?;
    if decoded_batches.is_empty() {
        return Err(RuntimeError::Decoder(
            "decoder returned empty Vec<DecodedBatch>; every source batch must produce at \
             least one DecodedBatch (zero-record allowed) to keep admission contiguous \
             (INV-ADMISSION-CONTIGUOUS)"
                .into(),
        ));
    }
    if decoded_batches.len() != 1 {
        // Phase 6 contract: one SinkCommit per source range. Phase
        // 5 supported multiple DecodedBatches per source batch, but
        // admission now registers a single (sequence, sequence)
        // range; producing multiple DecodedBatches would break
        // mark_committed (no matching pending entry for the second
        // range). Phase 6 design §Algorithms > Per-Source Decode
        // Workers (and §Open Questions Q5) makes this strict.
        return Err(RuntimeError::Decoder(
            format!(
                "Phase 6 expects one DecodedBatch per source batch; got {}",
                decoded_batches.len()
            )
            .into(),
        ));
    }
    let decoded = decoded_batches.into_iter().next().expect("len checked");

    let low = decoded.low_sequence;
    let high = decoded.high_sequence;
    if low != admitted_sequence || high != admitted_sequence {
        return Err(RuntimeError::Pipeline(format!(
            "decoded range {low}..={high} does not match admitted sequence \
             {admitted_sequence} (Phase 6 contract: one DecodedBatch per source batch with \
             low == high == descriptor.sequence)"
        )));
    }

    let row_count = match &decoded.records {
        DecodedRecords::Typed(t) => t.record_count() as u64,
    };

    if options.dry_run {
        debug!(low, high, rows = row_count, "dry-run: skipping sink write");
        return Ok(CommittedReport {
            range: (low, high),
            rows_written: row_count,
        });
    }

    let commit = build_commit(sink_id.clone(), low, high, decoded);
    let result = write_with_retry(sink, commit, options).await?;
    Ok(CommittedReport {
        range: (low, high),
        rows_written: result.rows_written,
    })
}

fn build_commit(
    sink_id: SinkId,
    low_sequence: u64,
    high_sequence: u64,
    batch: DecodedBatch,
) -> SinkCommit {
    let identity = CommitIdentity {
        source: batch.source.clone(),
        sink: sink_id,
        range: SequenceRange::new(low_sequence, high_sequence),
        schema_version: batch.schema_version,
    };
    SinkCommit { identity, batch }
}

async fn write_with_retry(
    sink: &Arc<dyn Sink>,
    commit: SinkCommit,
    options: &RuntimeOptions,
) -> RuntimeResult<SinkCommitResult> {
    let mut attempt = 0u32;
    loop {
        match sink.write(commit.clone()).await {
            Ok(result) => return Ok(result),
            Err(SinkCommitFailure::Fatal(e)) => return Err(RuntimeError::Sink(e)),
            Err(SinkCommitFailure::NotCommitted(e)) => {
                if attempt >= options.max_retry_attempts {
                    return Err(RuntimeError::Sink(e));
                }
                tokio::time::sleep(options.retry_backoff).await;
                attempt = attempt.saturating_add(1);
            }
            Err(SinkCommitFailure::MaybeCommitted(e)) => {
                match sink.check_committed(&commit.identity).await? {
                    CommitStatus::Committed => {
                        // The sink confirmed an earlier attempt
                        // committed; we don't get a fresh
                        // SinkCommitResult, but the range is durably
                        // written. Return a zero-row result so the
                        // runtime's progress.records_written doesn't
                        // double-count an already-acked range on
                        // replay.
                        return Ok(SinkCommitResult::default());
                    }
                    CommitStatus::NotCommitted | CommitStatus::Unknown => {
                        if attempt >= options.max_retry_attempts {
                            return Err(RuntimeError::Sink(e));
                        }
                        tokio::time::sleep(options.retry_backoff).await;
                        attempt = attempt.saturating_add(1);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_are_dry_run_logs() {
        let opts = RuntimeOptions::default();
        assert!(opts.dry_run, "default must be dry-run for safety");
        assert_eq!(opts.max_descriptors_per_poll, 1);
        assert!(matches!(
            opts.ack_flush_policy,
            AckFlushPolicy::EveryCommitGroup
        ));
        // New Phase 6 fields default to the pipelined profile.
        assert_eq!(opts.source_defaults.max_inflight_batches, 64);
        assert_eq!(opts.source_defaults.fetch_concurrency, 8);
        assert_eq!(opts.source_defaults.decode_concurrency, 4);
        assert_eq!(opts.sink.max_concurrent_commits, 4);
    }

    #[test]
    fn ack_flush_policy_default_is_every_commit_group() {
        assert_eq!(AckFlushPolicy::default(), AckFlushPolicy::EveryCommitGroup);
    }

    #[test]
    fn serial_profile_reproduces_phase_five_shape() {
        let bp = SourceBackpressureOptions::serial();
        assert_eq!(bp.max_inflight_batches, 1);
        assert_eq!(bp.fetch_concurrency, 1);
        assert_eq!(bp.decode_concurrency, 1);
    }

    #[test]
    fn backpressure_for_falls_back_to_defaults() {
        let opts = RuntimeOptions::default();
        let bp = opts.backpressure_for(&SourceId::from("any"));
        assert_eq!(bp.max_inflight_batches, 64);
    }

    #[test]
    fn backpressure_for_honors_override() {
        let mut opts = RuntimeOptions::default();
        let override_bp = SourceBackpressureOptions {
            max_inflight_batches: 5,
            ..SourceBackpressureOptions::default()
        };
        opts.source_overrides
            .insert(SourceId::from("hot"), override_bp);
        assert_eq!(
            opts.backpressure_for(&SourceId::from("hot"))
                .max_inflight_batches,
            5,
        );
        assert_eq!(
            opts.backpressure_for(&SourceId::from("other"))
                .max_inflight_batches,
            64,
        );
    }
}
