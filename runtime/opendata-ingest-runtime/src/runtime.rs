//! Serial orchestration loop for the ingest runtime.
//!
//! Phase 4.4c lands `Runtime` + `RuntimeBuilder` on top of the
//! concrete [`BufferSource`] and the rev-6 plugin traits (`Decoder`,
//! `Sink`, `IdempotencyContract`). The loop is serial: one source
//! batch fetched, decoded, written, and acked at a time. Phase 6
//! introduces parallel fetch + bounded queues; the trait surface
//! here does not change at that point.
//!
//! Sequence:
//!
//! 1. Poll [`BufferSource::next_descriptors`].
//! 2. For each descriptor, fetch via the paired fetch handle.
//! 3. Validate per-entry envelopes against the configured envelope.
//! 4. Run the [`Decoder`].
//! 5. For each [`DecodedBatch`] the decoder produces, register the
//!    pending range with the per-source `AckCoordinator` looked up
//!    via [`AckCoordinators`], construct a [`SinkCommit`] with an
//!    [`IdempotencyKey`], call [`Sink::write`], `mark_committed`,
//!    then `advance_frontier`. `MaybeCommitted` triggers a
//!    [`Sink::check_committed`] lookup before retry per RFC 0002
//!    rev 6.
//! 6. Advance the Buffer ack frontier via
//!    [`BufferSource::ack_through`] using the coordinator's
//!    `frontier()` and flush per the configured [`AckFlushPolicy`].
//!
//! Phase 5 expands the Phase 4 contiguous-frontier stub into the
//! full per-source pending-range state machine + registry
//! ([`AckCoordinators`]); see
//! `plans/odb-high-throughput/phase05-ack-correctness-design.md`
//! rev 6 for the invariant taxonomy.
//!
//! Dry-run mode skips the sink write and the ack/flush; the
//! coordinator's in-memory frontier still advances so the
//! `progress` watch reflects what would have shipped.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::ack_coordinator::AckCoordinators;
use crate::decoded_batch::{DecodedBatch, DecodedRecords};
use crate::decoder::Decoder;
use crate::envelope::{
    ConfiguredEnvelope, PayloadEncoding, SignalType, decode_envelopes, validate_consistent,
};
use crate::error::{RuntimeError, RuntimeResult};
use crate::idempotency::{DefaultIdempotencyContract, IdempotencyContract, IdempotencyScope};
use crate::sink::{CommitStatus, Sink, SinkCommit, SinkCommitFailure, SinkCommitResult};
use crate::source::{BufferSource, SourceBatch, SourceBudget};

/// How often the runtime calls [`BufferSource::flush_acks`] after
/// acking a source range. Mirrors `clickhouse-ingestor::ack::AckFlushPolicy`.
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
    /// Maximum number of descriptors to request per poll. Phase 4 is
    /// serial; v1 uses 1.
    pub max_descriptors_per_poll: usize,
    /// Max retries per source range before a non-fatal sink failure
    /// is bubbled up as `RuntimeError::Sink`. Mirrors the legacy
    /// writer's internal retry budget at the runtime layer.
    pub max_retry_attempts: u32,
    /// Sleep between retry attempts.
    pub retry_backoff: Duration,
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
        }
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
    /// all sources hosted by this runtime. Phase 5 ships a single
    /// source; the value is a snapshot of the lone coordinator's
    /// `pending_count` after each loop iteration.
    pub pending_ranges_total: usize,
}

pub struct Runtime {
    source: BufferSource,
    decoder: Arc<dyn Decoder>,
    sink: Arc<dyn Sink>,
    /// Phase 5 registry of per-source ack coordinators. Today's
    /// runtime is single-source so the registry holds exactly one
    /// entry, keyed by `source.id()`. Phase 6 multi-source wiring
    /// will populate N entries; the orchestration code already
    /// looks up by source id.
    coordinators: AckCoordinators,
    idempotency: Arc<dyn IdempotencyContract>,
    options: RuntimeOptions,
    groups_since_flush: u32,
    progress_tx: watch::Sender<RuntimeProgress>,
    progress_rx: watch::Receiver<RuntimeProgress>,
}

pub struct RuntimeBuilder {
    source: Option<BufferSource>,
    decoder: Option<Arc<dyn Decoder>>,
    sink: Option<Arc<dyn Sink>>,
    idempotency: Option<Arc<dyn IdempotencyContract>>,
    options: RuntimeOptions,
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder {
            source: None,
            decoder: None,
            sink: None,
            idempotency: None,
            options: RuntimeOptions::default(),
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

    /// Run until cancellation. On cancellation we flush the durable
    /// ack frontier (non-dry-run) so the last successful range is
    /// durable before the process exits.
    pub async fn run(mut self, shutdown: CancellationToken) -> RuntimeResult<()> {
        let mut progress = RuntimeProgress::default();
        info!(
            source = %self.source.id(),
            sink = %self.sink.id(),
            dry_run = self.options.dry_run,
            "starting runtime"
        );

        loop {
            let outcome = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    info!("shutdown requested");
                    break;
                }
                result = self.source.next_descriptors(
                    self.options.max_descriptors_per_poll,
                    SourceBudget::default(),
                ) => result,
            };
            let descriptors = outcome?;
            if descriptors.is_empty() {
                tokio::time::sleep(self.options.poll_interval).await;
                continue;
            }

            let handle = self.source.fetch_handle();
            for descriptor in descriptors {
                let sequence = descriptor.sequence;
                let source_batch = handle.fetch(descriptor).await?;
                progress.batches_read = progress.batches_read.saturating_add(1);
                progress.last_decoded_sequence = Some(sequence);

                let rows = self.handle_source_batch(source_batch).await?;
                progress.records_written = progress.records_written.saturating_add(rows);
                progress.source_ranges_committed =
                    progress.source_ranges_committed.saturating_add(1);
                let source_id = self.source.id().clone();
                if !self.options.dry_run
                    && let Some(coord) = self.coordinators.get(&source_id)
                    && let Some(frontier) = coord.frontier()
                {
                    progress.last_acked_sequence = Some(frontier);
                }
                progress.pending_ranges_total = self.coordinators.pending_total();
                let _ = self.progress_tx.send(progress);
            }
        }

        if !self.options.dry_run {
            self.source.flush_acks().await?;
        }
        let _ = self.progress_tx.send(progress);
        info!("runtime exited cleanly");
        Ok(())
    }

    async fn handle_source_batch(&mut self, batch: SourceBatch) -> RuntimeResult<u64> {
        // Per-entry envelope validation. The legacy runtime did this
        // before the decoder; preserve the ordering so envelope
        // failures route distinctly from decoder failures.
        let envelopes = decode_envelopes(&batch).map_err(|e| RuntimeError::Decoder(Box::new(e)))?;
        validate_consistent(&envelopes, &self.options.configured_envelope)
            .map_err(|e| RuntimeError::Decoder(Box::new(e)))?;

        // Fail closed when the decoder doesn't accept the configured
        // envelope. `validate_consistent` already proved every entry's
        // envelope matches the runtime config; `accepts` is the
        // decoder-plugin's own gate. RFC 0002 rev 6 §`Decoder` makes
        // this the runtime's responsibility to enforce.
        if let Some(envelope) = envelopes.first()
            && !self.decoder.accepts(envelope)
        {
            return Err(RuntimeError::Decoder(
                format!(
                    "decoder rejected configured envelope: version={} signal_type={:?} encoding={:?}",
                    envelope.version, envelope.signal_type, envelope.encoding,
                )
                .into(),
            ));
        }

        let decoded = self.decoder.decode(batch)?;
        let mut rows_written = 0u64;

        for db in decoded {
            let low = db.low_sequence;
            let high = db.high_sequence;
            let source_id = db.source.clone();
            let row_count = match &db.records {
                DecodedRecords::Typed(t) => t.record_count() as u64,
            };

            let coord = self.coordinators.get_mut(&source_id).ok_or_else(|| {
                RuntimeError::Ack(format!(
                    "no coordinator registered for source {source_id} (Runtime::build must register every source)",
                ))
            })?;
            coord.register_pending(low, high)?;

            if self.options.dry_run {
                debug!(low, high, rows = row_count, "dry-run: skipping sink write");
                coord.mark_committed(low, high)?;
                coord.advance_frontier();
                rows_written = rows_written.saturating_add(row_count);
                continue;
            }

            // `write_with_retry` borrows `&self` (read-only), so
            // drop the &mut on `coord` for the duration of the
            // await. We re-acquire after the write returns to
            // call `mark_committed` + `advance_frontier`.
            let commit = self.build_commit(source_id.clone(), low, high, db);
            let result = self.write_with_retry(commit).await?;
            let coord = self
                .coordinators
                .get_mut(&source_id)
                .expect("coordinator presence checked above; entry is not removed during write");
            coord.mark_committed(low, high)?;
            coord.advance_frontier();
            // Authoritative count from the sink, not the decoded
            // record count: in non-dry-run mode the sink owns row
            // accounting (and its own row-drop guard — see
            // ClickHouseSink). progress.records_written reflects what
            // was actually written.
            rows_written = rows_written.saturating_add(result.rows_written);
        }

        if !self.options.dry_run {
            let frontier = self
                .coordinators
                .get(self.source.id())
                .and_then(|c| c.frontier());
            if let Some(frontier) = frontier {
                self.source.ack_through(frontier).await?;
                self.groups_since_flush = self.groups_since_flush.saturating_add(1);
                let should_flush = match self.options.ack_flush_policy {
                    AckFlushPolicy::EveryCommitGroup => true,
                    AckFlushPolicy::EveryN { n } => self.groups_since_flush >= n.max(1),
                };
                if should_flush {
                    self.source.flush_acks().await?;
                    self.groups_since_flush = 0;
                }
            }
        }

        Ok(rows_written)
    }

    fn build_commit(
        &self,
        source_id: crate::source::SourceId,
        low_sequence: u64,
        high_sequence: u64,
        batch: DecodedBatch,
    ) -> SinkCommit {
        let idempotency_key = self.idempotency.key(IdempotencyScope {
            source: &source_id,
            sink: self.sink.id(),
            low_sequence,
            high_sequence,
            schema_version: batch.schema_version,
            // Phase 7 wires sink-config-derived chunking fingerprint.
            chunking_fingerprint: 0,
        });
        SinkCommit {
            source: source_id,
            sink: self.sink.id().clone(),
            low_sequence,
            high_sequence,
            batch,
            idempotency_key,
        }
    }

    async fn write_with_retry(&self, commit: SinkCommit) -> RuntimeResult<SinkCommitResult> {
        let mut attempt = 0u32;
        loop {
            match self.sink.write(commit.clone()).await {
                Ok(result) => return Ok(result),
                Err(SinkCommitFailure::Fatal(e)) => return Err(RuntimeError::Sink(e)),
                Err(SinkCommitFailure::NotCommitted(e)) => {
                    if attempt >= self.options.max_retry_attempts {
                        return Err(RuntimeError::Sink(e));
                    }
                    tokio::time::sleep(self.options.retry_backoff).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(SinkCommitFailure::MaybeCommitted(e)) => {
                    match self.sink.check_committed(&commit.idempotency_key).await? {
                        CommitStatus::Committed => {
                            // The sink confirmed an earlier attempt
                            // committed; we don't get a fresh
                            // SinkCommitResult, but the range is
                            // durably written. Return a zero-row
                            // result so the runtime's
                            // progress.records_written doesn't
                            // double-count an already-acked range on
                            // replay.
                            return Ok(SinkCommitResult::default());
                        }
                        // RFC 0002 rev 6: treat Unknown like
                        // NotCommitted; rely on sink-level dedupe.
                        CommitStatus::NotCommitted | CommitStatus::Unknown => {
                            if attempt >= self.options.max_retry_attempts {
                                return Err(RuntimeError::Sink(e));
                            }
                            tokio::time::sleep(self.options.retry_backoff).await;
                            attempt = attempt.saturating_add(1);
                        }
                    }
                }
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

    pub fn with_idempotency<I>(mut self, contract: I) -> Self
    where
        I: IdempotencyContract + 'static,
    {
        self.idempotency = Some(Arc::new(contract));
        self
    }

    pub fn with_options(mut self, options: RuntimeOptions) -> Self {
        self.options = options;
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
        let idempotency = self
            .idempotency
            .unwrap_or_else(|| Arc::new(DefaultIdempotencyContract));
        let mut coordinators = AckCoordinators::new();
        coordinators.register_source(source.id().clone(), source.last_acked_sequence())?;
        let (progress_tx, progress_rx) = watch::channel(RuntimeProgress::default());
        Ok(Runtime {
            source,
            decoder,
            sink,
            coordinators,
            idempotency,
            options: self.options,
            groups_since_flush: 0,
            progress_tx,
            progress_rx,
        })
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
    }

    #[test]
    fn ack_flush_policy_default_is_every_commit_group() {
        assert_eq!(AckFlushPolicy::default(), AckFlushPolicy::EveryCommitGroup);
    }
}
