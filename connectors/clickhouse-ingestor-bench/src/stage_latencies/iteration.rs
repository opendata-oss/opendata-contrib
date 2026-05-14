//! §Iteration Protocol for the Phase 7.1 stage-latency bench.
//!
//! Implements the exact protocol pseudocoded in
//! `plans/odb-high-throughput/phase07-clickhouse-throughput-design.md`
//! §Iteration Protocol — adapted for the in-memory dry-run shape
//! row 7.1 ships: a fresh `InMemoryFixture` per iteration (no real
//! ClickHouse to drop/recreate; `count_visible` / `*_duplicates`
//! are no-ops because there is no real sink to query), a `BenchSink`
//! standing in for the real sink so all four stage labels
//! (`source | fetch | decode | sink_dispatch`) actually emit
//! samples, and `progress.last_acked_sequence` as the timed-window
//! drain key just like the design specifies.
//!
//! Row 7.2 will extract this into a generic `iteration.rs` module
//! parameterized over a fixture trait.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use metrics_util::debugging::{DebugValue, Snapshot, Snapshotter};
use opendata_ingest_runtime::runtime::{AckFlushPolicy, Runtime, RuntimeOptions};
use opendata_ingest_runtime::sink::SinkId;
use tokio_util::sync::CancellationToken;

use crate::fixtures::{BenchSink, FakeDecoder, in_memory_fixture, produce_n_batches};

/// One iteration's per-stage observations + scalars, in clean units
/// per the design's §Bottleneck Attribution Methodology.
#[derive(Debug, Clone)]
pub struct IterationReport {
    pub iteration: usize,
    pub timed_started_unix_ms: u64,
    pub elapsed_seconds: f64,
    pub records_processed: u64,
    /// `Σ samples of runtime_stage_latency_seconds{stage=s}` per
    /// stage label. Missing stages map to an empty `Vec` (0
    /// stage_seconds) so the four-stage table is fully populated.
    pub stage_samples: StageSamples,
    /// Worker count active at each stage during this iteration
    /// (taken from `RuntimeOptions` — single source actor so
    /// `worker_count[source] = 1`).
    pub worker_counts: StageWorkerCounts,
}

impl IterationReport {
    /// `worker_utilization[s] = stage_seconds[s] / (elapsed × worker_count[s]) ∈ [0, 1]`
    /// in steady state. The bench acceptance gate requires every
    /// stage's value to fall in `[0, 1]`.
    pub fn worker_utilization(&self, stage: Stage) -> f64 {
        let stage_seconds = self.stage_samples.sum(stage);
        let workers = self.worker_counts.get(stage) as f64;
        let denom = self.elapsed_seconds * workers;
        if denom <= 0.0 {
            0.0
        } else {
            stage_seconds / denom
        }
    }

    /// `per_record_service_time[s] = stage_seconds[s] / records` (s/rec).
    pub fn per_record_service_time(&self, stage: Stage) -> f64 {
        let records = self.records_processed as f64;
        if records <= 0.0 {
            0.0
        } else {
            self.stage_samples.sum(stage) / records
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StageSamples {
    pub source: Vec<f64>,
    pub fetch: Vec<f64>,
    pub decode: Vec<f64>,
    pub sink_dispatch: Vec<f64>,
}

impl StageSamples {
    pub fn get(&self, stage: Stage) -> &[f64] {
        match stage {
            Stage::Source => &self.source,
            Stage::Fetch => &self.fetch,
            Stage::Decode => &self.decode,
            Stage::SinkDispatch => &self.sink_dispatch,
        }
    }
    pub fn sum(&self, stage: Stage) -> f64 {
        self.get(stage).iter().copied().sum()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StageWorkerCounts {
    pub source: u32,
    pub fetch: u32,
    pub decode: u32,
    pub sink_dispatch: u32,
}

impl StageWorkerCounts {
    pub fn get(&self, stage: Stage) -> u32 {
        match stage {
            Stage::Source => self.source,
            Stage::Fetch => self.fetch,
            Stage::Decode => self.decode,
            Stage::SinkDispatch => self.sink_dispatch,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    Source,
    Fetch,
    Decode,
    SinkDispatch,
}

impl Stage {
    pub const ALL: [Stage; 4] = [
        Stage::Source,
        Stage::Fetch,
        Stage::Decode,
        Stage::SinkDispatch,
    ];
    pub fn as_label(self) -> &'static str {
        match self {
            Stage::Source => "source",
            Stage::Fetch => "fetch",
            Stage::Decode => "decode",
            Stage::SinkDispatch => "sink_dispatch",
        }
    }
    pub fn parse(s: &str) -> Option<Stage> {
        match s {
            "source" => Some(Stage::Source),
            "fetch" => Some(Stage::Fetch),
            "decode" => Some(Stage::Decode),
            "sink_dispatch" => Some(Stage::SinkDispatch),
            _ => None,
        }
    }
}

/// Per-iteration config (the bench's outer driver replicates this
/// for every iteration; values are stable across iterations).
#[derive(Debug, Clone)]
pub struct IterationParams {
    pub warmup_payloads: u64,
    pub timed_payloads: u64,
    pub records_per_source_range: usize,
    pub runtime_options: RuntimeOptions,
    pub warmup_deadline: Duration,
    pub timed_deadline: Duration,
    /// Per design §Iteration Protocol: iterations whose timed window
    /// is shorter than this floor fail with a clear error. The
    /// design pins `MIN_TIMED_WINDOW_SECONDS = 20.0` for real-CH
    /// runs; row 7.1's in-memory dry-run lets the operator lower
    /// this for tests + CI workloads that can't realistically take
    /// 20 s of in-memory pipeline work.
    pub min_timed_window_seconds: f64,
}

/// Drive one iteration of the §Iteration Protocol. Each call builds
/// a fresh `InMemoryFixture` so per-iteration state (Buffer manifest,
/// object store, ack frontier) is clean.
pub async fn run_iteration(
    iter_idx: usize,
    params: &IterationParams,
    snapshotter: &Snapshotter,
) -> Result<IterationReport> {
    // ───── 1. Drain any residue from earlier iterations / harness
    //   construction. `Snapshotter::snapshot()` is destructive (it
    //   `swap(0, ...)`s counters/gauges and clears histograms) so a
    //   call here resets the recorder to a clean state for this
    //   iteration's warmup phase.
    let _residue = snapshotter.snapshot();

    // ───── 2. Build a fresh fixture (per-iteration isolation).
    //   Unlike the design's pseudocode (which prefills the entire
    //   workload upfront), row 7.1's in-memory dry-run splits the
    //   producer into a warmup phase + a timed phase. Reason: the
    //   in-memory BenchSink resolves writes in microseconds, so a
    //   single upfront prefill lets the runtime ack every payload
    //   before the bench reaches the warmup-snapshot boundary —
    //   collapsing the protocol into "everything is warmup, nothing
    //   is timed". The two-phase shape preserves the design's
    //   *intent* (warmup boundary observable; timed window captured
    //   cleanly) for in-memory workloads, with the trade-off that
    //   the producer runs concurrently with the runtime within each
    //   phase rather than fully ahead of it. Row 7.2's real-CH
    //   variant pays enough sink latency per chunk that the upfront
    //   prefill works as the design pseudocodes it.
    let manifest_path = format!("phase07/7.1/iter-{iter_idx}/manifest");
    let data_prefix = format!("phase07/7.1/iter-{iter_idx}/data");
    let fixture = in_memory_fixture(&manifest_path, &data_prefix).await;
    let total_payloads = params.warmup_payloads + params.timed_payloads;
    let warmup_highest_sequence = params.warmup_payloads.saturating_sub(1);
    let total_highest_sequence = total_payloads.saturating_sub(1);

    // ───── 3. Start the runtime ─────
    let sink = BenchSink::new(SinkId::from("phase07-stage-latencies"));
    let runtime = Runtime::builder()
        .add_source(fixture.source)
        .add_decoder(FakeDecoder)
        .set_sink(sink)
        .with_options(params.runtime_options.clone())
        .build()
        .map_err(|e| anyhow!("Runtime::build: {e}"))?;
    let bp = params
        .runtime_options
        .backpressure_for(&opendata_ingest_runtime::source::SourceId::from("buffer"));
    let worker_counts = StageWorkerCounts {
        source: 1,
        fetch: bp.fetch_concurrency,
        decode: bp.decode_concurrency,
        sink_dispatch: params.runtime_options.sink.max_concurrent_commits,
    };
    let mut progress = runtime.progress();
    let shutdown = CancellationToken::new();
    let runtime_task = {
        let s = shutdown.clone();
        tokio::spawn(async move { runtime.run(s).await })
    };

    // ───── 4. Produce the warmup payloads, then drain to the
    //   warmup boundary. The runtime is already running so the
    //   produce + drain phases overlap, but the warmup-boundary
    //   wait ensures every warmup payload is acked before the
    //   bench snapshots the baseline.
    if params.warmup_payloads > 0 {
        produce_n_batches(&fixture.producer, params.warmup_payloads).await;
    }
    let warmup_start = Instant::now();
    if params.warmup_payloads > 0 {
        loop {
            {
                let p = *progress.borrow();
                if let Some(s) = p.last_acked_sequence
                    && s >= warmup_highest_sequence
                {
                    break;
                }
            }
            if warmup_start.elapsed() > params.warmup_deadline {
                bail!(
                    "warmup did not drain within {:?} (last_acked={:?}, target={warmup_highest_sequence})",
                    params.warmup_deadline,
                    progress.borrow().last_acked_sequence,
                );
            }
            tokio::select! {
                res = progress.changed() => {
                    res.map_err(|e| anyhow!("progress channel closed during warmup: {e}"))?;
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    }

    // ───── 5. Drain the recorder at the warmup boundary ─────
    //   Snapshotter::snapshot() is destructive; this call discards
    //   the warmup-phase data so the next snapshot captures only the
    //   timed window's metrics.
    let _baseline = snapshotter.snapshot();
    let timed_start = Instant::now();
    let timed_started_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // ───── 6a. Produce the timed-phase payloads. Producer runs
    //   concurrently with the consumer; the timed-window drain wait
    //   below blocks until every timed payload is acked.
    if params.timed_payloads > 0 {
        produce_n_batches(&fixture.producer, params.timed_payloads).await;
    }
    fixture.producer.close().await.context("producer.close")?;

    // ───── 6b. Drain the timed window ─────
    //   Same poll-first / await-with-deadline shape as warmup so a
    //   runtime that races ahead of the bench harness doesn't
    //   deadlock the drain loop.
    loop {
        {
            let p = *progress.borrow();
            if let Some(s) = p.last_acked_sequence
                && s >= total_highest_sequence
            {
                break;
            }
        }
        if timed_start.elapsed() > params.timed_deadline {
            bail!(
                "timed window did not drain within {:?} (last_acked={:?}, target={total_highest_sequence})",
                params.timed_deadline,
                progress.borrow().last_acked_sequence,
            );
        }
        tokio::select! {
            res = progress.changed() => {
                res.map_err(|e| anyhow!("progress channel closed during timed window: {e}"))?;
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }
    }

    let timed_end = Instant::now();
    let elapsed_seconds = (timed_end - timed_start).as_secs_f64();
    if elapsed_seconds < params.min_timed_window_seconds {
        bail!(
            "timed window too short ({elapsed_seconds:.4}s; floor {:.4}s) — raise timed_payloads or lower min_timed_window_seconds",
            params.min_timed_window_seconds,
        );
    }

    // Snapshot final progress before shutdown so we capture the
    // post-drain records_written.
    let progress_final = *progress.borrow();

    // ───── 7. Graceful shutdown to flush in-flight acks ─────
    shutdown.cancel();
    runtime_task
        .await
        .map_err(|e| anyhow!("runtime task join: {e}"))?
        .map_err(|e| anyhow!("runtime.run: {e}"))?;

    // ───── 8. Final snapshot; this is the timed-window delta ─────
    //   (baseline drained the recorder; final captures everything
    //   between baseline and now — i.e. the timed window's metrics
    //   plus the shutdown drain). `Snapshot` is `!Clone` so we
    //   consume it here.
    let final_snapshot = snapshotter.snapshot();
    let stage_samples = collect_stage_samples_destructive(final_snapshot);

    Ok(IterationReport {
        iteration: iter_idx,
        timed_started_unix_ms,
        elapsed_seconds,
        records_processed: progress_final.records_written,
        stage_samples,
        worker_counts,
    })
}

/// Walk the snapshot and collect the histogram sample vectors for
/// every `runtime_stage_latency_seconds{stage=...}` series. Returns
/// the four-stage tuple; missing stages produce empty vecs.
/// Consumes the snapshot because `Snapshot` is `!Clone`.
pub fn collect_stage_samples_destructive(snapshot: Snapshot) -> StageSamples {
    let mut samples = StageSamples::default();
    for (key, _unit, _desc, value) in snapshot.into_vec() {
        if key.key().name() != opendata_ingest_runtime::metrics::STAGE_LATENCY_SECONDS {
            continue;
        }
        let DebugValue::Histogram(hist) = value else {
            continue;
        };
        let stage_label = key.key().labels().find_map(|l| {
            if l.key() == "stage" {
                Some(l.value().to_string())
            } else {
                None
            }
        });
        let stage = match stage_label.as_deref().and_then(Stage::parse) {
            Some(s) => s,
            None => continue,
        };
        let bucket: &mut Vec<f64> = match stage {
            Stage::Source => &mut samples.source,
            Stage::Fetch => &mut samples.fetch,
            Stage::Decode => &mut samples.decode,
            Stage::SinkDispatch => &mut samples.sink_dispatch,
        };
        bucket.extend(hist.iter().map(|v| v.into_inner()));
    }
    samples
}

/// Default per-iteration parameters tuned for the in-memory dry-run
/// port. Real-CH variants set their own.
pub fn default_runtime_options() -> RuntimeOptions {
    // Drive the full pipeline, not dry-run — the design's row 7.1
    // wants all four stage labels (including sink_dispatch) to emit
    // samples. Dry-run skips Sink::write and the Buffer ack/flush.
    RuntimeOptions {
        dry_run: false,
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        poll_interval: Duration::from_millis(5),
        ..Default::default()
    }
}

/// Construct an `Arc<Snapshotter>`-like handle around the bench
/// crate's process-global recorder. Provided for the binary +
/// integration tests so both go through the same idempotent install
/// site.
pub fn install_recorder() -> Arc<Snapshotter> {
    Arc::new(crate::metrics_recorder::init_metrics_recorder().clone())
}
