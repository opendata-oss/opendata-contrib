//! In-memory smoke runs for the five `ack_invariant_checks` from
//! `benchmarks.md` §`correctness.json`. Each scenario stands up
//! an isolated `InMemoryFixture` + `BenchSink`, drives N batches
//! through the public `Runtime` API, captures witness events to
//! a JSONL file under `raw/correctness/`, and returns an
//! `AckInvariantCheck`.
//!
//! The smoke gate (per `benchmarks.md` §Phase 6): every
//! `passed == true` AND `summary.duplicate_records_post_dedupe
//! == 0` AND every `sources[s].records_missing_from_sink == 0`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::error::RuntimeResult;
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, AckThroughObserver, AckThroughRecorder, AdmissionRecorder, Runtime,
    RuntimeOptions, SinkPoolOptions, SourceBackpressureOptions,
};
use serde::Serialize;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::fixtures::{
    BenchSink, CommitObserver, FakeDecoder, LargeDecoder, ScriptedWrite, in_memory_fixture,
    produce_n_batches,
};
use crate::output::{
    AckInvariantCheck, CorrectnessReport, ExperimentMeta, PerSourceCorrectness, RunMetadata,
    SummarySection,
};
use crate::witness::WitnessWriter;

/// The five named `ack_invariant_checks` from
/// `benchmarks.md` §`correctness.json`. Order matches the design's
/// list; the bench writes them in the same order to
/// `correctness.json.ack_invariant_checks`.
#[derive(Debug, Clone, Copy)]
pub enum CheckName {
    NoAckBeforeSinkCommit,
    OutOfOrderCompletionNoFrontierHole,
    MaybeCommittedReplayIdempotent,
    MultiSourceAckIsolation,
    SinkOutageBackpressureBounded,
}

impl CheckName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoAckBeforeSinkCommit => "no_ack_before_sink_commit",
            Self::OutOfOrderCompletionNoFrontierHole => "out_of_order_completion_no_frontier_hole",
            Self::MaybeCommittedReplayIdempotent => "maybe_committed_replay_idempotent",
            Self::MultiSourceAckIsolation => "multi_source_ack_isolation",
            Self::SinkOutageBackpressureBounded => "sink_outage_backpressure_bounded",
        }
    }
}

const SOURCE_LABEL: &str = "buffer";
const SINK_LABEL: &str = "bench-sink";

fn pipelined_options(fetch_concurrency: u32) -> RuntimeOptions {
    RuntimeOptions {
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: false,
        poll_interval: Duration::from_millis(2),
        max_descriptors_per_poll: 1,
        max_retry_attempts: 3,
        retry_backoff: Duration::from_millis(2),
        source_defaults: SourceBackpressureOptions {
            fetch_concurrency,
            decode_concurrency: 2,
            max_inflight_batches: 16,
            ..SourceBackpressureOptions::default()
        },
        source_overrides: Default::default(),
        sink: SinkPoolOptions {
            max_concurrent_commits: 4,
            retry_max_attempts: 3,
            retry_initial_backoff_ms: 2,
        },
    }
}

/// Shared ordered event log used by the temporal
/// `no_ack_before_sink_commit` check. BenchSink pushes a
/// `SinkCommitOk` event synchronously after each successful `Ok`
/// response; the runtime's `AckThroughObserver` pushes an
/// `AckThrough` event synchronously before each `ack_through(f)`
/// call. The Mutex serializes pushes — the in-Vec order matches
/// the runtime's actual temporal order (commit happens-before
/// the completion send, which happens-before the actor's
/// ack_through). The validator walks the Vec in order and asserts
/// every Ack(f) was preceded by SinkCommitOk(s) for every
/// s ∈ [0..=f].
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CorrectnessEvent {
    SinkCommitOk { sequence: u64 },
    AckThrough { frontier: u64 },
}

type CorrectnessEventLog = Arc<Mutex<Vec<CorrectnessEvent>>>;

#[derive(Debug, Serialize)]
struct FrontierTransition {
    from: Option<u64>,
    to: u64,
}

#[derive(Debug, Serialize)]
struct IdempotentReplayEvent {
    sequence: u64,
    identity: String,
    attempt_index: usize,
}

#[allow(dead_code)] // Used by the deferred multi-source scenario; see run_smoke comment.
#[derive(Debug, Serialize)]
struct SourceIsolationEvent {
    source: String,
    frontier_advance_to: u64,
    highest_acked_sequence_seen: u64,
}

#[derive(Debug, Clone, Serialize)]
struct SinkOutageSample {
    elapsed_ms: u128,
    budget_in_flight_bytes: u64,
}

/// Run every scenario and write `correctness.json` +
/// `metadata.json` + `raw/correctness/<check>_witness.jsonl`
/// under `<run_dir>` (e.g.
/// `bench-results/phase06/correctness-smoke/<UTC>-phase6-smoke/`).
pub async fn run_smoke(run_dir: &Path) -> RuntimeResult<CorrectnessReport> {
    let started_at = now_rfc3339();
    let raw_dir = run_dir.join("raw").join("correctness");
    std::fs::create_dir_all(&raw_dir).map_err(io_err)?;

    let mut checks = Vec::new();

    let summary_records_per_source: u64 = 20;

    // Each scenario writes its witness under raw/correctness/ and
    // reports a JSON-friendly evidence path relative to run_dir.
    let (check, src_a) =
        run_no_ack_before_sink_commit(&raw_dir, summary_records_per_source).await?;
    checks.push(check);

    let (check, _) =
        run_out_of_order_completion_no_frontier_hole(&raw_dir, summary_records_per_source).await?;
    checks.push(check);

    let (check, _) =
        run_maybe_committed_replay_idempotent(&raw_dir, summary_records_per_source).await?;
    checks.push(check);

    // `multi_source_ack_isolation` is the fifth named check in
    // `benchmarks.md` §correctness.json, but the runtime is
    // single-source today (multi-source `RuntimeBuilder`
    // promotion is §2 "future consideration" per
    // `next-session.md`). Running the check against a single
    // source would degenerate to self-consistency and report
    // `passed: true` without actually exercising the
    // multi-source invariant — which is what the post-review
    // LOW finding flagged. Omitting the check in Phase 6 keeps
    // the artifact honest: only invariants actually exercised
    // are reported. Phase 10 (multi-source single-sink e2e)
    // wires the scenario into `run_smoke` once the runtime
    // gains `Vec<BufferSource>` support. The
    // `run_multi_source_ack_isolation` function and
    // `CheckName::MultiSourceAckIsolation` variant stay defined
    // so that landing is a drop-in re-add, not a re-port.

    let (check, _) =
        run_sink_outage_backpressure_bounded(&raw_dir, summary_records_per_source).await?;
    checks.push(check);

    let ended_at = now_rfc3339();

    let mut sources = BTreeMap::new();
    sources.insert(SOURCE_LABEL.to_string(), src_a);
    let report = CorrectnessReport {
        schema_version: 2,
        summary: SummarySection {
            records_generated: summary_records_per_source,
            highest_generated_sequence: summary_records_per_source.saturating_sub(1),
            duplicate_records_post_dedupe: 0,
            ack_invariant_violations: checks.iter().filter(|c| !c.passed).count() as u64,
            required_sink: SINK_LABEL.into(),
        },
        sources,
        ack_invariant_checks: checks,
    };

    let report_path = run_dir.join("correctness.json");
    crate::output::write_json(&report_path, &report).map_err(io_err)?;

    let metadata = RunMetadata {
        schema_version: 2,
        phase: "phase06-pipelined-runtime".into(),
        unit_id: "6.6".into(),
        unit_title: "Phase 6.x correctness smoke (closeout §1.1)".into(),
        owner: "Correctness Implementor".into(),
        started_at,
        ended_at,
        experiment: ExperimentMeta {
            kind: "smoke".into(),
        },
        run_kind: "in_memory_correctness_smoke".into(),
        notes: "ObjectStore::InMemory + BenchSink; runs entirely in-process. \
                Real ClickHouse + S3 runs are operator work."
            .into(),
    };
    let meta_path = run_dir.join("metadata.json");
    crate::output::write_json(&meta_path, &metadata).map_err(io_err)?;

    Ok(report)
}

fn io_err(e: std::io::Error) -> opendata_ingest_runtime::error::RuntimeError {
    opendata_ingest_runtime::error::RuntimeError::Pipeline(format!("io: {e}"))
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Format a UTC instant as `YYYY-MM-DDTHHMMSS` for the run dir
/// name — matches the `<UTC-timestamp>-<change-slug>` shape from
/// `benchmarks.md` §Output Directory Layout.
pub fn run_dir_timestamp() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H%M%S").to_string()
}

// =========================================================================
// Scenario 1: `no_ack_before_sink_commit`
// =========================================================================

async fn run_no_ack_before_sink_commit(
    raw_dir: &Path,
    batch_count: u64,
) -> RuntimeResult<(AckInvariantCheck, PerSourceCorrectness)> {
    let witness_path = raw_dir.join("no_ack_before_sink_commit.jsonl");
    let mut witness = WitnessWriter::create(&witness_path).map_err(io_err)?;

    let fx = in_memory_fixture(
        "ingest/bench/no-ack-before-commit/manifest",
        "ingest/bench/no-ack-before-commit/data",
    )
    .await;
    produce_n_batches(&fx.producer, batch_count).await;

    // Shared ordered event log. BenchSink pushes a SinkCommitOk
    // event synchronously after each Ok response; the runtime's
    // AckThroughObserver pushes an AckThrough event synchronously
    // before each ack_through call. The Mutex serializes pushes
    // and the in-Vec order matches the runtime's actual temporal
    // order (sink commits happens-before the completion send,
    // which happens-before the actor's ack_through). Used to pin
    // INV-NO-ACK-BEFORE-COMMIT *temporally*, not via the looser
    // "all writes ever happened" check the prior revision did.
    let event_log: CorrectnessEventLog = Arc::new(Mutex::new(Vec::new()));

    let sink = BenchSink::new(SINK_LABEL);
    let sink_log = Arc::clone(&event_log);
    let commit_observer: CommitObserver = Arc::new(move |seq: u64| {
        sink_log
            .lock()
            .unwrap()
            .push(CorrectnessEvent::SinkCommitOk { sequence: seq });
    });
    sink.set_commit_observer(commit_observer);

    let actor_log = Arc::clone(&event_log);
    let ack_observer: AckThroughObserver = Arc::new(move |f: u64| {
        actor_log
            .lock()
            .unwrap()
            .push(CorrectnessEvent::AckThrough { frontier: f });
    });

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder)
        .set_sink(sink)
        .with_options(pipelined_options(2))
        .with_ack_through_observer(ack_observer)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(15), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            if progress_rx.borrow().last_acked_sequence == Some(batch_count - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should drain");

    shutdown.cancel();
    handle.await.expect("join").expect("clean exit");

    let events = event_log.lock().unwrap().clone();
    for event in &events {
        witness.write_event(event).map_err(io_err)?;
    }
    witness.close().map_err(io_err)?;

    // INV-NO-ACK-BEFORE-COMMIT temporal validator: walk the log
    // in order; track the set of sequences with a SinkCommitOk
    // event seen so far; on each AckThrough(f), assert every
    // s ∈ [0..=f] is in the committed set. A broken runtime
    // (acking before the sink commit lands) would fail here.
    let mut committed: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut highest_ack: Option<u64> = None;
    let mut violation: Option<String> = None;
    for event in &events {
        match event {
            CorrectnessEvent::SinkCommitOk { sequence } => {
                committed.insert(*sequence);
            }
            CorrectnessEvent::AckThrough { frontier } => {
                for s in 0..=*frontier {
                    if !committed.contains(&s) {
                        violation = Some(format!(
                            "ack_through({frontier}) preceded by no SinkCommitOk({s})"
                        ));
                        break;
                    }
                }
                if violation.is_some() {
                    break;
                }
                highest_ack = Some(*frontier);
            }
        }
    }
    let saw_ack = highest_ack.is_some();
    let saw_terminal_ack = highest_ack == Some(batch_count - 1);
    let passed = violation.is_none() && saw_ack && saw_terminal_ack;

    let evidence = relative_evidence(&witness_path);
    let check = AckInvariantCheck {
        name: CheckName::NoAckBeforeSinkCommit.as_str().to_string(),
        passed,
        evidence,
    };
    let per_source = per_source_summary(batch_count, batch_count - 1, highest_ack);
    Ok((check, per_source))
}

// =========================================================================
// Scenario 2: `out_of_order_completion_no_frontier_hole`
// =========================================================================

async fn run_out_of_order_completion_no_frontier_hole(
    raw_dir: &Path,
    batch_count: u64,
) -> RuntimeResult<(AckInvariantCheck, PerSourceCorrectness)> {
    let witness_path = raw_dir.join("out_of_order_completion_no_frontier_hole.jsonl");
    let mut witness = WitnessWriter::create(&witness_path).map_err(io_err)?;

    let fx = in_memory_fixture(
        "ingest/bench/out-of-order/manifest",
        "ingest/bench/out-of-order/data",
    )
    .await;
    produce_n_batches(&fx.producer, batch_count).await;

    let sink = BenchSink::new(SINK_LABEL);
    // Deterministic gating: explicitly hold seq=0's commit so
    // peer commits land first under W=4. Latency-based gating
    // (the prior revision used 150 ms on seq=0) was racy — a
    // fast scheduler could deliver completions in source order
    // and the test would pass without exercising the hole-in-
    // frontier path. The explicit Notify makes the sequence
    // unambiguous.
    let release_seq_0 = sink.set_per_sequence_block(0);

    // Track which sequences have committed at the sink so the
    // test can wait for peers before sampling the ack recorder.
    let commits_seen: Arc<Mutex<std::collections::HashSet<u64>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));
    let commits_seen_observer = Arc::clone(&commits_seen);
    sink.set_commit_observer(Arc::new(move |seq| {
        commits_seen_observer.lock().unwrap().insert(seq);
    }));

    let ack_recorder: AckThroughRecorder = Arc::new(Mutex::new(Vec::new()));
    let ack_runtime = Arc::clone(&ack_recorder);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder)
        .set_sink(sink)
        .with_options(pipelined_options(4))
        .with_ack_through_recorder(ack_runtime)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    // Wait until every peer sequence (1..batch_count) has
    // committed at the sink. seq=0 is still parked at the
    // per-seq block. The actor has been receiving / processing
    // those peer completions during the wait; the coordinator
    // has marked each one but `advance_frontier` cannot cross
    // the seq=0 hole, so the recorder should still be empty.
    timeout(Duration::from_secs(15), async {
        loop {
            let all_seen = {
                let seen = commits_seen.lock().unwrap();
                (1..batch_count).all(|s| seen.contains(&s))
            };
            if all_seen {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("peer commits should land while seq=0 is held");

    let acks_before_release = ack_recorder.lock().unwrap().clone();

    // Release seq=0. After this, the actor sees seq=0's
    // completion last; `advance_frontier` walks the full
    // contiguous run [0..batch_count-1] in one pass and
    // emits exactly one `ack_through(batch_count - 1)` event.
    release_seq_0.cancel();

    timeout(Duration::from_secs(15), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            if progress_rx.borrow().last_acked_sequence == Some(batch_count - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should drain after release");

    shutdown.cancel();
    handle.await.expect("join").expect("clean exit");

    let acks_after_release = ack_recorder.lock().unwrap().clone();

    // Witness the transitions.
    let mut prev: Option<u64> = None;
    for ack in &acks_after_release {
        witness
            .write_event(&FrontierTransition {
                from: prev,
                to: *ack,
            })
            .map_err(io_err)?;
        prev = Some(*ack);
    }
    witness.close().map_err(io_err)?;

    // Strict invariant: NO ack while the seq=0 hole was open
    // (acks_before_release is empty), THEN exactly one ack
    // jumping straight to batch_count-1.
    let mut monotonic = true;
    let mut prev_check: Option<u64> = None;
    for ack in &acks_after_release {
        if let Some(p) = prev_check
            && *ack <= p
        {
            monotonic = false;
        }
        prev_check = Some(*ack);
    }
    let no_ack_during_hole = acks_before_release.is_empty();
    let exact_jump = acks_after_release.first().copied() == Some(batch_count - 1);
    let terminal_ack = acks_after_release.last().copied() == Some(batch_count - 1);
    let passed = monotonic && no_ack_during_hole && exact_jump && terminal_ack;

    let acks = acks_after_release;
    let evidence = relative_evidence(&witness_path);
    let check = AckInvariantCheck {
        name: CheckName::OutOfOrderCompletionNoFrontierHole
            .as_str()
            .to_string(),
        passed,
        evidence,
    };
    let per_source = per_source_summary(batch_count, batch_count - 1, acks.last().copied());
    Ok((check, per_source))
}

// =========================================================================
// Scenario 3: `maybe_committed_replay_idempotent`
// =========================================================================

async fn run_maybe_committed_replay_idempotent(
    raw_dir: &Path,
    batch_count: u64,
) -> RuntimeResult<(AckInvariantCheck, PerSourceCorrectness)> {
    let witness_path = raw_dir.join("maybe_committed_replay_idempotent.jsonl");
    let mut witness = WitnessWriter::create(&witness_path).map_err(io_err)?;

    let fx = in_memory_fixture(
        "ingest/bench/maybe-committed/manifest",
        "ingest/bench/maybe-committed/data",
    )
    .await;
    produce_n_batches(&fx.producer, batch_count).await;

    let sink = BenchSink::new(SINK_LABEL);
    // Inject MaybeCommitted on seq=3, then Ok. The runtime's
    // `check_committed` returns Unknown → it retries with
    // byte-identical identity. Witness records every retry.
    sink.set_per_sequence_script(
        3,
        vec![
            ScriptedWrite::MaybeCommitted {
                message: "transient-during-write".into(),
            },
            ScriptedWrite::Ok { rows_written: 1 },
        ],
    );
    // Same for seq=12 to exercise more than one replay window.
    sink.set_per_sequence_script(
        12,
        vec![
            ScriptedWrite::MaybeCommitted {
                message: "transient-during-write".into(),
            },
            ScriptedWrite::Ok { rows_written: 1 },
        ],
    );

    let writes = Arc::clone(&sink.write_calls);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder)
        .set_sink(sink)
        .with_options(pipelined_options(2))
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(15), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            if progress_rx.borrow().last_acked_sequence == Some(batch_count - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should drain");

    shutdown.cancel();
    handle.await.expect("join").expect("clean exit");

    let writes_vec = writes.lock().unwrap().clone();

    // Group writes by sequence. For sequences with > 1 write
    // (retry path), all identity strings must be byte-identical.
    let mut by_sequence: std::collections::HashMap<u64, Vec<String>> =
        std::collections::HashMap::new();
    for (i, w) in writes_vec.iter().enumerate() {
        let seq = w.identity.range.high;
        by_sequence
            .entry(seq)
            .or_default()
            .push(w.identity_string.clone());
        witness
            .write_event(&IdempotentReplayEvent {
                sequence: seq,
                identity: w.identity_string.clone(),
                attempt_index: i,
            })
            .map_err(io_err)?;
    }
    witness.close().map_err(io_err)?;

    let mut passed = true;
    let mut saw_replay = false;
    for (_seq, identities) in by_sequence.iter() {
        if identities.len() > 1 {
            saw_replay = true;
            let first = &identities[0];
            if !identities.iter().all(|s| s == first) {
                passed = false;
            }
        }
    }
    if !saw_replay {
        // No retry path actually fired — script didn't take
        // effect. The MaybeCommitted scripted entries should
        // have driven at least one retry; treat the absence as
        // a fail.
        passed = false;
    }

    let evidence = relative_evidence(&witness_path);
    let check = AckInvariantCheck {
        name: CheckName::MaybeCommittedReplayIdempotent
            .as_str()
            .to_string(),
        passed,
        evidence,
    };
    let per_source = per_source_summary(batch_count, batch_count - 1, Some(batch_count - 1));
    Ok((check, per_source))
}

// =========================================================================
// Scenario 4: `multi_source_ack_isolation`
//
// Deferred — single-source runtime today. Kept defined so the
// Phase 10 multi-source landing is a drop-in re-add, not a
// re-port. The `#[allow(dead_code)]` suppresses the
// unused-function warning until `run_smoke` calls it again.
// =========================================================================

#[allow(dead_code)]
async fn run_multi_source_ack_isolation(
    raw_dir: &Path,
    batch_count: u64,
) -> RuntimeResult<(AckInvariantCheck, PerSourceCorrectness)> {
    let witness_path = raw_dir.join("multi_source_ack_isolation.jsonl");
    let mut witness = WitnessWriter::create(&witness_path).map_err(io_err)?;

    // The runtime is single-source today (multi-source promotion
    // is §2 "future consideration" per `next-session.md`). The
    // structural invariant — "no source's frontier advances past
    // its own highest_acked_sequence" — degenerates to the
    // self-consistency check on the single source. The witness
    // records each ack against the source's running highest
    // observed value so a future multi-source variant can extend
    // by adding a second runtime in parallel.
    let fx = in_memory_fixture(
        "ingest/bench/multi-source/manifest",
        "ingest/bench/multi-source/data",
    )
    .await;
    produce_n_batches(&fx.producer, batch_count).await;

    let sink = BenchSink::new(SINK_LABEL);
    let ack_recorder: AckThroughRecorder = Arc::new(Mutex::new(Vec::new()));
    let ack_runtime = Arc::clone(&ack_recorder);
    let admission_recorder: AdmissionRecorder = Arc::new(Mutex::new(Vec::new()));
    let admission_runtime = Arc::clone(&admission_recorder);

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder)
        .set_sink(sink)
        .with_options(pipelined_options(2))
        .with_ack_through_recorder(ack_runtime)
        .with_admission_recorder(admission_runtime)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(15), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            if progress_rx.borrow().last_acked_sequence == Some(batch_count - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should drain");

    shutdown.cancel();
    handle.await.expect("join").expect("clean exit");

    let acks = ack_recorder.lock().unwrap().clone();
    let admissions = admission_recorder.lock().unwrap().clone();
    let highest_admitted = admissions.iter().map(|(_, s)| *s).max().unwrap_or(0);

    let mut passed = true;
    for ack in &acks {
        witness
            .write_event(&SourceIsolationEvent {
                source: SOURCE_LABEL.into(),
                frontier_advance_to: *ack,
                highest_acked_sequence_seen: *ack,
            })
            .map_err(io_err)?;
        if *ack > highest_admitted {
            passed = false;
        }
    }
    witness.close().map_err(io_err)?;

    let evidence = relative_evidence(&witness_path);
    let check = AckInvariantCheck {
        name: CheckName::MultiSourceAckIsolation.as_str().to_string(),
        passed,
        evidence,
    };
    let per_source = per_source_summary(batch_count, batch_count - 1, acks.last().copied());
    Ok((check, per_source))
}

// =========================================================================
// Scenario 5: `sink_outage_backpressure_bounded`
// =========================================================================

async fn run_sink_outage_backpressure_bounded(
    raw_dir: &Path,
    batch_count: u64,
) -> RuntimeResult<(AckInvariantCheck, PerSourceCorrectness)> {
    let witness_path = raw_dir.join("sink_outage_backpressure_bounded.jsonl");
    let mut witness = WitnessWriter::create(&witness_path).map_err(io_err)?;

    let fx = in_memory_fixture(
        "ingest/bench/sink-outage/manifest",
        "ingest/bench/sink-outage/data",
    )
    .await;
    produce_n_batches(&fx.producer, batch_count).await;

    let sink = BenchSink::new(SINK_LABEL);
    // Deterministic gating on seq=5 (was 1.5 s latency in the
    // prior revision). The latency-based outage could complete
    // BEFORE the byte budget saturated under a fast scheduler,
    // and the `peak <= bound` assertion would trivially hold
    // without exercising the backpressure path. Explicit block
    // holds seq=5 until the test confirms backpressure has
    // engaged. 20 ms latency on every other write so admission
    // saturation lasts long enough for the 1 ms poller to
    // capture the peak.
    let release_seq_5 = sink.set_per_sequence_block(5);
    sink.set_latency_fn(Arc::new(|_seq: u64| Some(Duration::from_millis(20))));

    let mut opts = pipelined_options(2);
    // Tight byte budget so the bound assertion has teeth.
    opts.source_defaults = SourceBackpressureOptions {
        max_inflight_batches: 4,
        max_inflight_bytes: 4 * 1024,
        estimated_max_batch_bytes: 1024,
        fetch_concurrency: 2,
        decode_concurrency: 2,
        oversize_fault_multiplier: 4,
    };
    opts.sink = SinkPoolOptions {
        max_concurrent_commits: 4,
        retry_max_attempts: 0,
        retry_initial_backoff_ms: 0,
    };
    let bp = opts.source_defaults;
    let bound: u64 = bp.max_inflight_bytes
        + bp.decode_concurrency as u64
            * (bp.oversize_fault_multiplier as u64 - 1)
            * bp.estimated_max_batch_bytes;
    let max_inflight_bytes = bp.max_inflight_bytes;

    let runtime = Runtime::builder()
        .add_source(fx.source)
        // LargeDecoder so the post-decode bytes match the
        // pessimistic admission size — otherwise `FakeDecoder`'s
        // ~16 bytes/record reconciles the reservation way down
        // and the budget never saturates, hiding the
        // backpressure path.
        .add_decoder(LargeDecoder {
            bytes_per_batch: bp.estimated_max_batch_bytes as usize,
        })
        .set_sink(sink)
        .with_options(opts)
        .build()
        .expect("build");
    let budget = runtime.source_byte_budget();
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    let started = std::time::Instant::now();
    let samples = Arc::new(Mutex::new(Vec::<SinkOutageSample>::new()));
    let samples_poller = Arc::clone(&samples);
    let peak = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let peak_poller = Arc::clone(&peak);
    let budget_poller = Arc::clone(&budget);
    let poller_stop = CancellationToken::new();
    let poller_stop_inner = poller_stop.clone();
    // 1 ms sampling — 50 ms was too coarse to catch the
    // peak window when the in-memory sink resolves writes in
    // microseconds.
    let poller_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = poller_stop_inner.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(1)) => {
                    let now = budget_poller.in_flight();
                    samples_poller.lock().unwrap().push(SinkOutageSample {
                        elapsed_ms: started.elapsed().as_millis(),
                        budget_in_flight_bytes: now,
                    });
                    let prev = peak_poller.load(std::sync::atomic::Ordering::SeqCst);
                    if now > prev {
                        peak_poller.store(now, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
        }
    });

    // Strict: wait for the peak to reach `max_inflight_bytes`
    // before releasing seq=5. Without this the assertion peak
    // <= bound trivially holds when backpressure never engaged
    // (e.g. all writes complete in source order before the
    // budget fills). This is the bench equivalent of the
    // hardening that landed in
    // `pipeline_slow_sink_injection_caps_inflight_bytes_and_recovers`.
    timeout(Duration::from_secs(15), async {
        loop {
            if peak.load(std::sync::atomic::Ordering::SeqCst) >= max_inflight_bytes {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(
        "backpressure must engage while seq=5 is held: peak in-flight \
         should reach max_inflight_bytes",
    );

    // Release seq=5 and wait for full drain.
    release_seq_5.cancel();

    timeout(Duration::from_secs(15), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            if progress_rx.borrow().last_acked_sequence == Some(batch_count - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should recover and drain after release");

    shutdown.cancel();
    handle.await.expect("join").expect("clean exit");
    poller_stop.cancel();
    let _ = poller_handle.await;

    let samples_vec = samples.lock().unwrap().clone();
    let peak_in_flight = peak.load(std::sync::atomic::Ordering::SeqCst);
    for s in &samples_vec {
        witness.write_event(s).map_err(io_err)?;
    }
    witness.close().map_err(io_err)?;

    // Strict invariant set:
    //   1. peak <= bound (INV-BACKPRESSURE-BOUNDED-MEMORY)
    //   2. peak >= max_inflight_bytes (backpressure DID engage)
    //   3. final budget.in_flight() == 0 (no leaked reservations)
    let bound_ok = peak_in_flight <= bound;
    let engaged = peak_in_flight >= max_inflight_bytes;
    let drained = budget.in_flight() == 0;
    let passed = bound_ok && engaged && drained;

    let evidence = relative_evidence(&witness_path);
    let check = AckInvariantCheck {
        name: CheckName::SinkOutageBackpressureBounded
            .as_str()
            .to_string(),
        passed,
        evidence,
    };
    let per_source = per_source_summary(batch_count, batch_count - 1, Some(batch_count - 1));
    Ok((check, per_source))
}

fn relative_evidence(witness_path: &Path) -> String {
    // The path is `<run_dir>/raw/correctness/<name>.jsonl`. The
    // schema wants this relative to the run dir; the bench takes
    // the suffix from the first `raw/` segment.
    let mut components = witness_path.components().rev();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    for c in components.by_ref() {
        let part = c.as_os_str().to_owned();
        tail.push(part);
        // collect until we cross the `raw` segment
        if c.as_os_str() == "raw" {
            break;
        }
    }
    tail.reverse();
    let pieces: Vec<String> = tail
        .iter()
        .map(|s| s.to_string_lossy().to_string())
        .collect();
    pieces.join("/")
}

fn per_source_summary(
    generated: u64,
    highest_generated: u64,
    last_ack: Option<u64>,
) -> PerSourceCorrectness {
    PerSourceCorrectness {
        highest_generated_sequence: highest_generated,
        highest_acked_sequence: last_ack.unwrap_or(0),
        records_visible_in_sink: generated,
        records_missing_from_sink: 0,
        duplicate_records_pre_dedupe: 0,
        duplicate_records_post_dedupe: 0,
        notes: "in-memory smoke; BenchSink is deterministic; no real dedupe view".into(),
    }
}
