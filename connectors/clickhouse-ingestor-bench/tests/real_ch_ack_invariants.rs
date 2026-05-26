//! The 4 named `ack_invariant_checks` exercised against the
//! production `ClickHouseSink<OtlpLogsClickHouseAdapter>` end-to-end
//! via the `TestObservableSink` trait. Docker-gated.
//!
//! Each scenario installs the right hooks on a `RealClickHouseSink`
//! wrapper, drives a small workload through `Runtime::builder`, and
//! asserts the named invariant.
//!
//! Why these are here and not in the throughput matrix: invariants
//! don't drift with workload size; if `no_ack_before_sink_commit`
//! holds for 10 source ranges with deterministic per-sequence
//! blocking, it holds for 8000 source ranges in the throughput
//! matrix too. Running them at small workloads keeps total
//! wall-clock to ~minutes instead of the matrix's ~40-min cost.
//!
//! The fifth named check, `multi_source_ack_isolation`, stays
//! deferred — the runtime is single-source (multi-source
//! `RuntimeBuilder` support is future work).

#![cfg(feature = "real-ch")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use clickhouse_ingestor::writer::{ClickHouseWriter, WriterConfig};
use clickhouse_ingestor_bench::real_ch::{
    LogWorkload, LogWorkloadConfig, RealClickHouseFixture, RealClickHouseSink,
    workload::{WorkloadEnv, build_env},
};
use clickhouse_ingestor_bench::test_observable_sink::{
    CommitObserver, ScriptedWrite, SinkCommitFailureKind, TestObservableSink, WriteOutcome,
};
use opendata_ingest_clickhouse::adapter::logs::{LogsAdapterConfig, OtlpLogsClickHouseAdapter};
use opendata_ingest_clickhouse::sink::ClickHouseSink;
use opendata_ingest_otel::logs::OtlpLogsDecoder;
use opendata_ingest_runtime::identity::CommitIdentity;
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, AckThroughObserver, AckThroughRecorder, Runtime, RuntimeOptions,
    SinkPoolOptions, SourceBackpressureOptions,
};
use opendata_ingest_runtime::source::SourceId;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn pipelined_options() -> RuntimeOptions {
    RuntimeOptions {
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: false,
        poll_interval: Duration::from_millis(5),
        max_retry_attempts: 3,
        retry_backoff: Duration::from_millis(10),
        source_defaults: SourceBackpressureOptions {
            fetch_concurrency: 2,
            decode_concurrency: 2,
            max_inflight_batches: 16,
            ..SourceBackpressureOptions::default()
        },
        sink: SinkPoolOptions {
            max_concurrent_commits: 4,
            retry_max_attempts: 3,
            retry_initial_backoff_ms: 10,
        },
        ..Default::default()
    }
}

async fn fresh_fixture(table: &str) -> RealClickHouseFixture {
    let adapter_cfg = LogsAdapterConfig {
        database: "phase07_ack_invariants".into(),
        table: table.into(),
        ..Default::default()
    };
    RealClickHouseFixture::setup_testcontainers(
        adapter_cfg.database.clone(),
        adapter_cfg.table.clone(),
        adapter_cfg,
    )
    .await
    .expect("setup_testcontainers")
}

/// Build a fresh sink writer + adapter against the fixture's
/// container and wrap them in `RealClickHouseSink`. Mirrors the
/// matrix runner's pattern (`real_ch/runner.rs`): a per-scenario
/// writer keeps test scenarios isolated and lets us flip
/// serialization knobs without rebuilding the fixture.
fn build_real_sink(fixture: &RealClickHouseFixture) -> RealClickHouseSink {
    let adapter = Arc::new(OtlpLogsClickHouseAdapter::new(
        fixture.adapter_config.clone(),
    ));
    let writer_config = WriterConfig {
        endpoint: fixture.endpoint.clone(),
        user: fixture.writer.config().user.clone(),
        password: fixture.writer.config().password.clone(),
        request_timeout: fixture.writer.config().request_timeout,
        max_attempts: fixture.writer.config().max_attempts,
        initial_backoff: fixture.writer.config().initial_backoff,
        ..Default::default()
    };
    let sink_writer = Arc::new(ClickHouseWriter::new(writer_config));
    let inner = ClickHouseSink::new("phase07-ack-invariants", adapter, sink_writer);
    RealClickHouseSink::new("phase07-ack-invariants", inner)
}

fn workload_cfg(manifest_path: &str, data_prefix: &str) -> LogWorkloadConfig {
    LogWorkloadConfig {
        source_id: SourceId::from("phase07-ack-invariants"),
        records_per_source_range: 10,
        warmup_payloads: 0,
        timed_payloads: 0, // produce() drives the count directly
        manifest_path: manifest_path.into(),
        data_prefix: data_prefix.into(),
        ..Default::default()
    }
}

/// `no_ack_before_sink_commit` against the production sink.
/// Installs a commit observer that pushes a `SinkCommitOk` event
/// after each inner-sink Ok; installs an `AckThroughObserver` that
/// pushes an `AckThrough` event before each `ack_through`; walks
/// the shared ordered log and asserts every `AckThrough(f)` is
/// preceded by `SinkCommitOk(s)` for every `s ∈ [0..=f]`.
#[tokio::test]
async fn no_ack_before_sink_commit_against_real_clickhouse() {
    let fixture = fresh_fixture("logs_no_ack_before").await;
    let cfg = workload_cfg(
        "phase07/ack-invariants/no-ack/manifest",
        "phase07/ack-invariants/no-ack/data",
    );
    let env = build_env(&cfg).await.expect("build_env");
    let n: u64 = 6;
    LogWorkload::produce(&cfg, &env.producer, 0, n)
        .await
        .expect("produce");
    let WorkloadEnv {
        producer, source, ..
    } = env;

    let sink = build_real_sink(&fixture);

    #[derive(Debug, Clone)]
    enum Event {
        SinkCommitOk(u64),
        AckThrough(u64),
    }
    let event_log: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));

    let sink_log = Arc::clone(&event_log);
    let observer: Arc<dyn CommitObserver> = Arc::new(move |id: &CommitIdentity| {
        sink_log
            .lock()
            .unwrap()
            .push(Event::SinkCommitOk(id.range.high));
    });
    sink.set_commit_observer(observer);

    let actor_log = Arc::clone(&event_log);
    let ack_observer: AckThroughObserver = Arc::new(move |f: u64| {
        actor_log.lock().unwrap().push(Event::AckThrough(f));
    });

    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(OtlpLogsDecoder::new())
        .set_sink(sink.clone())
        .with_options(pipelined_options())
        .with_ack_through_observer(ack_observer)
        .build()
        .expect("build");
    let mut progress = runtime.progress();
    let shutdown = CancellationToken::new();
    let runtime_task = {
        let s = shutdown.clone();
        tokio::spawn(async move { runtime.run(s).await })
    };

    producer.close().await.expect("producer close");

    timeout(Duration::from_secs(60), async {
        loop {
            progress.changed().await.expect("progress channel closed");
            if progress.borrow().last_acked_sequence == Some(n - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should drain");

    shutdown.cancel();
    runtime_task.await.expect("join").expect("clean exit");

    let events = event_log.lock().unwrap().clone();
    let mut committed: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut saw_terminal_ack = false;
    for event in &events {
        match event {
            Event::SinkCommitOk(s) => {
                committed.insert(*s);
            }
            Event::AckThrough(f) => {
                for s in 0..=*f {
                    assert!(
                        committed.contains(&s),
                        "ack_through({f}) preceded by no SinkCommitOk({s}) in: {events:?}",
                    );
                }
                if *f == n - 1 {
                    saw_terminal_ack = true;
                }
            }
        }
    }
    assert!(saw_terminal_ack, "never saw terminal ack: {events:?}");
}

/// `out_of_order_completion_no_frontier_hole` against the production
/// sink. Block seq=0 deterministically, wait for peer commits to
/// land at the sink, prove `ack_through` did NOT fire during the
/// hold, release seq=0, and prove the final ack equals `n-1`.
#[tokio::test]
async fn out_of_order_completion_no_frontier_hole_against_real_clickhouse() {
    let fixture = fresh_fixture("logs_out_of_order").await;
    let cfg = workload_cfg(
        "phase07/ack-invariants/out-of-order/manifest",
        "phase07/ack-invariants/out-of-order/data",
    );
    let env = build_env(&cfg).await.expect("build_env");
    let n: u64 = 6;
    LogWorkload::produce(&cfg, &env.producer, 0, n)
        .await
        .expect("produce");
    let WorkloadEnv {
        producer, source, ..
    } = env;

    let sink = build_real_sink(&fixture);
    let release_seq_0 = sink.set_per_sequence_block(0);

    let commits_seen: Arc<Mutex<std::collections::HashSet<u64>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));
    let observer_log = Arc::clone(&commits_seen);
    let observer: Arc<dyn CommitObserver> = Arc::new(move |id: &CommitIdentity| {
        observer_log.lock().unwrap().insert(id.range.high);
    });
    sink.set_commit_observer(observer);

    let ack_recorder: AckThroughRecorder = Arc::new(Mutex::new(Vec::new()));

    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(OtlpLogsDecoder::new())
        .set_sink(sink.clone())
        .with_options(pipelined_options())
        .with_ack_through_recorder(Arc::clone(&ack_recorder))
        .build()
        .expect("build");
    let mut progress = runtime.progress();
    let shutdown = CancellationToken::new();
    let runtime_task = {
        let s = shutdown.clone();
        tokio::spawn(async move { runtime.run(s).await })
    };
    producer.close().await.expect("producer close");

    // Wait until every peer sequence has landed in CH.
    timeout(Duration::from_secs(60), async {
        loop {
            let all_seen = {
                let seen = commits_seen.lock().unwrap();
                (1..n).all(|s| seen.contains(&s))
            };
            if all_seen {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer commits should land while seq=0 is held");

    let acks_during_hold = ack_recorder.lock().unwrap().clone();
    assert!(
        acks_during_hold.is_empty(),
        "ack_through must not fire while the seq=0 hole exists; got: {acks_during_hold:?}",
    );

    release_seq_0.cancel();

    timeout(Duration::from_secs(60), async {
        loop {
            progress.changed().await.expect("progress channel closed");
            if progress.borrow().last_acked_sequence == Some(n - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should drain after release");

    shutdown.cancel();
    runtime_task.await.expect("join").expect("clean exit");

    let acks_after = ack_recorder.lock().unwrap().clone();
    assert!(
        !acks_after.is_empty(),
        "ack_through should have fired after release",
    );
    assert_eq!(
        *acks_after.last().unwrap(),
        n - 1,
        "final ack should be {} got {:?}",
        n - 1,
        acks_after,
    );
    for window in acks_after.windows(2) {
        assert!(window[0] < window[1], "non-monotonic acks: {acks_after:?}");
    }
}

/// `maybe_committed_replay_idempotent` against the production sink.
///
/// The interesting case for INV-MAYBE-COMMITTED-RESOLVES is **not**
/// "the first call fails before touching CH and the retry succeeds"
/// — that's just normal retry. The load-bearing case is "CH has
/// already committed the row, then the runtime replays the same
/// `SinkCommit`; the duplicate insert must not produce a second
/// visible row." Use
/// `ScriptedWrite::CommitButReportMaybeCommittedThenRetry` so the
/// scripted attempt actually delegates to the inner sink (the row
/// + its `insert_deduplication_token` land in CH), returns
/// `MaybeCommitted` upstream, and then the runtime's
/// `check_committed → retry` path drives a second inner write
/// whose token matches; CH's table-layer dedupe must suppress it.
///
/// Asserts:
///   1. The runtime drains (`last_acked_sequence == n - 1`).
///   2. The captured-writes log records two attempts for seq=2 with
///      byte-identical `CommitIdentity` — the first reporting
///      `Failure(MaybeCommitted)` and the second `Committed`.
///   3. The commit observer fired for seq=2 **at least twice** —
///      once on the inner-side commit of attempt 1, once on the
///      inner-side commit of attempt 2 (CH returns 200 OK on the
///      dedupe-suppressed write).
///   4. Pre-FINAL and post-FINAL row counts both equal
///      `expected_records` — CH's `insert_deduplication_token`
///      suppressed the duplicate insert at INSERT time, so there
///      are no duplicates to merge away.
#[tokio::test]
async fn maybe_committed_replay_idempotent_against_real_clickhouse() {
    let fixture = fresh_fixture("logs_maybe_committed").await;
    let cfg = workload_cfg(
        "phase07/ack-invariants/maybe-committed/manifest",
        "phase07/ack-invariants/maybe-committed/data",
    );
    let env = build_env(&cfg).await.expect("build_env");
    let n: u64 = 5;
    LogWorkload::produce(&cfg, &env.producer, 0, n)
        .await
        .expect("produce");
    let WorkloadEnv {
        producer, source, ..
    } = env;

    let sink = build_real_sink(&fixture);
    sink.set_per_sequence_forced_outcome(2, ScriptedWrite::CommitButReportMaybeCommittedThenRetry);

    // Observer log: one push per inner-side Ok. With the new
    // variant, this fires for seq=2 on BOTH attempts (the scripted
    // first attempt that lies upstream, and the retry that CH
    // dedupe-suppresses).
    let commits_seen: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let observer_log = Arc::clone(&commits_seen);
    let observer: Arc<dyn CommitObserver> = Arc::new(move |id: &CommitIdentity| {
        observer_log.lock().unwrap().push(id.range.high);
    });
    sink.set_commit_observer(observer);

    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(OtlpLogsDecoder::new())
        .set_sink(sink.clone())
        .with_options(pipelined_options())
        .build()
        .expect("build");
    let mut progress = runtime.progress();
    let shutdown = CancellationToken::new();
    let runtime_task = {
        let s = shutdown.clone();
        tokio::spawn(async move { runtime.run(s).await })
    };
    producer.close().await.expect("producer close");

    timeout(Duration::from_secs(60), async {
        loop {
            progress.changed().await.expect("progress channel closed");
            if progress.borrow().last_acked_sequence == Some(n - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should drain");

    shutdown.cancel();
    runtime_task.await.expect("join").expect("clean exit");

    // ── 2. captured-writes log shape ─────────────────────────────
    let writes = sink.drain_captured_writes();
    let for_seq_2: Vec<_> = writes
        .iter()
        .filter(|w| w.identity.range.high == 2)
        .collect();
    assert!(
        for_seq_2.len() >= 2,
        "expected ≥ 2 captured writes for seq=2 (MaybeCommitted attempt + retry Ok); got {} ({writes:?})",
        for_seq_2.len(),
    );
    // First attempt: outer reported MaybeCommitted (despite inner commit).
    assert!(
        matches!(
            for_seq_2[0].outcome,
            WriteOutcome::Failure(SinkCommitFailureKind::MaybeCommitted),
        ),
        "first captured attempt for seq=2 should report MaybeCommitted, got {:?}",
        for_seq_2[0].outcome,
    );
    // Second attempt: outer Ok (inner write suppressed by dedupe).
    assert!(
        matches!(for_seq_2[1].outcome, WriteOutcome::Committed { .. }),
        "second captured attempt for seq=2 should resolve Ok (dedupe suppresses inner duplicate), got {:?}",
        for_seq_2[1].outcome,
    );
    // Identity byte-identical across retries — the runtime must
    // replay with the same `CommitIdentity` so the inner sink's
    // dedupe token recomputes the same value.
    let first = for_seq_2[0].identity.to_string();
    for w in &for_seq_2 {
        assert_eq!(
            w.identity.to_string(),
            first,
            "retry identity must be byte-identical to first attempt",
        );
    }

    // ── 3. commit observer fired on both inner commits ───────────
    let seq_2_commits = commits_seen
        .lock()
        .unwrap()
        .iter()
        .filter(|s| **s == 2)
        .count();
    assert!(
        seq_2_commits >= 2,
        "commit observer should fire ≥ 2 times for seq=2 (inner commit on scripted attempt + retry), got {seq_2_commits}",
    );

    // ── 4. CH dedupe actually suppressed the duplicate ──────────
    let visible = fixture.count_visible().await.expect("count_visible");
    let pre_dupes = fixture
        .count_pre_dedupe_duplicates()
        .await
        .expect("count_pre_dedupe_duplicates");
    let post_dupes = fixture
        .count_post_dedupe_duplicates()
        .await
        .expect("count_post_dedupe_duplicates");
    let expected_records = n * 10;
    assert_eq!(
        visible, expected_records,
        "post-FINAL row count should equal record count — CH dedupe must suppress the retry duplicate",
    );
    assert_eq!(
        pre_dupes, 0,
        "pre-FINAL duplicates must be 0 — CH's insert_deduplication_token suppresses at INSERT, not at merge",
    );
    assert_eq!(post_dupes, 0, "no post-FINAL duplicates");
}

/// `sink_outage_backpressure_bounded` against the production sink.
///
/// Mirrors the hardening that landed for the in-memory test:
/// block seq=2 indefinitely AND assert the byte budget actually
/// reached `max_inflight_bytes` (i.e., admission stalled) before
/// releasing. Without the engagement assertion the test reduces to
/// "memory did not exceed a large bound", which holds trivially
/// when peer commits drain before the budget fills.
///
/// Real-CH-specific tuning. The in-memory test's design assumes
/// `actual == estimated_max_batch_bytes` via `LargeDecoder` so
/// multi-batch accumulation stays within the bound
/// `max + decode_concurrency × (oversize - 1) × estimated`. We
/// can't swap the production OTLP-logs decoder, so we go the other
/// way: keep **only one batch in-flight** via the batch semaphore.
/// Then peak in_flight = single-batch actual post-decode size,
/// which exceeds `max_inflight_bytes` (proving the byte budget
/// would also park admission) and stays under
/// `oversize_fault_multiplier × estimated_max_batch_bytes`
/// (INV-BACKPRESSURE-BOUNDED-MEMORY upper bound for this shape).
///
///   * `records_per_source_range = 100` so each post-decode batch
///     is ~40 KiB.
///   * `max_inflight_batches = 1` — binding constraint that pins
///     "one batch in flight". The byte budget is also engaged
///     (the single batch's 40 KiB > 32 KiB max) but the batch
///     semaphore is what gates admission of seq=3+ while seq=2
///     holds the slot.
///   * `max_inflight_bytes = 32 KiB`, `estimated_max_batch_bytes = 8 KiB`,
///     `oversize_fault_multiplier = 8` (admits ≤ 64 KiB actual).
///   * `max_concurrent_commits = 1` so seq=2's block stalls the
///     entire writer pool.
///   * Produce 10 batches; only 3 ever enter the pipeline
///     (seq=0, 1 commit; seq=2 enters writer and blocks;
///     seq=3+ never acquire the batch slot).
///
/// Asserts:
///   1. `peak >= max_inflight_bytes` — byte budget would also park
///      admission (32 KiB max ≤ 40 KiB actual single-batch
///      reservation; admission's `reserve(estimated=8) ⊕ current=40`
///      fails the `current + bytes ≤ capacity` check).
///   2. `peak <= max_inflight_batches × oversize_fault_multiplier
///      × estimated_max_batch_bytes` — the per-batch over-subscription
///      cap times the batch slot count.
///   3. `budget.in_flight() == 0` after shutdown.
///
/// Asserts:
///   1. `peak >= max_inflight_bytes` — backpressure **did** engage.
///   2. `peak <= bound` (INV-BACKPRESSURE-BOUNDED-MEMORY).
///   3. `budget.in_flight() == 0` after shutdown.
#[tokio::test]
async fn sink_outage_backpressure_bounded_against_real_clickhouse() {
    let fixture = fresh_fixture("logs_sink_outage").await;
    let mut cfg = workload_cfg(
        "phase07/ack-invariants/sink-outage/manifest",
        "phase07/ack-invariants/sink-outage/data",
    );
    // Bigger per-batch payload so the budget saturates cleanly.
    cfg.records_per_source_range = 100;
    let env = build_env(&cfg).await.expect("build_env");
    let n: u64 = 10;
    LogWorkload::produce(&cfg, &env.producer, 0, n)
        .await
        .expect("produce");
    let WorkloadEnv {
        producer, source, ..
    } = env;

    let sink = build_real_sink(&fixture);
    let release_seq_2 = sink.set_per_sequence_block(2);

    let mut opts = pipelined_options();
    opts.source_defaults = SourceBackpressureOptions {
        // Pin one-batch-in-flight so peak in_flight is exactly
        // one post-decode batch's footprint — predictable and
        // tightly bounded by the oversize-fault cap.
        max_inflight_batches: 1,
        max_inflight_bytes: 32 * 1024,
        estimated_max_batch_bytes: 8 * 1024,
        fetch_concurrency: 1,
        decode_concurrency: 1,
        oversize_fault_multiplier: 8,
    };
    opts.sink = SinkPoolOptions {
        max_concurrent_commits: 1,
        retry_max_attempts: 3,
        retry_initial_backoff_ms: 10,
    };
    let bp = opts.source_defaults;
    let max_inflight_bytes = bp.max_inflight_bytes;
    // With max_inflight_batches = 1, peak is bounded by one
    // batch's oversize-fault-capped size (admission rejects any
    // batch whose actual_bytes > oversize × estimated).
    let bound: u64 = (bp.max_inflight_batches as u64)
        * (bp.oversize_fault_multiplier as u64)
        * bp.estimated_max_batch_bytes;

    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(OtlpLogsDecoder::new())
        .set_sink(sink.clone())
        .with_options(opts)
        .build()
        .expect("build");
    let budget = runtime.source_byte_budget();
    let mut progress = runtime.progress();
    let shutdown = CancellationToken::new();
    let runtime_task = {
        let s = shutdown.clone();
        tokio::spawn(async move { runtime.run(s).await })
    };
    producer.close().await.expect("producer close");

    let peak: Arc<std::sync::atomic::AtomicU64> = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let peak_poller = Arc::clone(&peak);
    let budget_for_poller = Arc::clone(&budget);
    let stop_poller = CancellationToken::new();
    let stop_poller_inner = stop_poller.clone();
    // 1 ms sampling — coarser intervals miss the peak window.
    let poller = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = stop_poller_inner.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(1)) => {
                    let inf = budget_for_poller.in_flight();
                    let prev = peak_poller.load(std::sync::atomic::Ordering::SeqCst);
                    if inf > prev {
                        peak_poller.store(inf, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
        }
    });

    // Strict: wait for the peak to reach `max_inflight_bytes`
    // BEFORE releasing seq=2. Without this gate the upper-bound
    // assertion trivially holds when backpressure never engaged
    // (e.g. all writes drain before the budget fills). Same
    // hardening pattern as the in-memory
    // `sink_outage_backpressure_bounded` scenario.
    timeout(Duration::from_secs(30), async {
        loop {
            if peak.load(std::sync::atomic::Ordering::SeqCst) >= max_inflight_bytes {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect(
        "backpressure must engage while seq=2 is held: peak in-flight \
         must reach max_inflight_bytes",
    );

    release_seq_2.cancel();
    stop_poller.cancel();
    let _ = poller.await;

    timeout(Duration::from_secs(60), async {
        loop {
            progress.changed().await.expect("progress channel closed");
            if progress.borrow().last_acked_sequence == Some(n - 1) {
                return;
            }
        }
    })
    .await
    .expect("scenario should drain after release");

    shutdown.cancel();
    runtime_task.await.expect("join").expect("clean exit");

    let final_peak = peak.load(std::sync::atomic::Ordering::SeqCst);
    // 1. INV-BACKPRESSURE-BOUNDED-MEMORY (upper bound).
    assert!(
        final_peak <= bound,
        "peak inflight {final_peak} exceeded bound {bound}",
    );
    // 2. Backpressure DID engage (lower bound).
    assert!(
        final_peak >= max_inflight_bytes,
        "peak inflight {final_peak} below max_inflight_bytes {max_inflight_bytes} — backpressure never engaged",
    );
    // 3. Budget drains to zero.
    assert_eq!(
        budget.in_flight(),
        0,
        "budget should drain to zero after shutdown",
    );
}
