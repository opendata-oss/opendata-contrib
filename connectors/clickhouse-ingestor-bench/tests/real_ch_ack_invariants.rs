//! Phase 7.2c — the 4 named `ack_invariant_checks` exercised
//! against the production `ClickHouseSink<OtlpLogsClickHouseAdapter>`
//! end-to-end via the `TestObservableSink` trait. Docker-gated.
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
//! deferred — the runtime is single-source today (multi-source
//! `RuntimeBuilder` promotion lands in Phase 10 per the impl plan).

#![cfg(feature = "real-ch")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use clickhouse_ingestor::writer::{ClickHouseWriter, WriterConfig};
use clickhouse_ingestor_bench::real_ch::{
    LogWorkload, LogWorkloadConfig, RealClickHouseFixture, RealClickHouseSink,
    workload::{WorkloadEnv, build_env},
};
use clickhouse_ingestor_bench::test_observable_sink::{
    CommitObserver, ScriptedWrite, TestObservableSink,
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
/// Script seq=2 to return `MaybeCommitted` on the first call; the
/// runtime's `check_committed → retry` path delegates the second
/// call to the inner sink. Assert: (a) the runtime drains; (b) the
/// captured-writes log records two attempts for seq=2 with
/// byte-identical `CommitIdentity`; (c) post-FINAL CH row count
/// equals the total record count (no duplicates after dedupe).
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
    sink.set_per_sequence_forced_outcome(2, ScriptedWrite::MaybeCommittedThenOk);

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

    let writes = sink.drain_captured_writes();
    let for_seq_2: Vec<_> = writes
        .iter()
        .filter(|w| w.identity.range.high == 2)
        .collect();
    assert!(
        for_seq_2.len() >= 2,
        "expected ≥ 2 captured writes for seq=2 (one MaybeCommitted + one Ok on retry); got {} ({writes:?})",
        for_seq_2.len(),
    );
    let first = for_seq_2[0].identity.to_string();
    for w in &for_seq_2 {
        assert_eq!(
            w.identity.to_string(),
            first,
            "retry identity must be byte-identical to first attempt",
        );
    }

    let visible = fixture.count_visible().await.expect("count_visible");
    let post_dupes = fixture
        .count_post_dedupe_duplicates()
        .await
        .expect("count_post_dedupe_duplicates");
    let expected_records = n * 10;
    assert_eq!(
        visible, expected_records,
        "post-FINAL row count should equal record count",
    );
    assert_eq!(post_dupes, 0, "no post-FINAL duplicates");
}

/// `sink_outage_backpressure_bounded` against the production sink.
/// Block seq=2 indefinitely; peer commits land at the sink, the
/// runtime's in-flight queue fills, and admission engages
/// backpressure on subsequent descriptors. Assert: (a) peak
/// in-flight bytes stays under the configured bound; (b) the
/// budget drains to zero after release + shutdown.
#[tokio::test]
async fn sink_outage_backpressure_bounded_against_real_clickhouse() {
    let fixture = fresh_fixture("logs_sink_outage").await;
    let cfg = workload_cfg(
        "phase07/ack-invariants/sink-outage/manifest",
        "phase07/ack-invariants/sink-outage/data",
    );
    let env = build_env(&cfg).await.expect("build_env");
    let n: u64 = 30;
    LogWorkload::produce(&cfg, &env.producer, 0, n)
        .await
        .expect("produce");
    let WorkloadEnv {
        producer, source, ..
    } = env;

    let sink = build_real_sink(&fixture);
    let release_seq_2 = sink.set_per_sequence_block(2);

    let opts = pipelined_options();
    let max_bytes = opts.source_defaults.max_inflight_bytes;
    let est_max_bytes = opts.source_defaults.estimated_max_batch_bytes;
    let decode_conc = opts.source_defaults.decode_concurrency;
    let oversize = opts.source_defaults.oversize_fault_multiplier;

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

    let peak: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let peak_clone = Arc::clone(&peak);
    let budget_for_poller = Arc::clone(&budget);
    let stop_poller = CancellationToken::new();
    let stop_poller_inner = stop_poller.clone();
    let poller = tokio::spawn(async move {
        loop {
            if stop_poller_inner.is_cancelled() {
                return;
            }
            let inf = budget_for_poller.in_flight();
            {
                let mut p = peak_clone.lock().unwrap();
                if inf > *p {
                    *p = inf;
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    // Hold the block long enough for the runtime to push peer
    // commits through and engage admission backpressure.
    tokio::time::sleep(Duration::from_millis(500)).await;
    release_seq_2.cancel();
    stop_poller.cancel();
    poller.await.expect("poller");

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

    let final_peak = *peak.lock().unwrap();
    // INV-BACKPRESSURE-BOUNDED-MEMORY (Phase 6 design): peak ≤
    // max_inflight_bytes + decode_concurrency × (oversize - 1) ×
    // estimated_max_batch_bytes — accounts for the post-decode
    // reconciliation window where reservation > actual is possible.
    let bound =
        max_bytes + (decode_conc as u64) * (oversize.saturating_sub(1) as u64) * est_max_bytes;
    assert!(
        final_peak <= bound,
        "peak inflight {final_peak} exceeded bound {bound}",
    );
    assert_eq!(
        budget.in_flight(),
        0,
        "budget should drain to zero after shutdown",
    );
}
