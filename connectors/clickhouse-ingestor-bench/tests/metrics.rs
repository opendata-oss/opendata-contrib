//! Phase 6.x §1.2 closeout: process-level metrics integration
//! test. Drives a 10-batch end-to-end pipeline through the bench
//! crate's fixtures and asserts that every named series declared
//! in `opendata_ingest_runtime::metrics` has at least one sample
//! in the `DebuggingRecorder` snapshot. Additionally pins the
//! four `runtime_stage_inflight_bytes{stage=...}` label values
//! land in §1.4.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use clickhouse_ingestor_bench::fixtures::{BenchSink, FakeDecoder, LatencyFn, logs_envelope};
use clickhouse_ingestor_bench::metrics_recorder::init_metrics_recorder;
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use metrics_util::debugging::DebugValue;
use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::metrics as runtime_metrics;
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, Runtime, RuntimeOptions, SinkPoolOptions, SourceBackpressureOptions,
};
use opendata_ingest_runtime::source::BufferSource;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn options(retry_initial_backoff_ms: u64) -> RuntimeOptions {
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
        retry_backoff: Duration::from_millis(retry_initial_backoff_ms),
        source_defaults: SourceBackpressureOptions {
            fetch_concurrency: 2,
            decode_concurrency: 2,
            max_inflight_batches: 16,
            ..SourceBackpressureOptions::default()
        },
        source_overrides: Default::default(),
        sink: SinkPoolOptions {
            max_concurrent_commits: 4,
            retry_max_attempts: 3,
            retry_initial_backoff_ms,
        },
    }
}

/// Build a BufferSource with a unique `SourceId` per call. Tests
/// share a single process-wide global recorder; using unique
/// source IDs avoids cross-test interference when integration
/// tests run in parallel.
async fn unique_source(test_tag: &str) -> (buffer::Producer, BufferSource) {
    use std::sync::Arc;
    let store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let manifest_path = format!("ingest/bench-metrics/{test_tag}/manifest");
    let data_prefix = format!("ingest/bench-metrics/{test_tag}/data");
    let producer_config = buffer::ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.clone(),
        manifest_path: manifest_path.clone(),
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
        manifest_path: manifest_path.clone(),
        data_path_prefix: data_prefix.clone(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer = buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None)
        .await
        .expect("consumer");
    let source_id = format!("buffer-{test_tag}");
    let source = BufferSource::new(consumer, source_id, manifest_path, None);
    (producer, source)
}

async fn produce_n(producer: &buffer::Producer, n: u64) {
    use bytes::Bytes;
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

async fn drive_pipeline(
    test_tag: &str,
    batch_count: u64,
    sink: BenchSink,
    retry_initial_backoff_ms: u64,
) -> String {
    let (producer, source) = unique_source(test_tag).await;
    let source_label = source.id().0.clone();
    produce_n(&producer, batch_count).await;

    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(FakeDecoder)
        .set_sink(sink)
        .with_options(options(retry_initial_backoff_ms))
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
    .expect("pipeline should drain");

    shutdown.cancel();
    handle.await.expect("join").expect("clean exit");
    source_label
}

/// Sanity that the global recorder + snapshotter wiring is
/// correct: 10 macro-style increments accumulate to 10. (The
/// crate's workspace dep on `metrics-util 0.20` matters here —
/// 0.19 had a bug where re-resolved `counter!()` calls did not
/// share the underlying atomic.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_recorder_increments_accumulate_across_macro_reresolves() {
    let snapshotter = init_metrics_recorder();
    for _ in 0..10 {
        metrics::counter!("sanity_counter", "k" => "v").increment(1);
    }
    let snap = snapshotter.snapshot().into_vec();
    let v = snap
        .iter()
        .find(|(k, _, _, _)| k.key().name() == "sanity_counter")
        .and_then(|(_, _, _, dv)| match dv {
            DebugValue::Counter(c) => Some(*c),
            _ => None,
        });
    assert_eq!(
        v,
        Some(10),
        "10 macro increments should accumulate; got {v:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_recorder_captures_all_named_series_after_10_batches() {
    let snapshotter = init_metrics_recorder();

    let sink = BenchSink::new("metrics-bench-sink-named-series");
    let source_label = drive_pipeline("named-series", 10, sink, 2).await;

    let snapshot_vec = snapshotter.snapshot().into_vec();

    // Filter the snapshot to entries this test produced. Tests
    // share one process-wide recorder; the unique source label
    // (`buffer-named-series`) keeps assertions hermetic against
    // parallel test execution.
    let our_entries: Vec<_> = snapshot_vec
        .iter()
        .filter(|(k, _, _, _)| {
            k.key()
                .labels()
                .any(|l| l.key() == "source" && l.value() == source_label)
                || k.key()
                    .labels()
                    .any(|l| l.key() == "sink" && l.value() == "metrics-bench-sink-named-series")
        })
        .collect();

    let names: HashSet<&str> = our_entries
        .iter()
        .map(|(k, _, _, _)| k.key().name())
        .collect();

    // Every constant declared in `opendata_ingest_runtime::metrics`
    // (modulo BACKPRESSURE_REASON which only fires under retry
    // paths — covered by the second test) must have been emitted
    // at least once by a 10-batch happy-path run.
    let required = [
        runtime_metrics::STAGE_QUEUE_DEPTH,
        runtime_metrics::STAGE_INFLIGHT_BYTES,
        runtime_metrics::STAGE_LATENCY_SECONDS,
        runtime_metrics::ACK_FRONTIER,
        runtime_metrics::PENDING_RANGES,
        runtime_metrics::SINK_QUEUE_DEPTH,
        runtime_metrics::SINK_INFLIGHT_BYTES,
        runtime_metrics::SINK_COMMITS_TOTAL,
        runtime_metrics::DESCRIPTORS_HANDED_OUT_TOTAL,
        runtime_metrics::ACK_LAG_SECONDS,
    ];
    for name in &required {
        assert!(
            names.contains(name),
            "expected series {name} for source={source_label} in snapshot; saw {names:?}",
        );
    }

    // §1.4 closeout: per-stage breakdown of
    // `runtime_stage_inflight_bytes`. All four stage labels must
    // have been emitted at least once during a 10-batch run.
    let mut seen_stages: HashSet<String> = HashSet::new();
    for (key, _unit, _desc, _value) in &our_entries {
        if key.key().name() == runtime_metrics::STAGE_INFLIGHT_BYTES {
            for label in key.key().labels() {
                if label.key() == "stage" {
                    seen_stages.insert(label.value().to_string());
                }
            }
        }
    }
    for stage in ["source", "fetch", "decode", "sink_dispatch"] {
        assert!(
            seen_stages.contains(stage),
            "expected stage={stage} in runtime_stage_inflight_bytes \
             for source={source_label}; saw {seen_stages:?}",
        );
    }

    // `runtime_descriptors_handed_out_total{source}` should equal
    // the produced batch count exactly. Hermetic on the unique
    // source label.
    let descriptors_total: Option<u64> = our_entries
        .iter()
        .find(|(k, _, _, _)| k.key().name() == runtime_metrics::DESCRIPTORS_HANDED_OUT_TOTAL)
        .and_then(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => Some(*c),
            _ => None,
        });
    assert_eq!(
        descriptors_total,
        Some(10),
        "descriptors_handed_out_total for source={source_label} \
         should equal the 10 produced batches; saw {descriptors_total:?}",
    );

    // Review fix-up MEDIUM: after the pipeline fully drains, the
    // sink-inflight gauge must read zero. Previously the writer
    // worker only set the gauge when an envelope arrived, leaving
    // the last non-zero value sticky in the snapshot — a
    // post-drain reader would see stale data. The writer now
    // re-emits the gauge after every reservation drop; the
    // process-final value reads the post-drain
    // `stage_bytes.sink_dispatch` (which is 0).
    let sink_inflight_final: Option<f64> = snapshot_vec
        .iter()
        .find(|(k, _, _, _)| {
            k.key().name() == runtime_metrics::SINK_INFLIGHT_BYTES
                && k.key()
                    .labels()
                    .any(|l| l.key() == "sink" && l.value() == "metrics-bench-sink-named-series")
        })
        .and_then(|(_, _, _, value)| match value {
            DebugValue::Gauge(g) => Some(g.into_inner()),
            _ => None,
        });
    assert_eq!(
        sink_inflight_final,
        Some(0.0),
        "runtime_sink_inflight_bytes{{sink=metrics-bench-sink-named-series}} \
         must read 0 after the pipeline drains; saw {sink_inflight_final:?}",
    );
}

/// §1.4 / §1.2 sanity: forcing a sink-side retry produces a
/// `runtime_backpressure_reason{reason="retrying"}` counter
/// increment. The 10 ms retry backoff in the test config exceeds
/// the 10 ms gating threshold by construction (the helper's
/// `tokio::select!` re-polls the future every ~10 ms; the
/// counter increments at most once per call).
/// §1.4 sanity: forcing a sink-side retry with a backoff above
/// the 10 ms gating threshold produces a
/// `runtime_backpressure_reason{reason="retrying",source}`
/// counter increment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backpressure_reason_fires_on_retrying_sleep() {
    let snapshotter = init_metrics_recorder();

    // BenchSink with MaybeCommitted-then-Ok script on seq=0
    // forces the runtime through the retry sleep path. The
    // 50 ms backoff is well above the 10 ms gating threshold
    // so the `with_backpressure_timer` arm fires reliably.
    let sink = BenchSink::new("retrying-bench-sink");
    use clickhouse_ingestor_bench::test_observable_sink::{ScriptedWrite, TestObservableSink};
    sink.set_per_sequence_forced_outcome(0, ScriptedWrite::MaybeCommittedThenOk);
    let latency: LatencyFn = Arc::new(|seq: u64| {
        if seq == 0 {
            Some(Duration::from_millis(30))
        } else {
            None
        }
    });
    sink.set_latency_fn(latency);

    let source_label = drive_pipeline("retrying", 3, sink, 50).await;

    let snapshot_vec = snapshotter.snapshot().into_vec();
    let retrying_count: Option<u64> = snapshot_vec
        .iter()
        .find(|(k, _, _, _)| {
            k.key().name() == runtime_metrics::BACKPRESSURE_REASON
                && k.key()
                    .labels()
                    .any(|l| l.key() == "reason" && l.value() == "retrying")
                && k.key()
                    .labels()
                    .any(|l| l.key() == "source" && l.value() == source_label)
        })
        .and_then(|(_, _, _, v)| match v {
            DebugValue::Counter(c) => Some(*c),
            _ => None,
        });

    assert!(
        matches!(retrying_count, Some(n) if n >= 1),
        "BACKPRESSURE_REASON{{reason=retrying, source={source_label}}} \
         should have incremented at least once over a forced-retry run; \
         saw {retrying_count:?}",
    );
}
