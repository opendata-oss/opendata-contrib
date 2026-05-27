//! Process-level metrics integration test. Drives a 10-batch
//! end-to-end pipeline through the bench
//! crate's fixtures and asserts that every named series declared
//! in `opendata_ingest_runtime::metrics` has at least one sample
//! in the `DebuggingRecorder` snapshot. Additionally pins the
//! four `runtime_stage_inflight_bytes{stage=...}` label values.

use std::sync::Arc;
use std::time::Duration;

use clickhouse_ingestor_bench::fixtures::{BenchSink, FakeDecoder, LatencyFn, logs_envelope};
use clickhouse_ingestor_bench::metrics_recorder::init_metrics_recorder;
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use metrics_util::debugging::DebugValue;
use opendata_ingest_runtime::metrics::{
    RuntimeMetrics, SinkLabels, SourceLabels, SourceReasonLabels, StageLabels,
};
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, Runtime, RuntimeOptions, SinkPoolOptions, SourceBackpressureOptions,
};
use opendata_ingest_runtime::source::BufferSource;
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn options(retry_initial_backoff_ms: u64) -> RuntimeOptions {
    RuntimeOptions {
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
) -> (String, Arc<RuntimeMetrics>) {
    let (producer, source) = unique_source(test_tag).await;
    let source_label = source.id().0.clone();
    produce_n(&producer, batch_count).await;

    // Each test owns its own `RuntimeMetrics` and inspects the typed
    // Family fields directly after the pipeline drains. No shared
    // global recorder needed.
    let metrics = Arc::new(RuntimeMetrics::new());
    let runtime = Runtime::builder()
        .add_source(source)
        .add_decoder(FakeDecoder)
        .set_sink(sink)
        .with_options(options(retry_initial_backoff_ms))
        .with_runtime_metrics(Arc::clone(&metrics))
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
    (source_label, metrics)
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
    let sink_id = "metrics-bench-sink-named-series";
    let sink = BenchSink::new(sink_id);
    let (source_label, metrics) = drive_pipeline("named-series", 10, sink, 2).await;

    let source = SourceLabels {
        source: source_label.clone(),
    };
    let sink_labels = SinkLabels {
        sink: sink_id.to_string(),
    };

    // Direct-typed assertions on counters + gauges (those expose
    // `.get()`). Histogram counts go through `encode` since the
    // `prometheus-client` Histogram doesn't have a public count
    // accessor.
    let descriptors_total = metrics.descriptors_handed_out.get_or_create(&source).get();
    assert_eq!(
        descriptors_total, 10,
        "descriptors_handed_out_total for source={source_label} \
         should equal the 10 produced batches; saw {descriptors_total}",
    );

    assert!(
        metrics.bytes_fetched.get_or_create(&source).get() > 0,
        "bytes_fetched should be > 0 for source={source_label}",
    );
    assert!(
        metrics.records_decoded.get_or_create(&source).get() > 0,
        "records_decoded should be > 0 for source={source_label}",
    );

    for stage in ["source", "fetch", "decode", "sink_dispatch"] {
        let labels = StageLabels {
            stage: stage.to_string(),
            source: source_label.clone(),
        };
        let inflight_total = metrics.stage_inflight_bytes.get_or_create(&labels).get();
        assert_eq!(
            inflight_total, 0,
            "stage_inflight_bytes{{stage={stage}}} should read 0 post-drain; saw {inflight_total}",
        );
    }

    let sink_inflight_final = metrics
        .sink_inflight_bytes
        .get_or_create(&sink_labels)
        .get();
    assert_eq!(
        sink_inflight_final, 0,
        "runtime_sink_inflight_bytes{{sink={sink_id}}} \
         must read 0 after the pipeline drains; saw {sink_inflight_final}",
    );
    assert_eq!(
        metrics.sink_queue_depth.get_or_create(&sink_labels).get(),
        0,
        "sink_queue_depth must read 0 post-drain",
    );

    let ack_frontier = metrics.ack_frontier.get_or_create(&source).get();
    assert!(
        ack_frontier >= 9,
        "ack_frontier should reflect the last acked sequence (≥9); saw {ack_frontier}",
    );
    assert_eq!(
        metrics.pending_ranges.get_or_create(&source).get(),
        0,
        "pending_ranges must drain to 0 post-pipeline",
    );

    // Histogram + sink-commit-counter assertions go through `encode`:
    // we render the Registry to Prometheus text and assert each
    // expected `_bucket{le="+Inf"} N` series has N >= 1 for every
    // per-stage histogram, plus the sink_commits_total series shows
    // the expected committed count.
    let mut registry = Registry::default();
    metrics.register(&mut registry);
    let mut rendered = String::new();
    encode(&mut rendered, &registry).expect("encode");

    for stage in ["source", "fetch", "decode", "sink_dispatch"] {
        let expected = format!(
            "runtime_stage_latency_seconds_count{{stage=\"{stage}\",source=\"{source_label}\"}}"
        );
        let line = rendered
            .lines()
            .find(|l| l.starts_with(&expected))
            .unwrap_or_else(|| {
                panic!(
                    "stage_latency_seconds{{stage={stage}}} _count line missing; \
                     rendered output was:\n{rendered}"
                )
            });
        let count: u64 = line
            .rsplit(' ')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("could not parse trailing count from {line:?}"));
        assert!(
            count > 0,
            "stage_latency_seconds{{stage={stage}}} should have observations; \
             saw count={count}",
        );
    }

    let ack_count_line = format!("runtime_ack_lag_seconds_count{{source=\"{source_label}\"}}");
    let ack_count: u64 = rendered
        .lines()
        .find(|l| l.starts_with(&ack_count_line))
        .and_then(|l| l.rsplit(' ').next().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("ack_lag_seconds _count missing; rendered:\n{rendered}"));
    assert!(
        ack_count > 0,
        "ack_lag_seconds should have at least one observation; saw count={ack_count}",
    );

    let sink_commits_committed = format!(
        "runtime_sink_commits_total{{source=\"{source_label}\",sink=\"{sink_id}\",result=\"committed\"}}"
    );
    let sink_commits_count: u64 = rendered
        .lines()
        .find(|l| l.starts_with(&sink_commits_committed))
        .and_then(|l| l.rsplit(' ').next().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| {
            panic!("sink_commits_total{{result=committed}} missing; rendered:\n{rendered}")
        });
    assert_eq!(
        sink_commits_count, 10,
        "sink_commits_total{{result=committed}} should equal the 10 produced batches; \
         saw {sink_commits_count}",
    );
}

/// Sanity: forcing a sink-side retry produces a
/// `runtime_backpressure_reason{reason="retrying"}` counter
/// increment. The 10 ms retry backoff in the test config exceeds
/// the 10 ms gating threshold by construction (the helper's
/// `tokio::select!` re-polls the future every ~10 ms; the
/// counter increments at most once per call).
/// Sanity: forcing a sink-side retry with a backoff above
/// the 10 ms gating threshold produces a
/// `runtime_backpressure_reason{reason="retrying",source}`
/// counter increment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backpressure_reason_fires_on_retrying_sleep() {
    // Backpressure-reason counter now lives in RuntimeMetrics rather
    // than the metrics-rs global recorder; the snapshotter remains
    // installed for the other test in this binary but isn't read here.
    let _snapshotter = init_metrics_recorder();

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

    let (source_label, metrics) = drive_pipeline("retrying", 3, sink, 50).await;

    let retrying_count = metrics
        .backpressure_reason
        .get_or_create(&SourceReasonLabels {
            source: source_label.clone(),
            reason: "retrying".to_string(),
        })
        .get();

    assert!(
        retrying_count >= 1,
        "BACKPRESSURE_REASON{{reason=retrying, source={source_label}}} \
         should have incremented at least once over a forced-retry run; \
         saw {retrying_count}",
    );
}
