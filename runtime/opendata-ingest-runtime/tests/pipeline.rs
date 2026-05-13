//! Pipeline-specific integration tests landed across rows 6.2 – 6.6.
//!
//! Each test pins one named invariant from
//! `plans/odb-high-throughput/phase06-pipelined-runtime-design.md`
//! rev 8 §Test Plan > Pipeline correctness tests. Row 6.2 lands the
//! first test (admission ordering under parallel fetch); the file
//! grows with each subsequent row.

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use opendata_ingest_runtime::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::runtime::{
    AckFlushPolicy, AckThroughRecorder, AdmissionRecorder, Runtime, RuntimeOptions,
    SinkPoolOptions, SourceBackpressureOptions, TestFetchKillswitch,
};
use opendata_ingest_runtime::source::SourceId;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use support::{FakeDecoder, FakeSink, in_memory_buffer_source, logs_envelope};

fn options_with_fetch_concurrency(fetch_concurrency: u32) -> RuntimeOptions {
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
        max_retry_attempts: 0,
        retry_backoff: Duration::from_millis(0),
        source_defaults: SourceBackpressureOptions {
            fetch_concurrency,
            // Row 6.1's `serial()` profile is too tight under
            // parallelism: max_inflight_batches = 1 forces admission
            // to wait for each commit before issuing the next one.
            // Bump up so the actor can queue ahead of the fetch
            // pool and exercise the structural admission ordering.
            max_inflight_batches: 16,
            ..SourceBackpressureOptions::default()
        },
        source_overrides: Default::default(),
        sink: SinkPoolOptions::default(),
    }
}

/// INV-ADMISSION-CONTIGUOUS under parallel fetch.
///
/// Run 100 source batches through the runtime with
/// `fetch_concurrency = 4` and a synthetic fetch-delay function
/// that injects alternating fast / slow latencies. Even sequences
/// fetch at ~1 ms; odd sequences at ~10 ms. The 10× spread forces
/// fetch completion order to drift relative to admission order
/// (slow-fetching workers will fall behind), but the actor's
/// admission arm runs synchronously between `next_descriptors`
/// and `descriptor_tx.send`, so `register_pending` is called in
/// admission order regardless of downstream completion ordering.
///
/// Assertion: the admission recorder observes `(buffer, 0)`,
/// `(buffer, 1)`, …, `(buffer, 99)` in strict order.
#[tokio::test]
async fn pipeline_register_pending_called_in_admission_order() {
    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/admission-order/manifest",
        "ingest/test/pipeline/admission-order/data",
    )
    .await;
    let batch_count = 100u64;
    for i in 0..batch_count {
        fx.producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        fx.producer.flush().await.expect("flush");
    }

    let sink = FakeSink::new("fake-sink");
    let captured = Arc::clone(&sink.captured);

    let recorder: AdmissionRecorder = Arc::new(Mutex::new(Vec::new()));
    let recorder_runtime = Arc::clone(&recorder);

    // Uneven fetch latency: alternating fast/slow by sequence
    // parity. The spread is large enough that parallel fetch
    // workers reorder completion relative to admission, but the
    // admission arm is structurally serial.
    let delay_fn: Arc<dyn Fn(u64) -> Duration + Send + Sync> = Arc::new(|seq: u64| {
        if seq.is_multiple_of(2) {
            Duration::from_millis(1)
        } else {
            Duration::from_millis(10)
        }
    });

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(options_with_fetch_concurrency(4))
        .with_admission_recorder(recorder_runtime)
        .with_test_fetch_delay(delay_fn)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(30), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.source_ranges_committed >= batch_count {
                return;
            }
        }
    })
    .await
    .expect("runtime did not commit all batches in time");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let events = recorder.lock().unwrap().clone();
    assert_eq!(
        events.len(),
        batch_count as usize,
        "recorder should observe one (source, seq) per source batch",
    );
    let expected: Vec<(SourceId, u64)> = (0..batch_count)
        .map(|i| (SourceId::from("buffer"), i))
        .collect();
    assert_eq!(
        events, expected,
        "INV-ADMISSION-CONTIGUOUS: admission arm must call \
         register_pending in source-sequence order regardless of \
         per-worker fetch latency"
    );

    // Sanity: every batch reached the sink exactly once with the
    // expected range. Captures arrive in fetch-completion order
    // (not source-sequence order) under parallel fetch, so sort
    // before checking range coverage.
    let mut committed = captured.lock().unwrap().clone();
    committed.sort_by_key(|c| c.low_sequence);
    assert_eq!(committed.len(), batch_count as usize);
    for (i, c) in committed.iter().enumerate() {
        assert_eq!(c.low_sequence, i as u64);
        assert_eq!(c.high_sequence, i as u64);
    }

    fx.producer.close().await.expect("close producer");
}

/// Phase 5 ack-correctness invariant under
/// `fetch_concurrency = 8`. The 6.2 acceptance demands every
/// Phase 5 test pass under parallel fetch; this is the smoke
/// version — three batches, NotCommitted/NotCommitted/Ok script,
/// 8 fetch workers. Acks land only after the Ok lands; identity
/// stays byte-identical across retries.
#[tokio::test]
async fn pipeline_ack_correctness_under_fetch_concurrency_8() {
    use opendata_ingest_runtime::sink::CommitStatus;
    use opendata_ingest_runtime::sink::SinkId;
    use support::{ProgrammableSink, ScriptedWrite};

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/concurrent-ack/manifest",
        "ingest/test/pipeline/concurrent-ack/data",
    )
    .await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![
            ScriptedWrite::NotCommitted {
                message: "transient".into(),
            },
            ScriptedWrite::NotCommitted {
                message: "transient".into(),
            },
            ScriptedWrite::Ok { rows_written: 1 },
        ],
        CommitStatus::Unknown,
    );
    let writes = Arc::clone(&sink.write_calls);

    let mut opts = options_with_fetch_concurrency(8);
    opts.max_retry_attempts = 3;

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(10), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            if progress_rx.borrow().last_acked_sequence == Some(0) {
                return;
            }
        }
    })
    .await
    .expect("runtime did not advance ack frontier");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let (write_count, identity_count) = {
        let write_calls = writes.lock().unwrap();
        let count = write_calls.len();
        let identities: std::collections::HashSet<_> =
            write_calls.iter().map(|w| w.identity.clone()).collect();
        (count, identities.len())
    };
    assert_eq!(write_count, 3, "two retries before Ok");
    assert_eq!(
        identity_count, 1,
        "INV-SINK-RETRY-IDEMPOTENT: identity stable across retries",
    );

    fx.producer.close().await.expect("close producer");
}

mod large_records {
    use std::any::Any;
    use std::sync::Arc;

    use async_trait::async_trait;
    use opendata_ingest_runtime::decoded_batch::{
        BatchStats, DecodedBatch, DecodedRecords, SourceCoordinateColumns, TypedRecords,
        TypedSchema,
    };
    use opendata_ingest_runtime::decoder::Decoder;
    use opendata_ingest_runtime::envelope::MetadataEnvelope;
    use opendata_ingest_runtime::error::RuntimeResult;
    use opendata_ingest_runtime::identity::SchemaVersion;
    use opendata_ingest_runtime::source::SourceBatch;

    /// `TypedRecords` impl that reports a controllable
    /// `estimated_bytes`. Used by the byte-budget reconciliation
    /// test (row 6.3) to drive the post-decode total above the
    /// pessimistic admission reservation.
    pub struct LargeRecords {
        schema: TypedSchema,
        count: usize,
        bytes: usize,
    }

    impl LargeRecords {
        pub fn new(count: usize, bytes: usize) -> Self {
            Self {
                schema: TypedSchema {
                    name: "test.large.v1".into(),
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

    /// Decoder that emits one `DecodedBatch` with the configured
    /// `estimated_bytes` reported by `LargeRecords`. Otherwise
    /// identical to `FakeDecoder`.
    pub struct LargeDecoder {
        pub bytes_per_batch: usize,
    }

    #[async_trait]
    impl Decoder for LargeDecoder {
        fn accepts(&self, _envelope: &MetadataEnvelope) -> bool {
            true
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
}

/// Row 6.5 metrics smoke: drives a 10-batch pipeline through the
/// full stage matrix so every metric emission site (queue depth,
/// latency, inflight bytes, ack frontier, pending ranges, sink
/// commits, descriptors handed out, ack lag) runs at least once.
/// We don't snapshot the values — `metrics::with_local_recorder`
/// is thread-local while the workers are spawned via
/// `tokio::spawn`, so a `DebuggingRecorder`-based assertion is
/// brittle across the worker boundary. The benchmark harness in
/// row 6.6 lands a process-level recorder against a real
/// Prometheus endpoint where the named series are visible end to
/// end.
#[tokio::test]
async fn pipeline_metric_emission_smoke_10_batches() {
    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/metrics-smoke/manifest",
        "ingest/test/pipeline/metrics-smoke/data",
    )
    .await;
    for i in 0..10u64 {
        fx.producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        fx.producer.flush().await.expect("flush");
    }

    let sink = FakeSink::new("fake-sink");

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(options_with_fetch_concurrency(4))
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(10), async {
        loop {
            progress_rx.changed().await.expect("progress closed");
            if progress_rx.borrow().source_ranges_committed >= 10 {
                return;
            }
        }
    })
    .await
    .expect("runtime should commit all 10 batches");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    fx.producer.close().await.expect("close producer");
}

/// Row 6.4 `pipeline_graceful_drain_completes_inflight_commits`.
///
/// Run the pipeline against 20 source batches; gate the sink so
/// all 20 admit and queue ahead of the write stage; cancel the
/// external shutdown token while writes are parked; release the
/// sink. Assert: the runtime returns `Ok(())`; every admitted unit
/// produced exactly one `Sink::write` call; the final progress
/// snapshot's `last_acked_sequence` equals the highest committed
/// sequence (19); the durable Buffer ack frontier reflects the
/// same.
#[tokio::test]
async fn pipeline_graceful_drain_completes_inflight_commits() {
    use opendata_ingest_runtime::sink::{CommitStatus, SinkId};
    use support::{ProgrammableSink, ScriptedWrite};

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/graceful-drain/manifest",
        "ingest/test/pipeline/graceful-drain/data",
    )
    .await;
    let batch_count = 20u64;
    for i in 0..batch_count {
        fx.producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        fx.producer.flush().await.expect("flush");
    }

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        (0..batch_count)
            .map(|_| ScriptedWrite::Ok { rows_written: 1 })
            .collect(),
        CommitStatus::Unknown,
    );
    let token = sink.block_until_released(true);
    let write_calls = Arc::clone(&sink.write_calls);

    let recorder: AdmissionRecorder = Arc::new(Mutex::new(Vec::new()));
    let recorder_runtime = Arc::clone(&recorder);

    let mut opts = options_with_fetch_concurrency(4);
    opts.source_defaults = SourceBackpressureOptions {
        max_inflight_batches: batch_count as u32,
        fetch_concurrency: 4,
        decode_concurrency: 2,
        ..SourceBackpressureOptions::default()
    };
    opts.sink = SinkPoolOptions {
        max_concurrent_commits: 4,
        ..SinkPoolOptions::default()
    };

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
        .with_admission_recorder(recorder_runtime)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    // Wait until all 20 batches have admitted (the recorder is
    // updated synchronously in the actor's admission arm — the
    // gated sink keeps everything piled up downstream). Once the
    // recorder is full, the admission stage has finished its
    // work and the in-flight units are stalled on the sink write.
    timeout(Duration::from_secs(10), async {
        loop {
            if recorder.lock().unwrap().len() >= batch_count as usize {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all batches should admit while sink is gated");

    // Cancel admission with units still in-flight. Then release
    // the sink so the in-flight units can drain.
    shutdown.cancel();
    drop(token);

    let result = timeout(Duration::from_secs(15), handle)
        .await
        .expect("runtime should drain in-flight commits and exit Ok")
        .expect("runtime task join");
    result.expect("runtime exited cleanly");

    // Final progress snapshot.
    let p = *progress_rx.borrow_and_update();
    assert_eq!(
        p.source_ranges_committed, batch_count,
        "every admitted unit must finish during graceful drain",
    );
    assert_eq!(
        p.last_acked_sequence,
        Some(batch_count - 1),
        "durable ack frontier should equal the highest committed sequence",
    );

    let write_count = write_calls.lock().unwrap().len();
    assert_eq!(
        write_count, batch_count as usize,
        "every admitted unit produced exactly one Sink::write call",
    );

    fx.producer.close().await.expect("close producer");
}

/// Oversize-fault gate. The decoder produces a `DecodedBatch`
/// whose `estimated_bytes` is well above
/// `estimated_max_batch_bytes × oversize_fault_multiplier`; the
/// decode worker emits `RuntimeError::Pipeline("oversize decoded
/// batch: …")`. Pins the worst-case over-subscription cap from
/// row 6.3 (`SourceBackpressureOptions::oversize_fault_multiplier`).
/// Also asserts the saturating-mul edge case: with both
/// `estimated_max_batch_bytes = u64::MAX` and
/// `oversize_fault_multiplier = u32::MAX` the fault must not fire
/// (effective limit = `u64::MAX`, so any actual size is below).
#[tokio::test]
async fn pipeline_oversize_decoded_batch_halts_runtime() {
    use large_records::LargeDecoder;
    use opendata_ingest_runtime::error::RuntimeError;
    use opendata_ingest_runtime::sink::{CommitStatus, SinkId};
    use support::{ProgrammableSink, ScriptedWrite};

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/oversize/manifest",
        "ingest/test/pipeline/oversize/data",
    )
    .await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    // estimated_max_batch_bytes = 1 MiB, multiplier = 4 → limit 4
    // MiB. Decoder produces ~10 MiB. Fault must fire.
    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![ScriptedWrite::Ok { rows_written: 1 }],
        CommitStatus::Unknown,
    );
    let write_calls = Arc::clone(&sink.write_calls);
    let pessimistic: u64 = 1 << 20; // 1 MiB
    let actual: usize = 10 * (1usize << 20); // ~10 MiB

    let mut opts = options_with_fetch_concurrency(1);
    opts.source_defaults = SourceBackpressureOptions {
        max_inflight_batches: 1,
        max_inflight_bytes: 64 * 1024 * 1024,
        estimated_max_batch_bytes: pessimistic,
        fetch_concurrency: 1,
        decode_concurrency: 1,
        oversize_fault_multiplier: 4,
    };

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(LargeDecoder {
            bytes_per_batch: actual,
        })
        .set_sink(sink)
        .with_options(opts)
        .build()
        .expect("build");

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    let join = timeout(Duration::from_secs(5), handle)
        .await
        .expect("runtime should halt on oversize")
        .expect("runtime task join");
    let err = join.expect_err("oversize batch must halt the runtime");
    let msg = format!("{err}");
    assert!(
        matches!(err, RuntimeError::Pipeline(_)),
        "expected RuntimeError::Pipeline, got {err:?}",
    );
    assert!(
        msg.contains("oversize decoded batch"),
        "error message should call out the oversize fault: {msg}",
    );

    let write_count = write_calls.lock().unwrap().len();
    assert_eq!(
        write_count, 0,
        "oversize fault halts before the sink is touched",
    );

    let _ = shutdown;
    fx.producer.close().await.expect("close producer");
}

/// Saturating-mul edge case of the oversize gate. Setting both
/// `estimated_max_batch_bytes = u64::MAX` and
/// `oversize_fault_multiplier = u32::MAX` must NOT overflow the
/// product (the runtime uses `saturating_mul`), so the effective
/// limit is `u64::MAX` and no batch can trip the fault. Lets an
/// operator disable the fault explicitly without bumping into a
/// hidden overflow.
#[tokio::test]
async fn pipeline_oversize_fault_saturating_mul_disables_gate() {
    use large_records::LargeDecoder;
    use opendata_ingest_runtime::sink::{CommitStatus, SinkId};
    use support::{ProgrammableSink, ScriptedWrite};

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/oversize-saturating/manifest",
        "ingest/test/pipeline/oversize-saturating/data",
    )
    .await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![ScriptedWrite::Ok { rows_written: 1 }],
        CommitStatus::Unknown,
    );

    let mut opts = options_with_fetch_concurrency(1);
    opts.source_defaults = SourceBackpressureOptions {
        max_inflight_batches: 1,
        max_inflight_bytes: u64::MAX,
        estimated_max_batch_bytes: u64::MAX,
        fetch_concurrency: 1,
        decode_concurrency: 1,
        oversize_fault_multiplier: u32::MAX,
    };

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(LargeDecoder {
            bytes_per_batch: 10 * (1usize << 20),
        })
        .set_sink(sink)
        .with_options(opts)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    timeout(Duration::from_secs(5), async {
        loop {
            progress_rx.changed().await.expect("progress closed");
            if progress_rx.borrow().source_ranges_committed >= 1 {
                return;
            }
        }
    })
    .await
    .expect("runtime should commit the batch when the gate is disabled");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    fx.producer.close().await.expect("close producer");
}

/// MEDIUM-1 path: decode-time byte reconciliation. The pessimistic
/// admission reservation is small (`estimated_max_batch_bytes =
/// 4 KiB`); the decoder produces a `DecodedBatch` reporting a much
/// larger post-decode footprint (`1 MiB`). When the runtime parks
/// the sink mid-write, the per-source byte budget's `in_flight()`
/// reflects the reconciled (post-decode) total — proving the
/// decode worker called `reservation.reconcile(actual_bytes)` and
/// that the reservation actually expanded against the shared
/// budget.
#[tokio::test]
async fn pipeline_decode_byte_reconciliation_grows_reservation() {
    use large_records::LargeDecoder;
    use opendata_ingest_runtime::sink::{CommitStatus, SinkId};
    use support::{ProgrammableSink, ScriptedWrite};

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/byte-reconciliation/manifest",
        "ingest/test/pipeline/byte-reconciliation/data",
    )
    .await;
    fx.producer
        .produce(vec![Bytes::from_static(b"payload")], logs_envelope())
        .await
        .expect("produce");
    fx.producer.flush().await.expect("flush");

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        vec![ScriptedWrite::Ok { rows_written: 1 }],
        CommitStatus::Unknown,
    );
    // Gate the write so the test can observe the budget while the
    // reservation is still alive (it drops at WriteCompletion send,
    // which is downstream of write).
    let token = sink.block_until_released(true);

    let pessimistic_bytes: u64 = 4 * 1024;
    let decoded_bytes: usize = 1024 * 1024;
    let mut opts = options_with_fetch_concurrency(1);
    opts.source_defaults = SourceBackpressureOptions {
        max_inflight_batches: 1,
        max_inflight_bytes: 16 * 1024 * 1024,
        estimated_max_batch_bytes: pessimistic_bytes,
        fetch_concurrency: 1,
        decode_concurrency: 1,
        oversize_fault_multiplier: u32::MAX, // disable for this case
    };

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(LargeDecoder {
            bytes_per_batch: decoded_bytes,
        })
        .set_sink(sink)
        .with_options(opts)
        .build()
        .expect("build");
    let budget = runtime.source_byte_budget();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    // Wait until the gated write parks (`maybe_park` increments the
    // entry counter before the await). At that point admission has
    // reserved `pessimistic_bytes`, the decode worker has
    // reconciled to roughly `decoded_bytes`, and the reservation is
    // held by the sink writer task across the parked
    // `Sink::write`.
    timeout(Duration::from_secs(5), token.wait_for_entry())
        .await
        .expect("sink write should park");

    let in_flight = budget.in_flight();
    assert!(
        in_flight > pessimistic_bytes,
        "reservation must have grown past the pessimistic size: \
         in_flight={in_flight}, pessimistic={pessimistic_bytes}",
    );
    // Allow some slack for the source_coords overhead, but the
    // total should be in the ballpark of the decoder's
    // `estimated_bytes()`.
    assert!(
        in_flight >= decoded_bytes as u64,
        "reservation must have reconciled to at least the decoder's \
         estimated_bytes: in_flight={in_flight}, decoded_bytes={decoded_bytes}",
    );

    drop(token); // release the sink write
    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    assert_eq!(
        budget.in_flight(),
        0,
        "reservation must drop back to zero after sink commit",
    );

    fx.producer.close().await.expect("close producer");
}

mod hard_abort_sink {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use opendata_ingest_runtime::error::{RuntimeError, RuntimeResult};
    use opendata_ingest_runtime::identity::CommitIdentity;
    use opendata_ingest_runtime::sink::{
        CommitStatus, Sink, SinkBudget, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId,
    };
    use tokio::sync::Notify;

    /// Custom sink for `pipeline_hard_abort_during_parked_sink_unwinds_pipeline`.
    ///
    /// **Call-order-based**, not sequence-based, so the test is
    /// deterministic under any fetch/decode worker schedule: the
    /// *first* `Sink::write` call (whichever sequence reaches the
    /// sink first) parks on a `Notify` forever; every subsequent
    /// call returns `Fatal`. With W writer workers and ≥ 2 batches
    /// admitted, one writer parks and another fires Fatal —
    /// regardless of which source sequence each picks up.
    pub struct GatePeerSink {
        pub id: SinkId,
        pub parked_gate: Arc<Notify>,
        pub parked_entry_count: Arc<AtomicUsize>,
        pub write_call_count: Arc<AtomicUsize>,
        pub write_calls: Arc<Mutex<Vec<u64>>>,
    }

    impl GatePeerSink {
        pub fn new() -> Self {
            Self {
                id: SinkId::from("gate-peer"),
                parked_gate: Arc::new(Notify::new()),
                parked_entry_count: Arc::new(AtomicUsize::new(0)),
                write_call_count: Arc::new(AtomicUsize::new(0)),
                write_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Sink for GatePeerSink {
        fn id(&self) -> &SinkId {
            &self.id
        }
        fn write_budget(&self) -> SinkBudget {
            SinkBudget::default()
        }
        async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
            let seq = commit.identity.range.high;
            self.write_calls.lock().unwrap().push(seq);
            let call_idx = self.write_call_count.fetch_add(1, Ordering::SeqCst);
            if call_idx == 0 {
                // First call into the sink: park forever. The
                // writer worker awaits this future wrapped in
                // `select!` against the abort token; when the
                // supervisor cancels, the select! drops this
                // future and the sink call unwinds.
                self.parked_entry_count.fetch_add(1, Ordering::SeqCst);
                self.parked_gate.notified().await;
                Err(SinkCommitFailure::NotCommitted(
                    "should never reach here".into(),
                ))
            } else {
                Err(SinkCommitFailure::Fatal(
                    format!("test peer fatal on call {call_idx} (seq={seq})").into(),
                ))
            }
        }
        async fn check_committed(&self, _identity: &CommitIdentity) -> RuntimeResult<CommitStatus> {
            Ok::<CommitStatus, RuntimeError>(CommitStatus::Unknown)
        }
    }
}

/// §1.3c closeout: INV-ACK-CALLED-ON-ADVANCE.
///
/// The per-source actor's completion arm guards `source.ack_through(f)`
/// behind `f > last_ack_sent`, so out-of-order completions that
/// arrive but don't advance the coordinator's frontier MUST NOT
/// produce a redundant `ack_through` call (the buffer-side
/// `Consumer::ack_through` is durable-immediate per RFC 0003;
/// invoking it for a non-advancing frontier would re-fence the
/// manifest for no reason).
///
/// Setup:
/// - Produce 20 batches.
/// - `fetch_concurrency = 4`, `decode_concurrency = 4`,
///   `max_inflight_batches = 20`, `max_concurrent_commits = 4`.
/// - `ProgrammableSink` returns Ok for every write but is gated by
///   `block_until_released(true)`; the test releases the gate
///   after all 20 admissions land, so 4 writer workers race to
///   send their completions to the actor's single-consumer mpsc.
///   That contention forces some completions to arrive out of
///   source order — those non-advancing arrivals exercise the
///   `should_ack == false` branch.
///
/// Assertions:
/// - `recorded` is strictly monotonic (no duplicate frontier).
/// - `recorded.last() == Some(19)` (durable ack frontier reached
///   the highest committed sequence).
/// - `recorded.len() <= 20` (each call is one advance; if any
///   completion didn't advance, the recorder is shorter).
/// - `runtime.progress().last_acked_sequence == Some(19)`.
#[tokio::test]
async fn pipeline_ack_through_called_only_on_frontier_advance() {
    use opendata_ingest_runtime::sink::{CommitStatus, SinkId};
    use support::{ProgrammableSink, ScriptedWrite};

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/ack-on-advance/manifest",
        "ingest/test/pipeline/ack-on-advance/data",
    )
    .await;
    let batch_count = 20u64;
    for i in 0..batch_count {
        fx.producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        fx.producer.flush().await.expect("flush");
    }

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        (0..batch_count)
            .map(|_| ScriptedWrite::Ok { rows_written: 1 })
            .collect(),
        CommitStatus::Unknown,
    );
    let gate = sink.block_until_released(true);

    let admission_recorder: AdmissionRecorder = Arc::new(Mutex::new(Vec::new()));
    let admission_runtime = Arc::clone(&admission_recorder);
    let ack_recorder: AckThroughRecorder = Arc::new(Mutex::new(Vec::new()));
    let ack_runtime = Arc::clone(&ack_recorder);

    let mut opts = options_with_fetch_concurrency(4);
    opts.source_defaults = SourceBackpressureOptions {
        max_inflight_batches: batch_count as u32,
        fetch_concurrency: 4,
        decode_concurrency: 4,
        ..SourceBackpressureOptions::default()
    };
    opts.sink = SinkPoolOptions {
        max_concurrent_commits: 4,
        retry_max_attempts: 0,
        retry_initial_backoff_ms: 0,
    };

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
        .with_admission_recorder(admission_runtime)
        .with_ack_through_recorder(ack_runtime)
        .build()
        .expect("build");
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    // Wait until every batch admits (sink gated, so they queue
    // ahead of the write stage). Releasing the gate after this
    // point fans 4 writers loose to race on completion.
    timeout(Duration::from_secs(10), async {
        loop {
            if admission_recorder.lock().unwrap().len() >= batch_count as usize {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all 20 batches admit while sink gated");

    drop(gate);

    // Wait for full drain — final progress's last_acked_sequence
    // should equal the highest committed sequence.
    timeout(Duration::from_secs(10), async {
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
    .expect("runtime should drain to last_acked_sequence = 19");

    shutdown.cancel();
    handle
        .await
        .expect("runtime task join")
        .expect("runtime exited cleanly");

    let recorded = ack_recorder.lock().unwrap().clone();
    assert!(
        !recorded.is_empty(),
        "ack_through must fire at least once over a 20-batch run",
    );
    for window in recorded.windows(2) {
        let (prev, next) = (window[0], window[1]);
        assert!(
            next > prev,
            "INV-ACK-CALLED-ON-ADVANCE: recorder must be strictly \
             monotonic — saw {prev} followed by {next} in {recorded:?}",
        );
    }
    assert_eq!(
        recorded.last().copied(),
        Some(batch_count - 1),
        "final ack_through must reach the highest committed sequence",
    );
    assert!(
        recorded.len() <= batch_count as usize,
        "recorder length cannot exceed batch count: \
         len={} batch_count={batch_count}",
        recorded.len(),
    );

    fx.producer.close().await.expect("close");
}

/// §1.3a closeout: INV-DESCRIPTOR-LOSS-FATAL.
///
/// When the actor's admission arm calls `descriptor_tx.send` and
/// every fetch worker has dropped its `descriptor_rx` clone, the
/// channel is closed and `send` returns `Err`. The actor must
/// surface this as `RuntimeError::Pipeline("descriptor lost: ...")`
/// rather than hang or swallow the failure.
///
/// Setup:
/// - Produce 8 batches.
/// - `fetch_concurrency = 1`, `max_inflight_batches = 4`, gated
///   `ProgrammableSink` (writes parked).
/// - The actor admits the first 4 sequences synchronously while
///   the sink is gated; in-flight = 4 = max so admission then
///   parks on the budget.
/// - Test trips `TestFetchKillswitch`: the single fetch worker's
///   `select!` fires the killswitch arm, returns `Ok(())`, and
///   drops the only `descriptor_rx` clone.
/// - Test releases the sink. Completions drain; admission unparks
///   and tries to admit seq=4; `descriptor_tx.send` returns Err
///   (channel closed); the actor halts with the typed Pipeline
///   error.
///
/// The killswitch is the test-only path that avoids introducing a
/// `#[cfg(test)]` fetch-worker variant — production code never
/// supplies one, the worker's `select!` arm parks on
/// `std::future::pending` in that case (no behavior change).
#[tokio::test]
async fn pipeline_dropped_descriptor_send_halts_runtime() {
    use opendata_ingest_runtime::error::RuntimeError;
    use opendata_ingest_runtime::sink::{CommitStatus, SinkId};
    use support::{ProgrammableSink, ScriptedWrite};

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/dropped-descriptor/manifest",
        "ingest/test/pipeline/dropped-descriptor/data",
    )
    .await;
    let batch_count = 8u64;
    for i in 0..batch_count {
        fx.producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        fx.producer.flush().await.expect("flush");
    }

    let sink = ProgrammableSink::new(
        SinkId::from("programmable"),
        (0..batch_count)
            .map(|_| ScriptedWrite::Ok { rows_written: 1 })
            .collect(),
        CommitStatus::Unknown,
    );
    let gate = sink.block_until_released(true);

    let killswitch: TestFetchKillswitch = TestFetchKillswitch::new();
    let killswitch_runtime = killswitch.clone();
    let recorder: AdmissionRecorder = Arc::new(Mutex::new(Vec::new()));
    let recorder_runtime = Arc::clone(&recorder);

    let mut opts = options_with_fetch_concurrency(1);
    opts.source_defaults = SourceBackpressureOptions {
        max_inflight_batches: 4,
        fetch_concurrency: 1,
        decode_concurrency: 1,
        ..SourceBackpressureOptions::default()
    };
    opts.sink = SinkPoolOptions {
        max_concurrent_commits: 4,
        retry_max_attempts: 0,
        retry_initial_backoff_ms: 0,
    };

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
        .with_admission_recorder(recorder_runtime)
        .with_test_fetch_killswitch(killswitch_runtime)
        .build()
        .expect("build");

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    // Wait until in-flight hits the max (4) — fetch worker has
    // drained the descriptor channel into the parked sink writes
    // and is now parked on `descriptor_rx.recv` with the channel
    // empty.
    timeout(Duration::from_secs(5), async {
        loop {
            if recorder.lock().unwrap().len() >= 4 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("4 admissions while sink gated");

    // Trip the killswitch. The fetch worker's select! fires the
    // killswitch arm at its next suspension (already parked on
    // recv) and exits, dropping its descriptor_rx clone.
    killswitch.cancel();

    // Give the scheduler a moment to run the fetch worker to its
    // killswitch-arm exit BEFORE we release the sink. Without this
    // step the actor could observe a sink completion, attempt to
    // admit seq=4, and succeed (the channel still has a live
    // receiver) — that's not the failure mode under test.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Release the gated sink writes. Completions drain into the
    // actor; in_flight decrements; admission unblocks and tries to
    // admit seq=4 — descriptor_tx.send fails (zero receivers).
    drop(gate);

    let join = timeout(Duration::from_secs(5), handle)
        .await
        .expect("runtime must halt within 5s after killswitch + sink release")
        .expect("runtime task join");
    let err = join.expect_err("runtime must return Pipeline(descriptor lost)");
    let msg = format!("{err}");
    assert!(
        matches!(err, RuntimeError::Pipeline(_)),
        "expected RuntimeError::Pipeline, got {err:?}: {msg}",
    );
    assert!(
        msg.contains("descriptor lost"),
        "expected descriptor-lost message, got: {msg}",
    );

    let _ = shutdown;
    fx.producer.close().await.expect("close");
}

/// HIGH finding: hard abort must propagate through parked workers.
///
/// Two batches admitted; the sink gates by **call order**, not
/// sequence — the first `Sink::write` call parks on a `Notify`
/// forever, every subsequent call returns `Fatal`. With multiple
/// writer workers, one parks and another fires Fatal regardless of
/// which source sequence reached the sink first (eliminating the
/// schedule-sensitive seq-0-before-seq-1 ordering assumption).
/// `Fatal` → actor returns `Err(RuntimeError::Sink)` → supervisor
/// cancels `hard_abort_token` → the parked worker's `sink.write`
/// future is dropped by the `select!` arm and unwinds within
/// bounded time. The runtime must exit with the typed `Sink` error
/// (not a generic `Pipeline("hard abort …")`) and the exit must
/// happen quickly (under 2 seconds) — proving the parked worker
/// doesn't block supervisor join.
#[tokio::test]
async fn pipeline_hard_abort_during_parked_sink_unwinds_pipeline() {
    use hard_abort_sink::GatePeerSink;
    use opendata_ingest_runtime::error::RuntimeError;

    let fx = in_memory_buffer_source(
        "ingest/test/pipeline/hard-abort/manifest",
        "ingest/test/pipeline/hard-abort/data",
    )
    .await;
    for i in 0..2u64 {
        fx.producer
            .produce(
                vec![Bytes::from(format!("payload-{i}").into_bytes())],
                logs_envelope(),
            )
            .await
            .expect("produce");
        fx.producer.flush().await.expect("flush");
    }

    let sink = GatePeerSink::new();
    let parked_entry_count = Arc::clone(&sink.parked_entry_count);
    let write_call_count = Arc::clone(&sink.write_call_count);
    let write_calls = Arc::clone(&sink.write_calls);

    // Use single fetch + decode concurrency so the actor admits in
    // strict source-sequence order and the first `Sink::write` is
    // unambiguously seq=0. Keep `W=2` writer workers so the second
    // batch can be processed in parallel to the parked one —
    // otherwise the test would only exercise the recv-loop abort
    // path, not the parked-`sink.write` abort path.
    let mut opts = options_with_fetch_concurrency(1);
    opts.source_defaults = SourceBackpressureOptions {
        max_inflight_batches: 4,
        fetch_concurrency: 1,
        decode_concurrency: 1,
        ..SourceBackpressureOptions::default()
    };
    opts.sink = SinkPoolOptions {
        max_concurrent_commits: 2,
        retry_max_attempts: 0,
        retry_initial_backoff_ms: 0,
    };

    let runtime = Runtime::builder()
        .add_source(fx.source)
        .add_decoder(FakeDecoder::permissive())
        .set_sink(sink)
        .with_options(opts)
        .build()
        .expect("build");

    let shutdown = CancellationToken::new();
    let shutdown_run = shutdown.clone();
    let start = std::time::Instant::now();
    let handle = tokio::spawn(async move { runtime.run(shutdown_run).await });

    // Wait until the first `Sink::write` call has actually parked
    // before declaring victory on the setup. The parked_entry_count
    // is incremented inside the gate, after the call lands. A
    // higher write_call_count without the parked counter advancing
    // would mean we're in trouble (call entered but isn't the
    // gated one).
    let parked_wait_start = std::time::Instant::now();
    while parked_entry_count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        if parked_wait_start.elapsed() > Duration::from_secs(2) {
            panic!(
                "first sink.write call never entered the parked state; \
                 write_call_count={}, write_calls={:?}",
                write_call_count.load(std::sync::atomic::Ordering::SeqCst),
                write_calls.lock().unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Now the parked worker is in `Sink::write(seq=0).await`. A peer
    // worker should pick up seq=1 and return Fatal soon after; the
    // supervisor cancels the abort token; the parked worker exits.
    let join = timeout(Duration::from_secs(2), handle)
        .await
        .expect(
            "runtime must exit within 2s despite the parked sink \
             — proving hard_abort wakes the parked Sink::write",
        )
        .expect("runtime task join");
    let elapsed = start.elapsed();

    let err = join.expect_err("Fatal from peer must surface as runtime error");
    assert!(
        matches!(err, RuntimeError::Sink(_)),
        "expected typed RuntimeError::Sink (the original Fatal), got {err:?} \
         after {elapsed:?}",
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "runtime should exit promptly after Fatal; took {elapsed:?}",
    );

    let _ = shutdown;
    fx.producer.close().await.expect("close producer");
}
