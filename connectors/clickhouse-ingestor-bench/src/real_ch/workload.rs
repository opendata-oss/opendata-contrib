//! OTLP-logs workload generator for the real-ClickHouse
//! bench. Produces real `ExportLogsServiceRequest` payloads into an
//! in-memory Buffer via the production `Producer`, with one OTLP-logs
//! payload per Buffer manifest entry = one source range in the
//! runtime = one `SinkCommit` at the sink.
//!
//! Per the design's §Bench harness public types: one OTLP-logs
//! payload carries `records_per_source_range` log records; chunk
//! boundaries land at the sink based on `(records,
//! max_chunk_rows, max_chunk_bytes)`. This shape is what makes
//! the matrix sweep over `records_per_source_range` and
//! `max_chunk_rows` meaningful — the workload axis has to actually
//! cross the chunk thresholds for the sweep to be interesting.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use opendata_ingest_runtime::source::{BufferSource, SourceId};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;

use super::fixture::FixtureError;

#[derive(Debug, Clone)]
pub struct LogWorkloadConfig {
    /// `SourceId` the runtime registers the in-memory Buffer
    /// against.
    pub source_id: SourceId,
    /// Records per OTLP-logs payload = records per source range.
    /// Drives the realized chunk count per `Sink::write`.
    pub records_per_source_range: usize,
    /// Number of warmup payloads (produced *before* the timed
    /// window starts; drained to `last_acked_sequence ==
    /// warmup_highest_sequence` per §Iteration Protocol).
    pub warmup_payloads: usize,
    /// Number of timed payloads.
    pub timed_payloads: usize,
    /// `service.name` resource attribute on every payload.
    pub service_name: String,
    /// Buffer manifest path (kept stable across iterations only if
    /// the caller wants warm reuse; the bench's runner gives each
    /// iteration its own).
    pub manifest_path: String,
    /// Buffer data prefix.
    pub data_prefix: String,
}

impl Default for LogWorkloadConfig {
    fn default() -> Self {
        Self {
            source_id: SourceId::from("phase07-bench"),
            records_per_source_range: 100,
            warmup_payloads: 20,
            timed_payloads: 200,
            service_name: "phase07-bench".to_string(),
            manifest_path: "phase07/real-ch/manifest".to_string(),
            data_prefix: "phase07/real-ch/data".to_string(),
        }
    }
}

/// Returned by `LogWorkload::prefill`. The runtime drives the
/// bench's drain loops keyed on these sequence numbers.
#[derive(Debug, Clone, Copy)]
pub struct WorkloadHandle {
    pub warmup_highest_sequence: u64,
    pub highest_sequence: u64,
    /// Total records (warmup + timed) — the bench uses this for
    /// the post-drain `count_visible` correctness check.
    pub total_records: u64,
    /// Records produced in the timed window only — drives the
    /// throughput scalar.
    pub timed_records: u64,
}

/// In-memory Buffer + `BufferSource` paired together. The runner
/// builds one of these per iteration so per-iteration state stays
/// isolated.
pub struct WorkloadEnv {
    pub store: Arc<dyn ObjectStore>,
    pub producer: buffer::Producer,
    pub source: BufferSource,
    pub source_id: SourceId,
    pub manifest_path: String,
    pub data_prefix: String,
}

/// Build an in-memory Buffer + a `BufferSource` over it. The
/// runner uses this before prefilling so the producer + source
/// share a backing store.
pub async fn build_env(cfg: &LogWorkloadConfig) -> Result<WorkloadEnv, FixtureError> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let producer_config = buffer::ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: cfg.data_prefix.clone(),
        manifest_path: cfg.manifest_path.clone(),
        flush_interval: Duration::from_secs(24 * 3600),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1024,
        batch_compression: buffer::CompressionType::None,
    };
    let producer = buffer::Producer::with_object_store(
        producer_config,
        Arc::clone(&store),
        Arc::new(SystemClock),
    )
    .map_err(|e| FixtureError::Writer(format!("Producer::with_object_store: {e}")))?;
    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: cfg.manifest_path.clone(),
        data_path_prefix: cfg.data_prefix.clone(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer = buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None)
        .await
        .map_err(|e| FixtureError::Writer(format!("Consumer::with_object_store: {e}")))?;
    let source = BufferSource::new(consumer, cfg.source_id.0.clone(), &cfg.manifest_path, None);
    Ok(WorkloadEnv {
        store,
        producer,
        source,
        source_id: cfg.source_id.clone(),
        manifest_path: cfg.manifest_path.clone(),
        data_prefix: cfg.data_prefix.clone(),
    })
}

pub struct LogWorkload;

impl LogWorkload {
    /// Produce `n` OTLP-logs payloads with sequence numbers starting
    /// at `start_seq`. Flushes after every call so each payload
    /// lands as its own Buffer manifest entry (= one source range).
    /// Use this from the bench's runner when the iteration protocol
    /// needs to separate the warmup phase from the timed phase
    /// (`produce_warmup` → drain → snapshot baseline → `produce_timed`
    /// → drain → snapshot final). The design's pseudocode prefills
    /// the entire workload upfront, but for short workloads — including
    /// CI smoke runs — that lets the runtime race through every payload
    /// before the bench reaches the baseline snapshot, collapsing the
    /// protocol into "everything is warmup, nothing is timed". The
    /// split-producer shape preserves the design's intent (warmup
    /// boundary observable; timed window captured cleanly) at the
    /// cost of a small producer/consumer concurrency overlap during
    /// each phase.
    pub async fn produce(
        cfg: &LogWorkloadConfig,
        producer: &buffer::Producer,
        start_seq: u64,
        n: u64,
    ) -> Result<(), FixtureError> {
        let base_ts = now_ns();
        for batch_idx in 0..n {
            let seq = start_seq + batch_idx;
            let payload = make_logs(
                &cfg.service_name,
                cfg.records_per_source_range,
                base_ts + seq * 1_000_000_000,
            );
            producer
                .produce(vec![Bytes::from(payload)], logs_envelope())
                .await
                .map_err(|e| FixtureError::Writer(format!("producer.produce: {e}")))?;
            producer
                .flush()
                .await
                .map_err(|e| FixtureError::Writer(format!("producer.flush: {e}")))?;
        }
        Ok(())
    }

    /// Pre-compute the `WorkloadHandle` sequence values the runner
    /// uses to gate its drain loops. The caller produces warmup +
    /// timed in two phases via [`Self::produce`].
    pub fn handle(cfg: &LogWorkloadConfig) -> WorkloadHandle {
        let total = cfg.warmup_payloads + cfg.timed_payloads;
        WorkloadHandle {
            warmup_highest_sequence: (cfg.warmup_payloads as u64).saturating_sub(1),
            highest_sequence: (total as u64).saturating_sub(1),
            total_records: (cfg.records_per_source_range as u64) * (total as u64),
            timed_records: (cfg.records_per_source_range as u64) * (cfg.timed_payloads as u64),
        }
    }
}

fn make_logs(service: &str, record_count: usize, base_ts_ns: u64) -> Vec<u8> {
    let log_records = (0..record_count)
        .map(|i| LogRecord {
            time_unix_nano: base_ts_ns + i as u64,
            observed_time_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!("body-{i}"))),
            }),
            attributes: vec![KeyValue {
                key: "topic".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(format!("t-{i}"))),
                }),
            }],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        })
        .collect();
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service.to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

/// 4-byte metadata envelope matching the runtime's configured shape:
/// version=1, signal=Logs, encoding=OtlpProtobuf.
fn logs_envelope() -> Bytes {
    Bytes::from_static(&[1, 2, 1, 0])
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
