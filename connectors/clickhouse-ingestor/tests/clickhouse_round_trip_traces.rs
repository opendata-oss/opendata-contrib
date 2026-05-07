//! End-to-end test demonstrating how to extend the ingestor to a new
//! OTLP signal — in this case, OTLP traces (spans).
//!
//! The crate's public API ships only the OTLP logs adapter; this file
//! is the worked reference for "what does it take to add a new signal
//! type?". The `OtlpTracesDecoder` + `OtlpTracesClickHouseAdapter` here
//! are intentionally inline in the test directory rather than in `src/`,
//! so they're a demonstration of the extension pattern, not a public
//! commitment to traces support.
//!
//! To extend the ingestor for any new signal you need:
//!   1. (If the signal isn't already in `SignalType`) a new variant +
//!      byte mapping in `crate::envelope::SignalType`. This is the only
//!      change to the published crate.
//!   2. A `SignalDecoder` impl whose `Output` is the per-record type
//!      your adapter consumes.
//!   3. An `Adapter` impl whose `Input` is that record type. The
//!      adapter emits `InsertChunk`s that point at your target table
//!      with whatever column layout you want.
//!   4. A ClickHouse table DDL matching the adapter's column layout.
//!   5. A binary (or test) that wires `SignalDecoder` + `Adapter` +
//!      `ClickHouseWriter` into a `BufferConsumerRuntime`.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p clickhouse-ingestor --features integration-tests \
//!   --test clickhouse_round_trip_traces -- --nocapture
//! ```

#![cfg(feature = "integration-tests")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use clickhouse_ingestor::adapter::{Adapter, ClickHouseSettings, InsertChunk, RowValue};
use clickhouse_ingestor::commit_group::{CommitGroupBatch, CommitGroupThresholds, RecordSize};
use clickhouse_ingestor::envelope::{
    ConfiguredEnvelope, MetadataEnvelope, PayloadEncoding, SignalType,
};
use clickhouse_ingestor::error::{IngestorError, IngestorResult};
use clickhouse_ingestor::signal::{SignalDecoder, SourceCoordinates};
use clickhouse_ingestor::source::RawBufferBatch;
use clickhouse_ingestor::writer::{ClickHouseWriter, WriterConfig};
use clickhouse_ingestor::{AckFlushPolicy, BufferConsumerRuntime, RuntimeOptions};
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use sha2::{Digest, Sha256};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::clickhouse::ClickHouse as ClickHouseImage;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

// =====================================================================
// 1. Decoded record type
// =====================================================================
//
// One flat row per OTLP Span. Mirrors the shape `DecodedLogRecord` has
// for logs: source coordinates from the Buffer, plus the columns the
// adapter is going to write into ClickHouse. The flattening (resource ->
// scope -> span tree into a flat record list) happens in the decoder so
// the adapter doesn't have to deal with the nested OTLP shape.

#[derive(Debug, Clone)]
#[allow(dead_code)] // end_time_unix_nano kept in the struct for completeness; adapter uses duration_nanos
struct DecodedSpanRecord {
    source: SourceCoordinates,
    trace_id_hex: String,
    span_id_hex: String,
    parent_span_id_hex: String,
    name: String,
    kind: i32,
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    duration_nanos: u64,
    service_name: Option<String>,
    status_code: i32,
    status_message: String,
    resource_attributes: BTreeMap<String, String>,
    span_attributes: BTreeMap<String, String>,
}

// Approximation of in-memory size, used by the commit group for
// byte-thresholded flushing. Exactly matches the contract in
// `clickhouse_ingestor::commit_group::RecordSize` so the runtime can
// flush deterministically.
impl RecordSize for DecodedSpanRecord {
    fn approx_size_bytes(&self) -> usize {
        let attr_bytes: usize = self
            .resource_attributes
            .iter()
            .chain(self.span_attributes.iter())
            .map(|(k, v)| k.len() + v.len() + 8)
            .sum();
        96  // base struct + ids
        + self.name.len()
        + self.status_message.len()
        + self.service_name.as_ref().map_or(0, |s| s.len())
        + attr_bytes
    }
}

// Small helpers for OTLP attribute extraction. The crate's logs
// decoder has equivalent private helpers in src/signal.rs; we redeclare
// here so the test stays self-contained.
fn string_value(value: &Option<AnyValue>) -> Option<String> {
    match value.as_ref().and_then(|v| v.value.as_ref()) {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        Some(Value::BytesValue(bytes)) => Some(hex::encode(bytes)),
        Some(Value::ArrayValue(_)) | Some(Value::KvlistValue(_)) => Some("<complex>".into()),
        None => None,
    }
}

fn merge_attributes(src: &[KeyValue], dst: &mut BTreeMap<String, String>) {
    for kv in src {
        if let Some(value) = string_value(&kv.value) {
            dst.insert(kv.key.clone(), value);
        }
    }
}

// =====================================================================
// 2. SignalDecoder impl
// =====================================================================

#[derive(Debug, Default)]
struct OtlpTracesDecoder;

impl SignalDecoder for OtlpTracesDecoder {
    type Output = Vec<DecodedSpanRecord>;

    fn decode(
        &self,
        batch: &RawBufferBatch,
        _envelopes: &[MetadataEnvelope],
    ) -> IngestorResult<Self::Output> {
        let mut records = Vec::new();
        for entry in &batch.entries {
            let req = ExportTraceServiceRequest::decode(entry.raw_bytes.as_ref()).map_err(|e| {
                IngestorError::SignalDecode(format!(
                    "OTLP traces decode failed for sequence={} entry_index={}: {}",
                    batch.sequence, entry.entry_index, e
                ))
            })?;

            let mut record_index: u32 = 0;
            for resource_spans in &req.resource_spans {
                let mut resource_attributes = BTreeMap::new();
                let mut service_name = None;
                if let Some(resource) = &resource_spans.resource {
                    for kv in &resource.attributes {
                        if let Some(value) = string_value(&kv.value) {
                            if kv.key == "service.name" {
                                service_name = Some(value.clone());
                            }
                            resource_attributes.insert(kv.key.clone(), value);
                        }
                    }
                }

                for scope_spans in &resource_spans.scope_spans {
                    for span in &scope_spans.spans {
                        let mut span_attributes = BTreeMap::new();
                        merge_attributes(&span.attributes, &mut span_attributes);

                        let (status_code, status_message) = span
                            .status
                            .as_ref()
                            .map(|s| (s.code, s.message.clone()))
                            .unwrap_or((0, String::new()));

                        records.push(DecodedSpanRecord {
                            source: SourceCoordinates {
                                sequence: batch.sequence,
                                entry_index: entry.entry_index,
                                record_index,
                                manifest_path: batch.manifest_path.clone(),
                                data_path: batch.data_object_path.clone(),
                                ingestion_time_ms: entry.ingestion_time_ms,
                            },
                            trace_id_hex: hex::encode(&span.trace_id),
                            span_id_hex: hex::encode(&span.span_id),
                            parent_span_id_hex: hex::encode(&span.parent_span_id),
                            name: span.name.clone(),
                            kind: span.kind,
                            start_time_unix_nano: span.start_time_unix_nano,
                            end_time_unix_nano: span.end_time_unix_nano,
                            duration_nanos: span
                                .end_time_unix_nano
                                .saturating_sub(span.start_time_unix_nano),
                            service_name: service_name.clone(),
                            status_code,
                            status_message,
                            resource_attributes: resource_attributes.clone(),
                            span_attributes,
                        });
                        record_index += 1;
                    }
                }
            }
        }
        Ok(records)
    }
}

// =====================================================================
// 3. Adapter impl
// =====================================================================

#[derive(Debug, Clone)]
struct TracesAdapterConfig {
    database: String,
    table: String,
    adapter_version: u32,
    max_chunk_rows: usize,
    max_chunk_bytes: usize,
    insert_quorum: Option<String>,
    apply_deduplication_token: bool,
}

const TRACES_COLUMNS: &[&str] = &[
    "Timestamp",
    "TraceId",
    "SpanId",
    "ParentSpanId",
    "ServiceName",
    "SpanName",
    "SpanKind",
    "StatusCode",
    "StatusMessage",
    "DurationNanos",
    "ResourceAttributes",
    "SpanAttributes",
    "_odb_sequence",
    "_odb_entry_index",
    "_odb_record_index",
    "_odb_manifest_path",
    "_odb_data_path",
    "_odb_ingestion_time_ms",
    "_adapter_version",
];

#[derive(Debug, Clone)]
struct OtlpTracesClickHouseAdapter {
    config: TracesAdapterConfig,
    chunking_fingerprint: String,
}

impl OtlpTracesClickHouseAdapter {
    fn new(config: TracesAdapterConfig) -> Self {
        let fingerprint = chunking_fingerprint(&config);
        Self {
            config,
            chunking_fingerprint: fingerprint,
        }
    }
}

fn chunking_fingerprint(config: &TracesAdapterConfig) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"otlp-traces-clickhouse-adapter:v1\n");
    hasher.update(format!("database={}\n", config.database).as_bytes());
    hasher.update(format!("table={}\n", config.table).as_bytes());
    hasher.update(format!("adapter_version={}\n", config.adapter_version).as_bytes());
    hasher.update(format!("max_chunk_rows={}\n", config.max_chunk_rows).as_bytes());
    hasher.update(format!("max_chunk_bytes={}\n", config.max_chunk_bytes).as_bytes());
    hasher.update(
        format!(
            "insert_quorum={}\n",
            config.insert_quorum.as_deref().unwrap_or("")
        )
        .as_bytes(),
    );
    let digest = hasher.finalize();
    hex::encode(&digest[..8])
}

fn build_token(
    manifest_path: &str,
    config: &TracesAdapterConfig,
    fingerprint: &str,
    low: u64,
    high: u64,
    chunk_index: u32,
) -> String {
    format!(
        "{}:{}.{}:{}-{}:{}:{}:{}",
        manifest_path,
        config.database,
        config.table,
        low,
        high,
        config.adapter_version,
        fingerprint,
        chunk_index,
    )
}

fn span_row(rec: &DecodedSpanRecord, config: &TracesAdapterConfig) -> Vec<RowValue> {
    vec![
        RowValue::DateTime64Nanos(rec.start_time_unix_nano),
        RowValue::String(rec.trace_id_hex.clone()),
        RowValue::String(rec.span_id_hex.clone()),
        RowValue::String(rec.parent_span_id_hex.clone()),
        RowValue::LowCardinalityString(rec.service_name.clone().unwrap_or_default()),
        RowValue::LowCardinalityString(rec.name.clone()),
        RowValue::UInt8(rec.kind.clamp(0, u8::MAX as i32) as u8),
        RowValue::UInt8(rec.status_code.clamp(0, u8::MAX as i32) as u8),
        RowValue::String(rec.status_message.clone()),
        RowValue::UInt64(rec.duration_nanos),
        RowValue::StringMap(rec.resource_attributes.clone()),
        RowValue::StringMap(rec.span_attributes.clone()),
        RowValue::UInt64(rec.source.sequence),
        RowValue::UInt32(rec.source.entry_index),
        RowValue::UInt32(rec.source.record_index),
        RowValue::LowCardinalityString(rec.source.manifest_path.clone()),
        RowValue::String(rec.source.data_path.clone()),
        RowValue::Int64(rec.source.ingestion_time_ms),
        RowValue::UInt32(config.adapter_version),
    ]
}

impl Adapter for OtlpTracesClickHouseAdapter {
    type Input = DecodedSpanRecord;

    fn plan(&self, batch: CommitGroupBatch<Self::Input>) -> IngestorResult<Vec<InsertChunk>> {
        let CommitGroupBatch {
            mut records,
            low_sequence,
            high_sequence,
            bytes: _,
        } = batch;

        records.sort_by_key(|r| {
            (
                r.source.sequence,
                r.source.entry_index,
                r.source.record_index,
            )
        });

        let mut chunks = Vec::new();
        if records.is_empty() {
            return Ok(chunks);
        }

        let manifest_path = records[0].source.manifest_path.clone();
        if let Some(mismatch) = records
            .iter()
            .find(|r| r.source.manifest_path != manifest_path)
        {
            return Err(IngestorError::Adapter(format!(
                "commit group mixes manifest paths: first={first}, found={other}",
                first = manifest_path,
                other = mismatch.source.manifest_path,
            )));
        }

        let max_rows = self.config.max_chunk_rows.max(1);
        let max_bytes = self.config.max_chunk_bytes.max(1);
        let mut current: Vec<&DecodedSpanRecord> = Vec::new();
        let mut current_bytes: usize = 0;
        let mut chunk_index: u32 = 0;
        let emit = |slice: &[&DecodedSpanRecord],
                    chunk_index: u32,
                    config: &TracesAdapterConfig,
                    manifest_path: &str,
                    fingerprint: &str|
         -> InsertChunk {
            let rows: Vec<Vec<RowValue>> = slice.iter().map(|r| span_row(r, config)).collect();
            let token = build_token(
                manifest_path,
                config,
                fingerprint,
                low_sequence,
                high_sequence,
                chunk_index,
            );
            InsertChunk {
                database: config.database.clone(),
                table: config.table.clone(),
                columns: TRACES_COLUMNS.to_vec(),
                rows,
                settings: ClickHouseSettings {
                    insert_quorum: config.insert_quorum.clone(),
                    insert_deduplication_token: token.clone(),
                    apply_deduplication_token: config.apply_deduplication_token,
                },
                idempotency_token: token,
                chunk_index,
                observability_labels: vec![
                    ("table", config.table.clone()),
                    ("signal", "traces".to_string()),
                ],
            }
        };
        for record in &records {
            let rec_bytes = record.approx_size_bytes();
            let would_exceed_rows = current.len() + 1 > max_rows;
            let would_exceed_bytes = current_bytes + rec_bytes > max_bytes;
            if !current.is_empty() && (would_exceed_rows || would_exceed_bytes) {
                chunks.push(emit(
                    &current,
                    chunk_index,
                    &self.config,
                    &manifest_path,
                    &self.chunking_fingerprint,
                ));
                chunk_index += 1;
                current.clear();
                current_bytes = 0;
            }
            current.push(record);
            current_bytes += rec_bytes;
        }
        if !current.is_empty() {
            chunks.push(emit(
                &current,
                chunk_index,
                &self.config,
                &manifest_path,
                &self.chunking_fingerprint,
            ));
        }
        Ok(chunks)
    }
}

// =====================================================================
// 4. ClickHouse table DDL
// =====================================================================
//
// Mirrors the alpha logs DDL shape: `ReplacingMergeTree(_adapter_version)`
// partitioned by date, ORDER BY hybrid (query-useful prefix + dedupe
// suffix). The columns and ORDER BY differ — that's the whole point of
// the adapter being signal-specific.

fn traces_table_ddl(config: &TracesAdapterConfig) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.{table} (\n\
         Timestamp           DateTime64(9)               CODEC(Delta, ZSTD),\n\
         TraceId             String                      CODEC(ZSTD),\n\
         SpanId              String                      CODEC(ZSTD),\n\
         ParentSpanId        String                      CODEC(ZSTD),\n\
         ServiceName         LowCardinality(String),\n\
         SpanName            LowCardinality(String),\n\
         SpanKind            UInt8,\n\
         StatusCode          UInt8,\n\
         StatusMessage       String,\n\
         DurationNanos       UInt64,\n\
         ResourceAttributes  Map(LowCardinality(String), String),\n\
         SpanAttributes      Map(LowCardinality(String), String),\n\
         _odb_sequence            UInt64,\n\
         _odb_entry_index         UInt32,\n\
         _odb_record_index        UInt32,\n\
         _odb_manifest_path       LowCardinality(String),\n\
         _odb_data_path           String,\n\
         _odb_ingestion_time_ms   Int64,\n\
         _adapter_version         UInt32\n\
         )\n\
         ENGINE = ReplacingMergeTree(_adapter_version)\n\
         PARTITION BY toDate(Timestamp)\n\
         ORDER BY (toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)\n\
         TTL toDate(Timestamp) + INTERVAL 30 DAY",
        db = config.database,
        table = config.table,
    )
}

// =====================================================================
// 5. Test fixtures + the wiring
// =====================================================================

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Build an OTLP traces protobuf payload with `span_count` spans under a
/// single resource named `service`. Each span has a unique trace + span
/// id derived from the iteration index so dedupe assertions are
/// meaningful.
fn make_traces(service: &str, span_count: usize, base_ts_ns: u64) -> Vec<u8> {
    let spans = (0..span_count)
        .map(|i| Span {
            trace_id: vec![0x10 + i as u8; 16],
            span_id: vec![0x20 + i as u8; 8],
            trace_state: String::new(),
            parent_span_id: vec![],
            flags: 0,
            name: format!("op-{i}"),
            kind: 2, // SPAN_KIND_SERVER
            start_time_unix_nano: base_ts_ns + i as u64 * 1000,
            end_time_unix_nano: base_ts_ns + i as u64 * 1000 + 500,
            attributes: vec![KeyValue {
                key: "http.target".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(format!("/api/{i}"))),
                }),
            }],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: None,
        })
        .collect();
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service.to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

/// Per-entry envelope bytes for traces: version=1, signal_type=3 (Traces),
/// encoding=1 (OTLP protobuf), reserved=0.
fn traces_envelope() -> Bytes {
    Bytes::from_static(&[1, 3, 1, 0])
}

#[tokio::test]
async fn traces_round_trip_demonstrates_extension_pattern() -> Result<(), Box<dyn std::error::Error>>
{
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let clickhouse = ClickHouseImage::default().start().await?;
    let port = clickhouse.get_host_port_ipv4(8123).await?;
    let endpoint = format!("http://127.0.0.1:{port}");

    let writer = ClickHouseWriter::new(WriterConfig {
        endpoint: endpoint.clone(),
        user: "default".into(),
        password: String::new(),
        request_timeout: Duration::from_secs(15),
        max_attempts: 4,
        initial_backoff: Duration::from_millis(100),
    });

    let database = "responsive_test";
    let table = "traces_round_trip";
    writer
        .execute_statement(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
        .await?;
    let adapter_cfg = TracesAdapterConfig {
        database: database.into(),
        table: table.into(),
        adapter_version: 1,
        max_chunk_rows: 100,
        max_chunk_bytes: 1_000_000,
        insert_quorum: None,
        apply_deduplication_token: false,
    };
    writer
        .execute_statement(&traces_table_ddl(&adapter_cfg))
        .await?;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/clickhouse-rt-traces/manifest";
    let data_prefix = "ingest/test/clickhouse-rt-traces/data";

    let producer_config = buffer::ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.into(),
        manifest_path: manifest_path.into(),
        flush_interval: Duration::from_secs(24 * 3600),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1000,
        batch_compression: buffer::CompressionType::None,
    };
    let producer = buffer::Producer::with_object_store(
        producer_config,
        Arc::clone(&store),
        Arc::new(SystemClock),
    )?;

    let now = now_ns();
    producer
        .produce(
            vec![Bytes::from(make_traces("checkout", 3, now))],
            traces_envelope(),
        )
        .await?;
    producer
        .produce(
            vec![Bytes::from(make_traces("payment", 2, now + 1_000_000_000))],
            traces_envelope(),
        )
        .await?;
    producer.flush().await?;

    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer =
        buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None).await?;

    let runtime_options = RuntimeOptions {
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Traces,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        commit_group: CommitGroupThresholds {
            max_rows: 1000,
            max_bytes: 1_000_000,
            max_age: Duration::from_millis(100),
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: false,
        poll_interval: Duration::from_millis(20),
    };
    let runtime = BufferConsumerRuntime::new(
        consumer,
        OtlpTracesDecoder,
        OtlpTracesClickHouseAdapter::new(adapter_cfg.clone()),
        Some(writer.clone()),
        runtime_options,
    );
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let _ = timeout(Duration::from_secs(20), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.rows_inserted >= 5 && p.last_acked_sequence.is_some() {
                return p;
            }
        }
    })
    .await
    .expect("ingestor progress timeout");

    shutdown.cancel();
    let runtime_result = handle.await.expect("runtime task panicked");
    runtime_result.expect("runtime exited cleanly");

    let total_raw = writer
        .execute_statement(&format!("SELECT count() FROM {database}.{table}"))
        .await?;
    let total_final = writer
        .execute_statement(&format!("SELECT count() FROM {database}.{table} FINAL"))
        .await?;
    eprintln!(
        "row counts after runtime: raw={} FINAL={}",
        total_raw.trim(),
        total_final.trim()
    );
    assert_eq!(total_raw.trim(), "5", "expected 5 raw span rows");
    assert_eq!(total_final.trim(), "5", "expected 5 deduplicated span rows");

    // Sanity check span-shaped columns landed correctly.
    let services = writer
        .execute_statement(&format!(
            "SELECT count(DISTINCT ServiceName) FROM {database}.{table}"
        ))
        .await?;
    assert_eq!(services.trim(), "2", "expected 2 distinct services");

    let span_names = writer
        .execute_statement(&format!(
            "SELECT count(DISTINCT SpanName) FROM {database}.{table}"
        ))
        .await?;
    assert_eq!(
        span_names.trim(),
        "3",
        "expected op-0/op-1/op-2 (svc-a) but svc-b shares names"
    );

    producer.close().await?;
    Ok(())
}
