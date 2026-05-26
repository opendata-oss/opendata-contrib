//! OTLP logs decoder.
//!
//! Provides the [`opendata_ingest_runtime::decoder::Decoder`]
//! impl that connects the decoder to the runtime, plus the
//! [`TypedDecodedLogs`] newtype that wraps `Vec<DecodedLogRecord>`
//! behind the runtime's `TypedRecords` trait so a `DecodedBatch`
//! can carry the v1 records under `Arc<dyn TypedRecords>`.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use prost::Message;
use thiserror::Error;

use opendata_ingest_runtime::decoded_batch::{
    BatchStats, DecodedBatch, DecodedRecords, SourceCoordinateColumns, TypedRecords, TypedSchema,
};
use opendata_ingest_runtime::decoder::Decoder;
use opendata_ingest_runtime::envelope::{MetadataEnvelope, PayloadEncoding, SignalType};
use opendata_ingest_runtime::error::{RuntimeError, RuntimeResult};
use opendata_ingest_runtime::identity::SchemaVersion;
use opendata_ingest_runtime::source::{SourceBatch, SourceEntry};

#[derive(Debug, Error)]
pub enum OtelDecodeError {
    #[error("OTLP logs decode failed for sequence={sequence} entry_index={entry_index}: {source}")]
    Logs {
        sequence: u64,
        entry_index: u32,
        #[source]
        source: prost::DecodeError,
    },
}

/// Row-level Buffer source coordinates carried alongside each decoded
/// log record. `buffer_sequence` is the Buffer batch sequence the row
/// came from; combined with `entry_index` and `record_index` it forms
/// the row-unique `(buffer_sequence, entry_index, record_index)`
/// triple. The runtime's logical commit range uses the same Buffer
/// sequence axis ([`SequenceRange`]); the ClickHouse `_odb_sequence`
/// column carries `buffer_sequence` verbatim.
///
/// [`SequenceRange`]: opendata_ingest_runtime::identity::SequenceRange
#[derive(Debug, Clone)]
pub struct RowSourceCoordinates {
    pub buffer_sequence: u64,
    pub entry_index: u32,
    pub record_index: u32,
    pub manifest_path: String,
    pub data_path: String,
    pub ingestion_time_ms: i64,
}

/// One flattened OTLP log record with its Buffer source coordinates
/// and resource/scope/log attributes pre-merged into a single map.
#[derive(Debug, Clone)]
pub struct DecodedLogRecord {
    pub source: RowSourceCoordinates,
    pub timestamp_unix_nano: u64,
    pub observed_timestamp_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: String,
    pub body: String,
    pub service_name: Option<String>,
    pub resource_attributes: BTreeMap<String, String>,
    pub scope_name: Option<String>,
    pub log_attributes: BTreeMap<String, String>,
    pub trace_id_hex: String,
    pub span_id_hex: String,
}

/// Convenience alias retained for documentation; the decoder itself
/// returns `Vec<DecodedLogRecord>` so the runtime's
/// `D::Output = Vec<A::Input>` bound is satisfied without an extra
/// wrapper type.
pub type DecodedLogs = Vec<DecodedLogRecord>;

impl DecodedLogRecord {
    /// Cheap approximate byte size for the ClickHouse adapter's
    /// byte-aware chunker. Sink-side helper — kept here on the
    /// record type so the sink can compute it without re-deriving
    /// the OTLP layout.
    pub fn approx_size_bytes(&self) -> usize {
        let mut sz = std::mem::size_of::<Self>();
        sz += self.severity_text.len();
        sz += self.body.len();
        sz += self.service_name.as_ref().map_or(0, |s| s.len());
        sz += self.scope_name.as_ref().map_or(0, |s| s.len());
        sz += self.trace_id_hex.len() + self.span_id_hex.len();
        for (k, v) in &self.resource_attributes {
            sz += k.len() + v.len();
        }
        for (k, v) in &self.log_attributes {
            sz += k.len() + v.len();
        }
        sz
    }
}

#[derive(Debug, Default)]
pub struct OtlpLogsDecoder;

impl OtlpLogsDecoder {
    pub fn new() -> Self {
        Self
    }

    /// Decode every OTLP-protobuf entry in `batch` into flattened
    /// log records. Returns the records in `(entry_index,
    /// record_index)` order.
    pub fn decode_logs(
        &self,
        batch: &SourceBatch,
    ) -> Result<Vec<DecodedLogRecord>, OtelDecodeError> {
        let mut records = Vec::new();
        for entry in &batch.entries {
            decode_entry(batch, entry, &mut records)?;
        }
        Ok(records)
    }
}

/// Schema name advertised by [`TypedDecodedLogs`]. Sinks that need
/// to dispatch on schema name match against this string.
pub const OTLP_LOGS_SCHEMA_NAME: &str = "opendata.otel.logs.v1";

/// `TypedRecords` adapter that wraps `Vec<DecodedLogRecord>` so the
/// runtime's `DecodedRecords::Typed(Arc<dyn TypedRecords>)` carrier
/// can hand v1 records to ClickHouse. Sinks downcast through
/// [`TypedRecords::as_any`] to recover the typed view.
#[derive(Debug)]
pub struct TypedDecodedLogs {
    records: Vec<DecodedLogRecord>,
    schema: TypedSchema,
}

impl TypedDecodedLogs {
    pub fn new(records: Vec<DecodedLogRecord>) -> Self {
        Self {
            records,
            schema: TypedSchema {
                name: OTLP_LOGS_SCHEMA_NAME.into(),
                version: SchemaVersion(1),
            },
        }
    }

    pub fn records(&self) -> &[DecodedLogRecord] {
        &self.records
    }
}

impl TypedRecords for TypedDecodedLogs {
    fn record_count(&self) -> usize {
        self.records.len()
    }

    fn estimated_bytes(&self) -> usize {
        self.records.iter().map(|r| r.approx_size_bytes()).sum()
    }

    fn schema(&self) -> &TypedSchema {
        &self.schema
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Decoder for OtlpLogsDecoder {
    fn accepts(&self, envelope: &MetadataEnvelope) -> bool {
        envelope.version == 1
            && envelope.signal_type == SignalType::Logs
            && envelope.encoding == PayloadEncoding::OtlpProtobuf
    }

    fn decode(&self, batch: SourceBatch) -> RuntimeResult<Vec<DecodedBatch>> {
        let source = batch.source.clone();
        let manifest_path = batch.manifest_path.clone();
        let data_path = batch.data_object_path.clone();
        let sequence = batch.sequence;
        let entry_count = batch.entries.len() as u32;

        let records = self
            .decode_logs(&batch)
            .map_err(|e| RuntimeError::Decoder(Box::new(e)))?;

        let record_count = records.len();
        let decoded_byte_estimate: u64 = records.iter().map(|r| r.approx_size_bytes() as u64).sum();

        let mut sequences = Vec::with_capacity(record_count);
        let mut entry_indices = Vec::with_capacity(record_count);
        let mut record_indices = Vec::with_capacity(record_count);
        let mut ingestion_time_ms = Vec::with_capacity(record_count);
        for r in &records {
            sequences.push(r.source.buffer_sequence);
            entry_indices.push(r.source.entry_index);
            record_indices.push(r.source.record_index);
            ingestion_time_ms.push(r.source.ingestion_time_ms);
        }

        let source_columns = SourceCoordinateColumns {
            manifest_path,
            data_path,
            sequences,
            entry_indices,
            record_indices,
            ingestion_time_ms,
        };

        let stats = BatchStats {
            // Source-byte accounting (post-decompress) is not
            // populated by this decoder.
            source_byte_count: 0,
            decoded_byte_estimate,
        };

        let typed = Arc::new(TypedDecodedLogs::new(records));

        Ok(vec![DecodedBatch {
            source,
            low_sequence: sequence,
            high_sequence: sequence,
            source_entry_count: entry_count,
            records: DecodedRecords::Typed(typed),
            source_columns,
            stats,
            schema_version: SchemaVersion(1),
        }])
    }
}

fn decode_entry(
    batch: &SourceBatch,
    entry: &SourceEntry,
    records: &mut Vec<DecodedLogRecord>,
) -> Result<(), OtelDecodeError> {
    let req = ExportLogsServiceRequest::decode(entry.raw_bytes.as_ref()).map_err(|e| {
        OtelDecodeError::Logs {
            sequence: batch.sequence,
            entry_index: entry.entry_index,
            source: e,
        }
    })?;

    let mut record_index: u32 = 0;
    for resource_logs in &req.resource_logs {
        let mut resource_attributes = BTreeMap::new();
        let mut service_name = None;
        if let Some(resource) = &resource_logs.resource {
            for kv in &resource.attributes {
                if let Some(value) = string_value(&kv.value) {
                    if kv.key == "service.name" {
                        service_name = Some(value.clone());
                    }
                    resource_attributes.insert(kv.key.clone(), value);
                }
            }
        }

        for scope_logs in &resource_logs.scope_logs {
            let scope_name = scope_logs.scope.as_ref().map(|s| s.name.clone());
            for log in &scope_logs.log_records {
                let mut log_attributes = BTreeMap::new();
                merge_attributes(&log.attributes, &mut log_attributes);
                if let Some(scope) = &scope_logs.scope {
                    merge_attributes(&scope.attributes, &mut log_attributes);
                }

                records.push(DecodedLogRecord {
                    source: RowSourceCoordinates {
                        buffer_sequence: batch.sequence,
                        entry_index: entry.entry_index,
                        record_index,
                        manifest_path: batch.manifest_path.clone(),
                        data_path: batch.data_object_path.clone(),
                        ingestion_time_ms: entry.ingestion_time_ms,
                    },
                    timestamp_unix_nano: log.time_unix_nano,
                    observed_timestamp_unix_nano: log.observed_time_unix_nano,
                    severity_number: log.severity_number,
                    severity_text: log.severity_text.clone(),
                    body: any_value_to_string(&log.body),
                    service_name: service_name.clone(),
                    resource_attributes: resource_attributes.clone(),
                    scope_name: scope_name.clone(),
                    log_attributes,
                    trace_id_hex: hex::encode(&log.trace_id),
                    span_id_hex: hex::encode(&log.span_id),
                });
                record_index += 1;
            }
        }
    }
    Ok(())
}

fn merge_attributes(src: &[KeyValue], dst: &mut BTreeMap<String, String>) {
    for kv in src {
        if let Some(value) = string_value(&kv.value) {
            dst.insert(kv.key.clone(), value);
        }
    }
}

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

fn any_value_to_string(value: &Option<AnyValue>) -> String {
    string_value(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn make_request(records_per_resource: &[usize]) -> ExportLogsServiceRequest {
        let resource_logs = records_per_resource
            .iter()
            .enumerate()
            .map(|(r_idx, &count)| ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(format!("svc-{r_idx}"))),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: (0..count)
                        .map(|i| LogRecord {
                            time_unix_nano: 1_000 + i as u64,
                            observed_time_unix_nano: 0,
                            severity_number: 9,
                            severity_text: "INFO".into(),
                            body: Some(AnyValue {
                                value: Some(Value::StringValue(format!("body-{r_idx}-{i}"))),
                            }),
                            attributes: vec![KeyValue {
                                key: "rec".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue(format!("{r_idx}/{i}"))),
                                }),
                            }],
                            dropped_attributes_count: 0,
                            flags: 0,
                            trace_id: vec![0xaa; 16],
                            span_id: vec![0xbb; 8],
                            event_name: String::new(),
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect();
        ExportLogsServiceRequest { resource_logs }
    }

    fn batch_with_payloads(payloads: Vec<Vec<u8>>) -> SourceBatch {
        SourceBatch {
            source: "buffer".into(),
            sequence: 42,
            manifest_path: "ingest/test/manifest".into(),
            data_object_path: "ingest/test/data/abc.batch".into(),
            entries: payloads
                .into_iter()
                .enumerate()
                .map(|(i, payload)| SourceEntry {
                    entry_index: i as u32,
                    raw_bytes: Bytes::from(payload),
                    raw_metadata: Bytes::from_static(&[1, 2, 1, 0]),
                    ingestion_time_ms: 99,
                })
                .collect(),
        }
    }

    #[test]
    fn flattens_resource_scope_log_tree_and_assigns_record_index() {
        // Two resource_logs blocks: 2 records and 3 records.
        let req = make_request(&[2, 3]);
        let payload = req.encode_to_vec();
        let batch = batch_with_payloads(vec![payload]);

        let decoder = OtlpLogsDecoder::new();
        let decoded = decoder.decode_logs(&batch).expect("decode");
        assert_eq!(decoded.len(), 5);
        let indices: Vec<u32> = decoded.iter().map(|r| r.source.record_index).collect();
        assert_eq!(indices, vec![0, 1, 2, 3, 4]);
        // Resource attributes flow through.
        assert_eq!(decoded[0].service_name.as_deref(), Some("svc-0"));
        assert_eq!(decoded[2].service_name.as_deref(), Some("svc-1"));
        // Source coordinates use the per-batch buffer sequence and per-entry index.
        for rec in &decoded {
            assert_eq!(rec.source.buffer_sequence, 42);
            assert_eq!(rec.source.entry_index, 0);
            assert_eq!(rec.source.manifest_path, "ingest/test/manifest");
            assert_eq!(rec.source.ingestion_time_ms, 99);
        }
        assert_eq!(decoded[0].body, "body-0-0");
    }

    #[test]
    fn record_index_resets_per_entry() {
        // Two entries, each with one record. record_index should be 0 within
        // each entry — the (sequence, entry_index, record_index) triple is
        // what's globally unique, not record_index alone.
        let req = make_request(&[1]);
        let payload = req.encode_to_vec();
        let batch = batch_with_payloads(vec![payload.clone(), payload]);

        let decoded = OtlpLogsDecoder::new().decode_logs(&batch).expect("decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].source.entry_index, 0);
        assert_eq!(decoded[1].source.entry_index, 1);
        assert_eq!(decoded[0].source.record_index, 0);
        assert_eq!(decoded[1].source.record_index, 0);
    }

    #[test]
    fn empty_payload_yields_zero_rows_but_no_error() {
        let req = ExportLogsServiceRequest {
            resource_logs: vec![],
        };
        let payload = req.encode_to_vec();
        let batch = batch_with_payloads(vec![payload]);

        let decoded = OtlpLogsDecoder::new().decode_logs(&batch).expect("decode");
        assert!(decoded.is_empty(), "expected no decoded records");
    }

    #[test]
    fn malformed_payload_returns_signal_decode_error() {
        let batch = batch_with_payloads(vec![b"not-a-protobuf-message".to_vec()]);
        let err = OtlpLogsDecoder::new().decode_logs(&batch).unwrap_err();
        match err {
            OtelDecodeError::Logs { .. } => {}
        }
    }

    #[test]
    fn trace_and_span_ids_hex_encoded() {
        let req = make_request(&[1]);
        let payload = req.encode_to_vec();
        let batch = batch_with_payloads(vec![payload]);
        let decoded = OtlpLogsDecoder::new().decode_logs(&batch).expect("decode");
        assert_eq!(decoded[0].trace_id_hex, "a".repeat(32));
        assert_eq!(decoded[0].span_id_hex, "b".repeat(16));
    }
}
