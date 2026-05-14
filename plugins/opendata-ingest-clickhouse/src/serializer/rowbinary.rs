//! RowBinaryWithNamesAndTypes serializer (Phase 7.4). Encodes the
//! chunk's header (column count + names + types) followed by the
//! row stream per the ClickHouse RowBinary spec. The column → type
//! mapping is keyed off the OTLP-logs adapter's `COLUMNS` constant
//! so a future column reorder forces an update here.

use crate::adapter::{InsertChunk, write_varuint};
use crate::writer::WriterError;

use super::{ChunkSerializer, SerializationFormat};

#[derive(Debug, Default, Clone, Copy)]
pub struct RowBinarySerializer;

impl ChunkSerializer for RowBinarySerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::RowBinary
    }

    fn serialize(&self, chunk: &InsertChunk) -> Result<Vec<u8>, WriterError> {
        let mut out = Vec::with_capacity(chunk.rows.len() * 96);

        // Header: column count + names + types.
        write_varuint(&mut out, chunk.columns.len() as u64);
        for name in &chunk.columns {
            write_varuint(&mut out, name.len() as u64);
            out.extend_from_slice(name.as_bytes());
        }
        for name in &chunk.columns {
            let ty = clickhouse_type_for_column(name)?;
            write_varuint(&mut out, ty.len() as u64);
            out.extend_from_slice(ty.as_bytes());
        }

        // Rows.
        for row in &chunk.rows {
            if row.len() != chunk.columns.len() {
                return Err(WriterError::Serialization(format!(
                    "row has {} values but {} columns are expected",
                    row.len(),
                    chunk.columns.len(),
                )));
            }
            for value in row {
                value.write_row_binary(&mut out);
            }
        }
        Ok(out)
    }
}

/// Map an OTLP-logs adapter column name to its ClickHouse type
/// string. Phase 7.4's first cut hardcodes the table for OTLP logs;
/// row 9 (pluggable schemas) generalizes this to a sink-side
/// configurable mapping.
fn clickhouse_type_for_column(name: &str) -> Result<&'static str, WriterError> {
    let ty = match name {
        "Timestamp" => "DateTime64(9)",
        "ObservedTimestamp" => "DateTime64(9)",
        "SeverityText" => "LowCardinality(String)",
        "SeverityNumber" => "UInt8",
        "ServiceName" => "LowCardinality(String)",
        "Body" => "String",
        "ResourceAttributes" => "Map(LowCardinality(String), String)",
        "LogAttributes" => "Map(LowCardinality(String), String)",
        "TraceId" => "String",
        "SpanId" => "String",
        "_odb_sequence" => "UInt64",
        "_odb_entry_index" => "UInt32",
        "_odb_record_index" => "UInt32",
        "_odb_manifest_path" => "LowCardinality(String)",
        "_odb_data_path" => "String",
        "_odb_ingestion_time_ms" => "Int64",
        "_adapter_version" => "UInt32",
        other => {
            return Err(WriterError::Serialization(format!(
                "no RowBinary type table entry for column {other:?}; \
                 update `clickhouse_type_for_column` to match the \
                 adapter DDL",
            )));
        }
    };
    Ok(ty)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::adapter::logs::COLUMNS;
    use crate::adapter::{ClickHouseSettings, RowValue};

    /// Every column the OTLP-logs adapter ships must have a
    /// RowBinary type table entry. Catches column-reorder /
    /// column-add drift at `cargo test` time.
    #[test]
    fn rowbinary_type_table_covers_all_otlp_logs_columns() {
        for col in COLUMNS {
            clickhouse_type_for_column(col)
                .unwrap_or_else(|e| panic!("missing rowbinary type for column {col:?}: {e}"));
        }
    }

    #[test]
    fn write_row_binary_emits_expected_bytes_for_each_variant() {
        let mut out = Vec::new();
        RowValue::UInt8(0x5A).write_row_binary(&mut out);
        assert_eq!(out, vec![0x5A]);

        out.clear();
        RowValue::UInt32(0x0102_0304).write_row_binary(&mut out);
        assert_eq!(out, vec![0x04, 0x03, 0x02, 0x01]);

        out.clear();
        RowValue::UInt64(0x0807_0605_0403_0201).write_row_binary(&mut out);
        assert_eq!(out, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);

        out.clear();
        RowValue::Int32(-1).write_row_binary(&mut out);
        assert_eq!(out, vec![0xFF, 0xFF, 0xFF, 0xFF]);

        out.clear();
        RowValue::Int64(-1).write_row_binary(&mut out);
        assert_eq!(out, vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        out.clear();
        RowValue::DateTime64Nanos(0).write_row_binary(&mut out);
        assert_eq!(out, vec![0, 0, 0, 0, 0, 0, 0, 0]);

        out.clear();
        RowValue::String("hi".into()).write_row_binary(&mut out);
        assert_eq!(out, vec![0x02, b'h', b'i']);

        out.clear();
        RowValue::LowCardinalityString("abc".into()).write_row_binary(&mut out);
        assert_eq!(out, vec![0x03, b'a', b'b', b'c']);

        out.clear();
        let mut map = BTreeMap::new();
        map.insert("k".to_string(), "v".to_string());
        map.insert("aa".to_string(), "bb".to_string());
        RowValue::StringMap(map).write_row_binary(&mut out);
        // Map: count=2 (varuint), then ("aa","bb"), ("k","v") in BTree
        // iteration order (lex on key).
        assert_eq!(
            out,
            vec![
                0x02, // count = 2
                0x02, b'a', b'a', 0x02, b'b', b'b', 0x01, b'k', 0x01, b'v',
            ],
        );
    }

    #[test]
    fn rowbinary_serialize_header_for_otlp_logs() {
        // One column subset to keep the test compact: SeverityNumber.
        let chunk = InsertChunk {
            database: "responsive".into(),
            table: "logs".into(),
            columns: vec!["SeverityNumber"],
            rows: vec![vec![RowValue::UInt8(9)], vec![RowValue::UInt8(13)]],
            settings: ClickHouseSettings::default(),
            idempotency_token: "tok".into(),
            chunk_index: 0,
            observability_labels: vec![],
        };
        let bytes = RowBinarySerializer.serialize(&chunk).expect("serialize");
        // Header: 1 column, name "SeverityNumber", type "UInt8".
        let mut expected = Vec::new();
        expected.push(0x01); // column count
        expected.push(b"SeverityNumber".len() as u8);
        expected.extend_from_slice(b"SeverityNumber");
        expected.push(b"UInt8".len() as u8);
        expected.extend_from_slice(b"UInt8");
        expected.push(9);
        expected.push(13);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn rowbinary_rejects_row_column_mismatch() {
        let chunk = InsertChunk {
            database: "responsive".into(),
            table: "logs".into(),
            columns: vec!["SeverityNumber"],
            rows: vec![vec![RowValue::UInt8(1), RowValue::UInt8(2)]],
            settings: ClickHouseSettings::default(),
            idempotency_token: "tok".into(),
            chunk_index: 0,
            observability_labels: vec![],
        };
        let err = RowBinarySerializer.serialize(&chunk).unwrap_err();
        match err {
            WriterError::Serialization(msg) => assert!(msg.contains("row has 2 values")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
