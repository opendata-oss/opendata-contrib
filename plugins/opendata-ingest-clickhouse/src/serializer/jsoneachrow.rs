//! JSONEachRow serializer — renders each chunk as newline-delimited
//! JSON objects, one per row, behind the `ChunkSerializer` trait.

use serde_json::Value as JsonValue;

use super::{ChunkSerializer, SerializationFormat};
use crate::adapter::InsertChunk;
use crate::writer::WriterError;

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonEachRowSerializer;

impl ChunkSerializer for JsonEachRowSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::JsonEachRow
    }

    fn serialize(&self, chunk: &InsertChunk) -> Result<Vec<u8>, WriterError> {
        let mut out = Vec::with_capacity(chunk.rows.len() * 256);
        for row in &chunk.rows {
            if row.len() != chunk.columns.len() {
                return Err(WriterError::Serialization(format!(
                    "row has {} values but {} columns are expected",
                    row.len(),
                    chunk.columns.len()
                )));
            }
            let mut obj = serde_json::Map::with_capacity(row.len());
            for (col, value) in chunk.columns.iter().zip(row.iter()) {
                obj.insert((*col).to_string(), value.to_json());
            }
            let line = serde_json::to_vec(&JsonValue::Object(obj))
                .map_err(|e| WriterError::Serialization(e.to_string()))?;
            out.extend_from_slice(&line);
            out.push(b'\n');
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{ClickHouseSettings, RowValue};

    fn chunk(rows: Vec<Vec<RowValue>>) -> InsertChunk {
        InsertChunk {
            database: "responsive".into(),
            table: "logs".into(),
            columns: vec!["a", "b"],
            rows,
            settings: ClickHouseSettings {
                insert_quorum: Some("auto".into()),
                insert_deduplication_token: "tok".into(),
                apply_deduplication_token: true,
            },
            idempotency_token: "tok".into(),
            chunk_index: 0,
            observability_labels: vec![],
        }
    }

    #[test]
    fn jsoneachrow_emits_one_object_per_row() {
        let c = chunk(vec![
            vec![RowValue::String("x".into()), RowValue::UInt64(1)],
            vec![RowValue::String("y".into()), RowValue::UInt64(2)],
        ]);
        let body = JsonEachRowSerializer.serialize(&c).expect("serialize");
        let body_str = std::str::from_utf8(&body).expect("utf8");
        let lines: Vec<&str> = body_str.lines().collect();
        assert_eq!(lines.len(), 2);
        let row0: JsonValue = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(row0["a"], "x");
        assert_eq!(row0["b"], 1);
        let row1: JsonValue = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(row1["a"], "y");
        assert_eq!(row1["b"], 2);
    }

    #[test]
    fn jsoneachrow_rejects_row_column_mismatch() {
        let mut c = chunk(vec![vec![RowValue::String("x".into())]]);
        c.columns = vec!["a", "b"];
        let err = JsonEachRowSerializer.serialize(&c).unwrap_err();
        match err {
            WriterError::Serialization(msg) => assert!(msg.contains("row has 1 values")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
