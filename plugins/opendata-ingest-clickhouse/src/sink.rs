//! `opendata_ingest_runtime::Sink` impl for ClickHouse.
//!
//! Phase 4.4b shim that wraps the existing
//! [`OtlpLogsClickHouseAdapter::plan`] +
//! [`ClickHouseWriter::execute_all`] path. RFC 0002 rev 5 contract:
//!
//! - `Ok(_)` means the full route-level commit (every chunk for
//!   the range × this route) is durable.
//! - Retry of the same `SinkCommit` is idempotent: the adapter
//!   produces deterministic chunks keyed off `(low_sequence,
//!   high_sequence, chunk_index)`, and ClickHouse dedupes via
//!   `insert_deduplication_token` plus
//!   `ReplacingMergeTree(_adapter_version)`.
//! - `MaybeCommitted(_)` is returned on writer-classified
//!   retryable failures so the runtime can call
//!   [`Sink::check_committed`] before deciding whether to retry.
//!   `check_committed` returns `Unknown` (the short-window
//!   ClickHouse insert dedupe token has expired by the time the
//!   runtime asks); per RFC 0002 rev 5 the runtime treats
//!   `Unknown` like `NotCommitted` and retries, with table-level
//!   dedupe as the long-window backstop.

use std::sync::Arc;

use async_trait::async_trait;

use opendata_ingest_otel::logs::{DecodedLogRecord, TypedDecodedLogs};
use opendata_ingest_runtime::commit_group::{CommitGroupBatch, RecordSize};
use opendata_ingest_runtime::decoded_batch::DecodedRecords;
use opendata_ingest_runtime::error::{RuntimeError, RuntimeResult};
use opendata_ingest_runtime::idempotency::IdempotencyKey;
use opendata_ingest_runtime::sink::{
    CommitStatus, Sink, SinkBudget, SinkCommit, SinkCommitFailure, SinkCommitResult, SinkId,
};

use crate::adapter::Adapter;
use crate::adapter::logs::OtlpLogsClickHouseAdapter;
use crate::writer::{ClickHouseWriter, WriterErrorClass};

pub struct ClickHouseSink {
    id: SinkId,
    adapter: Arc<OtlpLogsClickHouseAdapter>,
    writer: Arc<ClickHouseWriter>,
    budget: SinkBudget,
}

impl ClickHouseSink {
    pub fn new(
        id: impl Into<SinkId>,
        adapter: Arc<OtlpLogsClickHouseAdapter>,
        writer: Arc<ClickHouseWriter>,
    ) -> Self {
        Self {
            id: id.into(),
            adapter,
            writer,
            budget: SinkBudget::default(),
        }
    }

    pub fn with_budget(mut self, budget: SinkBudget) -> Self {
        self.budget = budget;
        self
    }
}

fn fatal(msg: impl Into<String>) -> SinkCommitFailure {
    SinkCommitFailure::Fatal(msg.into().into())
}

#[async_trait]
impl Sink for ClickHouseSink {
    fn id(&self) -> &SinkId {
        &self.id
    }

    fn write_budget(&self) -> SinkBudget {
        self.budget
    }

    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure> {
        let SinkCommit {
            low_sequence,
            high_sequence,
            records,
            record_indices,
            ..
        } = commit;

        let DecodedRecords::Typed(typed) = records;
        let logs = typed
            .as_any()
            .downcast_ref::<TypedDecodedLogs>()
            .ok_or_else(|| {
                fatal(format!(
                    "ClickHouseSink expects TypedDecodedLogs (schema {}), got {:?}",
                    opendata_ingest_otel::logs::OTLP_LOGS_SCHEMA_NAME,
                    typed.schema(),
                ))
            })?;

        let selected: Vec<DecodedLogRecord> = match record_indices.as_deref() {
            Some(indices) => indices
                .iter()
                .map(|&i| {
                    logs.records()
                        .get(i as usize)
                        .cloned()
                        .ok_or_else(|| {
                            fatal(format!(
                                "ClickHouseSink: record_indices contains out-of-range index {i} (records.len() = {})",
                                logs.records().len()
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
            None => logs.records().to_vec(),
        };

        let bytes: usize = selected.iter().map(|r| r.approx_size_bytes()).sum();
        let group = CommitGroupBatch {
            records: selected,
            low_sequence,
            high_sequence,
            bytes,
        };

        let chunks = self
            .adapter
            .plan(group)
            .map_err(|e| SinkCommitFailure::Fatal(Box::new(e)))?;

        let bytes_written: u64 = chunks
            .iter()
            .map(|c| c.rows.iter().map(|r| r.len() as u64).sum::<u64>())
            .sum();
        let rows_written: u64 = chunks.iter().map(|c| c.rows_count() as u64).sum();

        match self.writer.execute_all(&chunks).await {
            Ok(()) => Ok(SinkCommitResult {
                bytes_written,
                rows_written,
            }),
            Err(e) => match e.class() {
                WriterErrorClass::Retryable => {
                    // ClickHouse can ambiguously commit on retryable
                    // outcomes (timeout after request body sent,
                    // 5xx after server-side commit); promote to
                    // MaybeCommitted so the runtime calls
                    // check_committed before the next attempt.
                    Err(SinkCommitFailure::MaybeCommitted(Box::new(e)))
                }
                WriterErrorClass::NonRetryable => Err(SinkCommitFailure::Fatal(Box::new(e))),
            },
        }
    }

    async fn check_committed(&self, _key: &IdempotencyKey) -> RuntimeResult<CommitStatus> {
        // Alpha ClickHouse dedupes at the table layer
        // (`ReplacingMergeTree(_adapter_version)`); the short-
        // window `insert_deduplication_token` is gone by the time
        // the runtime asks. Returning `Unknown` is RFC 0002 rev 5's
        // documented contract for this case.
        let _ = &self.adapter;
        let _ = &self.writer;
        Ok::<CommitStatus, RuntimeError>(CommitStatus::Unknown)
    }
}
