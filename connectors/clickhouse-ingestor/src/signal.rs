//! Transitional `SignalDecoder` trait.
//!
//! Phase 4.4a moved the OTLP logs decoder body to
//! `opendata-ingest-otel::logs`. The `SignalDecoder` trait stays
//! here for as long as `BufferConsumerRuntime` does — it is the
//! seam the existing serial runtime depends on. Phase 4.4c
//! replaces both with `opendata_ingest_runtime::Decoder` plus
//! `Runtime`.

pub use opendata_ingest_otel::logs::{
    DecodedLogRecord, DecodedLogs, OtelDecodeError, OtlpLogsDecoder, SourceCoordinates,
};
use opendata_ingest_runtime::envelope::MetadataEnvelope;
use opendata_ingest_runtime::source::SourceBatch;

use crate::error::{IngestorError, IngestorResult};

pub trait SignalDecoder {
    type Output;

    /// Decode a batch of entries. Envelopes are passed in lockstep
    /// with `batch.entries` so the decoder can dispatch on per-entry
    /// envelopes in future implementations; the alpha relies on
    /// [`opendata_ingest_runtime::envelope::validate_consistent`]
    /// having already enforced uniformity within a batch.
    fn decode(
        &self,
        batch: &SourceBatch,
        envelopes: &[MetadataEnvelope],
    ) -> IngestorResult<Self::Output>;
}

impl SignalDecoder for OtlpLogsDecoder {
    type Output = Vec<DecodedLogRecord>;

    fn decode(
        &self,
        batch: &SourceBatch,
        _envelopes: &[MetadataEnvelope],
    ) -> IngestorResult<Self::Output> {
        self.decode_logs(batch).map_err(Into::into)
    }
}

impl From<OtelDecodeError> for IngestorError {
    fn from(e: OtelDecodeError) -> Self {
        IngestorError::SignalDecode(e.to_string())
    }
}
