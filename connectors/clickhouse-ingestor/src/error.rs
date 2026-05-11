//! Top-level error type for the ingestor binary's wiring code.
//!
//! Phase 4 retired the in-tree runtime; this error enum used to be
//! the dispatch point for the serial runtime loop. After 4.4e it
//! shrinks to just the variants the config loader and integration
//! tests still spell.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestorError {
    #[error("metadata envelope: {0}")]
    Envelope(#[from] crate::envelope::EnvelopeError),

    #[error("signal decoder: {0}")]
    SignalDecode(String),

    #[error("adapter: {0}")]
    Adapter(String),

    #[error("clickhouse writer: {0}")]
    Writer(#[from] crate::writer::WriterError),

    #[error("config: {0}")]
    Config(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type IngestorResult<T> = Result<T, IngestorError>;
