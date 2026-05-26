//! Top-level error type for the ingestor binary's wiring code.
//!
//! Carries the variants the config loader and integration tests
//! spell.

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
