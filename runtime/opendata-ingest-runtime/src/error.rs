//! Runtime-level error type. Layer-specific errors flow through
//! [`RuntimeError`] so the orchestration loop in `runtime.rs` can
//! dispatch on a single shape.

use thiserror::Error;

pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Boxed error type used inside [`crate::sink::SinkCommitFailure`]
/// per RFC 0002 §`Sink`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("source: {0}")]
    Source(BoxError),

    #[error("decoder: {0}")]
    Decoder(BoxError),

    #[error("sink: {0}")]
    Sink(BoxError),

    #[error("idempotency: {0}")]
    Idempotency(BoxError),

    #[error("ack coordinator: {0}")]
    Ack(String),

    #[error("config: {0}")]
    Config(String),

    /// A trait method that v1 cannot reach was called. The static
    /// string names the offending method. Runtime code returns this
    /// rather than `panic!()`.
    #[error("unsupported in this runtime version: {0}")]
    Unsupported(&'static str),

    /// A pipeline stage encountered an unrecoverable structural
    /// violation that the per-source actor cannot resolve. Covers
    /// lost descriptors (INV-DESCRIPTOR-LOSS-FATAL), a fetch /
    /// decode worker panic, channel close on a non-shutdown path,
    /// oversize decoded batches, and decoded ranges that disagree
    /// with the admitted source range.
    #[error("pipeline error: {0}")]
    Pipeline(String),
}
