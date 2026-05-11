//! Runtime-level error type. Layer-specific errors flow through
//! [`RuntimeError`] so the orchestration loop in `runtime.rs` can
//! dispatch on a single shape.

use thiserror::Error;

pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Boxed error type used inside [`crate::sink::SinkCommitFailure`]
/// per RFC 0002 rev 6 §`Sink`.
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
    /// string names the offending method and the phase that fills it
    /// in. Per Phase 4 design rev 2 §Open Question 2, runtime code
    /// returns this rather than `panic!()`.
    #[error("unsupported in this runtime version: {0}")]
    Unsupported(&'static str),
}
