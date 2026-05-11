//! Sink-neutral ingest runtime.
//!
//! Phase 4.2 landed the trait skeletons; Phase 4.4c-0 aligned the
//! crate to RFC 0002 rev 6 (single-sink scope) + rev 8 (concrete
//! source side). Public trait surface today: `Decoder`, `Sink`,
//! `IdempotencyContract`. The source side is concrete: the runtime
//! owns a `BufferSource` + `Clone` `BufferSourceFetchHandle` (lands
//! in Phase 4.4c). Concrete sink implementations
//! (`ClickHouseSink`, etc.) live in their own plugin crates.
//!
//! Trait methods that v1 cannot reach return
//! [`error::RuntimeError::Unsupported`]; per the design doc rev 2
//! §Open Question 2, no `panic!()` lives in production paths.

pub mod ack_coordinator;
pub mod commit_group;
pub mod decoded_batch;
pub mod decoder;
pub mod envelope;
pub mod error;
pub mod idempotency;
pub mod metrics;
pub mod sink;
pub mod source;
