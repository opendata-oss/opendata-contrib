//! Sink-neutral ingest runtime.
//!
//! Phase 4.2 lands the trait skeletons (`SourceReader`,
//! `SourceFetchHandle`, `Decoder`, `Router`, `Sink`,
//! `IdempotencyContract`, `AckCoordinator`) per RFC 0002 rev 5 + the
//! Phase 4 design doc
//! `plans/odb-high-throughput/phase04-runtime-extraction-design.md`.
//! Concrete implementations (`BufferSourceReader`, `OtlpLogsDecoder`,
//! `ClickHouseSink`) and the orchestration loop land in Phases 4.3
//! and 4.4.
//!
//! Trait methods that v1 cannot reach return
//! [`error::RuntimeError::Unsupported`]; per the design doc rev 2
//! §Open Question 2, no `panic!()` lives in production paths.

pub mod ack_coordinator;
pub mod decoded_batch;
pub mod decoder;
pub mod envelope;
pub mod error;
pub mod idempotency;
pub mod metrics;
pub mod router;
pub mod sink;
pub mod source;
