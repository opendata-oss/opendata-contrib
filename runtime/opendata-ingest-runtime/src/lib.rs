//! Sink-neutral ingest runtime.
//!
//! Phase 4.2 landed the trait skeletons; Phase 4.4c-0 aligned the
//! crate to RFC 0002 rev 6 (single-sink scope) + rev 8 (concrete
//! source side). Public trait surface today: [`decoder::Decoder`] and
//! [`sink::Sink`]. The source side is concrete: the runtime owns a
//! [`source::BufferSource`] + `Clone` `BufferSourceFetchHandle`.
//! Concrete sink implementations (`ClickHouseSink`, etc.) live in
//! their own plugin crates.
//!
//! Runtime logical commit identity is the [`identity::CommitIdentity`]
//! struct projection — see RFC 0002 §Runtime/Sink Boundary. Sink-
//! physical dedupe tokens are sink-owned and never cross the runtime
//! surface.

pub mod ack_coordinator;
pub mod decoded_batch;
pub mod decoder;
pub mod envelope;
pub mod error;
pub mod identity;
pub mod metrics;
pub mod runtime;
pub mod sink;
pub mod source;
pub mod source_budget;
