//! Sink-neutral ingest runtime.
//!
//! Implements RFC 0002 (single-sink scope, concrete source side).
//! Public trait surface: [`decoder::Decoder`] and [`sink::Sink`]. The
//! source side is concrete: the runtime owns a
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
pub mod error;
pub mod identity;
pub mod metrics;
pub mod runtime;
pub mod sink;
pub mod source;
pub mod source_budget;
