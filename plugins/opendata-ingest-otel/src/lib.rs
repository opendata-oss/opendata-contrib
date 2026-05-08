//! OTLP signal decoders for the generic ingest runtime.
//!
//! Phase 4.4a homes the OTLP logs decoder body here. The
//! `opendata_ingest_runtime::Decoder` trait impl that connects this
//! decoder to the runtime lands in Phase 4.4b. The runtime crate
//! never depends on this crate; the dependency runs OTel→runtime
//! only.

pub mod logs;
