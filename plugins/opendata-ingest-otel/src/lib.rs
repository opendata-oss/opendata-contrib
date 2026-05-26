//! OTLP signal decoders for the generic ingest runtime.
//!
//! Homes the OTLP logs decoder and the
//! `opendata_ingest_runtime::Decoder` trait impl that connects it to
//! the runtime. The runtime crate never depends on this crate; the
//! dependency runs OTel→runtime only.

pub mod logs;
