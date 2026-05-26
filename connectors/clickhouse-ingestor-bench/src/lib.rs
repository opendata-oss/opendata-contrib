//! Bench harness for the OpenData → ClickHouse ingestor.
//!
//! The primary surface is the in-memory `correctness.json` smoke
//! run: five [`ack_invariant_checks`] driven against an in-memory
//! `ObjectStore` + scripted [`BenchSink`]. The harness writes its
//! artifacts under
//! `bench-results/phase06/correctness-smoke/<UTC>-phase6-smoke/`.
//! Real ClickHouse + S3 runs are operator work; the binary in
//! `src/main.rs` is wired for the smoke run.
//!
//! [`ack_invariant_checks`]: correctness::CheckName

pub mod correctness;
pub mod fixtures;
pub mod metrics_recorder;
pub mod output;
#[cfg(feature = "real-ch")]
pub mod real_ch;
pub mod stage_latencies;
pub mod test_observable_sink;
pub mod witness;
