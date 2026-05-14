//! Phase 6 bench harness for the OpenData → ClickHouse ingestor.
//!
//! Today's surface is the in-memory `correctness.json` smoke run
//! (Phase 6.x closeout §1.1): five [`ack_invariant_checks`] driven
//! against an in-memory `ObjectStore` + scripted [`BenchSink`].
//! The harness writes its artifacts under
//! `bench-results/phase06/correctness-smoke/<UTC>-phase6-smoke/`
//! per `plans/odb-high-throughput/benchmarks.md` §Output Directory
//! Layout. Real ClickHouse + S3 runs are operator work per
//! `plans/odb-high-throughput/local-development.md` §Running the
//! patched build; the binary in `src/main.rs` is wired only for
//! the smoke today.
//!
//! [`ack_invariant_checks`]: correctness::CheckName

pub mod correctness;
pub mod fixtures;
pub mod metrics_recorder;
pub mod output;
pub mod stage_latencies;
pub mod witness;
