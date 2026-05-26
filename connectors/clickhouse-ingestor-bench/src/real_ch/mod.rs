//! Real-ClickHouse bench harness.
//!
//! Feature-gated `real-ch` module that spins up a real ClickHouse
//! via `testcontainers-rs` and runs the runtime against it
//! end-to-end. The production `ClickHouseSink<OtlpLogsClickHouseAdapter>`
//! is wired into `Runtime::builder`; the bench reads OTLP-logs
//! protobuf payloads out of an in-memory Buffer, decodes them, and
//! writes them to the live ClickHouse table.
//!
//! The bench measures throughput and per-stage latency against a
//! working real-CH pipeline and writes the v2 artifact bundle. A
//! TestObservableSink trait and correctness-check refactor (needed
//! to run `ack_invariant_checks` against the production sink) is a
//! follow-up; this harness focuses on a real benchmark number
//! measurable end-to-end.

pub mod fixture;
pub mod matrix;
pub mod real_clickhouse_sink;
pub mod runner;
pub mod workload;

pub use real_clickhouse_sink::RealClickHouseSink;

pub use fixture::{FixtureError, RealClickHouseFixture};
pub use matrix::{
    MatrixConfig, MatrixPoint, MatrixPointResult, MatrixRunArtifacts, backfill_parent_aggregates,
    build_matrix_correctness, build_matrix_results, build_matrix_timeseries, run_matrix,
};
pub use runner::{RealChConfig, RealChIterationReport, run_real_ch};
pub use workload::{LogWorkload, LogWorkloadConfig, WorkloadHandle};
