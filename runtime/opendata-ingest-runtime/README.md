# opendata-ingest-runtime

Sink-agnostic, pipelined runtime that reads from an OpenData Buffer and writes to a configured sink. Implements [RFC 0002](../../rfcs/0002-generic-ingest-runtime.md).

The runtime owns the source side: one Buffer consumer with parallel fetch, byte-budgeted backpressure, and contiguous-completion ack coordination. The sink and the signal decoder are pluggable through two traits. The first published sink is `opendata-ingest-clickhouse` paired with the OTLP-logs decoder in `opendata-ingest-otel`.

## What it does

For each configured Buffer source, the runtime runs three pipelined stages with bounded queues between them:

1. **Fetch.** N workers per source pull `BatchDescriptor`s through `buffer::Consumer::next_descriptors`, then call `fetch_handle().fetch(d)` concurrently against the same fetch handle. Wraps the RFC 0003 read-ahead API directly.
2. **Decode.** M workers per source call `Decoder::decode`, producing one or more `DecodedBatch`es per input batch. Decoders own per-entry metadata parsing.
3. **Sink commit.** A shared writer pool calls `Sink::write(SinkCommit)` for each decoded range. The runtime branches on `SinkCommitFailure`: `MaybeCommitted` triggers a `check_committed` lookup; `NotCommitted` retries; `Fatal` halts.

A per-source `AckCoordinator` tracks contiguous completion: it acks the Buffer frontier through the highest source sequence whose sink commit succeeded *and* whose every lower sequence already committed. Out-of-order sink completion is normal; the durable frontier moves only when a contiguous run completes.

In-flight bytes are bounded per source. Admission reserves pessimistically (`estimated_max_batch_bytes`), the decode worker reconciles to the actual post-decode size, and the sink stage releases on commit. When the budget is exhausted, the admission arm parks until bytes are released; `runtime_backpressure_reason` records which budget caused the park.

K>1 admission amortizes manifest reads. Per admission cycle, the arm acquires up to `max_descriptors_per_poll` batch-and-byte gates, then calls `next_descriptors(K)` once. Descriptors register with the ack coordinator in source-sequence order before leaving the arm, so the contiguous-completion invariant holds at any K.

## Trait surface

Two traits, both `Send + Sync + 'static`:

```rust
#[async_trait]
pub trait Sink: Send + Sync + 'static {
    fn id(&self) -> &SinkId;
    fn write_budget(&self) -> SinkBudget;
    async fn write(&self, commit: SinkCommit) -> Result<SinkCommitResult, SinkCommitFailure>;
    async fn check_committed(&self, identity: &CommitIdentity) -> RuntimeResult<CommitStatus>;
}

pub trait Decoder: Send + Sync + 'static {
    fn accepts(&self, raw_metadata: &[u8]) -> bool;
    fn decode(&self, batch: SourceBatch) -> RuntimeResult<Vec<DecodedBatch>>;
}
```

`Sink::write` is the source-range commit unit: one call covers one source sequence range. `Ok(_)` means the full commit is durable; retry of the same `SinkCommit` must be idempotent. `check_committed` is called on replay (after a crash) or after a `MaybeCommitted` failure (ambiguous response from the sink); sinks that cannot tell return `CommitStatus::Unknown`, and the runtime retries the `write` and relies on the sink's table-level dedupe.

`Decoder::decode` consumes one `SourceBatch` and emits one or more `DecodedBatch`es covering the input sequence range. It is synchronous and must be fast (microsecond-scale); slow CPU work belongs in pre-computed state at decoder construction, not inside `decode`. The runtime treats per-entry metadata as opaque bytes; the decoder parses it.

## Identity model

Three identities cross the runtime/sink boundary:

1. The Buffer progress identity, `last_acked_sequence` on the Buffer consumer. Owned by the source.
2. The runtime logical commit identity, `CommitIdentity { source, sink, range, schema_version }`. A deterministic projection, byte-identical across replay. Owned by the runtime; consumed by the sink.
3. The sink physical dedupe identity: physical tokens, file paths, manifest entries. Owned by the sink, derived from `CommitIdentity` plus adapter config. The runtime never inspects sink-physical tokens.

`CommitIdentity` is the sink's input for idempotency. The same `(source, sink, range, schema_version)` produces a byte-identical struct on replay, so a sink deriving its physical dedupe token from `CommitIdentity` gets the same token every time the runtime retries the same range.

## Configuration

`RuntimeOptions` is the top-level config struct. It derives `Serialize` / `Deserialize` so a host service can embed it in its own config format.

```rust
pub struct RuntimeOptions {
    pub ack_flush_policy: AckFlushPolicy,
    pub dry_run: bool,
    pub poll_interval: Duration,
    pub max_descriptors_per_poll: usize,   // default 8 (K>1 amortization)
    pub max_retry_attempts: u32,
    pub retry_backoff: Duration,
    pub source_defaults: SourceBackpressureOptions,
    pub source_overrides: HashMap<SourceId, SourceBackpressureOptions>,
    pub sink: SinkPoolOptions,
}
```

Per-source backpressure and pool sizing:

```rust
pub struct SourceBackpressureOptions {
    pub max_inflight_batches: u32,        // default 64
    pub max_inflight_bytes: u64,          // default 256 MiB
    pub estimated_max_batch_bytes: u64,   // default 4 MiB; admission reservation
    pub fetch_concurrency: u32,           // default 8
    pub decode_concurrency: u32,          // default 4
    pub oversize_fault_multiplier: u32,   // default 4; decode-time cap
}
```

Shared sink writer pool:

```rust
pub struct SinkPoolOptions {
    pub max_concurrent_commits: u32,      // default 4
    pub retry_max_attempts: u32,          // default 6
    pub retry_initial_backoff_ms: u64,    // default 100
}
```

Ack flush policy:

```rust
pub enum AckFlushPolicy {
    EveryCommitGroup,        // default: replay window = 1 source range
    EveryN { n: u32 },       // operator opt-in: throughput at cost of explicit replay window
}
```

A serial-equivalent profile is available via `SourceBackpressureOptions::serial()` (sets `max_inflight_batches`, `fetch_concurrency`, `decode_concurrency` to 1). Useful for tests that want to assert behavior without parallelism.

## Metrics

The runtime emits a typed `prometheus-client` family. The host bin constructs `Arc<RuntimeMetrics>`, registers it against a shared `Registry`, and passes it to `RuntimeBuilder::with_runtime_metrics`. Names as registered:

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `runtime_backpressure_reason` | counter | source, reason | Backpressure events by cause: `source_budget`, `decode_budget`, `sink_budget`, `retrying`, `fatal_error` |
| `runtime_descriptors_handed_out` | counter | source | Descriptors admitted into the pipeline |
| `runtime_admission_next_descriptors_calls` | counter | source | Admission-arm invocations of `next_descriptors`; `descriptors_handed_out / calls` is K_effective |
| `runtime_admission_descriptors_per_call` | histogram | source | Distribution of K per admission cycle |
| `runtime_admission_extension_releases` | counter | source | Excess gates released when the buffer returned fewer descriptors than the loop acquired |
| `ingestor_bytes_fetched` | counter | source | Bytes pulled from the buffer source |
| `ingestor_records_decoded` | counter | source | Records produced by the decoder |
| `runtime_sink_commits` | counter | source, sink, result | Sink commit attempts: `committed`, `verified_already_committed`, `failed_retryable`, `failed_fatal` |
| `runtime_ack_frontier` | gauge | source | Highest source sequence acked through |
| `runtime_pending_ranges` | gauge | source | Pending commit ranges in the coordinator |
| `buffer_consumer_sequence_lag` | gauge | source | `head_sequence − last_acked_sequence` at last manifest interaction |
| `runtime_stage_queue_depth` | gauge | stage, source | Pending items in each per-stage channel |
| `runtime_stage_inflight_bytes` | gauge | stage, source | In-flight bytes per stage |
| `runtime_stage_latency_seconds` | histogram | stage, source | Per-batch stage latency, 1 ms to 30 s buckets |
| `runtime_sink_queue_depth` | gauge | sink | Pending sink commits in the dispatch queue |
| `runtime_sink_inflight_bytes` | gauge | sink | In-flight bytes pinned by concurrent sink commits |
| `runtime_ack_lag_seconds` | histogram | source | Time spent waiting on ack-through coordination |

## Wiring a new sink

The runtime is a library. A host binary constructs a `BufferSource`, a `Decoder`, a `Sink`, and a `RuntimeOptions`, then runs the builder:

```rust
use std::sync::Arc;
use opendata_ingest_runtime::{
    runtime::{Runtime, RuntimeOptions},
    sink::Sink,
    decoder::Decoder,
    source::BufferSource,
    metrics::RuntimeMetrics,
};
use prometheus_client::registry::Registry;

let source = BufferSource::new(/* SourceId + ConsumerConfig */).await?;
let decoder: Arc<dyn Decoder> = Arc::new(MyDecoder::new());
let sink: Arc<dyn Sink> = Arc::new(MySink::new(/* sink config */).await?);

let mut registry = Registry::default();
let metrics = Arc::new(RuntimeMetrics::new());
metrics.register(&mut registry);

let runtime = Runtime::builder()
    .with_source(source)
    .with_decoder(decoder)
    .with_sink(sink)
    .with_options(RuntimeOptions::default())
    .with_runtime_metrics(metrics)
    .build()?;

runtime.run().await?;
```

A new sink (Iceberg, another database, a custom file format) is a fresh crate implementing `Sink`. The contract a `Sink` implementation has to honor:

- Return a stable `SinkId` and a `SinkBudget` reflecting the sink's safe-concurrent-write ceiling.
- Make `write(commit)` idempotent under retry of the *same* `SinkCommit`. Derive any physical dedupe token deterministically from `commit.identity`.
- Distinguish `NotCommitted`, `MaybeCommitted`, and `Fatal` failure modes accurately. `MaybeCommitted` is for any response that leaves the sink's state ambiguous: timeout after request body sent, connection drop after success, 200 OK dropped on the network.
- Implement `check_committed(&identity)` against the sink's own state. Return `Unknown` when the sink cannot tell from the identity alone; the runtime then retries and relies on table-level dedupe.

See the ClickHouse sink in `plugins/opendata-ingest-clickhouse/src/` for a working implementation.

## Relationship to RFC 0002

[RFC 0002](../../rfcs/0002-generic-ingest-runtime.md) is the design source of truth: full trait specifications, the backpressure model, the invariants the runtime preserves (`INV-ADMISSION-CONTIGUOUS`, ack ordering), failure modes, and the alternatives considered. This README documents the public surface as shipped.
