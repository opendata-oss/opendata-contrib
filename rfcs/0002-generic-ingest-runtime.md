# RFC 0002: Generic Ingest Runtime

**Status**: Accepted

**Authors**:

- Apurva Mehta

## Summary

This RFC defines `opendata-ingest-runtime`, a runtime and trait API for
writing OpenData Buffer streams into a configured sink. It is the
sink-neutral generalization of the shipped ClickHouse ingestor.

The runtime solves three problems:

1. **Generalize to many sink targets.** Sink-specific code — ClickHouse
   today — is reduced to a `Sink` trait plus a decoder. The polling,
   decode orchestration, retry, ack, and backpressure machinery is
   shared and sink-neutral, so a new sink (Iceberg, another database) is
   a new crate implementing the trait, not a fork of the ingestor.
2. **Idempotent writes.** The runtime gives each Buffer source range a
   unique, deterministic identity (`CommitIdentity`), so a sink can make
   its writes idempotent under retry and crash-replay. At-least-once
   delivery from Buffer plus a deterministic identity is what lets a sink
   achieve exactly-once *effect* without the runtime having to guarantee
   exactly-once delivery.
3. **End-to-end pipelining.** Fetch, decode/encode, and write run as
   concurrent stages with bounded queues and a shared in-flight byte
   budget, and a per-source ack coordinator advances the Buffer ack
   frontier only after the sink has durably committed the corresponding
   range. This lets one service approach single-node network or
   sink-ingest limits. Parallel fetch and bulk ack build on the Buffer
   read-ahead API (opendata-buffer RFC 0003).

A runtime service hosts one or more Buffer sources writing to **one
configured sink**. To deliver the same upstream data to two sinks, run
two services with two Buffer queues; independent sinks do not share a
process, ack frontier, memory budget, or retry loop.

The shape is analogous to Kafka Connect, which provides a runtime and an
API so many systems can be connected without re-writing the plumbing
each time — here narrowed to sink connectors that read Buffer batch
files and manifests from object storage.

The first sink ported onto the runtime is the ClickHouse ingestor
(opendata-contrib RFC 0001), with no intentional behavior change.

## Motivation

We want an API and runtime for writing OpenData Buffer streams into
arbitrary downstream systems — the way Kafka Connect provides a runtime
and an API for moving data between Kafka and many systems, narrowed here
to sink connectors over Buffer. Three things drive the design:

- **Reach.** Different systems (ClickHouse today; Iceberg and others
  next) should be writable from Buffer without each one re-implementing
  polling, decode, retry, ack, and backpressure.
- **Throughput.** The runtime should be built for very high-throughput
  workloads — parallel fetch, parallel decode, parallel writes — not a
  serial poll-decode-write loop.
- **Idempotency.** The API should encode enough about each Buffer range
  that a sink connector can make its writes idempotent, so retries and
  crash-replay don't duplicate data.

The shipped ClickHouse ingestor (opendata-contrib RFC 0001) already does
all of this for one sink, but the machinery is fused to that sink:

1. The polling loop, commit grouping, and ack control live in the
   `clickhouse-ingestor` crate. A second sink would duplicate them or
   depend on a ClickHouse crate just to reuse them.
2. The decoded unit is a ClickHouse-shaped row (`Vec<RowValue>`) —
   row-oriented and JSON-leaning, not a useful interchange shape for a
   columnar sink or a binary ClickHouse path.
3. There is one source, one serial decode path, one writer pass — no
   per-source ack coordinator and no source/decoder/sink boundary.
4. The Buffer consumer API is serial: `next_batch` fuses manifest read,
   object fetch, and decode in one call, capping source throughput
   regardless of downstream concurrency.
5. Backpressure is implicit in the synchronous loop — no shared byte
   budget and no surfaced "backpressure reason".
6. The schema and target table are compiled in, so an operator can't
   retarget the same logs to a different table or sink without forking
   the binary.

The fix is to pull the runtime, ack control, and pipeline scaffolding
into their own crate, define the trait surface a sink plugs into, and
re-host the ClickHouse path on top of it without intentional behavior
change. That separates the correctness work (per-source ack frontier,
single-sink commit invariants, deterministic commit identity) from the
throughput work (parallel fetch, parallel decode).

## Goals

- Define a sink-neutral runtime crate, `opendata-ingest-runtime`, that
  owns polling, decode orchestration, retry, ack, and backpressure. The
  runtime hands the sink one commit unit per source range; how that
  becomes one or many physical writes is the sink's concern.
- Define the trait surface for decoding and for sinks, with no
  sink-specific types on the runtime boundary, plus a deterministic
  commit identity (`CommitIdentity`) for each source range that a sink
  can derive idempotent write tokens from.
- Define the per-source `AckCoordinator` state machine and the
  single-sink ack invariant:

  > For each source, ack advances only after the configured sink has
  > durably committed or verified prior commit for the relevant source
  > sequence range.

- Define the bounded-stage pipeline with a shared byte budget and
  source-aware fairness, so sink slowdown pauses source pulls without
  unbounded memory growth and one hot source can't permanently starve a
  low-volume source on the shared sink writer pool.
- Define pluggability: a sink is a crate that implements the `Sink`
  trait and registers by name, so adding a sink needs no runtime change.
- Define the configuration shape that supports **multiple sources and
  one sink in one process**, with independent per-source ack frontiers.
- Set validation criteria for the runtime extraction and for the
  pipelined runtime.

## Non-Goals

- Concrete sink implementations. The ClickHouse and Iceberg sinks each
  have their own RFCs and crates. This RFC defines what they plug into.
- **Same-process multi-sink fanout.** A runtime service has exactly one
  configured sink. Independent sinks deploy as separate runtime services
  with their own Buffer queues; producer-side queue duplication is the
  documented pattern for delivering the same upstream stream to two
  sinks.
- **Coordinating ack across two sinks (e.g. ClickHouse and Iceberg) in
  one runtime service.** Independent sinks should not share a runtime
  ack frontier, memory budget, retry loop, or process liveness.
- **Per-sink "skip-and-record" data-loss policy.** Without an explicit
  operator-facing data-loss policy, silently dropping a range from one
  sink is worse than halting; the runtime chooses safety, and a single
  sink per service avoids the question.
- Buffer producer-side concerns. Producer parallelism, exporter
  configuration, and manifest commit coordination are separate work in
  the `opendata-go` repo.
- Buffer wire format or manifest semantics. The descriptor read-ahead
  and `ack_through` API are specified in a separate `opendata`
  RFC; this RFC consumes them.
- Schema evolution and DDL ownership. Target table schemas are applied
  out of band; server-owned migrations are not in scope.
- WASM, dylib, or subprocess plugin ABIs. Deferred until the native
  hot path is measured. Mentioned here so the trait shapes do not
  preclude them.
- Exactly-once delivery as a runtime guarantee. The runtime preserves
  at-least-once and lets sinks provide their own idempotent commit
  semantics, derived deterministically from `CommitIdentity` plus the
  sink's own adapter configuration.

### Deployment Guidance for Independent Sinks

To write the same upstream stream into two independent sinks, define
two service configs with separate Buffer manifests and run two
processes:

```text
producer -> Buffer queue A -> runtime service A -> ClickHouse
         -> Buffer queue B -> runtime service B -> Iceberg
```

The producer side duplicates the OTel payload into both queues. Each
runtime service is a single-sink service with its own ack frontier,
memory budget, retry loop, and process. A slow or fatal sink can stall
its own source pulls without dragging down the other sink's pipeline.

## Background

This RFC builds on:

- **opendata-buffer RFC 0001 (Stateless Buffer)**: producer/consumer
  contract for object-storage-backed queues, including `Consumer::ack`'s
  strict in-order requirement and `flush()`'s durable dequeue.
- **opendata-buffer RFC 0003 (Read-Ahead and `ack_through`)**: adds
  `Consumer::next_descriptors`, concurrent-safe descriptor fetch, and
  `ack_through(sequence)`. The runtime depends on this for parallel
  fetch and bulk ack.
- **opendata-contrib RFC 0001 (ClickHouse Ingestor)**: shipped layering
  for the ClickHouse logs path. The generic runtime preserves that
  layering and generalizes only what must change to support a
  sink-neutral runtime that can host different sink types (one per
  service).

The relevant Buffer types (read-ahead form, `Consumer` calls collapsed
to what the runtime uses):

```rust
pub struct BatchDescriptor {
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
}

impl Consumer {
    pub async fn next_descriptors(&mut self, max: usize)
        -> Result<Vec<BatchDescriptor>>;
    pub fn fetch_handle(&self) -> ConsumerFetchHandle;
    pub async fn ack_through(&mut self, sequence: u64) -> Result<()>;
    pub async fn flush(&mut self) -> Result<()>;
}

#[derive(Clone)]
pub struct ConsumerFetchHandle { /* Arc<dyn ObjectStore> + manifest_path */ }

impl ConsumerFetchHandle {
    pub async fn fetch(&self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;
}
```

The runtime relies on three properties from this contract:

- **One active consumer per manifest**, enforced by epoch fencing. The
  runtime's per-source state machine assumes exclusive write access to
  ack state.
- **Manifest mutation is serialized**, but descriptor fetch is
  concurrency-safe. This is what makes parallel object fetch possible
  without changing manifest semantics.
- **Per-range metadata is per-entry, not per-batch**. The runtime
  flattens the per-entry metadata onto each materialized entry (see
  "Source Side" below). Buffer stores this metadata as an **opaque byte
  payload** — it never interprets it — and the runtime carries it
  through unchanged. Any envelope structure inside those bytes is a
  producer↔decoder convention the decoder owns, not part of the Buffer
  wire format or the runtime.

## Design

### Architecture

The runtime owns the horizontal stages between Buffer and the configured
sink. N source pipelines feed one shared sink writer pool:

```text
                          ╔═══ ingest-runtime process (1 sink) ════════════════════════════════════════════════════════════════════════╗
                          ║                                                                                                            ║
                          ║   ┌──────────────┐   ┌──────────────┐   ┌─────────────────┐                                                 ║
                          ║   │ Descriptor   │   │  Fetch+      │   │ Envelope +      │                                                 ║
                       ┌──╫───▶  poller      ├───▶  decompress  ├───▶ decode          ├──┐                                              ║
                       │  ║   │ (per source) │   │   workers    │   │   workers       │  │                                              ║
╔══Object Storage══╗   │  ║   └──────┬───────┘   └──────────────┘   └─────────────────┘  │ one SinkCommit                              ║
║                  ║   │  ║          │ N source pipelines                                │ per source range                            ║
║   Manifest(s) +  ║   │  ║          │                                                   ▼                                              ║
║   Batches        ╞══►┘  ║          │                                          ┌──────────────────┐                                    ║
║                  ║      ║          │                                          │ Shared sink      │                                    ║
╚═════════▲════════╝      ║          │                                          │ writer pool      │                                    ║   ╔══Sink═════════════╗
          │               ║          │                                          │ (with source     ├────────────────────────────────────╫───▶ ClickHouse OR     ║
          │               ║          │                                          │  fairness)       │                                    ║   ║ Iceberg OR fake   ║
          │               ║          │                                          └────────┬─────────┘                                    ║   ╚════════════════════╝
          │               ║          │                                                   │
          │               ║          │                                                   ▼                                              ║
          │               ║          │                                          ┌──────────────────┐                                    ║
          │               ║          └────── per-source ack frontier ──────────▶│ AckCoordinator   │                                    ║
          └───────────────╫───────── (only after configured sink commit for     │ (one per source) │                                    ║
                          ║          that range)                                └──────────────────┘                                    ║
                          ║                                                                                                            ║
                          ╚════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
```

Decode hands each `DecodedBatch` straight to the shared sink writer
pool as one `SinkCommit` per source range. There is no intermediate
accumulation stage on the runtime surface; if a sink wants to batch or
chunk, it does so inside the plugin.

What is generic vs. plugin:

| Layer | Owner |
|---|---|
| Source descriptor poller, fetch workers, decompression | Runtime (`BufferSource`) |
| Per-entry envelope materialization | Runtime |
| Decoder | Plugin (`Decoder`) |
| Logical commit identity (`CommitIdentity`) | Runtime |
| Sink write, physical idempotency tokens, chunk/file planning, retry classification | Plugin (`Sink`) |
| Per-source `AckCoordinator`, source ack/flush, multi-source fairness across the shared sink writer | Runtime |
| Metrics scaffolding | Runtime; plugins may add labeled metrics |

The runtime never inspects payload bytes after decode. The plugins
never call `Consumer::ack` or touch backpressure budgets directly.

Routing concepts (e.g. attribute-based table selection inside one sink)
are out of the runtime hot path. If different sources in the same
service need to land in different physical tables of the configured
sink, that is handled at the sink/schema-mapping layer (see
"Configuration Shape" and "Future Improvements"), not as a runtime
trait.

### Runtime/Sink Boundary

Three identities cross the runtime/plugin interface and must remain
distinct:

1. **Buffer/source progress identity** — `last_acked_sequence` on the
   Buffer `Consumer`. Owned by the source; durable across restarts;
   advances only after the runtime confirms a contiguous prefix of
   ranges has committed.
2. **Runtime logical commit identity** — `CommitIdentity { source,
   sink, range: SequenceRange, schema_version }`. A deterministic
   struct projection of the source range; identical across replay.
   Owned by the runtime; consumed by the sink.
3. **Sink physical write/dedupe identity** — whatever physical tokens,
   file paths, manifest entries, or transaction IDs a sink needs to
   dedupe its writes (ClickHouse `insert_deduplication_token`; Iceberg
   snapshot summary; etc.). Each sink derives these from
   `CommitIdentity` plus its own adapter configuration. Owned by the
   sink; never constructed or inspected by the runtime.

The trait surface follows from this split.

**The runtime** reads from the source and owns the manifest cursor (it
holds the per-source `&mut BufferSource`). It admits descriptors in
contiguous source-sequence order — no gaps — which is the property that
lets the ack coordinator advance the frontier by a simple
contiguous-prefix check. It orchestrates fetch and decode across
parallel workers under bounded queues and byte/batch budgets, runs the
retry loop (including resolving an ambiguous write via
`Sink::check_committed`), and coordinates per-source acks so the
frontier advances correctly even when ranges complete out of order.
Finally, it constructs the `CommitIdentity` for each range — a
deterministic projection of `(source, sink, range, schema_version)`,
with no hashing, fingerprinting, or sink-specific fields.

**The sink** receives one `SinkCommit` per source range and turns it
into durable writes. It reads the decoded records it understands,
validates its own invariants (row ordering, schema compatibility),
plans the physical writes (chunking, file boundaries, partitioning),
derives its physical idempotency tokens from the `CommitIdentity` plus
its own configuration, and executes the writes. How a range becomes one
or many physical operations is entirely the sink's decision; the runtime
never inspects it.

**The runtime → sink handoff types:**

```rust
pub struct SequenceRange {
    pub low: u64,
    pub high: u64,
}

pub struct CommitIdentity {
    pub source: SourceId,
    pub sink: SinkId,
    pub range: SequenceRange,
    pub schema_version: SchemaVersion,
}

pub struct SinkCommit {
    pub identity: CommitIdentity,
    pub batch: DecodedBatch,
}

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    fn id(&self) -> &SinkId;
    fn write_budget(&self) -> SinkBudget;

    async fn write(&self, commit: SinkCommit)
        -> Result<SinkCommitResult, SinkCommitFailure>;

    async fn check_committed(&self, identity: &CommitIdentity)
        -> RuntimeResult<CommitStatus>;
}
```

The runtime guarantees, and the sink may rely on, exactly two
properties of any source range:

1. `CommitIdentity` is byte-identical across replay — same source,
   sink, low, high, schema version produces the same struct.
2. The decoded records inside `DecodedBatch` are content-identical
   and order-identical across replay.

Everything else — chunking shape, per-chunk dedupe tokens, file
paths, write planning — is the sink's responsibility. The runtime
never inspects sink-internal planning, and no sink-shaped types
(chunk indices, fingerprints, planning batches) live in the runtime
crate.

**Implication for type layout.** The runtime crate exports
`CommitIdentity`, `SequenceRange`, `SchemaVersion`, `DecodedBatch`,
`SinkCommit`, the `Sink` trait, and the per-source coordination
primitives. It does **not** export commit-group batches, per-record
size traits, chunking helpers, or idempotency-key constructors. Each
sink defines its own planning shape (e.g., `ClickHouseAdapterBatch`
inside `opendata-ingest-clickhouse`) and its own physical token
construction.

### Trait Surface

The trait and type names below match the shipped `opendata-ingest-runtime`
crate. The invariants stated alongside them are the contract.

#### Source Side: Concrete `BufferSource`

The runtime owns a concrete `BufferSource` per configured source. The
source side is concrete rather than a trait because there is one source
type — OpenData Buffer — and a trait over it would be close to 1:1 with
`buffer::Consumer` / `ConsumerFetchHandle`. (The trade-off, and what it
would take to add a second source type, is in "A Source Trait Instead of
a Concrete Source" under Alternatives Considered.)

`BufferSource` is a thin wrapper over `buffer::Consumer`. The wrapper
earns its place by doing two things the bare consumer doesn't:

1. **It adapts Buffer's types into the runtime's sink-neutral types.**
   `fetch` flattens each batch's per-entry metadata onto the entries it
   returns and attaches runtime-only fields (`SourceId`, the manifest
   and data paths) that the sink needs as source coordinates but that
   `buffer` does not carry.
2. **It splits the mutable owner from the cloneable fetch handle.** The
   owner mutates the manifest cursor and the durable ack frontier under
   `&mut self`; the paired `BufferSourceFetchHandle` (cloneable,
   `Send + Sync + 'static`) is handed to N fetch worker tasks that pull
   objects concurrently under `&self`.

```rust
pub struct BufferSource {
    id: SourceId,
    consumer: buffer::Consumer,
    fetch_handle: BufferSourceFetchHandle,
}

impl BufferSource {
    pub fn id(&self) -> &SourceId;

    /// Fetch up to `max` new descriptors past the current cursor.
    /// Does not mutate the durable ack frontier. Returning fewer
    /// than `max` is allowed and signals "no more visible right
    /// now"; the runtime sleeps and retries.
    pub async fn next_descriptors(&mut self, max: usize, budget: SourceBudget)
        -> RuntimeResult<Vec<SourceBatchDescriptor>>;

    /// Cloneable fetch primitive for parallel workers. O(1) clone;
    /// the handle holds an `Arc<dyn ObjectStore>` and no manifest
    /// state.
    pub fn fetch_handle(&self) -> BufferSourceFetchHandle;

    /// Advance the durable ack frontier through (and including)
    /// `sequence`. Buffer's in-order ack requirement; the runtime
    /// guarantees monotonic advance.
    pub async fn ack_through(&mut self, sequence: u64) -> RuntimeResult<()>;

    /// Force the underlying Buffer consumer's durable checkpoint.
    /// Called on flush boundaries.
    pub async fn flush_acks(&mut self) -> RuntimeResult<()>;
}

#[derive(Clone)]
pub struct BufferSourceFetchHandle {
    inner: buffer::ConsumerFetchHandle,
}

impl BufferSourceFetchHandle {
    /// Fetch a descriptor's data object. Safe to call concurrently
    /// from N worker tasks against distinct descriptors. Never
    /// mutates manifest or ack state.
    pub async fn fetch(&self, descriptor: SourceBatchDescriptor)
        -> RuntimeResult<SourceBatch>;
}

pub struct SourceBatchDescriptor {
    pub source: SourceId,
    pub sequence: u64,
    pub location: String,
    pub per_range_metadata: Vec<SourceRangeMetadata>,
}

pub struct SourceBatch {
    pub source: SourceId,
    pub sequence: u64,
    pub manifest_path: String,
    pub data_object_path: String,
    pub entries: Vec<SourceEntry>,
}

pub struct SourceEntry {
    pub entry_index: u32,
    pub raw_bytes: bytes::Bytes,
    pub raw_metadata: bytes::Bytes,
    pub ingestion_time_ms: i64,
}
```

`BufferSource::next_descriptors` calls
`buffer::Consumer::next_descriptors(max)` and filters returned
descriptors against `budget.bytes_remaining`. `fetch_handle()` returns
a cheap clone. `ack_through` and `flush_acks` are pass-throughs to
`Consumer::ack_through` / `Consumer::flush`.
`BufferSourceFetchHandle::fetch` calls `ConsumerFetchHandle::fetch` and
flattens the result into a `SourceBatch`.

These types (`SourceId`, `SourceBatchDescriptor`, `SourceBatch`,
`SourceEntry`, `SourceBudget`, `SourceRangeMetadata`) stay sink-neutral
in the runtime crate. They exist to carry the source metadata the sink
needs — the source identity, the manifest and data object paths, and
the per-entry metadata flattened parallel to entries — none of which the
underlying `buffer` types expose in this shape.

For test fakes, the runtime crate introduces a `#[cfg(test)]` source
seam so the correctness harness can drive a fake source; production
callers stay on the concrete `BufferSource`.

#### `Decoder`

```rust
pub trait Decoder: Send + Sync + 'static {
    /// Whether this decoder handles entries carrying the given opaque
    /// per-entry metadata bytes. The runtime passes the bytes through
    /// without interpreting them.
    fn accepts(&self, raw_metadata: &[u8]) -> bool;

    fn decode(&self, batch: SourceBatch)
        -> RuntimeResult<Vec<DecodedBatch>>;
}
```

**The runtime treats per-entry metadata as opaque bytes.** It does not
parse, interpret, or validate them — it carries them through on each
`SourceEntry` and hands them to the decoder. The decoder owns the
metadata format: it decides via `accepts` whether it handles a given
payload, and validates the metadata inside `decode`, returning `Err`
on an unexpected or inconsistent payload, which the runtime treats as
fatal (no ack advances). Any envelope shape — for the OTLP decoders
shipping today, a small header naming the OTLP signal and encoding — is
defined and parsed entirely in the decoder's own crate, never in the
runtime.

The contract is **one decoder per source**. The runtime calls
`accepts` on the first entry's metadata as a fail-fast, then calls
`decode` per `SourceBatch`; `decode` consumes the whole batch,
validates every entry's metadata, and returns one `DecodedBatch`.

The trait is shaped slightly wider than that contract — `accepts` takes
one entry's metadata and `decode` returns a `Vec` — so a future runtime
can route entries with different metadata to different decoders and emit
several decoded batches per source batch. That dispatch is not built
today (a single source carries homogeneous metadata); see "Future
Improvements".

#### `DecodedBatch`

The decoded unit carries records plus enough source state to feed the
ack coordinator and to emit source-coordinate columns at sinks:

```rust
pub struct DecodedBatch {
    pub source: SourceId,
    pub low_sequence: u64,
    pub high_sequence: u64,
    /// Number of input source entries this batch represents (for stats
    /// and backpressure size estimation when the records vector is
    /// empty).
    pub source_entry_count: u32,
    pub records: DecodedRecords,
    pub source_columns: SourceCoordinateColumns,
    pub stats: BatchStats,
    pub schema_version: SchemaVersion,
}

pub enum DecodedRecords {
    /// Typed Rust records, reference-counted so the runtime can move
    /// the batch through async stages without copying records; sinks
    /// downcast through `as_any` to the concrete record type.
    Typed(Arc<dyn TypedRecords + Send + Sync>),
    // A columnar `Arrow(Arc<RecordBatch>)` variant is future work.
}

pub trait TypedRecords: Send + Sync {
    fn record_count(&self) -> usize;
    fn estimated_bytes(&self) -> usize;
    fn schema(&self) -> &TypedSchema;
    /// Sinks that need a uniform record view downcast through here.
    fn as_any(&self) -> &dyn std::any::Any;
}

pub struct SourceCoordinateColumns {
    pub manifest_path: String,
    pub data_path: String,
    pub sequences: Vec<u64>,        // one per record
    pub entry_indices: Vec<u32>,    // one per record
    pub record_indices: Vec<u32>,   // one per record
    pub ingestion_time_ms: Vec<i64>,// one per record
}
```

`SourceCoordinateColumns` ties each decoded record back to its place in
the source. A Buffer batch (one sequence) holds a list of entries (one
per producer append); the decoder expands each entry into zero or more
records. The parallel vectors map every output record to the batch it
came from (`sequences`), the entry within that batch (`entry_indices`),
and the record's position within the entry (`record_indices`), plus the
batch's manifest/data paths and per-entry ingestion time. Keeping these
parallel to the records rather than embedded in them lets each sink
materialize the subset it needs — as system columns, file metadata, or
otherwise — without the decoder having to know any sink's schema.

`source_entry_count` lets the per-source `AckCoordinator` advance the
input high-watermark even when `records` is empty, so a batch that
decodes to zero records still advances the Buffer ack frontier.

#### `Sink`

`Sink::write` is the **source-range commit unit**. One call covers
one source sequence range for the configured sink. Internally a sink
may chunk and parallelize as it sees fit (the ClickHouse sink today
plans `Vec<InsertChunk>` and writes them with per-chunk dedupe
tokens; the Iceberg sink writes one or more Parquet files and one
snapshot commit), but the sink must satisfy three contracts:

1. **`Ok(_)` means the full source-range commit is complete.** Every
   internal chunk/file/insert that the sink decided to write for this
   `SinkCommit` has durably landed. The runtime then marks the range
   committed for this source.
2. **Retry of the same `SinkCommit` must be idempotent.** If `write`
   returns `Err(NotCommitted)` or `Err(MaybeCommitted)`, the runtime
   may call `write(commit)` again with the same `SinkCommit`. The
   sink derives its physical write/dedupe tokens deterministically
   from the commit's `CommitIdentity` plus the sink's own adapter
   configuration so a second attempt does not produce duplicate
   data. ClickHouse uses `insert_deduplication_token` plus
   `ReplacingMergeTree(_adapter_version)`. Iceberg uses deterministic
   Parquet file paths plus a `check_committed` snapshot lookup.
3. **`check_committed(identity)` reflects source-range commit.**
   It returns `Committed` only when the full source range landed
   under that identity — not when some internal chunks landed and
   others didn't. Sinks that cannot tell whether all their internal
   pieces are present return `Unknown`; the runtime then re-attempts
   `write` and relies on the sink's idempotency to drop the
   redundant work.

The runtime does **not** require sinks to roll back partial state on
failure. ClickHouse cannot transactionally undo successful inserts;
Iceberg can cancel a snapshot commit but cannot reliably delete data
files written outside that commit. Requiring atomic-with-rollback
would force sinks into either a two-phase commit none of these systems
support cleanly, or aggressive cleanup paths that turn ambiguous
errors into data loss.

What the runtime *does* require: between `Err(_)` and the next `write`
retry, the sink must not produce divergent state for the same
`CommitIdentity`. A sink that uses a different commit identity on retry
violates the idempotency contract and will cause duplicate data.

```rust
#[async_trait::async_trait]
pub trait Sink: Send + Sync + 'static {
    fn id(&self) -> &SinkId;

    /// Maximum bytes-in-flight the runtime should hold for this sink
    /// before pausing upstream pulls (used for fairness across the
    /// sources sharing this sink writer pool).
    fn write_budget(&self) -> SinkBudget;

    /// Commit one source-range unit (the whole `SinkCommit` for one
    /// source sequence range). Returns `Ok` only when the full commit
    /// is durable per the rules above; returns the appropriate
    /// `SinkCommitFailure` variant otherwise. The runtime is allowed
    /// to retry the same `SinkCommit` after a non-fatal failure;
    /// implementations must keep retry idempotent (see
    /// "Commit Identity" below).
    async fn write(&self, commit: SinkCommit)
        -> Result<SinkCommitResult, SinkCommitFailure>;

    /// Inspect prior commit state for a given runtime commit identity.
    /// The runtime calls this on replay (after a crash) and on
    /// `MaybeCommitted` failure (after an ambiguous response from
    /// the sink). Sinks that cannot tell return
    /// `CommitStatus::Unknown`; the runtime then re-attempts the
    /// `write` and relies on the sink's table-level dedupe (e.g.
    /// `ReplacingMergeTree(_adapter_version)`) to clean up. Sinks
    /// that need a physical token recompute it from `identity` +
    /// the adapter's own configuration; the runtime supplies the
    /// logical identity only.
    async fn check_committed(&self, identity: &CommitIdentity)
        -> RuntimeResult<CommitStatus>;
}

pub struct SinkCommit {
    /// Runtime logical commit identity — byte-identical across replay
    /// for the same `(source, sink, range, schema_version)`. See
    /// "Commit Identity" below.
    pub identity: CommitIdentity,
    /// Decoded records + per-record source coordinates for this
    /// source range.
    pub batch: DecodedBatch,
}

pub struct SinkCommitResult {
    pub bytes_written: u64,
    pub rows_written: u64,
}

pub enum SinkCommitFailure {
    /// Sink definitively did not commit. Safe to retry the same
    /// `write` call. Examples: connection refused, 5xx before
    /// request body sent, fast-path validation rejection that
    /// cannot have produced state.
    NotCommitted(BoxError),
    /// Request did not return success, but the sink may have
    /// committed (e.g. timeout after request body was fully sent,
    /// connection drop after server-side commit, ClickHouse 200 OK
    /// dropped on the network). The runtime calls
    /// `check_committed(&identity)` before deciding whether to retry.
    MaybeCommitted(BoxError),
    /// Non-retryable. The runtime halts. Examples: schema mismatch,
    /// permissions error, malformed request that cannot succeed
    /// without code or schema changes.
    Fatal(BoxError),
}

pub enum CommitStatus {
    Committed,
    NotCommitted,
    /// Sink cannot tell from the identity alone (e.g. ClickHouse
    /// insert deduplication window has passed). The runtime treats
    /// this like `NotCommitted` for the retry decision, re-attempts
    /// the `write`, and relies on the sink's table-level dedupe
    /// (e.g. `ReplacingMergeTree(_adapter_version)`) to clean up
    /// any duplicate rows. `Unknown` is logged separately so an
    /// operator can audit how often it fires.
    Unknown,
}
```

#### Runtime Handling of `SinkCommitFailure`

The runtime branches on the failure variant before deciding what to do:

| Variant | Runtime action |
|---|---|
| `NotCommitted(_)` | Backoff and retry the same `write` call; the source's ack frontier does not advance for this range. After retry budget is exhausted, halt. |
| `MaybeCommitted(_)` | Call `check_committed(&commit.identity)`. If `Committed`, mark the range committed without writing again. If `NotCommitted`, retry the `write`. If `Unknown`, retry the `write` and rely on table-level dedupe. |
| `Fatal(_)` | Halt the runtime. The operator inspects the offending range and decides whether to fix the sink, fix the data, or use the documented escape hatch to advance past the range. |

This is the layer that protects single-sink idempotency under
ClickHouse insert timeouts and Iceberg catalog-commit timeouts.
Without it, a `MaybeCommitted` event would either ack-on-first-success
(data loss if the sink quietly didn't commit) or retry-on-failure
(duplicate Iceberg snapshot, stale ClickHouse insert dedupe token).

`check_committed(&identity)` is the primitive that lets a sink say
"I already have this; do not write it again." It is the same call
used for crash-replay (after a process restart, before the runtime
advances acks past the durable frontier) and for `MaybeCommitted`
resolution. The ClickHouse sink implements it as a no-op that
always returns `Unknown` because it dedupes at the table layer with
`ReplacingMergeTree(_adapter_version)`. The
Iceberg sink implements it by inspecting snapshot metadata for a
file whose path / token matches the one its adapter would
recompute from `identity`.

#### Commit Identity

The runtime hands the sink one logical identity per source-range
commit: a [`CommitIdentity`] struct projection of `(source, sink,
range, schema_version)`. The struct is a total, deterministic
function of those four inputs — no hashing, no fingerprinting, no
sink-specific data — so replay of the same source range produces
a byte-identical `CommitIdentity`. Sinks derive their physical
write/dedupe tokens from this identity plus their own adapter
configuration; the runtime never inspects sink-physical tokens.

```rust
pub struct SchemaVersion(pub u32);

/// Inclusive range over Buffer batch sequences.
pub struct SequenceRange {
    pub low: u64,
    pub high: u64,
}

pub struct CommitIdentity {
    pub source: SourceId,
    /// Identity of the configured sink. Kept in the projection so
    /// the same source-range identity is distinct across deployments
    /// that share an upstream Buffer (e.g. duplicated-queue
    /// deployments that fan out to ClickHouse and Iceberg in
    /// separate runtime services).
    pub sink: SinkId,
    pub range: SequenceRange,
    pub schema_version: SchemaVersion,
}
```

`range` is an inclusive span over Buffer batch sequences. The runtime
emits one commit per source batch and does not coalesce batches, so
`low == high` always today; the span is a range rather than a single
sequence only to leave room for a future runtime that merges several
small source batches into one commit. A sink should not assume
single-batch ranges.

`schema_version` identifies the version of the *decoded record schema* —
the shape the decoder produces and the sink writes. It is not the
producer's wire-envelope `version` byte, and it is not read from the
source: the decoder stamps it on every `DecodedBatch` as a property of
itself and its target schema. It is in the commit identity so that
changing the decoded schema changes the identity — a write under a new
schema is never deduped against an old-schema write of the same range.

The canonical `Display` projection is

```text
{source}:{sink}:{low}-{high}:{schema_version}
```

which the runtime uses for log fields and metric labels. A sink builds
its own physical dedupe token by extending this identity with whatever
its target system needs (e.g. a ClickHouse `insert_deduplication_token`
appends the adapter version, a chunking fingerprint, and a chunk index).
Those suffixes are a sink concern; the runtime never constructs or
inspects them.

### Per-Source Ack Coordinator

The ack coordinator is the piece that makes parallel writes safe. Buffer
ack must advance as a contiguous prefix of the sequence stream, but the
pipeline fetches, decodes, and commits ranges concurrently, so ranges
finish out of order. The coordinator reconciles those out-of-order
commits into a monotonic, gap-free ack frontier: it tracks which ranges
have committed and advances the durable Buffer ack only across the
contiguous committed prefix. Without it, concurrency could either
advance the ack past a hole (data loss on replay) or not advance it at
all.

There is **one coordinator per source**; coordinators are independent —
one source's committed range never advances another source's frontier.

#### State Machine

For each source, the coordinator tracks:

- A monotonic `acked_frontier` (the durable Buffer ack high-watermark).
- A set of `pending` sequence ranges, each carrying a single
  sink-commit bit.

```rust
pub struct AckCoordinator {
    source: SourceId,
    acked_frontier: Option<u64>,
    pending: BTreeMap<u64, PendingRange>,  // keyed by low_sequence
    flush_policy: AckFlushPolicy,
}

pub struct PendingRange {
    pub low: u64,
    pub high: u64,
    pub sink_committed: bool,
}
```

The state transitions are:

1. **Range becomes pending** when the runtime registers a decoded
   source range with the per-source `AckCoordinator`
   (`register_pending(low, high)`), or when an empty decoded batch
   still represents input progress (zero-record batch). Pending
   ranges are tracked by source sequence because concurrent fetch
   / decode / write can complete ranges out of order.
2. **Range becomes committed** when the configured sink reports a
   successful commit. The runtime issues **exactly one**
   `Sink::write` call per range at a time; the sink's contract (see
   "`Sink`" above) is that `Ok(_)` means the full source-range commit
   is complete and that retry of the same `SinkCommit` is idempotent.
   No per-chunk completion tracking inside the runtime. A range is
   marked committed when:
   - `Sink::write(commit)` returns `Ok(_)`, or
   - `Sink::write(commit)` returns `Err(MaybeCommitted)` and the
     subsequent `check_committed(&commit.identity)` returns
     `Committed`, or
   - On replay after restart, `check_committed(&identity)` returns
     `Committed` before the runtime would have re-attempted the
     `write`.
3. **Frontier advances** to the highest contiguous committed sequence
   from the current `acked_frontier`. The coordinator never advances
   over a hole.
4. **Flush** calls `BufferSource::ack_through(frontier)` and then
   `BufferSource::flush_acks` per the configured `AckFlushPolicy`
   (default: flush after every committed source range).

> **Why a single-bit per range is sufficient.** The scope of one
> configured sink per runtime service, plus the sink contract —
> `Ok(_)` means the full source-range commit is complete, and retry
> of the same `SinkCommit` is idempotent — reduces completion
> tracking to a single-bit-per-range check at the runtime layer.
> Finer-grained tracking per chunk or per route is unnecessary:
> chunking is sink-internal and cross-sink fanout is not a runtime
> concern; see "Alternatives: Same-Process Multi-Sink Fanout".

#### Crash Semantics

- **Crash before sink commit**: nothing in the Buffer ack moves.
  The source replays the range.
- **Crash after sink commit, before ack flush**: the ack frontier did
  not advance (the range was committed but not yet flushed). On
  replay the runtime calls `check_committed(&identity)` *before*
  re-attempting `Sink::write`:
  - The sink returns `Committed`; the runtime marks the range
    committed without rewriting and advances the frontier on the next
    loop iteration.
  - The sink returns `NotCommitted` or `Unknown`; the runtime calls
    `Sink::write` and waits for success. Idempotent retry handles the
    duplicate case.
- **Crash mid-`MaybeCommitted` resolution** (write returned ambiguous,
  process died before `check_committed` resolved): replay re-enters
  the `check_committed` path; the sink's answer is the source of
  truth.
- **Crash after ack flush**: source will not replay this range. The
  configured sink must already have committed it, and the runtime
  advanced the frontier only after that commit. This is the
  invariant.

#### Single-Sink Ack Invariant

> **For each source, Buffer ack advances only after the configured
> sink has either durably committed the relevant source sequence
> range or reported `Committed` for that range's `CommitIdentity`.**

This is the single sentence to re-validate for any change that
touches ack flow. The correctness harness demonstrates this
invariant under deterministic out-of-order range completion,
retry / `MaybeCommitted` resolution, replay, and multi-source ack
isolation (one source's frontier never advances another source's
frontier).

### Backpressure Model

Stages communicate through bounded queues plus a shared in-flight byte
budget per source. The configured sink owns a single shared writer
budget across all sources hosted by the service, with source-aware
fairness so a hot source cannot permanently starve a low-volume
source:

| Stage | Backpressure trigger |
|---|---|
| Descriptor poll | Source descriptor queue full or in-flight bytes ≥ `source.max_inflight_bytes` |
| Object fetch | Fetch worker semaphore full (per source) |
| Decompress | Decompress worker semaphore full (per source) |
| Decode | Decode worker semaphore full (per source) |
| Sink dispatch | Per-source sink-commit channel full (one `SinkCommit` per source range; bounded by `sink.<name>.max_concurrent_commits`) |
| Sink write | Sink-wide budget (`Sink::write_budget`); slow sink fills its writer queue and stalls upstream for every source feeding it |
| Ack | Frontier blocked on uncommitted pending range (per source) |

The runtime does **not** carry a commit-group stage between decode
and sink dispatch: each `DecodedBatch` flows directly to the
shared sink writer pool as one `SinkCommit` per source range. Any
sink-internal chunking threshold the adapter wants to apply lives
inside the plugin crate, not in the runtime config.

Required configuration knobs (a later config schema RFC may rename
these, but the semantics carry through):

- `source.max_inflight_batches`
- `source.max_inflight_bytes`
- `source.estimated_max_batch_bytes` — the per-batch byte reservation.
  The manifest does not carry object size, so the runtime reserves this
  estimate for every batch. Defaults to `source.max_inflight_bytes /
  source.max_inflight_batches` rounded up; operators override when the
  workload is known to use larger batches.
- `source.fetch_concurrency`
- `source.decompress_concurrency`
- `decode.concurrency`
- `sink.<name>.max_concurrent_commits`
- `sink.<name>.retry.max_attempts`
- `sink.<name>.retry.initial_backoff_ms`

Chunking thresholds (e.g. ClickHouse's `max_chunk_rows` /
`max_chunk_bytes`) belong to the sink plugin's own config schema,
not to this list. They never appear on the runtime/sink boundary.

#### Byte Budget Accounting

In-flight bytes are tracked from descriptor reservation through sink
commit. The accounting rule is the same regardless of source:

1. **At descriptor reservation** (when `next_descriptors` produces a
   batch and the runtime decides whether to fetch it): reserve
   `source.estimated_max_batch_bytes` from the source's in-flight
   budget. The manifest carries no object size, so this estimate is the
   reservation for every batch.
2. **After fetch and decode**: the reservation is reconciled to the
   actual `SourceBatch` payload size (post-decompress, pre-decode)
   plus the decoded `DecodedBatch.estimated_bytes()`. The previous
   reservation is released; the actual size is held for as long as
   the batch is in any in-flight stage.
3. **At sink commit success**: the reservation is released from the
   source's slice of the budget.
4. **On retry / `MaybeCommitted` resolution**: the reservation
   persists until the range is decisively committed or the runtime
   halts.

The runtime never uses HEAD requests against object storage to discover
sizes. The `estimated_max_batch_bytes` reservation is deliberately
pessimistic: it overcounts in the common case, so the source poller
pauses a little sooner than it strictly has to.

The correctness harness demonstrates this by showing
`runtime_stage_inflight_bytes{stage,source}` rising and the source
poller backing off on a slow-sink injection — the test is in the
correctness harness, not in benchmark numbers.

Required metrics (stage-labeled):

- `runtime_stage_queue_depth{stage,source}`
- `runtime_stage_inflight_bytes{stage,source}`
- `runtime_stage_latency_seconds{stage,source}`
- `runtime_ack_frontier{source}` (gauge)
- `runtime_pending_ranges{source}` (gauge)
- `runtime_backpressure_reason{source,reason}` (counter; `reason` ∈
  `source_budget`, `decode_budget`, `sink_budget`, `retrying`,
  `fatal_error`)
- `runtime_sink_queue_depth{sink}` (gauge)
- `runtime_sink_inflight_bytes{sink}` (gauge)
- `runtime_sink_commits_total{source,sink,result}` (`result` ∈
  `committed`, `verified_already_committed`, `failed_retryable`,
  `failed_fatal`)

These metrics are how an operator audits the runtime under
concurrency without re-reading code.

### Source Reader: Buffer Implementation

See "Source Side: Concrete `BufferSource`" under Trait Surface for the
canonical description of `BufferSource` and `BufferSourceFetchHandle`.
The source side is concrete, so there is no separate trait
implementation to describe here.

### Decoders and Sinks

This RFC defines the runtime-side *interfaces* — the `Decoder` and
`Sink` traits and the types that cross them. The concrete
implementations are defined in their own crates and follow-up RFCs:

- The OTLP decoder (`logs`, and later `metrics`/`traces`) lives in the
  decoder plugin crate.
- The ClickHouse sink — the first integration, ported from the shipped
  ingestor with no intentional behavior change — and a future Iceberg
  sink each live in their own crate and RFC.

A runtime service runs one configured sink; each source's `DecodedBatch`
flows to that sink for the range it covers.

### Configuration Shape

The runtime configuration treats sources as a list and the sink as a
single top-level block. **Validation rejects more than one sink in
one service.** To write to two sinks, define two service configs with
separate Buffer manifests and run two runtime processes.

```yaml
runtime:
  poll_interval_ms: 250
  shared_metrics_bind_addr: 0.0.0.0:9090

sources:
  - id: service_logs
    type: buffer
    buffer:
      manifest_path: ingest/otel/logs/manifest
      data_prefix: ingest/otel/logs/data
      object_store:
        type: Aws
        bucket: opendata-otel-logs
        region: us-west-2
    # The decoder owns the metadata envelope it expects; the runtime
    # passes per-entry metadata through as opaque bytes. The operator
    # only selects which decoder to register.
    decoder: otlp_logs
    ack:
      # Flush the ack frontier after each committed source range (one
      # SinkCommit per range).
      policy: every_source_range
    backpressure:
      max_inflight_batches: 64
      max_inflight_bytes: 268435456    # 256 MiB
      fetch_concurrency: 8
      decompress_concurrency: 4
      decode_concurrency: 4

sink:
  id: logs_clickhouse
  type: clickhouse
  endpoint: https://...clickhouse.cloud:8443
  database: observability
  table: logs
  schema_ref: builtin/otel_logs_clickhouse_v1
  insert_quorum: auto
  apply_deduplication_token: true
  max_concurrent_commits: 4
  # Chunking is sink-internal: each adapter owns its own thresholds.
  # The runtime never inspects max_chunk_*.
  max_chunk_rows: 100000
  max_chunk_bytes: 33554432
  retry:
    max_attempts: 6
    initial_backoff_ms: 100
```

Key shape decisions:

- **`sink` is singular.** Two sinks => two runtime services, with
  independent Buffer queues and processes.
- **Sources are a top-level list keyed by `id`.** Each source has its
  own ack and backpressure config; sharing a pool would couple
  high-volume and low-volume sources.
- **`schema_ref` selects a built-in template.** The string
  `builtin/<name>` resolves to a compiled-in template. User-supplied
  schema/mapping documents are future work (see "Future Improvements").
- **The sink declares retry and concurrency at the sink level.** The
  runtime applies them; sink plugins do not implement their own retry
  loops.
- **If different sources need different physical tables in the same
  sink, that is a sink/schema-mapping concern**, not a generic
  runtime route. The sink config can carry an explicit
  `source_mappings` / `tables` / `schema_ref_by_source` structure;
  this is a sink-side feature, not a runtime trait.

### Plugin Model

A plugin is a crate that depends on `opendata-ingest-runtime` and
implements the trait it owns — a `Decoder` (e.g. an OTLP decoder crate)
or a `Sink` (e.g. a ClickHouse or Iceberg crate). A binary crate links
the plugins it needs and assembles the runtime through a builder:

```rust
let runtime = Runtime::builder()
    .add_source(BufferSource::new(consumer, "logs", manifest_path, None))
    .add_decoder(OtlpLogsDecoder::new())   // opendata-ingest-otel
    .set_sink(ClickHouseSink::new(...))    // opendata-ingest-clickhouse
    .with_options(runtime_options)
    .build()?;
runtime.run(shutdown_token).await?;
```

The decoder it registers owns the metadata envelope it expects, so the
runtime stays metadata-agnostic. Adding a sink is: write a crate
implementing `Sink`, link it, set it on the builder, ship a new
binary — no runtime change.

Two later forms of pluggability are out of scope here and tracked in
"Future Improvements": declarative schema/mapping documents (target
table schemas and projections defined out of Rust), and dynamic WASM or
subprocess plugins for off-hot-path transforms. Rust `cdylib` plugins
are rejected outright — Rust has no stable ABI and a dynamic-library
boundary on the row path is the wrong first optimization.

### Source Coordinates

The runtime exposes, per record, where the record came from in the
source. The coordinates and their provenance:

| Coordinate | Provided by | Provenance |
|---|---|---|
| sequence | Runtime | `SourceBatchDescriptor.sequence` |
| entry index | Runtime | `SourceEntry.entry_index` |
| record index | Decoder | record position within an entry |
| manifest path | Runtime | configured per source |
| data object path | Runtime | `SourceBatch.data_object_path` |
| ingestion time | Runtime | per-entry metadata |
| schema version | Decoder | `DecodedBatch.schema_version` |

These are not columns the runtime writes — they are carried in
`SourceCoordinateColumns`, parallel to records, and each sink decides
which ones to materialize and how to name them. The ClickHouse sink, for
example, writes the subset it keeps as `_odb_*` system columns; another
sink might attach them to file metadata instead.

### Multi-Source, Single-Sink Service

The runtime hosts N sources -> 1 sink in one process. The invariants
across sources:

- **Each source has its own `AckCoordinator`** and its own ack
  frontier. A failure in one source must not advance another source's
  Buffer ack. Multi-source ack isolation is exercised by the
  correctness harness.
- **The configured sink is shared across sources.** The sink writer
  pool applies source-aware fairness so one hot source does not
  permanently starve a low-volume source. The first cut is bounded
  per-source queues feeding a shared sink semaphore; weighted
  fairness can be revisited later if the bench surfaces a need.
- **Two sources feeding the same target table** is allowed but adds a
  dedupe-key constraint: the table's dedupe key must include the source
  identity (e.g. `_odb_manifest_path` or `source_id`, or the equivalent
  for a non-ClickHouse sink), so identical sequence ranges from
  different sources don't collide. The config validator enforces this.
- **Process readiness fails** when any source halts. Marking individual
  sources optional is a future revision; named here so the validator can
  adopt it later without churn.

### Operational Surface

- **Dry-run** (per source): full pipeline, including decode and the
  sink's plan/serialize step, but `Sink::write` is replaced with a
  no-op that returns success without side effects, and `ack_through`
  is skipped. Dry-run is set at startup and toggling it requires a
  process restart.
- **Graceful shutdown**: stop admitting new descriptors per source,
  drain all in-flight `SinkCommit`s through the configured sink,
  advance and flush each source's ack frontier, exit. The runtime
  owns shutdown propagation through `tokio_util::CancellationToken`.
- **Hard crash recovery**: each source resumes from its last *flushed*
  ack frontier; replay is idempotent at sinks that implement
  `check_committed`, and dedupable at sinks that do not.

### Failure Modes

- **Source unreachable**: `next_descriptors`/`fetch` errors retry under
  the source's retry policy. The pipeline drains and stalls. No
  ack advance.
- **Decode failure**: fail closed. No ack advance. Halt, alert, operator
  inspects.
- **Sink unreachable, retryable**: writer retries within budget.
  Backpressure builds, source pulls pause. No ack advance.
- **Sink non-retryable**: halt. The runtime never silently drops a
  range. Operator either fixes the underlying issue or, with explicit
  config, advances the source past the bad range and accepts data
  loss for that range.
- **AckCoordinator inconsistent state** (a programming bug, e.g.
  a duplicate `register_pending` for an already-pending range):
  treated as fatal. The runtime never papers over a coordinator
  invariant violation.

## Alternatives Considered

### Keep the Runtime Inside `clickhouse-ingestor`

Adding a second sink in-tree is mechanically possible. Rejected because
it forces every future sink to depend on a ClickHouse-shaped crate, and
it makes the ClickHouse-specific row type (`Vec<RowValue>`) the de facto
cross-sink interchange shape. That is a worse permanent abstraction
than splitting the crate now.

### Use Raw OTLP Protobuf as the Cross-Sink Unit

The runtime could carry source bytes through to the sink and let the
sink decode them. Rejected because every sink would re-decode the same
bytes, OTLP tree flattening is non-trivial, and the runtime could not
apply schema-aware backpressure or let each sink chunk its writes
against a known row count.

### Use `Vec<RowValue>` as the Cross-Sink Unit

The current ClickHouse path uses a row-of-`RowValue` shape. Rejected as
the runtime-wide unit because:

- `RowValue` is row-oriented and JSON-leaning, which is exactly the
  serialization path the ClickHouse throughput work aims to leave behind.
- Iceberg/Delta/Hudi expect Arrow or Parquet-shaped columnar batches.
- The `RowValue` enum bakes in ClickHouse types (`LowCardinalityString`,
  `DateTime64Nanos`).

The runtime keeps `RowValue` as an internal detail of the ClickHouse
sink instead.

### Same-Process Multi-Sink Fanout

Considered: one runtime service hosts multiple sinks (e.g. ClickHouse
and Iceberg) and routes the same source data to both, ack-ing only
when both have committed. Rejected because:

- **Independent sinks should not share process fate.** A slow or
  fatal sink would block ack for unrelated sinks.
- **Partial success requires a complex operator-facing data-loss
  policy.** "One sink halts, others healthy" turns into either
  whole-runtime halt (loses availability for the healthy sink) or
  per-sink skip-and-record (silent data loss without an explicit
  policy).
- **Backpressure incentives invert.** A shared byte budget that
  multiplies across sinks penalizes multi-sink runs; tracking
  per-sink completion adds runtime state for a feature most
  deployments don't want.

The cleaner deployment is producer-side queue duplication plus
isolated single-sink runtime services, one per sink. Each service has
its own ack frontier, memory budget, retry loop, and process
liveness. Independent failure stays independent.

### Per-Sink Ack Frontiers (One AckCoordinator per Sink)

Considered: in a hypothetical multi-sink runtime, each sink advances
its own Buffer ack. Rejected because Buffer has one active consumer
per manifest (epoch fenced); per-sink frontiers would require a
separate per-sink checkpoint store and a reconciliation algorithm to
derive the source ack frontier. That multi-checkpoint complexity is
moot under the single-sink scope: ack frontiers are per-source against
the single configured sink. The section is kept because it is the
argument against same-process fanout.

### WASM-First Plugin Boundary

Defer. The hot path is decode → sink-plan → sink write.
A WASM boundary on that path is premature optimization for an
extensibility story that natively-linked plugin crates already cover.
Once the native path is benchmarked, WASM is a candidate for transforms
that are off the hottest path (e.g. attribute enrichment).

### `cdylib` Rust Plugin Boundary

Rejected. Rust has no stable ABI; a dylib boundary in the record path
forces serialization at the boundary, which negates the point of native
plugins. WASM or subprocess plugins are better long-term answers.

### Push the Ack Coordinator Into the Sink

The sink could call `source.ack_through` itself. Rejected because:

- It would couple the sink to the source's manifest API (today
  Buffer; tomorrow possibly Kafka or file scan).
- Multiple sources sharing one sink writer would each need to plumb
  their own ack callback through the sink, fragmenting the contract
  the sink has to honor. The runtime is the natural coordination
  point — it owns the per-source `AckCoordinator` and the source
  reader, the sink owns its idempotent commit semantics.

### A Source Trait Instead of a Concrete Source

Considered: define a `SourceReader` / `SourceFetchHandle` trait pair and
route the runtime through `Box<dyn SourceReader>`, to keep the door open
for non-Buffer sources. Rejected because there is one source type and
the trait methods would be close to 1:1 over `buffer::Consumer` /
`ConsumerFetchHandle` — API surface to maintain for an abstraction with
a single implementer. Test fakes get a `#[cfg(test)]` seam inside the
runtime crate instead; production callers stay concrete.

The source data types (`SourceBatchDescriptor`, `SourceBatch`,
`SourceEntry`) are already source-shape-agnostic and don't depend on
Buffer, so introducing a source trait later — if a second source type
ever lands — stays a contained change inside the runtime crate.

## Future Improvements

These do not require changing the trait shapes in this RFC.

- **Per-entry decoder dispatch within a single source**: relax the
  homogeneous-envelope requirement so entries with different envelopes
  in one source route to different decoders. Already accommodated by the
  `Decoder::accepts` shape.
- **Arrow `DecodedRecords` variant**: a columnar record carrier
  alongside `Typed`, gated by benchmarks comparing the two, ahead of a
  columnar sink or a binary ClickHouse path.
- **Optional sources / per-source readiness** in the config validator
  and the metrics surface.
- **Declarative schema/mapping documents**: move target table schemas
  and projections out of Rust so an operator can retarget a table
  without rebuilding the binary. The decoder stays compiled in (OTLP
  tree flattening is semantic, not generic protobuf decode); only the
  schema/mapping is declarative. Gets its own follow-up RFC.
- **Sink-side schema mapping for multi-source -> multi-table**: when
  one sink should land different sources in different physical
  tables, encode the mapping in the sink config (e.g.
  `source_mappings`, `schema_ref_by_source`). This is sink/schema
  work, not a generic runtime route.
- **WASM plugins** for off-hot-path transforms (attribute enrichment,
  schema migrations, filter rules).
- **Deployment tooling for duplicate producer outputs** when one
  upstream stream must feed multiple sinks. This is an
  `opendata-go` / Helm / operator concern, not a runtime trait
  change.
- **Optional multi-sink runtime** can be reconsidered only with
  explicit isolation guarantees and a documented per-sink
  data-loss policy.
- **Dynamic source registration** for sources that are not declared
  in the YAML config (e.g. discovered manifests, multi-tenant
  routing).
- **Cross-source coordination primitives** if a future use case needs
  ordered ack across two manifests; today this is explicitly out of
  scope.
- **Replay tooling**: operator-directed replay from a chosen sequence
  inside Buffer's retained range. The seam already exists —
  `BufferSource` takes an initial sequence on construction.
- **A source trait for non-Buffer sources** (Kafka direct, OTLP HTTP
  push, file scan). See "A Source Trait Instead of a Concrete Source"
  in Alternatives Considered; the runtime data types are
  source-shape-agnostic, so this stays a contained change.

## Validation Criteria

### Runtime Extraction Without Behavior Change

- The OTLP logs path runs end-to-end through the runtime crate's
  traits. The `clickhouse-ingestor` binary is registry/config wiring
  on top of `opendata-ingest-runtime` and `opendata-ingest-clickhouse`.
- Existing ClickHouse ingestor tests pass unchanged.
- `cargo test -p opendata-ingest-runtime -p opendata-ingest-clickhouse`
  is green.
- Behavior is equivalent: same metrics names, same dry-run semantics,
  same rows landed in ClickHouse for the same input.

### Ack Coordinator and Single-Sink Correctness

- A fake source and a fake single sink (success, retryable failure,
  permanent failure, slow, ambiguous) reproduce each crash point in the
  Crash Semantics section.
- Tests demonstrate the single-sink ack invariant under
  out-of-order range completion, retry, `MaybeCommitted` resolution,
  and replay.
- Multi-source ack isolation: one source's committed range never
  advances another source's frontier.
- `check_committed` contract tests for at least one concrete sink.

### Pipelined Runtime Under Concurrency

- Fetch, decode, and write workers run concurrently across multiple
  sources; all stage queue and inflight metrics are exposed.
- A sink slowdown injection pauses source pulls within one
  pipeline-traversal window; recovery resumes without unbounded
  memory growth.
- The shared sink writer pool applies source fairness so a hot source
  does not permanently starve a low-volume source.
- The single-sink correctness tests still pass under concurrency knobs
  greater than 1.
