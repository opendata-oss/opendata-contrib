# RFC 0002: Generic Ingest Runtime

**Status**: Draft

**Authors**:

- Apurva Mehta

## Summary

This RFC defines `opendata-ingest-runtime`, a sink-neutral runtime that consumes
OpenData Buffer streams (and, later, other sources) and writes decoded records
to **one configured sink per runtime service**. It generalizes the layering
from the shipped ClickHouse ingestor (RFC 0001) into traits and a per-source
`AckCoordinator` that holds even when multiple sources share one sink and
ranges complete out of order.

A single runtime service hosts N sources -> 1 sink. **Independent sinks are
isolated by independent Buffer queues and runtime service instances.** If the
same upstream data must land in two sinks, the documented deployment is
producer-side queue duplication plus two isolated runtime processes.
Independent sinks should not share a runtime ack frontier, memory budget,
retry loop, or process liveness.

The runtime is built around four trait boundaries: `SourceReader`, `Decoder`,
`Sink`, and `IdempotencyContract`. The pipeline is staged with bounded queues
and a shared in-flight byte budget, so a slow or failing sink pauses upstream
work without unbounded memory growth. For each source, Buffer ack advances
only when the configured sink has durably committed the corresponding source
sequence range, or has verified that the range was already committed by an
earlier attempt.

The decoded unit starts as typed Rust records (compatible with the
current `DecodedLogRecord` path) and migrates to Arrow `RecordBatch`
before the first lakehouse sink lands and before ClickHouse moves off
JSONEachRow. Pluggability starts at native plugin crates plus
declarative schema/mapping config; WASM and subprocess plugins are
deferred until the native path is measured.

The first integration is RFC 0001's ClickHouse ingestor, ported through
the generic runtime without intentional behavior changes. The next sink
is an append-only Iceberg writer (separate RFC), deployed as a standalone
single-sink runtime service.

## Motivation

RFC 0001 ships an ingestor that polls Buffer, validates per-entry
metadata envelopes, decodes OTLP logs, coalesces records into commit
groups, plans deterministic ClickHouse insert chunks, executes them, and
acks Buffer only after all chunks succeed. The layering is sound, but
six things are tied to the ClickHouse sink today:

1. **The runtime is ClickHouse-specific.** The runtime polling loop, the
   commit group, and the ack controller live in the `clickhouse-ingestor`
   crate. A different sink (e.g. Iceberg) would either duplicate them or
   import a ClickHouse-specific crate just to reuse them.
2. The adapter trait outputs `Vec<InsertChunk>` with a
   ClickHouse-shaped `Row = Vec<RowValue>`. That row type is
   row-oriented, JSON-leaning, and not a useful interchange format for
   columnar sinks like Iceberg/Delta or for a binary ClickHouse path.
3. **The runtime is single-source / single-loop.** One Buffer manifest,
   one serial decode path, one writer pass. There is no per-source ack
   coordinator, no source/decoder/sink boundary, and no model for
   multiple sources sharing one sink in the same process.
4. The Buffer consumer API is serial: `Consumer::next_batch` combines
   manifest read, object fetch, and decode in one call. That ceiling
   limits source throughput regardless of decode/sink concurrency.
5. **Backpressure is implicit** in the synchronous pipeline. Slow sinks
   slow the loop, but there is no shared byte budget and no explicit
   "backpressure reason" surfaced in metrics.
6. The schema and target table are compiled into the binary. There is
   no path for an operator to write the same OTLP logs to a custom
   ClickHouse table or a new Iceberg table without forking the binary.

The high-throughput design (`plans/odb-high-throughput`) calls for one
service that can host **multiple Buffer sources for one sink**, preserve
at-least-once with per-source idempotency at the sink, and approach
single-node network or sink-ingest limits. None of that fits inside the
ClickHouse ingestor as written. Independent sinks (e.g. ClickHouse and
Iceberg) deploy as separate runtime services with their own Buffer
queues; same-process multi-sink fanout is out of scope (see
"Alternatives: Same-Process Multi-Sink Fanout").

The cheapest path forward is to pull the runtime, ack control, commit
grouping, and pipeline scaffolding into a separate crate, define the
trait surface that source readers and sinks plug into, and re-host the
ClickHouse logs path on top of it without intentional behavior changes.
That refactor isolates the correctness work (per-source ack frontier,
single-sink commit invariants, idempotency contract) from the throughput
work (parallel fetch, parallel decode, columnar representation, binary
serialization).

## Goals

- Define a sink-neutral runtime crate, `opendata-ingest-runtime`, that
  owns polling, decode orchestration, per-source commit grouping, retry,
  ack, and backpressure.
- Define the trait surface for source reading, decoding, sinks, and
  idempotency, with no ClickHouse-specific types.
- Define the per-source `AckCoordinator` state machine and the
  single-sink ack invariant:

  > For each source, ack advances only after the configured sink has
  > durably committed or verified prior commit for the relevant source
  > sequence range.

- Define the bounded-stage pipeline with shared byte budget and
  source-aware fairness, so sink slowdown pauses source pulls without
  unbounded memory growth and a hot source does not permanently starve
  a low-volume source on the shared sink writer pool.
- Define the columnar migration path: typed records first, Arrow
  `RecordBatch` before the first lakehouse sink and before ClickHouse
  binary serialization.
- Define pluggability: native plugin crates as the v1 path, declarative
  schema/mapping for target tables as the v2 path, dynamic plugins as a
  measured follow-up.
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
  sink is worse than halting; v1 chooses safety and a single sink
  per service avoids the question.
- Buffer producer-side concerns. Producer parallelism, exporter
  configuration, and manifest commit coordination are separate work in
  the `opendata-go` repo.
- Buffer wire format or manifest semantics. The descriptor read-ahead
  and `ack_through` API are specified in a separate `opendata`
  RFC; this RFC consumes them.
- Schema evolution and DDL ownership. v1 still expects target table
  schemas to be applied out of band. Declarative schema/mapping config
  is scoped here, but server-owned migrations are not.
- WASM, dylib, or subprocess plugin ABIs. Deferred until the native
  hot path is measured. Mentioned here so the trait shapes do not
  preclude them.
- Exactly-once delivery as a runtime guarantee. The runtime preserves
  at-least-once and lets sinks provide their own idempotent commit
  semantics through the `IdempotencyContract` boundary.
- Replacement of the ClickHouse alpha's correctness model. RFC 0001's
  dedupe and crash semantics carry over to the ClickHouse sink as it
  ports to the runtime.

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
  `ack_through(sequence)`. The runtime depends on this RFC for
  parallel fetch and bulk ack; it can ship a serial-fallback path while
  RFC 0003 is in flight.
- **opendata-contrib RFC 0001 (ClickHouse Ingestor)**: shipped layering
  for the ClickHouse logs path. The generic runtime preserves every
  layer in 0001 and renames or generalizes only what must change to
  support a sink-neutral runtime that can host different sink types
  (one per service).
- **`plans/odb-high-throughput/odb-high-throughput-ingestor-design.md`**:
  the design narrative this RFC formalizes. The design doc is the
  product story; this RFC is the contract.

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
  preserves the `RawEntry` materialization from RFC 0001 (see "Source
  Reader" below).

## Design

### Architecture

The runtime owns the horizontal stages between Buffer (or another source)
and the configured sink. N source pipelines feed one shared sink writer
pool:

```text
                          ╔═══ ingest-runtime process (1 sink) ════════════════════════════════════════════════════════════════════════╗
                          ║                                                                                                            ║
                          ║   ┌──────────────┐   ┌──────────────┐   ┌─────────────────┐   ┌──────────────┐                              ║
                          ║   │ Descriptor   │   │  Fetch+      │   │ Envelope+Signal │   │ CommitGroup  │                              ║
                       ┌──╫───▶  poller      ├───▶  decompress  ├───▶ decoder         ├───▶ per source   │                              ║
                       │  ║   │ (per source) │   │   workers    │   │   workers       │   │              │                              ║
╔══Object Storage══╗   │  ║   └──────┬───────┘   └──────────────┘   └─────────────────┘   └──────┬───────┘                              ║
║                  ║   │  ║          │ N source pipelines                                        │                                      ║
║   Manifest(s) +  ║   │  ║          │                                                           ▼                                      ║
║   Batches        ╞══►┘  ║          │                                                  ┌──────────────────┐                            ║
║                  ║      ║          │                                                  │ Shared sink      │                            ║
╚═════════▲════════╝      ║          │                                                  │ writer pool      │                            ║   ╔══Sink═════════════╗
          │               ║          │                                                  │ (with source     ├────────────────────────────╫───▶ ClickHouse OR     ║
          │               ║          │                                                  │  fairness)       │                            ║   ║ Iceberg OR fake   ║
          │               ║          │                                                  └────────┬─────────┘                            ║   ╚════════════════════╝
          │               ║          │                                                           │
          │               ║          │                                                           ▼                                      ║
          │               ║          │                                                  ┌──────────────────┐                            ║
          │               ║          └────────── per-source ack frontier ──────────────▶│ AckCoordinator   │                            ║
          └───────────────╫─────────  (only after configured sink commit for that range) │ (one per source) │                            ║
                          ║                                                              └──────────────────┘                            ║
                          ║                                                                                                            ║
                          ╚════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
```

What is generic vs. plugin:

| Layer | Owner |
|---|---|
| Source descriptor poller, fetch workers, decompression | Runtime (per source plugin shape) |
| Per-entry envelope materialization (RFC 0001 `RawEntry`) | Runtime |
| Signal decoder | Plugin (`Decoder`) |
| Per-source commit group, deterministic chunking | Runtime |
| Sink write, retry classification, idempotency check | Plugin (`Sink` + `IdempotencyContract`) |
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

### Trait Surface

All trait names below are placeholders the implementation may rename;
the invariants are the contract.

#### `SourceReader` and `SourceFetchHandle`

The source side splits into two traits, mirroring RFC 0003's
`Consumer` / `ConsumerFetchHandle` shape. The runtime owns one
`SourceReader` per source (`&mut self` for descriptor poll and ack)
and clones an N-way `SourceFetchHandle` into fetch worker tasks.

```rust
#[async_trait::async_trait]
pub trait SourceReader: Send + 'static {
    /// Stable identifier for this source; used in metric labels and
    /// idempotency keys.
    fn id(&self) -> &SourceId;

    /// Fetch up to `max` new descriptors past the current cursor.
    /// Must not mutate the durable ack frontier. Returning fewer than
    /// `max` is allowed and signals "no more visible right now"; the
    /// runtime will sleep and retry.
    async fn next_descriptors(&mut self, max: usize, budget: SourceBudget)
        -> RuntimeResult<Vec<SourceBatchDescriptor>>;

    /// Construct a cloneable handle for fetching descriptors
    /// concurrently. Construction is O(1); the handle holds shared
    /// references to whatever the source needs (object store handle,
    /// HTTP client, etc.) and no manifest state.
    fn fetch_handle(&self) -> Box<dyn SourceFetchHandle>;

    /// Advance the durable ack frontier through (and including)
    /// `sequence`. Implementations are responsible for the in-order
    /// requirement of the underlying source; the runtime guarantees
    /// monotonic advance.
    async fn ack_through(&mut self, sequence: u64) -> RuntimeResult<()>;

    /// Force the underlying source's durable checkpoint. The runtime
    /// calls this on flush boundaries.
    async fn flush_acks(&mut self) -> RuntimeResult<()>;
}

/// Cloneable, concurrency-safe fetch primitive. The runtime calls
/// `fetch` from N workers in parallel against distinct descriptors.
/// Implementations must not touch manifest or ack state here.
#[async_trait::async_trait]
pub trait SourceFetchHandle: Send + Sync {
    async fn fetch(&self, descriptor: SourceBatchDescriptor)
        -> RuntimeResult<SourceBatch>;

    /// Object-safe clone. The default `Clone` derive does not work
    /// across `dyn Trait`; implementations return a new boxed handle.
    fn clone_box(&self) -> Box<dyn SourceFetchHandle>;
}

impl Clone for Box<dyn SourceFetchHandle> {
    fn clone(&self) -> Self { self.clone_box() }
}

pub struct SourceBatchDescriptor {
    pub source: SourceId,
    pub sequence: u64,
    pub location: String,
    pub per_range_metadata: Vec<SourceRangeMetadata>,
    /// Object size in bytes, when the source can supply it without an
    /// extra round trip. The Buffer source reader passes this through
    /// from `BatchDescriptor.object_bytes` (RFC 0003), which is
    /// `None` until the manifest format carries object size as a
    /// follow-up. When `None`, the runtime's byte-budget accounting
    /// uses the configured `source.estimated_max_batch_bytes` as a
    /// pessimistic reservation; see "Backpressure Model > Byte
    /// Budget Accounting" below.
    pub object_bytes: Option<u64>,
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

`SourceBatch` is a renamed superset of RFC 0001's `RawBufferBatch`. The
Buffer implementation of `SourceReader` shells out to
`buffer::Consumer::next_descriptors` / `Consumer::ack_through` /
`Consumer::flush` for the manifest-owner methods, and to
`buffer::ConsumerFetchHandle::fetch` (RFC 0003) inside its
`SourceFetchHandle`. It reuses the existing `split_into_raw_entries`
materialization to convert each `buffer::ConsumedBatch` into a
`SourceBatch`.

The two-trait split is what keeps `SourceReader` itself only `Send`
(it owns mutable manifest state) while `SourceFetchHandle: Send +
Sync` lets fetch workers run concurrently. `SourceReader::fetch_handle`
returns a fresh boxed handle; the runtime clones it into N workers
via `Box<dyn SourceFetchHandle>`'s `Clone` impl.

The traits are async to keep the door open for non-Buffer sources
later (Kafka, file scan, direct OTLP push). Adding sources should
not require runtime changes; they only need to honor in-order acks
within a source and provide a concurrency-safe fetch handle.

#### `Decoder`

```rust
pub trait Decoder: Send + Sync + 'static {
    fn accepts(&self, envelope: &MetadataEnvelope) -> bool;

    fn decode(&self, batch: SourceBatch)
        -> RuntimeResult<Vec<DecodedBatch>>;
}

pub struct MetadataEnvelope {
    pub version: u8,
    pub signal_type: SignalType,
    pub encoding: PayloadEncoding,
}
```

v1 contract: **one decoder per source**. The runtime calls
`accepts(envelope)` once per source, with the source's configured
envelope, at startup or on first non-empty batch. If `accepts` returns
false, the runtime fails closed (mirroring RFC 0001). `decode` is then
called per `SourceBatch` and consumes the whole batch.

The trait shape is wider than the v1 contract on purpose: the decoder
is invoked per-batch, but `accepts(envelope)` takes a single envelope
so a future runtime can dispatch entries with different envelopes to
different decoders. **That future dispatch is not implemented in v1**
because `Decoder::decode(&self, batch: SourceBatch)` consumes the
entire batch; supporting it requires either splitting `SourceBatch`
upstream of decoders (a runtime change) or evolving the trait to
`decode(&self, batch: SourceBatch, entry_indices: &[u32])` (a trait
change). Either path is a follow-up RFC; v1 keeps the homogeneous-
envelope-per-source rule from RFC 0001.

`decode` returns `Vec<DecodedBatch>` so a future per-signal split can
emit multiple decoded batches (e.g. mixed signals in a future
multi-signal source). **For v1 each `Decoder` returns at most one
`DecodedBatch` per call.**

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
    /// Typed Rust records, today: `Vec<DecodedLogRecord>` from RFC 0001.
    /// Wrapped in `Arc` so the runtime can hand the batch through
    /// async stages (commit-group append, sink-writer queue, retry
    /// path) without copying records; sinks that need a typed view
    /// downcast through `as_any`.
    Typed(Arc<dyn TypedRecords + Send + Sync>),
    /// Arrow columnar batch. `RecordBatch` is internally `Arc`-shared
    /// across columns, but we wrap it in an outer `Arc` so the
    /// `DecodedRecords` enum is `Clone` cheaply across stages.
    Arrow(Arc<arrow_array::RecordBatch>),
}

impl Clone for DecodedRecords {
    /// O(1) reference-count clone. Used by the runtime to pass the
    /// decoded batch through async stages (commit-group, sink-writer
    /// queue, retry) without copying records.
    fn clone(&self) -> Self { /* trivial */ unimplemented!() }
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

Two design decisions worth flagging:

- **The `DecodedRecords` enum exists for migration, not as a permanent
  shape.** v1 ships Typed only; Arrow lands in Phase 7 of the impl plan
  alongside benches that compare the two. Once Arrow is in, Typed is
  retained for the OTLP-logs path until ClickHouse and Iceberg are both
  on Arrow, then removed.
- **`source_columns` is parallel to records, not embedded in them.**
  This lets the ClickHouse adapter materialize source columns as system
  columns (RFC 0001 `_odb_*`), and lets the Iceberg writer attach them
  to snapshot metadata or as Parquet columns, without forcing every
  decoder to know either sink's schema.
- **Both `DecodedRecords` and `SourceCoordinateColumns` are
  reference-counted.** A `DecodedBatch` produced by a decoder is
  consumed once by the runtime, which then constructs one `SinkCommit`
  for that source range and hands it to the configured sink. The
  `SinkCommit` holds an `Arc` clone of the records and source columns
  so the runtime can keep a copy on the retry path (for `MaybeCommitted`
  resolution) without copying record data. Sinks that need to
  materialize a projection of the batch (e.g. an Iceberg writer that
  writes Parquet columns from a subset of fields) do so on their own
  thread, reading through the Arc.

`source_entry_count` lets the commit group and the ack coordinator
advance the input high-watermark even when `records` is empty, mirroring
RFC 0001's "Input progress is independent of output rows" property.

#### Routing (Future)

There is no `Router` trait in v1. A runtime service has one configured
sink and the entire `DecodedBatch` flows to that sink for the source
range it covers. If a future use case requires record-level routing
inside one sink (e.g. attribute-based table selection), that is
handled at the sink/schema-mapping layer (see "Configuration Shape" and
"Future Improvements") rather than as a runtime trait. Cross-sink
fanout in one runtime process is explicitly out of scope; see
"Alternatives: Same-Process Multi-Sink Fanout".

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
   sink must use the runtime's `IdempotencyKey` (or a sink-internal
   identifier derived from it deterministically — e.g. the same key
   plus a stable internal chunk index) so a second attempt does not
   produce duplicate data. ClickHouse uses `insert_deduplication_token`
   plus `ReplacingMergeTree(_adapter_version)`. Iceberg uses
   deterministic Parquet file paths plus a `check_committed` snapshot
   lookup.
3. **`check_committed(idempotency_key)` reflects source-range commit.**
   It returns `Committed` only when the full source range landed
   under that key — not when some internal chunks landed and others
   didn't. Sinks that cannot tell whether all their internal pieces
   are present return `Unknown`; the runtime then re-attempts `write`
   and relies on the sink's idempotency to drop the redundant work.

The runtime does **not** require sinks to roll back partial state on
failure. ClickHouse cannot transactionally undo successful inserts;
Iceberg can cancel a snapshot commit but cannot reliably delete data
files written outside that commit. Requiring atomic-with-rollback
would force sinks into either a two-phase commit none of these systems
support cleanly, or aggressive cleanup paths that turn ambiguous
errors into data loss. The "idempotent retry" rule is the right
contract for both ClickHouse insert dedupe and Iceberg snapshot-based
identity.

What the runtime *does* require: between `Err(_)` and the next
`write` retry, the sink must not produce divergent state for the
same `IdempotencyKey`. A sink that decides on retry to use a
different chunking, a different Parquet schema, or a different
internal commit identity violates the idempotency contract and will
cause duplicate data.

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
    /// "Idempotency Contract" below).
    async fn write(&self, commit: SinkCommit)
        -> Result<SinkCommitResult, SinkCommitFailure>;

    /// Inspect prior commit state for a given idempotency key. The
    /// runtime calls this on replay (after a crash) and on
    /// `MaybeCommitted` failure (after an ambiguous response from
    /// the sink). Sinks that cannot tell return
    /// `CommitStatus::Unknown`; the runtime then re-attempts the
    /// `write` and relies on the sink's table-level dedupe (e.g.
    /// `ReplacingMergeTree(_adapter_version)`) to clean up.
    async fn check_committed(&self, key: &IdempotencyKey)
        -> RuntimeResult<CommitStatus>;
}

pub struct SinkCommit {
    pub source: SourceId,
    /// Identity of the configured sink. The runtime service hosts a
    /// single sink, so this value is constant across calls; it is
    /// retained in the commit shape for observability (metric labels)
    /// and idempotency-key collision resistance across deployments.
    pub sink: SinkId,
    pub low_sequence: u64,
    pub high_sequence: u64,
    pub schema_version: SchemaVersion,
    pub idempotency_key: IdempotencyKey,
    /// O(1) Arc clone of the decoder's output for this source range.
    pub records: DecodedRecords,
    pub source_columns: Arc<SourceCoordinateColumns>,
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
    /// `check_committed(idempotency_key)` before deciding whether
    /// to retry.
    MaybeCommitted(BoxError),
    /// Non-retryable. The runtime halts. Examples: schema mismatch,
    /// permissions error, malformed request that cannot succeed
    /// without code or schema changes.
    Fatal(BoxError),
}

pub enum CommitStatus {
    Committed,
    NotCommitted,
    /// Sink cannot tell from idempotency key alone (e.g. ClickHouse
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
| `MaybeCommitted(_)` | Call `check_committed(key)`. If `Committed`, mark the range committed without writing again. If `NotCommitted`, retry the `write`. If `Unknown`, retry the `write` and rely on table-level dedupe. |
| `Fatal(_)` | Halt the runtime. The operator inspects the offending range and decides whether to fix the sink, fix the data, or use the documented escape hatch to advance past the range. |

This is the layer that protects single-sink idempotency under
ClickHouse insert timeouts and Iceberg catalog-commit timeouts.
Without it, a `MaybeCommitted` event would either ack-on-first-success
(data loss if the sink quietly didn't commit) or retry-on-failure
(duplicate Iceberg snapshot, stale ClickHouse insert dedupe token).

`check_committed(key)` is the primitive that lets a sink say "I already
have this; do not write it again." It is the same call used for
crash-replay (after a process restart, before the runtime advances
acks past the durable frontier) and for `MaybeCommitted` resolution.
The ClickHouse sink implements it as a no-op that always returns
`Unknown` because alpha ClickHouse dedupes at the table layer with
`ReplacingMergeTree(_adapter_version)`. The Iceberg sink implements it
by inspecting snapshot metadata for a file whose key matches.

#### `IdempotencyContract`

The runtime-level idempotency key identifies a single (source range)
commit at the configured sink. **It does not include `chunk_index`**:
the sink's `write` call covers a whole source range, so the runtime
never observes individual chunks. Sinks that internally chunk (e.g.
ClickHouse insert chunks, Iceberg Parquet files) construct their own
per-chunk identifiers from the runtime's `IdempotencyKey` plus a
sink-internal index.

```rust
pub trait IdempotencyContract: Send + Sync {
    fn key(&self, scope: IdempotencyScope<'_>) -> IdempotencyKey;
}

pub struct IdempotencyScope<'a> {
    pub source: &'a SourceId,
    /// Stable identity of the configured sink. Kept in the scope so
    /// the same source-range key is distinct across deployments that
    /// share an upstream Buffer (e.g. duplicated-queue deployments
    /// that fan out to ClickHouse and Iceberg in separate runtime
    /// services).
    pub sink: &'a SinkId,
    pub low_sequence: u64,
    pub high_sequence: u64,
    pub schema_version: SchemaVersion,
    /// Pure-function hash of every input that affects how the sink
    /// will internally chunk and order this commit (commit-group
    /// thresholds, ordering rule id, sink-specific config). Two
    /// runtime configurations that produce the same internal
    /// chunk/file boundaries have the same fingerprint; any change
    /// that could move a record produces a different fingerprint
    /// and therefore a different idempotency key.
    pub chunking_fingerprint: u64,
}

pub struct IdempotencyKey(pub String);
```

The default implementation produces:

```text
{source}:{sink}:{low}-{high}:{schema_version}:{chunking_fingerprint}
```

This is RFC 0001's per-chunk token format with `chunk_index` stripped
and `route` replaced by `sink`: the runtime hands the configured sink
one job per source range, so a single key is enough at the runtime
layer. Including `sink` in the key keeps duplicated-queue deployments
(same upstream stream feeding two isolated runtime services) free of
key collisions across services.

The ClickHouse sink, internally, builds RFC 0001's full token by
appending its own `chunk_index`:

```text
{runtime_idempotency_key}:{chunk_index}
```

The Iceberg sink does the analogous thing for its Parquet file
identity. Sinks own that suffix; the runtime never constructs it.

### Per-Source Ack Coordinator

The ack coordinator is the single most important piece of correctness
that changes from RFC 0001 to this RFC. There is **one coordinator per
source**; coordinators are independent — one source's committed range
never advances another source's frontier.

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

1. **Range becomes pending** when decoded records enter the
   per-source commit group, or when an empty decoded batch still
   represents input progress (zero-record batch). Pending ranges are
   tracked by source sequence because concurrent fetch / decode /
   write can complete ranges out of order.
2. **Range becomes committed** when the configured sink reports a
   successful commit. The runtime issues **exactly one**
   `Sink::write` call per range at a time; the sink's contract (see
   "`Sink`" above) is that `Ok(_)` means the full source-range commit
   is complete and that retry of the same `SinkCommit` is idempotent.
   No per-chunk completion tracking inside the runtime. A range is
   marked committed when:
   - `Sink::write(commit)` returns `Ok(_)`, or
   - `Sink::write(commit)` returns `Err(MaybeCommitted)` and the
     subsequent `check_committed(key)` returns `Committed`, or
   - On replay after restart, `check_committed(key)` returns
     `Committed` before the runtime would have re-attempted the
     `write`.
3. **Frontier advances** to the highest contiguous committed sequence
   from the current `acked_frontier`. The coordinator never advances
   over a hole.
4. **Flush** calls `SourceReader::ack_through(frontier)` and then
   `SourceReader::flush_acks` per the configured `AckFlushPolicy`
   (default: every commit group, mirroring RFC 0001).

> **Why a single-bit per range is sufficient.** Earlier drafts of this
> RFC tracked completion per `(route, chunk_index)` and then per
> `(range, route)`. The current scope (one configured sink per
> runtime service) plus the sink contract — `Ok(_)` means the full
> source-range commit is complete, and retry of the same `SinkCommit`
> is idempotent — collapses this into a single-bit-per-range check
> at the runtime layer. Cross-sink fanout is not a runtime concern;
> see "Alternatives: Same-Process Multi-Sink Fanout".

#### Crash Semantics

- **Crash before sink commit**: nothing in the Buffer ack moves.
  Source replays the range. Same outcome as RFC 0001.
- **Crash after sink commit, before ack flush**: the ack frontier did
  not advance (the range was committed but not yet flushed). On
  replay the runtime calls `check_committed(key)` *before*
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
> range or reported `Committed` for that range's idempotency key.**

This is the single sentence the gate reviewer should re-validate at
every phase that touches ack flow. Tests in Phase 5 of the impl plan
must demonstrate this invariant under deterministic out-of-order
range completion, retry / `MaybeCommitted` resolution, replay, and
multi-source ack isolation (one source's frontier never advances
another source's frontier).

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
| Per-source commit-group | Source's commit group at row/byte/age threshold |
| Sink write | Sink-wide budget (`Sink::write_budget`); slow sink fills its writer queue and stalls upstream for every source feeding it |
| Ack | Frontier blocked on uncommitted pending range (per source) |

Required configuration knobs (these names are stable across phases; a
later config schema RFC may rename, but the semantics carry through):

- `source.max_inflight_batches`
- `source.max_inflight_bytes`
- `source.estimated_max_batch_bytes` — pessimistic reservation per
  batch when `BatchDescriptor.object_bytes` is `None`. Defaults to
  `source.max_inflight_bytes / source.max_inflight_batches` rounded
  up; operators override when the workload is known to use larger
  batches.
- `source.fetch_concurrency`
- `source.decompress_concurrency`
- `decode.concurrency`
- `commit_group.max_rows`
- `commit_group.max_bytes`
- `commit_group.max_age_ms`
- `sink.<name>.max_concurrent_commits`
- `sink.<name>.retry.max_attempts`
- `sink.<name>.retry.initial_backoff_ms`

#### Byte Budget Accounting

In-flight bytes are tracked from descriptor reservation through sink
commit. The accounting rule is the same regardless of source:

1. **At descriptor reservation** (when `next_descriptors` produces a
   batch and the runtime decides whether to fetch it):
   - If `descriptor.object_bytes == Some(n)`, reserve `n` bytes from
     the source's in-flight budget.
   - If `descriptor.object_bytes == None` (Buffer's current case;
     RFC 0003 reserves the field but the manifest does not yet carry
     it), reserve `source.estimated_max_batch_bytes` bytes.
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

The runtime never uses HEAD requests against object storage to
discover sizes. The `estimated_max_batch_bytes` fallback is intentional
slack: it overcounts in the common case and the source poller pauses
sooner than it strictly has to. When a future Buffer manifest format
revision (or an opt-in producer-side metadata extension) provides
`object_bytes`, the accounting becomes tight.

Phase 6 of the impl plan must demonstrate this by showing
`runtime_stage_inflight_bytes{stage,source}` rising and the source
poller backing off on a slow-sink injection — the test is in the
correctness harness, not in benchmark numbers.

Required metrics (stage-labeled):

- `runtime_source_queue_depth{stage,source}`
- `runtime_source_inflight_bytes{stage,source}`
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

These metrics are how the gate reviewer audits Phase 6
("Re-run correctness harness under concurrency") without re-reading
code.

### Columnar Migration

The runtime supports both `DecodedRecords::Typed` and
`DecodedRecords::Arrow`. Phase ordering:

1. **Phase 4 (Runtime extraction)**: Typed only. The OTLP logs decoder
   continues to produce `Vec<DecodedLogRecord>`. A `TypedRecords`
   adapter wraps it and the ClickHouse sink downcasts back. Behavior is
   equivalent to RFC 0001.
2. **Phase 7 (Schema/mapping + Arrow prototype)**: an Arrow OTLP logs
   decoder is built behind a feature flag. Benchmarks compare:
   per-record allocation, end-to-end stage latency, ClickHouse
   serialization cost, projected Iceberg/Parquet write cost.
3. **Phase 8 (ClickHouse throughput)**: ClickHouse moves to a binary
   format (`RowBinaryWithNamesAndTypes` or Native), reading from
   `Arrow`.
4. **Phase 9 (Iceberg sink)**: Iceberg reads from `Arrow` directly.
5. **Post-Phase 9**: Typed path is retired for OTLP logs. The runtime
   may keep `DecodedRecords::Typed` available for unusual signals
   that do not have a clean Arrow representation, but the default
   becomes Arrow.

Arrow-vs-typed is a measured decision, not a stylistic one. The Phase 7
benchmark gates the migration; if Arrow loses on the targeted workloads,
the migration stalls and we revisit the runtime contract.

### Source Reader: Buffer Implementation

`opendata-ingest-runtime` ships one source reader,
`BufferSourceReader`, that wraps `buffer::Consumer` plus a paired
`BufferSourceFetchHandle` that wraps `buffer::ConsumerFetchHandle`
(RFC 0003).

`BufferSourceReader` (manifest owner, `&mut self`):

- `next_descriptors(max, budget)` calls
  `Consumer::next_descriptors(max)` and rejects descriptors past
  `budget.bytes_remaining` to keep the source under
  `source.max_inflight_bytes`. Each returned `SourceBatchDescriptor`
  carries `object_bytes` straight through from
  `BatchDescriptor.object_bytes` (RFC 0003), which is `None` until
  the manifest format extension lands; the runtime then falls back to
  `source.estimated_max_batch_bytes` for budget reservation.
- `fetch_handle()` returns `Box::new(BufferSourceFetchHandle {
  inner: consumer.fetch_handle() })`.
- `ack_through(seq)` calls `Consumer::ack_through(seq)`.
- `flush_acks` calls `Consumer::flush()`.

`BufferSourceFetchHandle` (cloneable fetcher, `&self`):

- `fetch(descriptor)` calls
  `ConsumerFetchHandle::fetch(buffer_descriptor)` (the underlying
  `buffer::BatchDescriptor`, reconstructed from `SourceBatchDescriptor`),
  receives a `buffer::ConsumedBatch`, and runs `split_into_raw_entries`
  to produce a `SourceBatch`.
- `clone_box()` clones the inner `ConsumerFetchHandle` (which is `Clone`)
  and returns a new `Box<dyn SourceFetchHandle>`.

`split_into_raw_entries` (today in `clickhouse-ingestor::source`) is
pulled into the runtime crate so it can be reused by future sources
that surface per-range metadata in the same shape.

If RFC 0003 of opendata-buffer is not yet released, the runtime can
fall back to a serial path that calls `Consumer::next_batch` and
emits a single descriptor whose location is the just-fetched batch.
This compatibility path is required only during Phase 4
(extraction) and removed in Phase 6 (pipelining).

### Decoder: v1 Defaults

v1 ships:

- `OtlpLogsDecoder`: pulled from `clickhouse-ingestor::signal`,
  unchanged behavior. `accepts(envelope)` returns true for `(version=1,
  signal_type=Logs, encoding=OtlpProtobuf)`.
- A future `OtlpMetricsDecoder` once metrics targets ship; not v1.

Per-entry envelope dispatch across decoders is supported by the trait
shape but not implemented in v1. v1 fails closed on mixed envelopes
within a single source, mirroring RFC 0001.

There is no `Router` trait in v1. Each source's `DecodedBatch` flows
to the single configured sink for the runtime service.

### Sink Plugins: ClickHouse and Iceberg

This RFC does not specify the ClickHouse or Iceberg sinks; they have
their own RFCs and crates. The required compatibility points:

- **ClickHouse**: the existing `Adapter::plan -> Vec<InsertChunk>` path
  becomes the body of `clickhouse_sink::write`. The deterministic
  chunking, idempotency token construction, and ClickHouse settings
  carry over unchanged. `check_committed` returns `Unknown`; the
  ClickHouse sink relies on `ReplacingMergeTree(_adapter_version)` for
  long-window dedupe, exactly as RFC 0001 documents.
- **Iceberg**: writes Parquet data files keyed on `IdempotencyKey`,
  commits via the configured catalog, and implements `check_committed`
  by inspecting snapshot metadata for a file whose key matches.
  Detailed semantics in the Iceberg RFC.

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
    envelope:
      version: 1
      signal_type: logs
      encoding: otlp_protobuf
    decoder: otlp_logs
    commit_group:
      max_rows: 100000
      max_bytes: 33554432
      max_age_ms: 1000
    ack:
      policy: every_commit_group
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
  retry:
    max_attempts: 6
    initial_backoff_ms: 100
```

Key shape decisions:

- **`sink` is singular.** Two sinks => two runtime services, with
  independent Buffer queues and processes.
- **Sources are a top-level list keyed by `id`.** Each source has its
  own commit-group, ack, and backpressure config; sharing a pool
  would couple high-volume and low-volume signals.
- **`schema_ref` selects a built-in template or a user-supplied
  schema/mapping file** (Phase 7). The string `builtin/<name>` resolves
  to a compiled-in template; any other value is a path to a user
  schema/mapping document.
- **The sink declares retry and concurrency at the sink level.** The
  runtime applies them; sink plugins do not implement their own retry
  loops.
- **If different sources need different physical tables in the same
  sink, that is a sink/schema-mapping concern**, not a generic
  runtime route. The sink config can carry an explicit
  `source_mappings` / `tables` / `schema_ref_by_source` structure;
  this is a sink-side feature, not a runtime trait.

### Pluggability Levels

#### Level 1: Native Plugin Crates (v1)

Each plugin is a crate that depends on `opendata-ingest-runtime` and
implements the trait it owns:

- `opendata-ingest-otel`: `OtlpLogsDecoder`, future
  `OtlpMetricsDecoder`, `OtlpTracesDecoder`.
- `opendata-ingest-clickhouse`: `Sink` for ClickHouse.
- `opendata-ingest-iceberg`: `Sink` for Iceberg append-only.

A binary crate links the plugins it needs and registers them by name:

```rust
let mut registry = PluginRegistry::new();
registry.register_decoder("otlp_logs", OtlpLogsDecoder::new(...));
registry.register_sink_factory("clickhouse",
    Box::new(ClickHouseSinkFactory::default()));
registry.register_sink_factory("iceberg",
    Box::new(IcebergSinkFactory::default()));

let runtime = Runtime::new(config, registry).await?;
runtime.run(shutdown_token).await?;
```

Adding a new sink is: write a crate, register a factory by name, ship
a new binary. No runtime changes, no config schema changes beyond the
new sink's named config block.

#### Level 2: Declarative Schema/Mapping (Phase 7)

Target table schemas and projections move out of Rust where practical:

- A schema/mapping document defines columns, types, partition keys,
  source-coordinate column choices, and any attribute-flattening rules.
- Built-in templates ship in the binary (`otel_logs_clickhouse_v1`,
  `otel_logs_iceberg_v1`). User-provided documents are validated at
  startup.
- For OTLP, the **decoder is still compiled in** because tree
  flattening (resource/scope/log) is semantic, not generic protobuf
  decode.
- A user can target a custom ClickHouse table without rebuilding the
  binary by supplying their own schema/mapping document.

This RFC does not specify the schema document format; that is Phase 7
work and gets its own follow-up RFC.

#### Level 3: Dynamic Plugins (Future)

WASM plugins for transforms that are not on the hottest path, or
subprocess/gRPC plugins for company-specific enrichment. Considered
explicitly so the trait shapes do not preclude them, but not
implemented until the native path is benchmarked. Rust `cdylib`
plugins are rejected: Rust has no stable ABI and a dynamic library
boundary on the row path is the wrong first optimization.

### System Columns and Source Coordinates

Source-coordinate column ownership generalizes RFC 0001's table:

| Column | Owned by | Provenance |
|---|---|---|
| `_odb_sequence` | Runtime | `SourceBatchDescriptor.sequence` |
| `_odb_entry_index` | Runtime | `SourceEntry.entry_index` |
| `_odb_record_index` | Decoder | Flat record index within an entry |
| `_odb_manifest_path` | Runtime | Configured per source |
| `_odb_data_path` | Runtime | `SourceBatch.data_object_path` |
| `_odb_ingestion_time_ms` | Runtime | Per-range `Metadata.ingestion_time_ms` |
| `_adapter_version` / `_schema_version` | Sink | Sink schema version |

Sinks decide which coordinate columns to materialize. The runtime
provides them as `SourceCoordinateColumns` parallel to records; the
sink projects them according to its target schema.

### Multi-Source, Single-Sink Service

The runtime hosts N sources -> 1 sink in one process. The invariants
across sources:

- **Each source has its own `AckCoordinator`** and its own ack
  frontier. A failure in one source must not advance another source's
  Buffer ack. Multi-source ack isolation is a Phase 5 correctness
  test.
- **The configured sink is shared across sources.** The sink writer
  pool applies source-aware fairness so one hot source does not
  permanently starve a low-volume source. The first cut is bounded
  per-source queues feeding a shared sink semaphore; weighted
  fairness can be revisited later if the bench surfaces a need.
- **Two sources feeding the same target table** is allowed but adds a
  dedupe-key constraint inherited from RFC 0001's "Future:
  Config-Driven Multi-Source Ingestors": if two sources share a
  target table, the table's dedupe key must include `_odb_manifest_path`
  or `source_id` (or the equivalent for non-ClickHouse sinks). The
  config validator enforces this.
- **Process readiness fails** when any required source halts. v1 fails
  readiness on any source halt. Optional sources can be marked
  `optional: true` in a future revision; named here so the validator
  can adopt it later without churn.

### Operational Surface

- **Dry-run** (per source): full pipeline, including decode and the
  sink's plan/serialize step, but `Sink::write` is replaced with a
  no-op that returns success without side effects, and `ack_through`
  is skipped. Carry-over from RFC 0001. Toggling dry-run requires a
  process restart for the same reason as RFC 0001.
- **Graceful shutdown**: drain all in-flight per-source commit groups,
  write through the configured sink, advance and flush each source's
  ack frontier, exit. The runtime owns shutdown propagation through
  `tokio_util::CancellationToken`.
- **Hard crash recovery**: each source resumes from its last *flushed*
  ack frontier; replay is idempotent at sinks that implement
  `check_committed`, and dedupable at sinks that do not.

### Failure Modes

Inherited and generalized from RFC 0001:

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
sink decode them. Rejected because every sink implementation would
re-decode the same bytes, OTLP tree flattening is non-trivial, and
the runtime would not be able to size commit groups by row count or
apply schema-aware backpressure. The cost is borne even more sharply
when the same decoded shape is reused across sink types in
duplicated-queue deployments — every service decodes from scratch.

### Use `Vec<RowValue>` as the Cross-Sink Unit

The current ClickHouse path uses a row-of-`RowValue` shape. Rejected as
the runtime-wide unit because:

- `RowValue` is row-oriented and JSON-leaning, which is exactly the
  serialization path the ClickHouse perf phase wants to leave.
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
derive the source ack frontier, which is the multi-checkpoint
complexity RFC 0001 explicitly rejected for the Kafka connector
design. This alternative is moot under the rev-6 single-sink scope:
ack frontiers are per-source against the single configured sink. The
section is kept for context because it is the argument against
re-introducing same-process fanout in a later revision.

### WASM-First Plugin Boundary

Defer. The hot path is decode → commit-group → sink-plan → sink write.
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

## Future Improvements

These do not require changing the trait shapes in this RFC.

- **Per-entry signal dispatch within a single source**: relax v1's
  homogeneous-envelope requirement. Already supported by the
  `Decoder::accepts` shape.
- **Optional sources / per-source readiness** in the config validator
  and the metrics surface.
- **Sink-side schema mapping for multi-source -> multi-table**: when
  one sink should land different sources in different physical
  tables, encode the mapping in the sink config (e.g.
  `source_mappings`, `schema_ref_by_source`). This is sink/schema
  work, not a generic runtime route.
- **Arrow-only DecodedRecords** after Phase 9, removing the typed
  fallback for OTLP logs.
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
  inside Buffer's retained range. RFC 0001 defers this; the runtime
  inherits the deferral and exposes the seam (`SourceReader` already
  takes an initial sequence on construction).

## Validation Criteria

Phase-aligned with the impl plan.

### Phase 4 Exit (Runtime Extraction Without Perf Changes)

- The OTLP logs path runs end-to-end through the runtime crate's
  traits. The `clickhouse-ingestor` binary is registry/config wiring
  on top of `opendata-ingest-runtime` and `opendata-ingest-clickhouse`.
- Existing ClickHouse alpha tests pass unchanged.
- `cargo test -p opendata-ingest-runtime -p opendata-ingest-clickhouse`
  is green.
- Behavior is equivalent: same metrics names, same dry-run semantics,
  same rows landed in ClickHouse for the same input.

### Phase 5 Exit (Ack Coordinator and Single-Sink Correctness Harness)

- A fake source and a fake (single) sink (success, retryable failure,
  permanent failure, slow, ambiguous) reproduce every crash-point in
  the design doc.
- Tests demonstrate the single-sink ack invariant under
  out-of-order range completion, retry, `MaybeCommitted` resolution,
  and replay.
- Multi-source ack isolation: one source's committed range never
  advances another source's frontier.
- `check_committed` contract tests for at least one concrete sink
  (ClickHouse stub OK; Iceberg follows in Phase 9).

### Phase 6 Exit (Pipelined Runtime Under Concurrency)

- Fetch, decode, and write workers run concurrently across multiple
  sources; all stage queue and inflight metrics are exposed.
- A sink slowdown injection pauses source pulls within one
  commit-group window; recovery resumes without unbounded memory growth.
- The shared sink writer pool applies source fairness so a hot source
  does not permanently starve a low-volume source.
- Phase 5 correctness tests still pass under concurrency knobs greater
  than 1.

### Phase 7 Exit (Schema/Mapping + Arrow Prototype)

- A user-provided schema/mapping document targets a non-default
  ClickHouse table without binary rebuild.
- Arrow vs. typed benchmark numbers are recorded with workload shape,
  hardware, and config.

### Phase 9 Exit (Standalone Iceberg Sink Service)

- One Buffer source feeds an Iceberg-configured runtime service
  end-to-end. ClickHouse is not required to be present in the same
  process.
- Crash-after-Iceberg-commit-before-ack is verified idempotent on
  replay (`check_committed` returns `Committed`, no duplicate Parquet
  file is written).
- The ack coordinator advances only after the Iceberg sink commits
  (or verifies).

### Phase 10 Exit (Multi-Source Single-Sink E2E)

- Multiple Buffer sources feed one runtime service into a single
  configured sink (e.g. ClickHouse). Per-source ack frontiers and
  bounded backpressure both hold under load.
- Optional deployment proof: the same upstream stream is duplicated
  into two queues and consumed by two isolated runtime services
  (e.g. one ClickHouse, one Iceberg) — each service is a single-sink
  service with its own ack frontier.

## Revision History

| Date | Description |
|---|---|
| 2026-05-07 | Initial draft. Generalizes RFC 0001 into a sink-neutral runtime; defines source/decoder/router/sink traits, AckCoordinator state machine, fanout invariant, columnar migration path, pluggability levels, and validation criteria phase by phase. |
| 2026-05-07 (rev 2) | Phase 0 gate revision. (1) `Sink::write` is now atomic per (range, route); chunk_index removed from runtime IdempotencyKey (sinks build per-chunk identifiers internally); AckCoordinator tracks one bit per (range, route). (2) `DecodedRecords` switches `Box<dyn TypedRecords>` → `Arc<dyn TypedRecords + Send + Sync>` and `RecordBatch` → `Arc<RecordBatch>`; `SinkCommit.source_columns` is `Arc<SourceCoordinateColumns>`; fanout is O(1) Arc clones, no record copies. (3) Byte-budget accounting documented end-to-end with `BatchDescriptor.object_bytes` (RFC 0003) and `source.estimated_max_batch_bytes` pessimistic-reservation fallback; HEAD requests explicitly avoided. (4) New `SinkCommitFailure { NotCommitted, MaybeCommitted, Fatal }` enum; runtime calls `check_committed` on `MaybeCommitted` before retry. (5) Decoder per-entry routing marked future (current trait consumes whole `SourceBatch`; v1 = one decoder per source). |
| 2026-05-07 (rev 3) | Phase 0 gate reconciliation. (a) Split `SourceReader` into `SourceReader: Send + 'static` (manifest owner, `&mut self` next_descriptors / ack_through / flush_acks) and `SourceFetchHandle: Send + Sync` (cloneable, concurrency-safe `fetch`). The earlier draft claimed `fetch(&self)` was concurrent on a `Send`-only trait, which did not match RFC 0003's `&mut self` `fetch_descriptor`. The new shape mirrors RFC 0003. (b) Updated the Buffer source-reader implementation section to describe `BufferSourceReader` + `BufferSourceFetchHandle` and to call `ConsumerFetchHandle::fetch` (RFC 0003 rev 2), not the stale `Consumer::fetch_descriptor(&self)`. (c) Reworded the `Sink::write` contract: dropped "partial success is the sink's problem to clean up" (too strong for ClickHouse / Iceberg); replaced with a three-rule contract — `Ok(_)` means full route-level commit, retry of the same `SinkCommit` must be idempotent, `check_committed` reflects route-level (not internal-chunk) commit. The runtime does not require atomic-with-rollback. |
| 2026-05-07 (rev 4) | `Sink::write` rustdoc reworded from "Commit one (range, route) atomically" to "Commit one route-level unit ..." and explicitly references the idempotent-retry contract. The "atomically" wording revived the rolled-back-state interpretation that rev 3's surrounding prose had walked back. |
| 2026-05-07 (rev 5) | AckCoordinator narrative reworded to drop "atomic per (range, route)" — both the state-transition step (#2) and the "Why route-level tracking is sufficient" callout now say "`Ok(_)` means the full route-level commit is complete and retry of the same `SinkCommit` is idempotent." Pure wording fix; the contract has been route-level + idempotent-retry since rev 3. |
| 2026-05-08 (rev 6) | **Scope changed from same-process multi-sink fanout to multi-source / single-sink runtime service.** A runtime service hosts N sources -> 1 sink. Independent sinks are isolated through producer-side duplicate queues and separate runtime processes. `Router`, `RouteId`, `RouteAssignment`, `SinkCommit.route`, `IdempotencyScope.route`, and `SinkCommit.record_indices` are removed from the core runtime contract (the per-record `SourceCoordinateColumns.record_indices` column stays — that is the OTel within-entry record index). `SinkCommit` is keyed by `{source, sink}`; `IdempotencyScope` keys by `sink` (not `route`); `IdempotencyKey` shape is `{source}:{sink}:{low}-{high}:{schema_version}:{chunking_fingerprint}`. `AckCoordinator` is per-source with one sink-commit bit per range (`register_pending` / `mark_committed` / `frontier`); out-of-order range completion is handled by tracking pending ranges and never advancing the contiguous frontier over a hole. `MaybeCommitted` handling is unchanged. Configuration uses singular `sink:`; validator rejects multi-sink config. Multi-source section renamed to "Multi-Source, Single-Sink Service"; backpressure model gains source fairness on the shared sink writer pool. Metrics relabeled (`runtime_source_*`, `runtime_sink_*`, `runtime_sink_commits_total{source,sink,result}`). Phase 9 is a standalone Iceberg runtime service (no co-host with ClickHouse); Phase 10 is multi-source / single-sink e2e plus an optional duplicated-queue deployment proof. New "Same-Process Multi-Sink Fanout" alternative explains why the previous shape was rejected. |
