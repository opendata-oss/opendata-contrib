# RFC 0002: Generic Ingest Runtime

**Status**: Accepted

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

The runtime is built around two plugin trait boundaries — `Decoder`
and `Sink` — plus a concrete `BufferSource` on the source side.
Logical commit identity is a deterministic struct projection
(`CommitIdentity`); no trait is needed for its construction. For v1, the source side is not a trait: Heracles has one
source type (OpenData Buffer), and a trait surface would be near 1:1 with
`buffer::Consumer` / `ConsumerFetchHandle` with no second caller to
justify it. See "Source Side: Concrete `BufferSource` for v1" and
"Alternatives: Re-introduce a `SourceReader` Trait Now". The pipeline is
staged with bounded queues and a shared in-flight byte budget, so a slow
or failing sink pauses upstream work without unbounded memory growth. For each source, Buffer ack advances
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

The high-throughput design calls for one service that can host
**multiple Buffer sources for one sink**, preserve
at-least-once with per-source idempotency at the sink, and approach
single-node network or sink-ingest limits. None of that fits inside the
ClickHouse ingestor as written. Independent sinks (e.g. ClickHouse and
Iceberg) deploy as separate runtime services with their own Buffer
queues; same-process multi-sink fanout is out of scope (see
"Alternatives: Same-Process Multi-Sink Fanout").

The cheapest path forward is to pull the runtime, ack control, and
pipeline scaffolding into a separate crate, define the trait
surface that source readers and sinks plug into, and re-host the
ClickHouse logs path on top of it without intentional behavior
changes. That refactor isolates the correctness work (per-source
ack frontier, single-sink commit invariants, deterministic logical
commit identity) from the throughput work (parallel fetch,
parallel decode, columnar representation, binary serialization).
Chunking shape — how a sink turns one source-range `SinkCommit`
into one or many physical writes — is sink-internal; the runtime
hands the sink one `SinkCommit` per source range and never
inspects how the sink plans physical writes.

## Goals

- Define a sink-neutral runtime crate, `opendata-ingest-runtime`, that
  owns polling, decode orchestration, retry, ack, and backpressure.
  One source-range `DecodedBatch` → one `SinkCommit` (RFC 0002
  §Runtime/Sink Boundary); chunking config is owned by each sink
  plugin, not by the runtime.
- Define the trait surface for source reading, decoding, and sinks,
  with no ClickHouse-specific types. Logical commit identity is a
  deterministic struct projection (`CommitIdentity`); no contract
  trait is needed for its construction.
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
  semantics, derived deterministically from `CommitIdentity` plus the
  sink's own adapter configuration.
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
- **The high-throughput ingestor design narrative**: the companion
  design doc this RFC formalizes. The design doc is the product story;
  this RFC is the contract.

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
                          ║   ┌──────────────┐   ┌──────────────┐   ┌─────────────────┐                                                 ║
                          ║   │ Descriptor   │   │  Fetch+      │   │ Envelope+Signal │                                                 ║
                       ┌──╫───▶  poller      ├───▶  decompress  ├───▶ decoder         ├──┐                                              ║
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

There is **no `CommitGroup` stage** between decode and the shared
sink writer pool. The runtime emits one `SinkCommit` per source
range. Any future sink-side chunking accumulator lives inside the
sink plugin, not on the runtime surface.

What is generic vs. plugin:

| Layer | Owner |
|---|---|
| Source descriptor poller, fetch workers, decompression | Runtime (`BufferSource` concrete for v1) |
| Per-entry envelope materialization (RFC 0001 `RawEntry`) | Runtime |
| Signal decoder | Plugin (`Decoder`) |
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

**Runtime responsibilities:**

- Source IO and manifest cursor ownership (per-source
  `&mut BufferSource`).
- Descriptor admission and `INV-ADMISSION-CONTIGUOUS`.
- Fetch and decode orchestration (parallel workers, bounded queues,
  byte/batch budgets).
- Per-source ack coordination (`AckCoordinator`; frontier advancement
  under out-of-order completion).
- Retry orchestration (`write_with_retry`; `MaybeCommitted` resolution
  via `Sink::check_committed`).
- `CommitIdentity` construction — a deterministic projection of
  `(source, sink, sequence_range, schema_version)`. No hashing, no
  fingerprinting, no sink-specific fields.

**Sink/plugin responsibilities:**

- Downcasting `DecodedBatch` to the typed records the sink understands.
- Validating sink-specific invariants (single manifest path, row
  ordering, schema compatibility).
- Planning physical writes — chunking, file boundaries, partition
  selection. The runtime hands the sink one `SinkCommit` per source
  range; the sink decides how that range becomes one or many physical
  operations.
- Computing physical idempotency tokens from `CommitIdentity` +
  adapter configuration. The runtime supplies the logical identity;
  the sink turns it into whatever primitive its target system
  requires.
- Executing the writes.

**Handoff shape:**

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

All trait names below are placeholders the implementation may rename;
the invariants are the contract.

#### Source Side: Concrete `BufferSource` for v1

The runtime owns a concrete `BufferSource` per configured source.
There is no source-side trait surface in v1: Heracles has one source
type (OpenData Buffer), and a trait surface would be near 1:1 with
`buffer::Consumer` / `ConsumerFetchHandle` (RFC 0003) with no second
caller to justify it. See "Re-introduce a `SourceReader` Trait Now"
under Alternatives Considered for the trade-off.

`BufferSource` wraps `buffer::Consumer` (manifest owner, `&mut self`)
and exposes a paired `BufferSourceFetchHandle` (cloneable,
`Send + Sync + 'static`) that wraps `buffer::ConsumerFetchHandle`. The
split mirrors RFC 0003: the owner mutates manifest cursors and the
durable ack frontier under `&mut self`; the handle is cloned into N
fetch worker tasks under `&self`.

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
    /// Object size in bytes, when the source can supply it without an
    /// extra round trip. `BufferSource` passes this through from
    /// `BatchDescriptor.object_bytes` (RFC 0003), which is `None`
    /// until the manifest format carries object size as a follow-up.
    /// When `None`, the runtime's byte-budget accounting uses the
    /// configured `source.estimated_max_batch_bytes` as a pessimistic
    /// reservation; see "Backpressure Model > Byte Budget Accounting".
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

`SourceBatch` is a renamed superset of RFC 0001's `RawBufferBatch`.
`BufferSource::next_descriptors` calls
`buffer::Consumer::next_descriptors(max)` and filters returned
descriptors against `budget.bytes_remaining`. `fetch_handle()` returns
a cheap clone. `ack_through` and `flush_acks` are pass-throughs to
`Consumer::ack_through` / `Consumer::flush`.
`BufferSourceFetchHandle::fetch` calls `ConsumerFetchHandle::fetch`
(RFC 0003) and runs `split_into_raw_entries` to produce a `SourceBatch`.

The data types above (`SourceId`, `SourceBatchDescriptor`,
`SourceBatch`, `SourceEntry`, `SourceBudget`, `SourceRangeMetadata`)
stay sink-neutral in the runtime crate. They carry runtime-only
fields — `SourceId` for idempotency keys and metric labels;
`manifest_path` for the `_odb_manifest_path` system column;
pre-flattened `per_range_metadata` parallel to entries — that the
underlying buffer types don't.

If RFC 0003 of opendata-buffer is not yet released, `BufferSource`
falls back to a serial path that calls `Consumer::next_batch` and
emits a single descriptor whose location is the just-fetched batch.
`BufferSourceFetchHandle::fetch` then pops from an internal
sequence-keyed cache of pre-fetched batches. This compatibility path
is dropped once the read-ahead consumer ships.

For test fakes in the correctness harness, the runtime crate
introduces a `#[cfg(test)]` source seam — production callers stay on
the concrete `BufferSource`. The seam shape (small trait under
`#[cfg(test)]` vs. a `Source` enum vs. handcrafted fixtures) is an
implementation detail of the harness.

If/when a non-Buffer source lands (Kafka direct, OTLP HTTP push,
file scan), reintroducing a `SourceReader` trait is a local change
inside the runtime crate. Sinks and the correctness harness do not
depend on the source shape, so the cost of waiting is bounded.

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
    /// async stages (decode handoff, sink-writer queue, retry path)
    /// without copying records; sinks that need a typed view
    /// downcast through `as_any`.
    Typed(Arc<dyn TypedRecords + Send + Sync>),
    /// Arrow columnar batch. `RecordBatch` is internally `Arc`-shared
    /// across columns, but we wrap it in an outer `Arc` so the
    /// `DecodedRecords` enum is `Clone` cheaply across stages.
    Arrow(Arc<arrow_array::RecordBatch>),
}

impl Clone for DecodedRecords {
    /// O(1) reference-count clone. Used by the runtime to pass the
    /// decoded batch through async stages (sink-writer queue,
    /// retry path) without copying records.
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
  shape.** The current path ships Typed only; an Arrow variant is future
  work, to land alongside benches that compare the two. Once Arrow is in,
  Typed is retained for the OTLP-logs path until ClickHouse and Iceberg
  are both on Arrow, then removed.
- **`source_columns` is parallel to records, not embedded in them.**
  This lets the ClickHouse adapter materialize source columns as system
  columns (RFC 0001 `_odb_*`), and lets the Iceberg writer attach them
  to snapshot metadata or as Parquet columns, without forcing every
  decoder to know either sink's schema.
- **`DecodedRecords` is reference-counted; `SourceCoordinateColumns`
  is plain-owned.** A `DecodedBatch` produced by a decoder is
  consumed once by the runtime, which constructs one `SinkCommit`
  for that source range and hands it to the configured sink.
  `DecodedRecords` wraps its payload in `Arc` (`Typed(Arc<dyn
  TypedRecords>)` or `Arrow(Arc<RecordBatch>)`) so the
  `SinkCommit` clone on the retry path is O(1) for the
  potentially-large record payload. `SourceCoordinateColumns`,
  by contrast, is a plain owned struct of small `Vec`s
  (`Vec<u64>` / `Vec<u32>` / `Vec<i64>` parallel to records);
  the retry-path `clone()` deep-copies those vectors. The Vecs
  are sized by the row count of one source range and the retry
  rate is low, so the deep copy is acceptable; if a future
  workload demonstrates measurable retry-path overhead from
  source-column cloning, wrapping the struct in `Arc` is a
  local change. Sinks that need to materialize a projection of
  the batch (e.g. an Iceberg writer that writes Parquet columns
  from a subset of fields) do so on their own thread; they read
  records through the `Arc` and either project columns
  directly or own the resulting projection.

`source_entry_count` lets the per-source `AckCoordinator` advance
the input high-watermark even when `records` is empty, mirroring
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
errors into data loss. The "idempotent retry" rule is the right
contract for both ClickHouse insert dedupe and Iceberg snapshot-based
identity.

What the runtime *does* require: between `Err(_)` and the next
`write` retry, the sink must not produce divergent state for the
same `CommitIdentity`. A sink that decides on retry to use a
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
always returns `Unknown` because alpha ClickHouse dedupes at the
table layer with `ReplacingMergeTree(_adapter_version)`. The
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

/// Inclusive range over Buffer batch sequences. Single-batch ranges
/// have `low == high` — the runtime never coalesces source ranges.
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

The canonical `Display` projection is

```text
{source}:{sink}:{low}-{high}:{schema_version}
```

which the runtime uses for log fields and metric labels. The
ClickHouse sink, internally, builds its full per-chunk
`insert_deduplication_token` by appending its adapter version, its
chunking fingerprint (a hash of its own adapter configuration), and
its `chunk_index`:

```text
{manifest_path}:{database}.{table}:{low}-{high}:{adapter_version}:{chunking_fingerprint}:{chunk_index}
```

The Iceberg sink does the analogous thing for its Parquet file
identity. Sinks own those suffixes; the runtime never constructs
them. The chunking fingerprint is a sink concern — every sink's
adapter configuration produces its own — and it never appears on
the runtime surface.

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
   (default: every committed source range, mirroring RFC 0001's
   per-commit-group flush cadence).

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
  Source replays the range. Same outcome as RFC 0001.
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
- `source.estimated_max_batch_bytes` — pessimistic reservation per
  batch when `BatchDescriptor.object_bytes` is `None`. Defaults to
  `source.max_inflight_bytes / source.max_inflight_batches` rounded
  up; operators override when the workload is known to use larger
  batches.
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

### Columnar Migration

The `DecodedRecords` enum is shaped to support both
`DecodedRecords::Typed` and a future `DecodedRecords::Arrow`. The
current state and the planned migration:

1. **Typed only (current)**: the OTLP logs decoder produces
   `Vec<DecodedLogRecord>`. A `TypedRecords` adapter wraps it and the
   ClickHouse sink downcasts back. Behavior is equivalent to RFC 0001.
2. **Arrow prototype (future work)**: an Arrow OTLP logs decoder built
   behind a feature flag, with benchmarks comparing per-record
   allocation, end-to-end stage latency, ClickHouse serialization cost,
   and projected Iceberg/Parquet write cost.
3. **ClickHouse binary format (future work)**: ClickHouse moves to a
   binary format (`RowBinaryWithNamesAndTypes` or Native), reading from
   `Arrow`.
4. **Iceberg sink (future work)**: Iceberg reads from `Arrow` directly.
5. **Typed retirement (future work)**: the Typed path is retired for
   OTLP logs. The runtime may keep `DecodedRecords::Typed` available for
   unusual signals that do not have a clean Arrow representation, but the
   default becomes Arrow.

Arrow-vs-typed is a measured decision, not a stylistic one. The Arrow
benchmark gates the migration; if Arrow loses on the targeted workloads,
the migration stalls and the runtime contract is revisited.

### Source Reader: Buffer Implementation

See "Source Side: Concrete `BufferSource` for v1" under Trait Surface.
That section is the canonical description of `BufferSource` and
`BufferSourceFetchHandle`, including the RFC 0003 read-ahead path,
the serial-`next_batch` compatibility fallback, and the
`split_into_raw_entries` materialization. There is no separate trait
implementation to describe — the source side is concrete for v1.

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
- **Iceberg**: writes Parquet data files whose keys are derived
  deterministically from the runtime's `CommitIdentity` plus the
  sink's adapter configuration; commits via the configured catalog;
  and implements `check_committed(&identity)` by inspecting snapshot
  metadata for a file whose key matches the one the adapter would
  recompute. Detailed semantics in the Iceberg RFC.

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
    ack:
      # `every_source_range` mirrors the runtime contract — one
      # SinkCommit per source range. The legacy `every_commit_group`
      # token is still accepted on the YAML side for backward
      # compatibility with older configs.
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
  high-volume and low-volume signals.
- **`schema_ref` selects a built-in template or a user-supplied
  schema/mapping file** (see Level 2, future work). The string
  `builtin/<name>` resolves to a compiled-in template; any other value
  is a path to a user schema/mapping document.
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

#### Level 2: Declarative Schema/Mapping (Future)

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

This RFC does not specify the schema document format; that is future
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
  Buffer ack. Multi-source ack isolation is exercised by the
  correctness harness.
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
- **Graceful shutdown**: stop admitting new descriptors per source,
  drain all in-flight `SinkCommit`s through the configured sink,
  advance and flush each source's ack frontier, exit. The runtime
  owns shutdown propagation through `tokio_util::CancellationToken`.
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
the runtime would not be able to apply schema-aware backpressure or
let each sink chunk its writes against a known row count. The cost is borne even more sharply
when the same decoded shape is reused across sink types in
duplicated-queue deployments — every service decodes from scratch.

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
derive the source ack frontier, which is the multi-checkpoint
complexity RFC 0001 explicitly rejected for the Kafka connector
design. This alternative is moot under the single-sink scope:
ack frontiers are per-source against the single configured sink. The
section is kept for context because it is the argument against
re-introducing same-process fanout.

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

### Re-introduce a `SourceReader` Trait Now

Considered: define `SourceReader` / `SourceFetchHandle` as production
traits and route the runtime through `Box<dyn SourceReader>` to keep
the door open for non-Buffer sources (Kafka direct, OTLP HTTP push,
file scan). Rejected for v1 because:

- There is one source impl (`BufferSource`) and no second source on
  the roadmap.
- The trait methods would be near 1:1 over `buffer::Consumer` /
  `ConsumerFetchHandle`, costing API-surface maintenance for an
  option we may never exercise.
- Test fakes use a `#[cfg(test)]` seam inside the runtime crate;
  production callers stay concrete.

Reintroduction, if and when a second source materializes, is a local
change inside the runtime crate. The data types
(`SourceBatchDescriptor`, `SourceBatch`, `SourceEntry`) are
source-shape-agnostic and stay; sinks and the correctness harness
don't depend on the source shape. The cost of waiting is bounded.

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
- **Arrow-only DecodedRecords** once the Iceberg sink lands, removing
  the typed fallback for OTLP logs.
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
  inherits the deferral and exposes the seam (`BufferSource` already
  takes an initial sequence on construction).
- **Reintroduce a `SourceReader` trait** if/when a non-Buffer source
  lands (Kafka direct, OTLP HTTP push, file scan). See "Re-introduce
  a `SourceReader` Trait Now" in Alternatives Considered. The runtime
  data types (`SourceBatchDescriptor`, `SourceBatch`, `SourceEntry`)
  are source-shape-agnostic, so this is a local change inside the
  runtime crate.

## Validation Criteria

### Runtime Extraction Without Behavior Change

- The OTLP logs path runs end-to-end through the runtime crate's
  traits. The `clickhouse-ingestor` binary is registry/config wiring
  on top of `opendata-ingest-runtime` and `opendata-ingest-clickhouse`.
- Existing ClickHouse alpha tests pass unchanged.
- `cargo test -p opendata-ingest-runtime -p opendata-ingest-clickhouse`
  is green.
- Behavior is equivalent: same metrics names, same dry-run semantics,
  same rows landed in ClickHouse for the same input.

### Ack Coordinator and Single-Sink Correctness

- A fake source and a fake (single) sink (success, retryable failure,
  permanent failure, slow, ambiguous) reproduce every crash-point in
  the design doc.
- Tests demonstrate the single-sink ack invariant under
  out-of-order range completion, retry, `MaybeCommitted` resolution,
  and replay.
- Multi-source ack isolation: one source's committed range never
  advances another source's frontier.
- `check_committed` contract tests for at least one concrete sink
  (ClickHouse stub OK; an Iceberg sink is future work).

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

### Schema/Mapping and Arrow Prototype (Future Work)

- A user-provided schema/mapping document targets a non-default
  ClickHouse table without binary rebuild.
- Arrow vs. typed benchmark numbers are recorded with workload shape,
  hardware, and config.

### Standalone Iceberg Sink Service (Future Work)

- One Buffer source feeds an Iceberg-configured runtime service
  end-to-end. ClickHouse is not required to be present in the same
  process.
- Crash-after-Iceberg-commit-before-ack is verified idempotent on
  replay (`check_committed` returns `Committed`, no duplicate Parquet
  file is written).
- The ack coordinator advances only after the Iceberg sink commits
  (or verifies).

### Multi-Source Single-Sink E2E (Future Work)

- Multiple Buffer sources feed one runtime service into a single
  configured sink (e.g. ClickHouse). Per-source ack frontiers and
  bounded backpressure both hold under load.
- Optional deployment proof: the same upstream stream is duplicated
  into two queues and consumed by two isolated runtime services
  (e.g. one ClickHouse, one Iceberg) — each service is a single-sink
  service with its own ack frontier.
