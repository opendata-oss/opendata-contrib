# RFC 0002: Generic Ingest Runtime

**Status**: Draft

**Authors**:

- Apurva Mehta

## Summary

This RFC defines `opendata-ingest-runtime`, a sink-neutral runtime that consumes
OpenData Buffer streams (and, later, other sources) and routes decoded records
to one or more target sinks. It generalizes the layering from the shipped
ClickHouse ingestor (RFC 0001) into traits and a per-source `AckCoordinator`
that hold even when multiple sinks fan out from the same source and complete
out of order.

The runtime is built around five trait boundaries: `SourceReader`,
`Decoder`, `Router`, `Sink`, and `IdempotencyContract`. The pipeline is
staged with bounded queues and a shared in-flight byte budget, so a slow
or failing sink pauses upstream work without unbounded memory growth.
Buffer ack advances only when every routed sink has durably committed
the corresponding source sequence range, or has verified that the chunk
was already committed by an earlier attempt.

The decoded unit starts as typed Rust records (compatible with the
current `DecodedLogRecord` path) and migrates to Arrow `RecordBatch`
before the first lakehouse sink lands and before ClickHouse moves off
JSONEachRow. Pluggability starts at native plugin crates plus
declarative schema/mapping config; WASM and subprocess plugins are
deferred until the native path is measured.

The first integration is RFC 0001's ClickHouse ingestor, ported through
the generic runtime without intentional behavior changes. The next sink
is an append-only Iceberg writer (separate RFC). The same runtime hosts
both in one process.

## Motivation

RFC 0001 ships an ingestor that polls Buffer, validates per-entry
metadata envelopes, decodes OTLP logs, coalesces records into commit
groups, plans deterministic ClickHouse insert chunks, executes them, and
acks Buffer only after all chunks succeed. The layering is sound, but
six things are tied to the ClickHouse sink today:

1. The runtime polling loop, the commit group, and the ack controller
   live in the `clickhouse-ingestor` crate. A second sink would either
   duplicate them or import a ClickHouse-specific crate just to reuse
   them.
2. The adapter trait outputs `Vec<InsertChunk>` with a
   ClickHouse-shaped `Row = Vec<RowValue>`. That row type is
   row-oriented, JSON-leaning, and not a useful interchange format for
   columnar sinks like Iceberg/Delta or for a binary ClickHouse path.
3. The runtime is single-sink. There is no fanout, no per-sink
   completion tracking, and no per-source ack frontier independent of
   sink completion order.
4. The Buffer consumer API is serial: `Consumer::next_batch` combines
   manifest read, object fetch, and decode in one call. That ceiling
   limits source throughput regardless of decode/sink concurrency.
5. Backpressure is implicit in the synchronous pipeline. Slow sinks slow
   the loop, but there is no shared byte budget and no explicit
   "backpressure reason" surfaced in metrics.
6. The schema and target table are compiled into the binary. There is
   no path for an operator to write the same OTLP logs to a custom
   ClickHouse table or a new Iceberg table without forking the binary.

The high-throughput design (`plans/odb-high-throughput`) calls for one
service that can host multiple Buffer sources, route to multiple sinks,
preserve at-least-once with per-sink idempotency, and approach
single-node network or sink-ingest limits. None of that fits inside the
ClickHouse ingestor as written.

The cheapest path forward is to pull the runtime, ack control, commit
grouping, and pipeline scaffolding into a separate crate, define the
trait surface that source readers and sinks plug into, and re-host the
ClickHouse logs path on top of it without intentional behavior changes.
That refactor isolates the correctness work (per-source ack frontier,
fanout invariants, idempotency contract) from the throughput work
(parallel fetch, parallel decode, columnar representation, binary
serialization).

## Goals

- Define a sink-neutral runtime crate, `opendata-ingest-runtime`, that
  owns polling, decode orchestration, routing, commit grouping, retry,
  ack, and backpressure.
- Define the trait surface for source reading, decoding, routing, sinks,
  and idempotency, with no ClickHouse-specific types.
- Define the per-source `AckCoordinator` state machine and the fanout
  invariant: source ack advances only after every routed sink has
  durably committed or verified prior commit of the relevant source
  sequence range.
- Define the bounded-stage pipeline with shared byte budget, so target
  slowdown pauses source pulls without unbounded memory growth.
- Define the columnar migration path: typed records first, Arrow
  `RecordBatch` before the first lakehouse sink and before ClickHouse
  binary serialization.
- Define pluggability: native plugin crates as the v1 path, declarative
  schema/mapping for target tables as the v2 path, dynamic plugins as a
  measured follow-up.
- Define the configuration shape that supports multiple sources and
  multiple sinks in one process with independent ack frontiers.
- Set validation criteria for the runtime extraction and for the
  pipelined runtime.

## Non-Goals

- Concrete sink implementations. The ClickHouse and Iceberg sinks each
  have their own RFCs and crates. This RFC defines what they plug into.
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
  support multiple sinks.
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
    pub async fn fetch_descriptor(&self, descriptor: BatchDescriptor)
        -> Result<ConsumedBatch>;
    pub async fn ack_through(&mut self, sequence: u64) -> Result<()>;
    pub async fn flush(&self) -> Result<()>;
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

The runtime owns the horizontal stages between Buffer (or another
source) and the routed sinks:

```text
                          ╔═══ ingest-runtime process ═════════════════════════════════════════════════════════════════════════════════════╗
                          ║                                                                                                                ║
                          ║   ┌──────────────┐   ┌──────────────┐   ┌─────────────────┐   ┌──────────────┐   ┌──────────────┐               ║
                          ║   │ Descriptor   │   │  Fetch+      │   │ Envelope+Signal │   │ Router       │   │ CommitGroup  │               ║
                       ┌──╫───▶  poller      ├───▶  decompress  ├───▶ decoder         ├───▶ per record   ├───▶ per route    │               ║
                       │  ║   │ (per source) │   │   workers    │   │   workers       │   │              │   │              │               ║
╔══Object Storage══╗   │  ║   └──────┬───────┘   └──────────────┘   └─────────────────┘   └──────────────┘   └──────┬───────┘               ║
║                  ║   │  ║          │                                                                              │                       ║
║   Manifest +     ║   │  ║          │                                                                              │                       ║
║   Batches        ╞══►┘  ║          │                                                                              ▼                       ║
║                  ║      ║          │                                                                       ┌──────────────┐               ║
╚═════════▲════════╝      ║          │                                                                       │ Sink writer  │               ║   ╔══Sinks═════════╗
          │               ║          │                                                                       │   workers    │               ║   ║                ║
          │               ║          │                                                                       │  per sink    ├───────────────╫───▶ ClickHouse,    ║
          │               ║          │                                                                       │              │               ║   ║ Iceberg, fake, ║
          │               ║          │                                                                       └──────┬───────┘               ║   ║ ...            ║
          │               ║          │                                                                              │                       ║   ╚════════════════╝
          │               ║          │                                                                              ▼                       ║
          │               ║          │                                                                       ┌──────────────┐               ║
          │               ║          └─────────────────────── ack frontier per source ──────────────────────▶│ Ack          │               ║
          └───────────────╫───────────  (only after every routed sink commits the sequence range)──────────  │ coordinator  │               ║
                          ║                                                                                  └──────────────┘               ║
                          ║                                                                                                                 ║
                          ╚═════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
```

What is generic vs. plugin:

| Layer | Owner |
|---|---|
| Source descriptor poller, fetch workers, decompression | Runtime (per source plugin shape) |
| Per-entry envelope materialization (RFC 0001 `RawEntry`) | Runtime |
| Signal decoder | Plugin (`Decoder`) |
| Routing | Plugin (`Router`) |
| Commit group, deterministic chunking | Runtime |
| Sink write, retry classification, idempotency check | Plugin (`Sink` + `IdempotencyContract`) |
| Ack coordinator, source ack/flush, backpressure | Runtime |
| Metrics scaffolding | Runtime; plugins may add labeled metrics |

The runtime never inspects payload bytes after decode. The plugins
never call `Consumer::ack` or touch backpressure budgets directly.

### Trait Surface

All trait names below are placeholders the implementation may rename;
the invariants are the contract.

#### `SourceReader`

```rust
#[async_trait::async_trait]
pub trait SourceReader: Send + 'static {
    /// Stable identifier for this source; used in metric labels and
    /// idempotency keys.
    fn id(&self) -> &SourceId;

    /// Fetch up to `max` new descriptors past the current cursor. Must
    /// not mutate the durable ack frontier. Returning fewer than `max`
    /// is allowed and signals "no more visible right now"; the runtime
    /// will sleep and retry.
    async fn next_descriptors(&mut self, max: usize, budget: SourceBudget)
        -> RuntimeResult<Vec<SourceBatchDescriptor>>;

    /// Fetch a single descriptor. May be called concurrently from
    /// multiple workers. Implementations should not mutate manifest
    /// state here.
    async fn fetch(&self, descriptor: SourceBatchDescriptor)
        -> RuntimeResult<SourceBatch>;

    /// Advance the durable ack frontier through (and including)
    /// `sequence`. Implementations are responsible for the in-order
    /// requirement of the underlying source; the runtime guarantees
    /// monotonic advance.
    async fn ack_through(&mut self, sequence: u64) -> RuntimeResult<()>;

    /// Force the underlying source's durable checkpoint. The runtime
    /// calls this on flush boundaries.
    async fn flush_acks(&mut self) -> RuntimeResult<()>;
}

pub struct SourceBatchDescriptor {
    pub source: SourceId,
    pub sequence: u64,
    pub location: String,
    pub per_range_metadata: Vec<SourceRangeMetadata>,
    pub size_hint: Option<u64>,
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
`buffer::Consumer::next_descriptors`/`fetch_descriptor`/`ack_through`
and reuses the existing `split_into_raw_entries` materialization.

The trait is async to keep the door open for non-Buffer sources later
(Kafka, file scan, direct OTLP push). Adding sources should not require
runtime changes; they only need to honor in-order acks within a source.

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

The runtime invokes `accepts(envelope)` per-entry and shards a single
`SourceBatch` across decoders if multiple are configured. v1 expects a
single decoder per source and falls back to RFC 0001's "fail closed on
mismatch" semantics; per-entry routing is a future option (RFC 0001
"Future: Per-entry signal routing").

`decode` returns `Vec<DecodedBatch>` so a single `SourceBatch` can fan
out across signals (e.g. mixed batches in a future router-aware mode).
For v1 each `Decoder` returns at most one `DecodedBatch` per call.

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
    Typed(Box<dyn TypedRecords>),
    /// Arrow columnar batch. See "Columnar Migration" below.
    Arrow(arrow_array::RecordBatch),
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

`source_entry_count` lets the commit group and the ack coordinator
advance the input high-watermark even when `records` is empty, mirroring
RFC 0001's "Input progress is independent of output rows" property.

#### `Router`

```rust
pub trait Router: Send + Sync + 'static {
    fn routes(&self) -> &[RouteId];

    fn route(&self, batch: &DecodedBatch)
        -> RuntimeResult<Vec<RouteAssignment>>;
}

pub struct RouteAssignment {
    pub route: RouteId,
    /// Indices into the batch's records that go to this route. `None`
    /// means "all records." For v1 the default router emits one
    /// assignment per route covering all records (no record-level
    /// filtering).
    pub indices: Option<Vec<u32>>,
}
```

The default router maps `(source, signal_type) -> [route0, route1,
...]` from configuration and assigns every record in a `DecodedBatch`
to every route. v2 routers can shard by attribute, tenant, or any
record-level predicate.

The `Router` is invoked once per `DecodedBatch`. It does not see
source bytes; if a use case needs byte-level routing, it belongs in
the decoder layer, not the router.

#### `Sink`

```rust
#[async_trait::async_trait]
pub trait Sink: Send + Sync + 'static {
    fn id(&self) -> &SinkId;

    /// Maximum bytes-in-flight the runtime should hold for this sink
    /// before pausing upstream pulls (used for fairness across sinks).
    fn write_budget(&self) -> SinkBudget;

    async fn write(&self, commit: SinkCommit)
        -> RuntimeResult<SinkCommitResult>;

    async fn check_committed(&self, key: &IdempotencyKey)
        -> RuntimeResult<CommitStatus>;
}

pub struct SinkCommit {
    pub source: SourceId,
    pub route: RouteId,
    pub low_sequence: u64,
    pub high_sequence: u64,
    pub schema_version: SchemaVersion,
    pub idempotency_key: IdempotencyKey,
    pub records: DecodedRecords,
    pub source_columns: SourceCoordinateColumns,
}

pub struct SinkCommitResult {
    pub bytes_written: u64,
    pub rows_written: u64,
}

pub enum CommitStatus {
    Committed,
    NotCommitted,
    /// Sink cannot tell from idempotency key alone (e.g. ClickHouse
    /// insert deduplication window has passed). Treated as
    /// `NotCommitted` for retry, but logged so an operator can audit.
    Unknown,
}
```

`check_committed(key)` is the primitive that lets a sink say "I already
have this; do not write it again" on replay. The ClickHouse sink
implements it as a no-op that always returns `Unknown` because alpha
ClickHouse dedupes at the table layer with `ReplacingMergeTree`. The
Iceberg sink implements it by inspecting snapshot metadata for a file
keyed on the `IdempotencyKey`. Sinks that genuinely cannot detect prior
commit return `Unknown` and accept that replay may admit duplicates,
which the table-level mechanism (or the operator) must clean up.

#### `IdempotencyContract`

The idempotency key is a pure function of source coordinates, route,
schema version, and chunking parameters. The runtime constructs it; the
sink consumes it.

```rust
pub trait IdempotencyContract: Send + Sync {
    fn key(&self, scope: IdempotencyScope) -> IdempotencyKey;
}

pub struct IdempotencyScope<'a> {
    pub source: &'a SourceId,
    pub route: &'a RouteId,
    pub low_sequence: u64,
    pub high_sequence: u64,
    pub schema_version: SchemaVersion,
    pub chunking_fingerprint: u64,
    pub chunk_index: u32,
}

pub struct IdempotencyKey(pub String);
```

The default implementation produces:

```text
{source}:{route}:{low}-{high}:{schema_version}:{chunking_fingerprint}:{chunk_index}
```

This is the same shape as RFC 0001's per-chunk token, generalized so
non-ClickHouse sinks can reuse it.

### Per-Source Ack Coordinator

The ack coordinator is the single most important piece of correctness
that changes from RFC 0001 to this RFC.

#### State Machine

For each source, the coordinator tracks:

- A monotonic `acked_frontier` (the durable Buffer ack high-watermark).
- A set of `pending` sequence ranges, each annotated with the routes it
  was assigned to.
- A per-range, per-route `committed` flag.

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
    pub required_routes: HashSet<RouteId>,
    pub committed_routes: HashSet<RouteId>,
}
```

The state transitions are:

1. **Range becomes pending** when the runtime hands a `DecodedBatch`
   to the router and gets back `RouteAssignment[]`. The required_routes
   set is the union of route ids in those assignments. Empty
   assignments (a `DecodedBatch` that no route claimed) still register
   a pending range so input progress can advance.
2. **Range becomes complete-on-route** when a sink reports a successful
   commit (via `Sink::write` returning `Ok` *or* `check_committed`
   returning `Committed`).
3. **Range becomes complete** when `committed_routes ==
   required_routes`.
4. **Frontier advances** to the highest contiguous complete sequence
   from the current `acked_frontier`. The coordinator never advances
   over a hole.
5. **Flush** calls `SourceReader::ack_through(frontier)` and then
   `SourceReader::flush_acks` per the configured `AckFlushPolicy`
   (default: every commit group, mirroring RFC 0001).

The required_routes set is computed once per pending range and frozen.
Subsequent record-level filtering by routers does not retroactively
add or remove routes for the range. If a range is `required_routes =
{}`, it is complete the moment it is registered (zero-record batch with
no routes claiming it).

#### Crash Semantics

- **Crash before any sink commit**: nothing in the Buffer ack moves.
  Source replays the range. Same outcome as RFC 0001.
- **Crash after one sink commit, before another**: ack frontier did not
  advance (range incomplete). Source replays. The committed sink's
  `check_committed(key)` should return `Committed` and the runtime
  marks the route complete without rewriting; the uncommitted sink
  replays normally. Sinks whose `check_committed` returns `Unknown`
  re-attempt and rely on their own dedupe (e.g. ClickHouse table-level
  dedupe).
- **Crash after all sink commits, before ack flush**: same as above,
  except every sink reports `Committed` on replay. Frontier advances
  and flushes on the next loop iteration.
- **Crash after ack flush**: source will not replay this range. Every
  sink must already have committed it. The runtime never advances the
  frontier without all required commits, so this is the invariant.

#### Fanout Invariant

> **Buffer ack advances only after every routed sink has either
> durably committed the relevant source sequence range or reported
> `Committed` for the range's idempotency key.**

This is the single sentence the gate reviewer should re-validate at
every phase that touches ack flow. Tests in Phase 5 of the impl plan
must demonstrate this invariant under deterministic out-of-order
completion, partial-fanout failure, and replay.

### Backpressure Model

Stages communicate through bounded queues plus a shared in-flight byte
budget per source:

| Stage | Backpressure trigger |
|---|---|
| Descriptor poll | Source descriptor queue full or in-flight bytes ≥ `source.max_inflight_bytes` |
| Object fetch | Fetch worker semaphore full |
| Decompress | Decompress worker semaphore full |
| Decode | Decode worker semaphore full |
| Route + commit-group | Per-route commit group at row/byte/age threshold |
| Sink write | Sink-specific budget (`Sink::write_budget`); slow sink fills its writer queue and stalls upstream |
| Ack | Frontier blocked on incomplete pending range |

Required configuration knobs (these names are stable across phases; a
later config schema RFC may rename, but the semantics carry through):

- `source.max_inflight_batches`
- `source.max_inflight_bytes`
- `source.fetch_concurrency`
- `source.decompress_concurrency`
- `decode.concurrency`
- `commit_group.max_rows`
- `commit_group.max_bytes`
- `commit_group.max_age_ms`
- `sink.<name>.max_concurrent_commits`
- `sink.<name>.retry.max_attempts`
- `sink.<name>.retry.initial_backoff_ms`

Required metrics (stage-labeled):

- `runtime_stage_queue_depth{stage,source}`
- `runtime_stage_inflight_bytes{stage,source}`
- `runtime_stage_latency_seconds{stage,source}`
- `runtime_ack_frontier{source}` (gauge)
- `runtime_pending_ranges{source}` (gauge)
- `runtime_backpressure_reason{source,reason}` (counter; `reason` ∈
  `source_budget`, `decode_budget`, `sink_budget`, `retrying`,
  `fatal_error`)
- `runtime_route_commits_total{source,route,result}` (`result` ∈
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
`BufferSourceReader`, that wraps `buffer::Consumer`:

- `next_descriptors(max, budget)` calls
  `Consumer::next_descriptors(max)` and rejects descriptors past
  `budget.bytes_remaining` to keep the source under
  `source.max_inflight_bytes`.
- `fetch(descriptor)` calls `Consumer::fetch_descriptor(descriptor)`.
- `ack_through(seq)` calls `Consumer::ack_through(seq)`.
- `flush_acks` calls `Consumer::flush()`.
- `split_into_raw_entries` (today in `clickhouse-ingestor::source`) is
  pulled into the runtime crate so it can be reused by future sources
  that surface per-range metadata in the same shape.

If RFC 0003 of opendata-buffer is not yet released, the runtime can
fall back to a serial path that calls `Consumer::next_batch` and
emits a single descriptor whose location is the just-fetched batch.
This compatibility path is required only during Phase 4
(extraction) and removed in Phase 6 (pipelining).

### Decoder and Router: v1 Defaults

v1 ships:

- `OtlpLogsDecoder`: pulled from `clickhouse-ingestor::signal`,
  unchanged behavior. `accepts(envelope)` returns true for `(version=1,
  signal_type=Logs, encoding=OtlpProtobuf)`.
- A future `OtlpMetricsDecoder` once metrics targets ship; not v1.
- `StaticRouter`: routes every `DecodedBatch` to a fixed list of
  `RouteId`s, configured per source.

Per-entry envelope routing across decoders is supported by the trait
shape but not implemented in v1. v1 fails closed on mixed envelopes
within a single source, mirroring RFC 0001.

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

The runtime configuration treats sources and sinks as named, indexed
resources; routes are the wires between them.

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
    routes: [logs_clickhouse, logs_iceberg]
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

sinks:
  - id: logs_clickhouse
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

  - id: logs_iceberg
    type: iceberg
    catalog:
      type: rest
      endpoint: https://catalog.example.com
    namespace: observability
    table: logs
    schema_ref: builtin/otel_logs_iceberg_v1
    object_store:
      type: Aws
      bucket: opendata-iceberg-logs
      region: us-west-2
    max_concurrent_commits: 2
```

Key shape decisions:

- **Sources and sinks are top-level lists keyed by `id`.** Routes are
  references by id, not nested objects. This keeps multi-fanout
  configurations readable when one source feeds many sinks.
- **`schema_ref` selects a built-in template or a user-supplied
  schema/mapping file** (Phase 7). The string `builtin/<name>` resolves
  to a compiled-in template; any other value is a path to a user
  schema/mapping document.
- **Each source has its own commit-group, ack, and backpressure
  config.** Different signals have different volume profiles; sharing a
  pool would couple them.
- **Sinks declare retry and concurrency at the sink level.** The
  runtime applies them; sink plugins do not implement their own retry
  loops.

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

### Multi-Source, Multi-Target Service

The runtime hosts N sources × M sinks in one process. The invariants
across sources:

- **Each source has its own `AckCoordinator`** and its own ack
  frontier. A failure in one source must not advance another source's
  Buffer ack.
- **Sinks may be shared across sources** (one Iceberg writer connection
  pool can serve many sources). The sink applies per-source budgets so
  one hot source does not starve a low-volume source.
- **One source feeding two sinks (fanout)** is the v1 target.
- **Two sources feeding the same sink table** is allowed but adds a
  dedupe-key constraint inherited from RFC 0001's "Future:
  Config-Driven Multi-Source Ingestors": if two sources share a target
  table, the table's dedupe key must include `_odb_manifest_path` (or
  the equivalent for non-ClickHouse sinks). The config validator
  enforces this.
- **Process readiness fails** when any required source halts. Optional
  sources can be marked `optional: true` in config and their failure
  reports as degraded, not failed. v1 ships without `optional`; it is
  named here so the validator can adopt it later without churn.

### Operational Surface

- **Dry-run** (per source): full pipeline, including decode, route, and
  sink planning, but `Sink::write` is replaced with a no-op that
  returns success without side effects, and `ack_through` is skipped.
  Carry-over from RFC 0001. Toggling dry-run requires a process
  restart for the same reason as RFC 0001.
- **Graceful shutdown**: drain all in-flight commit groups, write
  through every sink, advance and flush ack frontiers, exit. The
  runtime owns shutdown propagation through `tokio_util::CancellationToken`.
- **Hard crash recovery**: source resumes from the last *flushed* ack
  frontier; replay is idempotent at sinks that implement
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
- **Partial fanout failure (one sink fatal, others healthy)**: the
  pending range stays incomplete; the ack frontier does not advance;
  the runtime halts because there is no clean way to ack only some
  sinks' commits. v1 chooses safety over availability here.
  v2 may add per-sink "skip-and-record" modes, but they require an
  operator-facing data-loss policy.
- **AckCoordinator inconsistent state** (a programming bug, e.g.
  duplicate route assignment): treated as fatal. The runtime never
  papers over a coordinator invariant violation.

## Alternatives Considered

### Keep the Runtime Inside `clickhouse-ingestor`

Adding a second sink in-tree is mechanically possible. Rejected because
it forces every future sink to depend on a ClickHouse-shaped crate, and
it makes the ClickHouse-specific row type (`Vec<RowValue>`) the de facto
cross-sink interchange shape. That is a worse permanent abstraction
than splitting the crate now.

### Use Raw OTLP Protobuf as the Cross-Sink Unit

The runtime could carry source bytes through to every sink and let each
sink decode independently. Rejected for fanout: every sink would
re-decode the same bytes, OTLP tree flattening is non-trivial, and the
runtime would not be able to size commit groups by row count or apply
schema-aware backpressure.

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

### Per-Sink Ack Frontiers (One AckCoordinator per Sink)

Considered: each sink advances its own Buffer ack. Rejected because
Buffer has one active consumer per manifest (epoch fenced). The "one
ack frontier per source" invariant matches the underlying contract
exactly. Sink-level frontiers would require a separate per-sink
checkpoint store and a reconciliation algorithm to derive the source
ack frontier, which is the multi-checkpoint complexity RFC 0001
explicitly rejected for the Kafka connector design.

### WASM-First Plugin Boundary

Defer. The hot path is decode → route → commit-group → sink write. A
WASM boundary on that path is premature optimization for an
extensibility story that natively-linked plugin crates already cover.
Once the native path is benchmarked, WASM is a candidate for transforms
that are off the hottest path (e.g. attribute enrichment).

### `cdylib` Rust Plugin Boundary

Rejected. Rust has no stable ABI; a dylib boundary in the record path
forces serialization at the boundary, which negates the point of native
plugins. WASM or subprocess plugins are better long-term answers.

### Push the Ack Coordinator Into Each Sink

Sinks could call `source.ack_through` themselves. Rejected because:

- It would couple every sink to the source's manifest API (today
  Buffer; tomorrow possibly Kafka or file scan).
- Fanout becomes: every sink races to ack independently. Either we
  ack-on-first (lose data on the slower sink) or we add coordination
  back. The runtime is the natural coordination point.

## Future Improvements

These do not require changing the trait shapes in this RFC.

- **Per-entry signal routing within a single source**: relax v1's
  homogeneous-envelope requirement. Already supported by the
  `Decoder::accepts` shape.
- **Optional sources / per-source readiness** in the config validator
  and the metrics surface.
- **Per-sink "skip-and-record" mode** for partial-fanout fatal failure,
  with an operator-facing data-loss policy.
- **Arrow-only DecodedRecords** after Phase 9, removing the typed
  fallback for OTLP logs.
- **WASM plugins** for off-hot-path transforms (attribute enrichment,
  schema migrations, filter rules).
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

### Phase 5 Exit (Ack Coordinator and Correctness Harness)

- A fake source and fake sinks (success, retryable failure, permanent
  failure, slow, slower) reproduce every crash-point and partial-fanout
  scenario in the design doc.
- Property tests demonstrate the fanout invariant under randomized
  completion orders.
- `check_committed` contract tests for at least one concrete sink
  (ClickHouse stub OK; Iceberg follows in Phase 9).

### Phase 6 Exit (Pipelined Runtime)

- Fetch, decode, and write workers run concurrently; all stage queue
  and inflight metrics are exposed.
- A target slowdown injection pauses source pulls within one
  commit-group window; recovery resumes without unbounded memory growth.
- Phase 5 correctness tests still pass under concurrency knobs greater
  than 1.

### Phase 7 Exit (Schema/Mapping + Arrow Prototype)

- A user-provided schema/mapping document targets a non-default
  ClickHouse table without binary rebuild.
- Arrow vs. typed benchmark numbers are recorded with workload shape,
  hardware, and config.

### Phase 9 Exit (Iceberg Sink, Two-Sink Fanout)

- One Buffer source feeds ClickHouse and Iceberg in the same process.
- Crash-after-Iceberg-commit-before-ack is verified idempotent on
  replay (`check_committed` returns `Committed`, no duplicate Parquet
  file is written).
- The ack coordinator advances only after both sinks commit (or
  verify).

## Revision History

| Date | Description |
|---|---|
| 2026-05-07 | Initial draft. Generalizes RFC 0001 into a sink-neutral runtime; defines source/decoder/router/sink traits, AckCoordinator state machine, fanout invariant, columnar migration path, pluggability levels, and validation criteria phase by phase. |
