# clickhouse-ingestor

A standalone service that consumes OTLP logs from an OpenData Buffer and writes them to a ClickHouse table.

The runtime is generic at the Buffer-reader and metadata-envelope layers; signal-specific decoding and ClickHouse table mapping live in pluggable adapters. The current alpha **ships an OTLP logs adapter** as the published binary; the trait surfaces (`SignalDecoder`, `Adapter`) are public so you can compose your own decoder + adapter for any other signal type — see ["Add a new signal type"](#add-a-new-signal-type) below for a worked OTLP traces example.

The design (ack semantics, dedupe, idempotency tokens, schema ownership) is described in [`rfcs/0001-clickhouse-ingestor.md`](../../rfcs/0001-clickhouse-ingestor.md). The first part of this README is for operators who want to run the published binary against an OTLP logs source; the last section is for developers who want to extend it.

## What you need

1. **An OpenData Buffer that's already receiving OTLP logs.** Typically this is an OTel collector with the [OpenData OTel exporter](https://github.com/opendata-oss/opendata-go/tree/main/exporter/opendataexporter) configured for the `logs` signal, writing to an S3 bucket. The ingestor needs the bucket name, region, the manifest object path (e.g. `ingest/otel/logs/manifest`), and the data prefix (e.g. `ingest/otel/logs/data`).
2. **A reachable ClickHouse instance.** ClickHouse Cloud and self-hosted both work. The ingestor uses sync inserts over HTTPS (no `async_insert`); if your cluster is replicated, you'll want to set `insert_quorum` (see config below).
3. **Object-store credentials.** For AWS S3, the host needs read access to the data prefix and read+write+delete on the manifest object (the consumer rewrites the manifest on dequeue/flush). On EKS this is typically an IRSA role; locally, AWS_* env vars or a profile.
4. **ClickHouse credentials.** A user with `INSERT` on the target table (and `SELECT` to verify). Passed in via env vars, never via the YAML file (so config can sit in a ConfigMap without leaking secrets).

## Get the binary

Pick one:

**Pre-built binary** — released to GitHub for `linux/x86_64`, `linux/aarch64`, `darwin/aarch64`, `windows/x86_64`:

```sh
# Latest release: https://github.com/opendata-oss/opendata-contrib/releases
curl -fL -o clickhouse-ingestor.tar.gz \
  https://github.com/opendata-oss/opendata-contrib/releases/download/clickhouse-ingestor%2Fv0.1.2/clickhouse-ingestor-0.1.2-x86_64-unknown-linux-gnu.tar.gz
tar xzf clickhouse-ingestor.tar.gz
./clickhouse-ingestor --help
```

**Container image** — `linux/amd64` only:

```sh
docker pull ghcr.io/opendata-oss/clickhouse-ingestor:0.1.2
```

**Build from source** — for any other platform, or to develop against:

```sh
git clone https://github.com/opendata-oss/opendata-contrib
cd opendata-contrib
cargo build --release --locked --manifest-path connectors/clickhouse-ingestor/Cargo.toml
# binary at target/release/clickhouse-ingestor
```

## Create the ClickHouse table

The alpha logs schema is `ReplacingMergeTree(_adapter_version)` partitioned by date, ordered by `(toDate(Timestamp), ServiceName, _odb_*)`. Apply this DDL once against your target database:

```sql
CREATE TABLE IF NOT EXISTS responsive.logs (
    Timestamp           DateTime64(9)               CODEC(Delta, ZSTD),
    ObservedTimestamp   DateTime64(9)               CODEC(Delta, ZSTD),
    SeverityText        LowCardinality(String),
    SeverityNumber      UInt8,
    ServiceName         LowCardinality(String),
    Body                String                      CODEC(ZSTD),
    ResourceAttributes  Map(LowCardinality(String), String),
    LogAttributes       Map(LowCardinality(String), String),
    TraceId             String                      CODEC(ZSTD),
    SpanId              String                      CODEC(ZSTD),
    _odb_sequence            UInt64,
    _odb_entry_index         UInt32,
    _odb_record_index        UInt32,
    _odb_manifest_path       LowCardinality(String),
    _odb_data_path           String,
    _odb_ingestion_time_ms   Int64,
    _adapter_version         UInt32
)
ENGINE = ReplacingMergeTree(_adapter_version)
PARTITION BY toDate(Timestamp)
ORDER BY (toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)
TTL toDate(Timestamp) + INTERVAL 30 DAY;
```

Notes:

- Replace `responsive.logs` with your own database / table name; reflect the same names in the config below.
- The library function `clickhouse_ingestor::adapter::logs::logs_table_ddl(&LogsAdapterConfig)` returns the same DDL parameterised by config — useful if you embed the ingestor as a library.
- Any change to a column that participates in `ORDER BY` requires a new table plus backfill (RFC 0001 dedupe constraint). Non-key columns can change with an `_adapter_version` bump.
- The 30-day TTL is the alpha default; adjust to your retention requirement.

## Configure

The binary takes a single `--config <path>` flag pointing at a YAML file. Credentials come from env vars (`INGESTOR__CLICKHOUSE__USER`, `INGESTOR__CLICKHOUSE__PASSWORD`, plus the standard `AWS_*` vars for S3) so the YAML file is safe to commit / mount as a ConfigMap.

A complete production-shaped config:

```yaml
buffer:
  manifest_path: ingest/otel/logs/manifest
  data_prefix:   ingest/otel/logs/data
  object_store:
    type: Aws
    region: us-west-2
    bucket: my-otel-logs-bucket

clickhouse:
  endpoint: https://your-cluster.clickhouse.cloud:8443
  database: responsive
  table: logs
  insert_quorum: auto      # set to a number or "auto" if replicated; omit otherwise

runtime:
  dry_run: false           # see "Dry-run" below
  poll_interval_ms: 250
  retry_max_attempts: 6
  retry_initial_backoff_ms: 100
  request_timeout_secs: 30

commit_group:
  max_rows: 100000         # flush whenever any threshold is hit
  max_bytes: 33554432      # 32 MiB
  max_age_ms: 1000

ack:
  policy: every_commit_group   # or every_n with `n: <int>` to amortise flush()

adapter:
  adapter_version: 1
  max_chunk_rows: 100000
  max_chunk_bytes: 33554432
  apply_deduplication_token: true   # set false for non-replicated single-node ClickHouse

metrics_server:
  bind_addr: 0.0.0.0:9090       # /metrics + /-/healthy
```

### What each section does

- **`buffer`** — what to read. The S3 bucket / prefix / manifest path the OTel collector is writing into.
- **`clickhouse`** — where to write. Endpoint + database/table that match the DDL you applied.
- **`runtime`** — polling cadence, retry budget, request timeout. Defaults work for prod; tune `poll_interval_ms` lower if your producer is bursty and you want lower latency.
- **`commit_group`** — coalesces decoded records across multiple Buffer batches into ClickHouse-sized inserts. Flushes when any threshold trips. Keep `max_rows`/`max_bytes` in the 1k–100k row / 1–32 MiB range that ClickHouse likes; lower `max_age_ms` reduces ingest latency at the cost of more, smaller inserts.
- **`ack`** — when to advance the durable Buffer position. `every_commit_group` is safest (each successful insert is durably acked). `every_n` amortises the manifest-write cost of `flush()` at the cost of a bounded replay window after a crash.
- **`adapter`** — chunking and dedupe. `adapter_version` is the merge tiebreaker for `ReplacingMergeTree`; bump it when you change non-key column mappings so a replay resolves to the new mapping. Leave `apply_deduplication_token: true` for replicated tables (the dedupe token is a backstop against ack-then-crash double-insert); set it false for single-node dev/test where the setting is rejected.
- **`metrics_server`** — Prometheus scrape endpoint + `/-/healthy` liveness probe.

### Env-var overrides

Any field is overridable via `INGESTOR__<SECTION>__<FIELD>` (double-underscore separator, uppercase). Common ones:

```sh
export INGESTOR__CLICKHOUSE__USER=ingestor
export INGESTOR__CLICKHOUSE__PASSWORD=<from-secret-manager>
export INGESTOR__RUNTIME__DRY_RUN=false   # flip without editing the YAML
```

### AWS credentials

Standard AWS SDK chain — `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, EC2/ECS instance profile, or EKS IRSA role. **Note:** the underlying Rust `object_store` crate does not resolve `AWS_PROFILE` / SSO profiles — for local development against AWS, materialise static credentials via `aws configure export-credentials --profile <name> --format process` and export them, or use `aws sts assume-role`.

The IAM permissions you need on the bucket (least-privilege):

- `s3:GetObject` on the data prefix (`<bucket>/<data_prefix>/*`)
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the manifest object (`<bucket>/<manifest_path>`)
- `s3:ListBucket` on the bucket, scoped via `prefix` condition keys to the data prefix and manifest

The ingestor only writes the manifest, never the data prefix.

## Run

```sh
# direct binary
clickhouse-ingestor --config /etc/clickhouse-ingestor/config.yaml

# container
docker run --rm \
  -v $(pwd)/config.yaml:/etc/clickhouse-ingestor/config.yaml:ro \
  -e INGESTOR__CLICKHOUSE__USER=ingestor \
  -e INGESTOR__CLICKHOUSE__PASSWORD=$CH_PASSWORD \
  -e AWS_REGION=us-west-2 \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -p 9090:9090 \
  ghcr.io/opendata-oss/clickhouse-ingestor:0.1.2 \
  --config /etc/clickhouse-ingestor/config.yaml
```

The process holds an exclusive Buffer manifest epoch — running two replicas pointing at the same manifest is unsafe. K8s deployments should use `Recreate` strategy and a single replica per (manifest, database, table).

### Dry-run

`runtime.dry_run: true` runs the full pipeline (read → decode → adapter) but **skips the ClickHouse insert, the Buffer ack, and the manifest flush**. The metric `last_decoded_sequence` advances; `last_acked_sequence` stays absent. Useful for validating that your manifest, decoder, and adapter mappings are healthy before flipping to live writes.

Flipping `dry_run` from `true` → `false` requires a process restart. The in-memory fetch cursor advances during dry-run but durable ack does not, so a hot flip would skip every batch already decoded since startup. Procedure: change config → restart. On startup the consumer resumes from the earliest unacked sequence.

## Verify

Once the process is up:

1. **Logs.** You should see `configuration loaded ...` and `starting buffer consumer runtime ...` near startup. After each successful commit-group flush, a `writer reported successful insert` line with `rows`, `low`, `high` fields naming the sequence range that just landed.
2. **Metrics.** `curl localhost:9090/metrics`. Watch:
   - `ingestor_last_decoded_sequence` — advances as the consumer reads batches.
   - `ingestor_last_acked_sequence` — advances after each successful insert + flush. Absent in dry-run.
   - `ingestor_rows_inserted_total` — should track the row volume you expect.
   - `ingestor_clickhouse_failures_total` — should stay at zero. Non-zero means the writer is hitting retryable or non-retryable errors; check logs.
   - `ingestor_time_since_last_successful_ack_seconds` — primary alerting signal in non-dry-run mode. If it climbs unboundedly, the ingestor is stuck.
3. **ClickHouse.** Wait for the first commit group to land (default: 1 second after the first record), then:
   ```sql
   SELECT count() FROM responsive.logs;
   SELECT * FROM responsive.logs ORDER BY Timestamp DESC LIMIT 5;
   ```
   `_odb_*` columns should be populated on every row.

## Troubleshoot

- **`ingestor_last_decoded_sequence` doesn't advance.** Either the manifest is empty, you can't reach S3, or the manifest path is wrong. Check the process's S3 access and the `buffer.manifest_path` value. The [`buffer-inspect`](https://github.com/opendata-oss/opendata) CLI in the OpenData repo is the right read-only tool to validate the manifest exists and contains what you expect — it never writes.
- **`Mixed envelopes within a single Buffer batch are a fail-closed condition`.** The adapter received a batch whose per-entry metadata envelopes don't agree on signal type or encoding. Almost always means the manifest is being written by more than one producer with different settings, or the producer was upgraded mid-batch. The fix is on the producer side; the ingestor will retry the batch indefinitely until the underlying state is consistent.
- **`unknown signal_type` / `unknown encoding`.** The batch's metadata envelope is from a future producer version this binary doesn't know about. Halt and surface — fix is to upgrade the ingestor.
- **`permission_denied: write_package` from your CI when building the image.** GHCR-side gotcha: if you've previously published the same image name from a different repo, GitHub denies write access until you grant the new repo Write under the package's "Manage Actions access" settings.
- **Table TTL drops your test data.** The DDL has a 30-day TTL on `Timestamp`. If you're testing with synthetic timestamps in the past, ClickHouse will accept and immediately TTL-evict the rows (`written_rows: N` in the response, `count() = 0` after merges). Anchor test timestamps at `now()`.
- **`apply_deduplication_token: true` against a non-replicated table.** Single-node ClickHouse rejects the `insert_deduplication_token` setting. Set `false` for non-replicated test environments.
- **You configured 0 replicas / two replicas.** A single replica is required: the Buffer consumer holds an exclusive manifest epoch. Two replicas race; the second one fences the first and you'll see thrashing.

## Stop / restart / replay

- **Stop.** `SIGINT` / `SIGTERM`. The process drains in-flight inserts (or aborts them on retry budget exhaustion), cancels the metrics server, and exits. No data loss — unacked sequences will be re-read on next startup.
- **Pause.** Scale to 0 replicas. The Buffer manifest is left in a consistent state; the producer keeps writing into the bucket.
- **Resume.** Scale to 1 replica. The consumer resumes from the earliest unacked sequence on the manifest.
- **Replay from a specific sequence.** Not yet exposed via config; tracked at [opendata-contrib#4](https://github.com/opendata-oss/opendata-contrib/issues/4). Until then, replay requires Buffer-level tooling to manually rewind the manifest's acked range — talk to whoever owns the source-side pipeline.

Re-running the ingestor over already-inserted batches is safe: `ReplacingMergeTree(_adapter_version)` collapses duplicates on merge, the per-chunk `insert_deduplication_token` is a backstop, and queries that need exactly-once semantics should use `FINAL` (or query-time dedupe).

## Add a new signal type

The published binary is wired for OTLP logs only — but everything below the binary is generic. The runtime, commit-group coalescer, ack controller, and ClickHouse writer are all signal-agnostic; only the decoder and the adapter are signal-specific. To pipe a different kind of data (OTLP traces, OTLP metrics, your own protobuf, etc.) into ClickHouse, you implement two traits and write a binary that wires them up.

The crate's integration tests include a **complete worked example for OTLP traces**: [`tests/clickhouse_round_trip_traces.rs`](tests/clickhouse_round_trip_traces.rs). Read that file first if you want a runnable reference. The summary below mirrors what it does.

### What you write

**1. A decoded record type.** One flat struct per logical row, carrying [`SourceCoordinates`](src/signal.rs) (sequence/entry/record indices that uniquely identify the row) plus the fields you'll write into ClickHouse:

```rust
#[derive(Debug, Clone)]
struct DecodedSpanRecord {
    source: SourceCoordinates,
    trace_id_hex: String,
    span_id_hex: String,
    name: String,
    duration_nanos: u64,
    service_name: Option<String>,
    // ...
}

impl RecordSize for DecodedSpanRecord {
    fn approx_size_bytes(&self) -> usize { /* ... */ }
}
```

`RecordSize` is what the commit group uses to enforce `max_bytes` — a rough estimate is fine; deterministic across replays is what matters.

**2. A `SignalDecoder` impl.** Walks the protobuf payload (or whatever encoding you have) and flattens it into a `Vec<DecodedSpanRecord>`. The `record_index` you assign per record is the third coordinate of the `(sequence, entry_index, record_index)` triple that identifies a row — keep it monotonic within an entry:

```rust
impl SignalDecoder for OtlpTracesDecoder {
    type Output = Vec<DecodedSpanRecord>;

    fn decode(
        &self,
        batch: &RawBufferBatch,
        _envelopes: &[MetadataEnvelope],
    ) -> IngestorResult<Self::Output> {
        // ... walk ResourceSpans -> ScopeSpans -> Span, emit records
    }
}
```

**3. An `Adapter` impl.** Consumes a drained `CommitGroupBatch<DecodedSpanRecord>` and emits a deterministic `Vec<InsertChunk>`. Each chunk carries the columns + rows for one ClickHouse `INSERT`, plus an idempotency token of the form `{manifest_path}:{database}.{table}:{low}-{high}:{adapter_version}:{chunking_fingerprint}:{chunk_index}`:

```rust
impl Adapter for OtlpTracesClickHouseAdapter {
    type Input = DecodedSpanRecord;

    fn plan(&self, batch: CommitGroupBatch<Self::Input>) -> IngestorResult<Vec<InsertChunk>> {
        // 1. Sort records by (sequence, entry_index, record_index) for stable chunking.
        // 2. Split into chunks bounded by max_chunk_rows + max_chunk_bytes.
        // 3. For each chunk, emit a InsertChunk with rows mapped via your row builder.
        // 4. Token = build_token(manifest_path, config, fingerprint, low, high, chunk_index).
    }
}
```

The `chunking_fingerprint` is a stable hash of every input that affects chunk boundaries (database, table, max_chunk_rows, max_chunk_bytes, adapter_version). It's part of the token so a configuration change between partial-success and ack doesn't collide tokens with rows produced under different rules. See `chunking_fingerprint()` in the worked example for the canonical shape.

**4. A ClickHouse table DDL.** Match the columns your adapter emits. The alpha logs DDL uses `ReplacingMergeTree(_adapter_version)` partitioned by `toDate(Timestamp)` and `ORDER BY (toDate(Timestamp), <high-cardinality-prefix>, _odb_sequence, _odb_entry_index, _odb_record_index)`. The `_odb_*` suffix is what gives `(sequence, entry_index, record_index)` a unique role in the dedupe key — preserve it. The leading prefix is your call: pick columns that give time + service skipping for your common queries.

**Key constraint:** any change to a column that participates in `ORDER BY` requires a **new table** and a backfill or rewrite. Non-`ORDER BY` column changes are fine with an `_adapter_version` bump (the merge tiebreaker resolves replays to the new mapping).

**5. A binary or test that wires it up.** The `clickhouse_ingestor` crate is a library too — your binary builds a `BufferConsumerRuntime` from the consumer + your decoder + your adapter + a writer:

```rust
let runtime = BufferConsumerRuntime::new(
    consumer,
    OtlpTracesDecoder,                                 // your SignalDecoder
    OtlpTracesClickHouseAdapter::new(adapter_cfg),     // your Adapter
    Some(writer),                                      // ClickHouseWriter (or None for dry-run)
    RuntimeOptions {
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Traces,    // <- match what the producer wrote
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        commit_group: CommitGroupThresholds { /* max_rows, max_bytes, max_age */ },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: false,
        poll_interval: Duration::from_millis(20),
    },
);
runtime.run(shutdown).await
```

`SignalType` already covers the OTLP signal types (`Logs`, `Metrics`, `Traces`); for arbitrary non-OTLP payloads you'd extend that enum (additive, just byte-mapping + a name) or carry signal-type-as-data inside the envelope's `reserved` byte and dispatch in your decoder.

### Verifying it works

The traces test runs the same testcontainers + Buffer Producer + runtime + ClickHouse pattern as the logs test:

```sh
cargo test -p clickhouse-ingestor --features integration-tests \
  --test clickhouse_round_trip_traces -- --nocapture
```

Both tests should pass on a machine with Docker available — they exercise the full producer → manifest → consumer → decoder → adapter → writer → `SELECT count()` loop against a real ClickHouse container. If you change the runtime, commit-group, or writer code, run both as a regression check; the traces test is the one that proves the public trait surfaces are usable for new signals.

## See also

- [RFC 0001 — ClickHouse Ingestor](../../rfcs/0001-clickhouse-ingestor.md): full design, ack/retry semantics, dedupe model.
- [`tests/clickhouse_round_trip.rs`](tests/clickhouse_round_trip.rs): logs end-to-end test (the published path).
- [`tests/clickhouse_round_trip_traces.rs`](tests/clickhouse_round_trip_traces.rs): traces end-to-end test (the worked extension example).
- [opendata-go OpenData OTel exporter](https://github.com/opendata-oss/opendata-go/tree/main/exporter/opendataexporter): the producer side that writes OTLP logs into the Buffer.
- [`opendata-buffer` crate](https://crates.io/crates/opendata-buffer): the underlying Buffer producer/consumer library.
