# opendata-contrib

Connectors and integrations for the [OpenData](https://github.com/opendata-oss/opendata) ecosystem.

The core OpenData repo holds the storage primitives (Buffer, Log, Timeseries, KeyValue). This repo holds the things that connect those primitives to external sinks. The two headline pieces are the **generic ingest runtime**, a sink-agnostic pipelined Buffer-to-sink runtime, and the **ClickHouse logs sink** built on it.

## Architecture in one paragraph

A producer pushes data into an OpenData Buffer queue, a stateless object-store-backed manifest with no broker on the path. The generic ingest runtime in this repo consumes from Buffer through the read-ahead API (opendata-buffer RFC 0003), runs fetch / decode / sink as pipelined stages with a shared in-flight byte budget, and acks the Buffer frontier only after the sink has durably committed each range. A sink is a `Sink` trait implementation; a signal decoder is an `Adapter` implementation. The first published sink is `opendata-ingest-clickhouse` paired with the OTLP-logs decoder in `opendata-ingest-otel`, wired together by the `clickhouse-ingestor` binary.

## Workspace layout

```
opendata-contrib/
├── runtime/
│   └── opendata-ingest-runtime/      Sink-agnostic ingest runtime (RFC 0002)
├── plugins/
│   ├── opendata-ingest-clickhouse/   ClickHouse Sink implementation
│   └── opendata-ingest-otel/         OTLP signal decoder
├── connectors/
│   └── clickhouse-ingestor/          Standalone binary that wires it all together
├── rfcs/
│   ├── 0001-clickhouse-ingestor.md   ClickHouse-sink design
│   └── 0002-generic-ingest-runtime.md   Runtime design
└── Cargo.toml                        Cargo workspace
```

## Crates

### `opendata-ingest-runtime` (library)

Sink-agnostic runtime documented by [RFC 0002](rfcs/0002-generic-ingest-runtime.md). Provides the `Sink` and `Adapter` traits, pipelined fetch / decode / sink execution, K>1 admission, byte-budget backpressure, per-source ack coordination, and a `prometheus-client` metrics surface. Build a new sink (Iceberg, another database) as a fresh crate implementing the trait, not as a fork of the ingestor. See [`runtime/opendata-ingest-runtime/README.md`](runtime/opendata-ingest-runtime/README.md) for design and configuration.

### `opendata-ingest-clickhouse` (library)

ClickHouse `Sink` implementation. RowBinary and JSONEachRow row encoders, ReplacingMergeTree-compatible table mapping, deterministic per-range idempotency tokens for at-least-once retries that compose into exactly-once effect. Pair with an `Adapter` (the OTel plugin ships one for OTLP logs).

### `opendata-ingest-otel` (library)

OTLP signal decoder. Owns the gateway-receive envelope produced by the [OpenData OTel exporter](https://github.com/opendata-oss/opendata-go/tree/main/exporter/opendataexporter) and emits decoded rows the ClickHouse sink can encode. The runtime treats the envelope as opaque bytes; this crate unpacks it.

### `clickhouse-ingestor` (binary)

The published binary. Image `ghcr.io/opendata-oss/clickhouse-ingestor:0.2.0`. Wires the runtime to the OTel decoder and the ClickHouse sink, loads config from a YAML file plus env vars, and serves Prometheus metrics. Design rationale in [RFC 0001](rfcs/0001-clickhouse-ingestor.md); operator setup in [`connectors/clickhouse-ingestor/README.md`](connectors/clickhouse-ingestor/README.md).

## Tutorial

End-to-end local pipeline (OTel client → otel-collector → MinIO-backed Buffer → clickhouse-ingestor → ClickHouse → query) lives in [`tutorial/`](tutorial/). Stands up with `docker compose up` and a sample telemetry generator.

## Releases

Releases are cut from GitHub Actions, not from the local checkout. The flow is two workflows:

1. **`publish.yml`** (manual `workflow_dispatch`): pick a connector and a `patch` / `minor` / `major` bump. The workflow runs `cargo set-version`, runs the test suite, commits the bump on `main`, and pushes a `<connector>/v<X.Y.Z>` tag.
2. **`build-binaries.yml`** (fires on the pushed tag): creates a GitHub Release, builds binaries across `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`, and pushes the container image to `ghcr.io/opendata-oss/<connector>:vX.Y.Z` (also tagged `:X.Y` and `:latest` via `docker/metadata-action`).

For ad-hoc branch builds without cutting a release, use `build-image.yml`'s `workflow_dispatch` trigger; it produces image tags of the form `<branch>-<sha>`.

To build locally for a platform not in the matrix:

```sh
cargo build --release --locked --manifest-path connectors/clickhouse-ingestor/Cargo.toml
```

## License

MIT. See [LICENSE](LICENSE).
