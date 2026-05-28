# opendata-contrib

Connectors and integrations for the [OpenData](https://github.com/opendata-oss/opendata) ecosystem.

The core OpenData repo holds the storage primitives (Buffer, Log, Timeseries, KeyValue). This repo holds the things that connect those primitives to external sinks. The two headline pieces are the **generic ingest runtime**, a sink-agnostic pipelined Buffer-to-sink runtime, and the **ClickHouse logs sink** built on it.

## Architecture in one paragraph

A producer pushes data into an OpenData Buffer queue, a stateless object-store-backed manifest with no broker on the path. The generic ingest runtime in this repo consumes from Buffer through the read-ahead API (opendata-buffer [RFC 0003](https://github.com/opendata-oss/opendata/blob/main/buffer/rfcs/0003-consumer-read-ahead.md), runs fetch / decode / sink as pipelined stages with a shared in-flight byte budget, and acks the Buffer frontier only after the sink has durably committed each range. A sink is a `Sink` trait implementation; a signal decoder is an `Adapter` implementation. The first published sink is `opendata-ingest-clickhouse` paired with the OTLP-logs decoder in `opendata-ingest-otel`, wired together by the `clickhouse-ingestor` binary, which effectively allows consuming high volumes of OTLP logs from Buffer and writing them to clickhouse.

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

Sink-agnostic runtime documented by [RFC 0002](rfcs/0002-generic-ingest-runtime.md). Provides the `Sink` and `Adapter` traits, pipelined fetch / decode / sink execution, byte-budget backpressure, ack coordination, and a `prometheus-client` metrics surface. Build a new sink (Iceberg, another database) as a fresh crate implementing the trait, not as a fork of the ingestor. See [`runtime/opendata-ingest-runtime/README.md`](runtime/opendata-ingest-runtime/README.md) for design and configuration. 

This runtime can consume extremely high throughput workloads with modest hardware.

### `opendata-ingest-clickhouse` (library)

ClickHouse `Sink` implementation that builds on the pipelined runtime above to consume data from Buffer and write to Clickhouse. Includes RowBinary and JSONEachRow row encoders, ReplacingMergeTree-compatible table mapping, deterministic per-range idempotency tokens for effective exactly-once. Pair with an `Adapter` (the OTel plugin ships one for OTLP logs) to write different data types to Clickhouse..

### `opendata-ingest-otel` (library)

OTLP signal decoder. Owns the gateway-receive envelope produced by the [OpenData OTel exporter](https://github.com/opendata-oss/opendata-go/tree/main/exporter/opendataexporter) and emits decoded rows. The runtime treats the envelope as opaque bytes; this crate unpacks it.

### `clickhouse-ingestor` (binary)

The packaged Clickhouse Sink connector. It wires the runtime to the OTel decoder and the ClickHouse sink, loads config from a YAML file plus env vars, and serves Prometheus metrics. Operator setup in [`connectors/clickhouse-ingestor/README.md`](connectors/clickhouse-ingestor/README.md).

## Tutorial

End-to-end local pipeline (OTel client → otel-collector → MinIO-backed Buffer → clickhouse-ingestor → ClickHouse → query) lives in [`tutorial/`](tutorial/). Stands up with `docker compose up` and a sample telemetry generator.

## Build

```sh
cargo build --release --locked --manifest-path connectors/clickhouse-ingestor/Cargo.toml
```

## License

MIT. See [LICENSE](LICENSE).
