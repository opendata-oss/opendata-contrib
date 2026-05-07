# opendata-contrib

Connectors and integrations for the [OpenData](https://github.com/opendata-oss/opendata) ecosystem.

The core OpenData repo holds the storage primitives — Buffer, Log, Timeseries, KeyValue. This repo holds the things that connect those primitives to the outside world: ingestors that move data into external sinks, exporters, adapters, and the RFCs that describe their contracts.

## Layout

```
opendata-contrib/
├── connectors/                  Production connectors and adapters
│   └── clickhouse-ingestor/     OpenData Buffer → ClickHouse ingestor
├── rfcs/                        Connector and integration design documents
│   └── 0001-clickhouse-ingestor.md
└── Cargo.toml                   Cargo workspace
```

## Connectors

### `clickhouse-ingestor`

A reusable Rust runtime that consumes OpenData Buffer batches and writes them into ClickHouse. Its design is described in [`rfcs/0001-clickhouse-ingestor.md`](rfcs/0001-clickhouse-ingestor.md).

The runtime is generic at the Buffer-reader and metadata-envelope layers. Signal-specific decoding and ClickHouse table mapping live in pluggable adapters; the alpha ships an OTLP-logs adapter.

Container images are published to GHCR as `ghcr.io/opendata-oss/clickhouse-ingestor:<tag>` by the `build-image.yml` workflow.

## License

MIT — see [LICENSE](LICENSE).
