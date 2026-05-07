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

## Releases

Tag a connector release as `<connector>/v<semver>` (e.g. `clickhouse-ingestor/v0.1.0`). The `release.yml` workflow then publishes:

- A GitHub Release with a `linux/amd64` binary tarball (`clickhouse-ingestor-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz`) and `.sha256`.
- A container image at `ghcr.io/opendata-oss/clickhouse-ingestor:vX.Y.Z`.

The two jobs are independent — either can be re-run without affecting the other. For pre-release branch builds (no tag), use the `build-image.yml` workflow's `workflow_dispatch` trigger; it produces image tags of the form `<branch>-<sha>`.

For platforms not covered by the published binary (macOS, ARM Linux), build from source:

```sh
cargo build --release --locked --manifest-path connectors/clickhouse-ingestor/Cargo.toml
```

## License

MIT — see [LICENSE](LICENSE).
