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

Releases are cut from GitHub Actions, not from the local checkout. The flow is two workflows:

1. **`publish.yml`** (manual `workflow_dispatch`): pick a connector and a `patch` / `minor` / `major` bump. The workflow runs `cargo set-version`, runs the test suite, commits the bump on `main`, and pushes a `<connector>/v<X.Y.Z>` tag.
2. **`build-binaries.yml`** (fires on the pushed tag): creates a GitHub Release, builds binaries across `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`, and pushes the container image to `ghcr.io/opendata-oss/<connector>:vX.Y.Z` (also tagged `:X.Y` and `:latest` via `docker/metadata-action`).

For ad-hoc branch builds without cutting a release, use `build-image.yml`'s `workflow_dispatch` trigger; it produces image tags of the form `<branch>-<sha>`.

To build locally for a platform not in the matrix:

```sh
cargo build --release --locked --manifest-path connectors/clickhouse-ingestor/Cargo.toml
```

## License

MIT — see [LICENSE](LICENSE).
