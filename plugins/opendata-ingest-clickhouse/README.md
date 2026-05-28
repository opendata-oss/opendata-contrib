# opendata-ingest-clickhouse

ClickHouse `Sink` plugin for [`opendata-ingest-runtime`](../../runtime/opendata-ingest-runtime/).

Implements `opendata_ingest_runtime::sink::Sink` against a ClickHouse HTTP endpoint. Houses the adapter (row builder + chunker), the writer (HTTP client), the serializer (RowBinary or JSONEachRow), the metrics surface, and the ClickHouse-specific config.

## What's in here

- **`sink.rs`**: the `Sink` impl. Derives the per-`SinkCommit` idempotency token from `CommitIdentity` plus adapter config, dispatches to the writer, classifies failures into `NotCommitted` / `MaybeCommitted` / `Fatal`, and serves `check_committed` lookups against the table's dedupe state.
- **`adapter/`**: drains a `DecodedBatch` into `InsertChunk`s sized by `max_chunk_rows` and `max_chunk_bytes`. Sorts records by `(sequence, entry_index, record_index)` for stable chunking across replays.
- **`writer.rs`**: HTTP client. Supports `per_call` (new client per request) and `pooled` (shared client) modes; applies `insert_deduplication_token` on replicated tables.
- **`serializer/`**: `RowBinary` (validated at 175k rps) and `JSONEachRow` (conservative default). Selected via the `sink.serialization_format` config field.
- **`metrics.rs`**: ClickHouse-specific counters and histograms; registers against the host's shared `prometheus-client` `Registry`.

The schema mapping is `ReplacingMergeTree(_adapter_version)` with `_odb_*` system columns supplying the dedupe key suffix `(_odb_sequence, _odb_entry_index, _odb_record_index)`. RFC 0001 has the full design.

## How to use it

The published `clickhouse-ingestor` binary wires this plugin to the OTLP-logs decoder from `opendata-ingest-otel` plus the runtime; see [`connectors/clickhouse-ingestor/`](../../connectors/clickhouse-ingestor/) for the binary and its operator README.

Pairing with a different decoder (a non-OTLP signal, your own protobuf): construct the same `ClickHouseSink` with the same adapter config, hand the runtime your decoder, and run a new binary. See the [runtime README](../../runtime/opendata-ingest-runtime/README.md) for the wiring pattern.

## See also

- [RFC 0001 — ClickHouse Ingestor](../../rfcs/0001-clickhouse-ingestor.md)
- [RFC 0002 — Generic Ingest Runtime](../../rfcs/0002-generic-ingest-runtime.md)
- [Runtime README](../../runtime/opendata-ingest-runtime/README.md)
