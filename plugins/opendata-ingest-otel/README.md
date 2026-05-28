# opendata-ingest-otel

OTLP signal decoders for [`opendata-ingest-runtime`](../../runtime/opendata-ingest-runtime/).

Implements `opendata_ingest_runtime::decoder::Decoder` for OTLP payloads written by the [OpenData OTel exporter](https://github.com/opendata-oss/opendata-go/tree/main/exporter/opendataexporter). The runtime treats per-entry metadata as opaque bytes; this crate owns the envelope parser and the OTLP protobuf decoders.

## What's in here

- **`envelope.rs`**: parses the gateway-receive envelope the OTel exporter writes into each Buffer entry's metadata. Carries the signal type, the encoding, and the gateway-receive timestamp.
- **`logs.rs`**: OTLP logs decoder. Walks `ResourceLogs → ScopeLogs → LogRecord`, flattens into the row shape the ClickHouse adapter expects (`Timestamp`, `ServiceName`, `Body`, `ResourceAttributes`, `LogAttributes`, `TraceId`, `SpanId`, plus the `_odb_*` provenance columns).
- **`lib.rs`**: re-exports.

Traces and metrics decoders are not in this crate today. The traces example in `clickhouse-ingestor`'s integration tests shows the same pattern against the connector's older `SignalDecoder` trait.

## How to use it

The published `clickhouse-ingestor` binary uses this crate's logs decoder. To wire a different sink (Iceberg, Postgres) behind the same OTLP-logs decoder, depend on this crate, depend on `opendata-ingest-runtime`, and follow the runtime README's "Wiring a new sink" pattern.

To wire a different signal type into the same ClickHouse sink (traces, metrics, custom protobufs), see the worked example in [`connectors/clickhouse-ingestor/`](../../connectors/clickhouse-ingestor/) under "Extending the ingestor".

## See also

- [RFC 0002 — Generic Ingest Runtime](../../rfcs/0002-generic-ingest-runtime.md): `Decoder` trait, opaque-metadata contract.
- [OpenData OTel exporter](https://github.com/opendata-oss/opendata-go/tree/main/exporter/opendataexporter): the producer that writes the OTLP payloads + envelopes this crate decodes.
- [Runtime README](../../runtime/opendata-ingest-runtime/README.md)
