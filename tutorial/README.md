# Brokerless OTel-to-ClickHouse pipeline (local)

End-to-end pipeline you can run on your laptop in about two minutes:

```
                 OTLP/gRPC
   telemetrygen ───────────▶  otel-collector  ─ writes batches ▶  MinIO  (S3-compatible Buffer queue)
                                                                     │
                                                                     │ reads manifest + batches
                                                                     ▼
                                                        clickhouse-ingestor  ─ HTTP inserts ▶  ClickHouse
```

No Kafka, no broker, no managed queue. The OTel collector writes OTLP-protobuf payloads into an OpenData Buffer queue backed by MinIO. The ClickHouse ingestor reads from that queue, decodes the OTLP, and inserts rows into a `ReplacingMergeTree` table. The collector and the ingestor coordinate through one manifest object in MinIO; there is no direct network path between them.

## What's running

| Service | Image | What it does |
|---|---|---|
| `minio` | `minio/minio:latest` | S3-compatible object store; backs the Buffer queue |
| `clickhouse` | `clickhouse/clickhouse-server:24.10.1.2812` | Destination database; table DDL applied at startup |
| `otel-collector` | `ghcr.io/opendata-oss/otel-collector:v0.4.0` | Receives OTLP, writes to MinIO-backed Buffer |
| `clickhouse-ingestor` | `ghcr.io/opendata-oss/clickhouse-ingestor:0.2.0` | Reads from Buffer, inserts into ClickHouse |
| `telemetrygen` (on demand) | `ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest` | OTLP load generator |

## Prereqs

- Docker 24+ with Compose v2 (`docker compose`, not `docker-compose`)
- `curl` for the verification step
- Roughly 1 GiB of free RAM and a couple of free cores

## Run it

```sh
# 1. Bring everything up. First run pulls ~1 GB of images.
docker compose up -d

# 2. Wait for healthchecks to settle (~10 s).
docker compose ps

# 3. Send 30 seconds of synthetic OTLP logs at 1000 rps.
docker compose run --rm telemetrygen
```

`telemetrygen` writes to the collector's OTLP/gRPC endpoint. The collector batches the payloads, hands them to the OpenData producer, which uploads each batch to MinIO and CAS-appends its location to the queue manifest. The ingestor sees the new entries on its next poll (250 ms by default), fetches them, decodes the OTLP, and inserts rows into ClickHouse.

## Verify

Count the rows that landed:

```sh
curl -u ingestor:ingestor \
  --data-urlencode "query=SELECT count() FROM tutorial.logs" \
  http://localhost:8123
```

A 30-second run at 1000 rps should report `30000` (give or take a small number; the ingestor may be a few hundred ms behind when you query). Wait a beat and re-run if the count is short.

Look at a sample row:

```sh
curl -u ingestor:ingestor \
  --data-urlencode "query=SELECT ServiceName, Body, _odb_sequence, _odb_entry_index, _odb_record_index FROM tutorial.logs ORDER BY Timestamp DESC LIMIT 3 FORMAT JSONEachRow" \
  http://localhost:8123
```

The `_odb_*` columns are the per-row provenance the ingestor stamps onto every record: the Buffer sequence number that delivered it, the entry index within the batch, and the record index within the entry. Together they form the unique-row key that the `ReplacingMergeTree(_adapter_version)` table uses to dedupe replays.

Open the operator endpoints if you want to poke around:

- MinIO console: <http://localhost:9001> (`minioadmin` / `minioadmin`). The buffer batches live under `tutorial-buffer/otel/logs/data/`; the manifest is at `tutorial-buffer/otel/logs/manifest`.
- Ingestor metrics: <http://localhost:9090/metrics>. Watch `runtime_ack_frontier`, `runtime_descriptors_handed_out`, `runtime_sink_commits`.

## What's in each config

- [`docker-compose.yml`](docker-compose.yml): the service graph plus env vars. The Rust ingestor talks to MinIO through the standard AWS env chain (`AWS_ENDPOINT`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ALLOW_HTTP=true`, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false`); no hardcoded endpoint in the YAML.
- [`otel-collector-config.yaml`](otel-collector-config.yaml): receivers (OTLP gRPC + HTTP), batch processor, opendata exporter pointed at MinIO. Tuned modestly so the tutorial doesn't fight your laptop. See the [exporter README](https://github.com/opendata-oss/opendata-go/tree/main/exporter/opendataexporter) for the full knob list.
- [`ingestor-config.yaml`](ingestor-config.yaml): buffer source, ClickHouse target, runtime/ack/adapter/sink sections. `apply_deduplication_token: false` because single-node ClickHouse rejects the `insert_deduplication_token` setting; flip it to `true` against a replicated table.
- [`clickhouse-init.sql`](clickhouse-init.sql): table DDL. `ReplacingMergeTree(_adapter_version)` ordered by `(toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)`.

## Send your own data

`telemetrygen` is a stand-in. Point any OTLP/gRPC or OTLP/HTTP client at the collector and it lands in ClickHouse the same way:

```sh
# OTel SDK in Python, Go, Node, etc.: set the endpoint to
#   localhost:4317 (gRPC) or http://localhost:4318 (HTTP)
# and the collector will route logs through this pipeline.
```

The collector accepts logs on the `opendata` exporter today. Traces and metrics decoders are tracked as follow-ups in `opendata-ingest-otel`.

## Customize the pipeline

A few starting points if you want to take this past the tutorial:

- **Push the collector harder.** Raise `flush_size_bytes`, `encode_concurrency`, `upload_concurrency`, and `manifest_append_batch_size` in `otel-collector-config.yaml`. The validated workload (175k rps sustained 8h) ran with 64 MiB flushes, `upload_concurrency: 8`, `manifest_append_batch_size: 16`.
- **Push the ingestor harder.** Raise `sink.max_concurrent_commits` and tune `adapter.max_chunk_rows` / `max_chunk_bytes` toward what your ClickHouse cluster ingests cleanly. Watch `runtime_stage_latency_seconds{stage="sink"}` for backpressure.
- **Switch to RowBinary.** `sink.serialization_format: row_binary` is the wire format the 175k-rps run used. `json_each_row` is the conservative default.
- **Point at real S3.** Remove `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`, set real AWS credentials, update `buffer.object_store.bucket`, and the same pipeline runs against S3 unchanged.

## Cleanup

```sh
docker compose down -v
```

The `-v` flag also drops the MinIO and ClickHouse volumes.

## What to read next

- [Runtime README](../runtime/opendata-ingest-runtime/): public API surface, how to build a new sink against the runtime.
- [RFC 0002](../rfcs/0002-generic-ingest-runtime.md): generic ingest runtime design — Sink and Decoder traits, pipelining, ack coordinator.
- [RFC 0001](../rfcs/0001-clickhouse-ingestor.md): ClickHouse sink design — ack/retry semantics, dedupe model, schema ownership.
- [Buffer architecture](https://github.com/opendata-oss/opendata-docs/blob/main/docs/buffer/architecture.mdx): the underlying Buffer queue, including the read-ahead API the ingestor consumes through.
- [opendata-go producer](https://github.com/opendata-oss/opendata-go): the producer-side library plus the OTel exporter wiring used by `otel-collector`.
