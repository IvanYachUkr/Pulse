# Kafka Stream

Kafka infrastructure utilities and Redpanda configuration for the streaming pipeline.

## Modules

| File | Role |
|------|------|
| `arrow_writer.py` | Feather + LZ4/ZSTD writer — batched, date-partitioned Arrow shard output |
| `anomalous_query_producer.py` | Publishes flagged anomalies to the `anomalous_queries` Kafka topic |
| `docker-compose-local.yml` | Redpanda broker + Console UI for local development |
| `docker-compose-extern.yml` | Redpanda with external network advertising (multi-machine setups) |

## Redpanda (Local)

```bash
cd kafka_stream
docker-compose -f docker-compose-local.yml up -d
```

| Service | Port | URL |
|---------|------|-----|
| Kafka Broker | 9092 | `localhost:9092` |
| Panda Proxy | 8082 | `localhost:8082` |
| Admin API | 9644 | `localhost:9644` |
| Console UI | 8080 | [http://localhost:8080](http://localhost:8080) |

## Arrow Writer

The `ArrowWriter` class provides high-performance batched writes:

- **Format**: Apache Arrow Feather (IPC)
- **Compression**: LZ4 (default, fastest) or ZSTD (2x smaller)
- **Partitioning**: Automatic date-based file naming (`shard_YYYY-MM-DD_N.arrow`)
- **Batching**: Configurable batch size with periodic flush

### Performance

| Format | Write Speed | File Size |
|--------|------------|-----------|
| Parquet (gzip) | 1x baseline | Smallest |
| **Feather (LZ4)** | **47x faster** | 7x larger |
| Feather (ZSTD) | 28x faster | 3.5x larger |
