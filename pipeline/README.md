# Pipeline

Core streaming pipeline modules that consume Kafka messages, run ML inference, aggregate statistics, and produce anomaly events.

## Modules

| File | Role |
|------|------|
| `config.py` | Shared configuration — paths, environment variables, topic names |
| `producer.py` | Replays a Parquet file through Kafka with configurable time acceleration |
| `consumer.py` | Main ML consumer — batched inference (ONNX), async daily retraining, Arrow shard writes |
| `consumer_aggregate.py` | Stream analytics — classifies queries, writes 10-minute window aggregates to SQLite |
| `consumer_ml.py` | Anomaly sink — consumes flagged anomalies and writes to DuckDB lakehouse |
| `train_model.py` | Subprocess training script — called by `consumer.py` for non-blocking model retraining |

## Data Flow

```
                     redshift.query_events (Kafka topic)
                                │
           ┌────────────────────┼────────────────────┐
           ▼                    ▼                    ▼
    consumer.py         consumer_aggregate.py   consumer_ml.py
    (ML engine)         (stream analytics)      (anomaly sink)
           │                    │                    │
    ┌──────┴──────┐             │                    │
    ▼             ▼             ▼                    ▼
 _data/arrow/  _data/models/  _data/               _data/store_ml/
 (shards)      (ONNX)        stream_stats.sqlite   (DuckDB lakehouse)
```

## CLI Reference

### Producer

```bash
python pipeline/producer.py \
    --data-file _data/input/sorted_4days.parquet \
    --speedup 600 \
    --reset
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--data-file` | `_data/input/sorted_4days.parquet` | Input Parquet file |
| `--speedup` | `60` | Time acceleration (600 = 1hr → 6s) |
| `--reset` | `false` | Start from beginning of data file |

### Engine Consumer (ML)

```bash
python pipeline/consumer.py \
    --auto-offset-reset earliest \
    --inference-batch-size 65536 \
    --inference-workers 4 \
    --idle-timeout 30
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--topic` | `redshift.query_events` | Kafka topic |
| `--bootstrap` | `localhost:9092` | Kafka broker |
| `--group` | `redshift-stats-consumer` | Consumer group ID |
| `--inference-batch-size` | `8192` | Batch size for ONNX inference |
| `--inference-workers` | `4` | Parallel ONNX workers |
| `--db-dir` | `_data/arrow` | Arrow shard output directory |
| `--model-dir` | `_data/models` | Model directory |
| `--training-logs-dir` | `_data/logs` | Training log directory |
| `--retrain-every-n-days` | `1` | Training frequency |
| `--idle-timeout` | `0` | Exit after N idle seconds (0=disabled) |
| `--use-gpu` | `false` | Enable GPU (slower for this workload) |

### Aggregate Consumer

```bash
python pipeline/consumer_aggregate.py \
    --window-minutes 10 \
    --retention-days 8
```

### ML Consumer

```bash
python pipeline/consumer_ml.py \
    --window-minutes 10
```

## Configuration (`config.py`)

Key environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LAKEHOUSE_BASE_DIR` | `_data` | Base directory for all data storage |
| `MODEL_DIR` | `_data/models` | ONNX model directory |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
