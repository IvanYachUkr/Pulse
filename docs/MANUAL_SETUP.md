# 🛠️ Manual Setup Guide

Run Pulse without Docker — each component runs as a separate process on your machine.

> **Prefer Docker?** See [DOCKER.md](DOCKER.md) for a single-command setup.

---

## Prerequisites

### Infrastructure
- **Docker & Docker Compose** — only for running Kafka/Redpanda (the message broker)
- **Python 3.12+**

### Python Environment

Create and activate a virtual environment:

```bash
python -m venv venv
```

**Windows (PowerShell):**

```powershell
.\venv\Scripts\Activate.ps1
```

**Linux/macOS:**

```bash
source venv/bin/activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

> ⚠️ **GPU Notice:** If you want to use GPU (`--use-gpu`), install **onnxruntime-gpu** and ensure you have the **NVIDIA CUDA Toolkit** and **cuDNN** installed.
> More details: [ONNX GPU Installation](https://onnxruntime.ai/docs/install/#cuda-and-cudnn)
>
> ```bash
> pip uninstall -y onnxruntime
> pip install onnxruntime-gpu
> ```

---

## Getting a Data File

See the [Data File](DOCKER.md#getting-a-data-file) section in the Docker guide — the same instructions apply. Place your sorted Parquet file at `_data/input/sorted_4days.parquet`.

---

## Starting Kafka (Redpanda)

Two docker-compose configurations are provided in `kafka_stream/`:

### Local Development (default)

```bash
cd kafka_stream
docker-compose -f docker-compose-local.yml up -d
```

Verify it's running:

```bash
docker exec redpanda rpk cluster health
```

| Service | Port | URL |
|---------|------|-----|
| Kafka Broker | 9092 | `localhost:9092` |
| Panda Proxy | 8082 | `localhost:8082` |
| Admin API | 9644 | `localhost:9644` |
| Console UI | 8080 | [http://localhost:8080](http://localhost:8080) |

### External Network

For accessing Kafka from other machines on the network:

```bash
cd kafka_stream
# Edit the file first: replace 192.168.178.21 with your machine's IP
docker-compose -f docker-compose-extern.yml up -d
```

| Service | Internal Port | External Port |
|---------|---------------|---------------|
| Kafka (internal) | 29092 | `redpanda:29092` |
| Kafka (external) | 9092 | `<your-ip>:9092` |
| Panda Proxy | 8082 | `<your-ip>:8082` |
| Console UI | 8080 | [http://localhost:8080](http://localhost:8080) |

---

## Using Run Scripts

The easiest way to start everything manually.

### Start the Pipeline

**Windows:**

```powershell
.\run_pipeline.bat
# Or start fresh:
.\run_pipeline.bat --clean
```

**Linux/macOS:**

```bash
./run_pipeline.sh
# Or start fresh:
./run_pipeline.sh --clean
```

The scripts will:
1. Start Kafka (Redpanda) if not running
2. Start Aggregate Consumer (stream analytics for dashboard)
3. Start ML Consumer (anomaly sink for dashboard)
4. Start Engine Consumer (ML inference + training)
5. Start Producer (600x speedup replay)
6. Start Dashboard (FastAPI + Preact)

### Stop the Pipeline

**Windows:**

```powershell
.\stop_pipeline.bat
```

**Linux/macOS:**

```bash
./stop_pipeline.sh
```

### Verbose Mode

For demos, edit the configuration at the top of `run_pipeline.bat` or `run_pipeline.sh`:

```bash
# Change:
QUIET_FLAG=--quiet
# To:
QUIET_FLAG=
```

### Clean vs Incremental

| Flag | Behavior |
|------|----------|
| *(no flag)* | **Incremental** — keeps existing models, data, and consumer offsets |
| `--clean` | **Clean** — removes all Arrow shards, models, logs, SQLite data, Kafka consumer groups, then starts fresh |

By default (no flag), the pipeline resumes from where it left off.

### Access Points

| Service | URL |
|---------|-----|
| **Dashboard** | [http://localhost:8507](http://localhost:8507) |
| **Kafka Console** | [http://localhost:8080](http://localhost:8080) |

### Run Script Configuration

Both scripts use `docker-compose-local.yml` by default. To switch to external network:

| File | Line to Change |
|------|---------------|
| `run_pipeline.bat` | Line 69: change `docker-compose-local.yml` → `docker-compose-extern.yml` |
| `run_pipeline.sh` | Line 74: change `docker-compose-local.yml` → `docker-compose-extern.yml` |
| `stop_pipeline.sh` | Line 36: change `docker-compose-local.yml` → `docker-compose-extern.yml` |

---

## Running Individual Components

For fine-grained control, start each component separately.

### Producer (Data Ingress)

```bash
python pipeline/producer.py \
    --data-file _data/input/sorted_4days.parquet \
    --speedup 600 \
    --reset
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--data-file` | `_data/input/sorted_4days.parquet` | Parquet file with query events |
| `--speedup` | `60` | Time acceleration factor (600 = 1 hour in 6 seconds) |
| `--reset` | `false` | Start from beginning of data file |

### Engine Consumer (ML Inference + Training)

```bash
python pipeline/consumer.py \
    --auto-offset-reset earliest \
    --inference-batch-size 65536 \
    --inference-workers 4 \
    --idle-timeout 30 \
    --quiet
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--topic` | `redshift.query_events` | Kafka topic to consume |
| `--bootstrap` | `localhost:9092` | Kafka broker address |
| `--group` | `redshift-stats-consumer` | Consumer group ID |
| `--auto-offset-reset` | `latest` | `earliest` or `latest` |
| `--inference-batch-size` | `8192` | Batch size for ML inference |
| `--inference-workers` | `4` | Parallel ONNX workers |
| `--consumer-id` | `0` | Consumer instance ID (0=leader) |
| `--retrain-every-n-days` | `1` | Training frequency |
| `--max-days` | `0` | Stop after N days (0=unlimited) |
| `--idle-timeout` | `0` | Exit after N seconds idle (0=disabled) |
| `--use-gpu` | `false` | Enable GPU (not recommended, slower) |
| `--quiet` | `false` | Suppress frequent output |

### Aggregate Consumer (Stream Analytics)

```bash
python pipeline/consumer_aggregate.py \
    --window-minutes 10 \
    --retention-days 8 \
    --quiet
```

### ML Consumer (Anomaly Sink)

```bash
python pipeline/consumer_ml.py \
    --window-minutes 10 \
    --quiet
```

### Dashboard

```bash
python dashboard/api.py
```

Access at [http://localhost:8507](http://localhost:8507).

---

## Clean Environment (Manual)

To manually clean all generated data:

**Linux/macOS:**

```bash
# Stop Kafka
cd kafka_stream && docker-compose -f docker-compose-local.yml down
cd ..

# Remove all generated data
rm -rf _data/arrow _data/models _data/logs _data/checkpoints _data/store_ml
rm -rf _data/stream_stats.sqlite* _data/archive
rm -rf producer_state.json
```

**Windows (PowerShell):**

```powershell
# Stop Kafka
cd kafka_stream; docker-compose -f docker-compose-local.yml down
cd ..

# Remove all generated data
Remove-Item -Recurse -Force _data/arrow, _data/models, _data/logs, _data/checkpoints, _data/store_ml -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force _data/stream_stats.sqlite*, _data/archive -ErrorAction SilentlyContinue
Remove-Item -Force producer_state.json -ErrorAction SilentlyContinue
```

---

## Useful Kafka Commands

```bash
# Check cluster health
docker exec redpanda rpk cluster health

# List topics
docker exec redpanda rpk topic list

# Create topic with partitions
docker exec redpanda rpk topic create redshift.query_events -p 4

# Delete consumer group (reset offsets)
docker exec redpanda rpk group delete redshift-stats-consumer

# View consumer group lag
docker exec redpanda rpk group describe redshift-stats-consumer
```

---

## Horizontal Scaling (Experimental)

Multi-consumer support enables scaling inference across multiple machines or containers.

### How It Works

- Each consumer uses `--consumer-id N` (0, 1, 2, ...) for unique shard filenames
- Kafka partitions are distributed across consumers in the same consumer group
- Consumer 0 acts as the leader and triggers training
- Dynamic detection identifies active consumers from shard files
- Day-complete markers coordinate training across consumers

### Usage

```bash
# Machine 1 (leader — triggers training)
python pipeline/consumer.py --consumer-id 0 --auto-offset-reset earliest

# Machine 2 (follower)
python pipeline/consumer.py --consumer-id 1 --auto-offset-reset earliest
```

> **Note:** For optimal scaling, consumers should run on separate machines with shared storage (NFS, S3) for training data.
