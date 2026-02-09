# 🐳 Docker Usage Guide

Run the entire Pulse pipeline in a single Docker container — no Python setup required.

---

## Prerequisites

- **Docker** & **Docker Compose** (v2)

That's it. Everything else is bundled in the image.

---

## Getting a Data File

The pipeline replays a Parquet file of Redshift query events through Kafka. You need to provide your own:

1. **Obtain** a slice (or the full dataset) from the [Amazon Redset](https://github.com/amazon-science/redset) dataset.
2. **Sort by date** — the Parquet file **must** be sorted by the timestamp column so the time-acceleration replay works correctly.
3. **Place** the file at:

   ```
   kafka_stream/data/sorted_4days.parquet
   ```

   Or use any filename and override with the `DATA_FILE` environment variable (see [Configuration](#configuration) below).

> **Note:** If no data file is found, the pipeline starts in **dashboard-only** mode (no producer, no data ingress).

---

## Quick Start

### Pull and Run (recommended)

The pre-built image is published on GitHub Container Registry:

```bash
docker compose up
```

This will:
1. Pull `ghcr.io/ivanyachukr/pulse-pipeline:latest` (first run only)
2. Start Redpanda (Kafka broker)
3. Start the Kafka Console UI
4. Start the pipeline (producer + 3 consumers + dashboard)

### Build Locally

After cloning the repo, build from source:

```bash
docker compose up --build
```

Use `--build` whenever you've made local code changes and want them reflected in the container.

---

## Clean vs Incremental Runs

### Default Behavior (Incremental)

By default, `docker compose up` is an **incremental run**:
- The producer **replays from the beginning** of the data file (it always uses `--reset` internally).
- Previous ML models, Arrow shards, and SQLite data **persist inside the container** from the last run.
- Kafka consumer groups **resume from their last committed offset** if the container wasn't removed.

> **In practice**, if you ran `docker compose down` (which removes containers), the next `docker compose up` is effectively a clean run since the container's filesystem is recreated.

### Forcing a Clean Run

> **⚠️ Important:** Close the dashboard browser tab **before** performing a clean restart. An open dashboard holds connections to SQLite/DuckDB databases inside the container, which can prevent proper cleanup.

To explicitly wipe all previous data (models, databases, shards, logs) before starting:

```bash
CLEAN=true docker compose up
```

Or on **Windows PowerShell**:

```powershell
$env:CLEAN="true"; docker compose up
```

This removes:
- Arrow shards (`data/db/`)
- Trained ONNX models (`out_models/`)
- Training logs (`training_logs/`)
- SQLite databases (`dashboard/databases/`)
- Lakehouse data (`dashboard/lakehouse_stats/`, `dashboard/lakehouse_ml/`)
- Producer state (`producer_state.json`)
- QuixStreams state (`state/`)

### When to Use Clean

| Scenario | Recommendation |
|----------|---------------|
| First run | Not needed — container starts fresh |
| Code changes (rebuilt image) | Not needed — container starts fresh |
| Want to reset without removing containers | Use `CLEAN=true` |
| Debugging stale data issues | Use `CLEAN=true` |

---

## Stopping & Restarting

### Stop Everything

```bash
docker compose down
```

This stops and **removes** all containers. The next `docker compose up` starts fresh containers.

> **⚠️ Important:** Close the dashboard browser tab before stopping. An open tab holds database connections that may interfere with a clean restart.

### Stop Without Removing

```bash
docker compose stop
```

Containers are paused. Resume with:

```bash
docker compose start
```

### Restart Just the Pipeline

```bash
docker compose restart pipeline
```

Restarts only the pipeline container (keeps Kafka running). Useful when you just want to replay data.

---

## Terminal Visibility

### Foreground Mode (see all logs)

```bash
docker compose up
```

All logs stream to your terminal. Press `Ctrl+C` to stop.

### Detached Mode (background)

```bash
docker compose up -d
```

Containers run in the background. To view logs:

```bash
# All services
docker compose logs -f

# Pipeline only
docker logs -f pulse-pipeline

# Last 50 lines
docker logs --tail 50 pulse-pipeline
```

### Verbose Pipeline Output

By default the pipeline runs in `--quiet` mode (suppresses high-frequency per-batch logs). To see full output:

```bash
QUIET_FLAG="" docker compose up
```

Or on **Windows PowerShell**:

```powershell
$env:QUIET_FLAG=""; docker compose up
```

---

## Configuration

All pipeline behavior is configurable through environment variables in `docker-compose.yml` or via the command line:

| Variable | Default | Description |
|----------|---------|-------------|
| `SPEEDUP` | `600` | Time acceleration factor (600 = 1 hour of data in 6 seconds) |
| `BATCH_SIZE` | `65536` | Inference batch size for the ML consumer |
| `IDLE_TIMEOUT` | `10` | Seconds of idle before the engine consumer exits |
| `QUIET_FLAG` | `--quiet` | Set to empty string `""` for verbose output |
| `DATA_FILE` | `kafka_stream/data/sorted_4days.parquet` | Path to the input Parquet file (inside the container) |
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:29092` | Kafka broker address (don't change unless you know what you're doing) |
| `CLEAN` | *(unset)* | Set to `true` to wipe all data before starting |

### Example: Custom Configuration

```bash
SPEEDUP=100 QUIET_FLAG="" CLEAN=true docker compose up
```

---

## Mounting Custom Data

The `docker-compose.yml` mounts `./kafka_stream/data` into the container:

```yaml
volumes:
  - ./kafka_stream/data:/app/kafka_stream/data
```

To use a data file in a different location:

```bash
# Option 1: Copy your file into the expected directory
cp /path/to/my_data.parquet kafka_stream/data/sorted_4days.parquet

# Option 2: Override the volume mount and DATA_FILE env var
docker run -v /path/to/data:/app/kafka_stream/data \
  -e DATA_FILE=kafka_stream/data/my_data.parquet \
  ghcr.io/ivanyachukr/pulse-pipeline:latest
```

---

## Access Points

| Service | URL |
|---------|-----|
| **Dashboard** | [http://localhost:8507](http://localhost:8507) |
| **Kafka Console** | [http://localhost:8080](http://localhost:8080) |

---

## Troubleshooting

### Dashboard shows "Please select an instance"
The pipeline needs time to process data. Wait 30–60 seconds after startup, then click **All** in the sidebar.

### No instances appear
- Check that your data file exists: `ls kafka_stream/data/`
- Check pipeline logs: `docker logs pulse-pipeline`
- Ensure the Parquet file is sorted by timestamp

### Container exits immediately
- Check logs: `docker logs pulse-pipeline`
- Common cause: Kafka not ready yet (should self-heal via retry loop)

### Stale dashboard after code changes
The dashboard uses `Cache-Control: no-cache` headers, so a hard refresh (`Ctrl+Shift+R`) should always load the latest version.
