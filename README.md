<!-- ============================================================
Pulse README with "safe" GitHub animations (SVG/GIF only, no JS)
Paste this as README.md
============================================================ -->

<p align="center">
  <img src="assets/cosmic_pulse.gif" alt="Pulse Logo" width="900">
</p>

<p align="center">
  <img
    src="https://capsule-render.vercel.app/api?type=soft&height=60&color=0:ff2d55,100:7a00ff&text=Pulse%20ML%20Pipeline:%20Real-time%20Outlier%20Detection&fontSize=36&fontColor=ffffff"
    alt="Pulse ML Pipeline – Real-time Outlier Detection"
  />
</p>

<p align="center">
  <em>Don't let unoptimized queries and black-box algorithms swallow your cloud database budget like a black hole.<br/>
  Pulse brings real-time ML outlier detection to the edge of your data warehouse — so you can see what's really going on before it's too late.</em>
</p>

<br/>

Production-ready Kafka streaming pipeline with real-time ML inference and asynchronous model training.

## 💡 Why Pulse?

- **🚀 Production-Ready ONNX Inference** — Optimized ML inference with ONNX Runtime supporting both CPU and GPU execution providers
- **📦 Optimized File Format** — Apache Arrow Feather format with LZ4/ZSTD compression (47x faster writes than Parquet)
- **⚡ Fully Async Training** — Model training runs in a detached subprocess (~90s average), never blocking real-time inference
- **🔄 Automatic Model Lifecycle** — Daily retraining with rolling window data; new models automatically loaded for next inference cycle
- **📊 End-to-End Batch Processing** — Batched writes, reads, inference, and training for maximum throughput
- **🛠️ Optimized Libraries** — orjson (3.6x faster JSON), PyArrow (columnar storage), pandas (vectorized ops)
- **📡 Kafka Streaming** — Reliable, persistent message exchange with consumer groups and offset management
- **📈 High Throughput** — 17,700 msg/s sustained throughput (300x+ real-time capacity)
- **🔧 Parallel Inference** — Configurable worker pool for multi-core CPU utilization
- **🐳 One-Command Docker Deployment** — Full stack (Kafka + Pipeline + Dashboard) with `docker compose up`

---

## 🚀 Quick Start (Docker)

> **📖 Full Docker reference:** [docs/DOCKER.md](docs/DOCKER.md) — building locally, clean runs, environment variables, terminal options
>
> **📖 Manual setup (no Docker):** [docs/MANUAL_SETUP.md](docs/MANUAL_SETUP.md) — Python venv, individual component commands, run scripts

### 1. Clone and Get Data

```bash
git clone https://github.com/IvanYachUkr/Pulse.git
cd Pulse
```

Place a **date-sorted** Parquet file from the [Amazon Redset](https://github.com/amazon-science/redset) dataset at:

```
_data/input/sorted_4days.parquet
```

### 2. Start

```bash
docker compose up
```

This pulls the pre-built image, starts Kafka, and runs the entire pipeline. Wait 30–60 seconds for data to flow, then open the dashboard.

### 3. Access

| Service | URL |
|---------|-----|
| **Dashboard** | [http://localhost:8507](http://localhost:8507) |
| **Kafka Console** | [http://localhost:8080](http://localhost:8080) |

### 4. Stop

```bash
docker compose down
```

> **⚠️ Important:** Close the dashboard browser tab **before** restarting the pipeline from scratch. An open dashboard holds connections to SQLite/DuckDB databases inside the container, which can prevent proper cleanup on restart.

---

## 🎯 Overview

This pipeline processes query events from Kafka, detects outliers using ONNX models, and automatically retrains models daily. Optimized for high throughput with Apache Arrow Feather format achieving **47x faster writes** than Parquet.

### Data Flow

```
Kafka → Consumer → Feather Writer → Daily Files (.arrow)
                 ↓
              Inference (ONNX)
                 ↓
           Anomaly Detection
                 
Daily Files → Async Training (subprocess) → New Models → Next Day Inference
```

### Key Features

* **Real-time inference:** Batch inference with ONNX runtime
* **Asynchronous training:** Non-blocking model retraining in subprocess (~90s average)
* **High performance:** 17,700 msg/s throughput (300x+ real-time capacity)
* **Optimized storage:** Apache Arrow Feather format (47x faster writes than Parquet-gzip)
* **Compression options:** LZ4 (maximum speed) or ZSTD (2x better compression)
* **Fast JSON parsing:** orjson (3.6x faster than stdlib)

---

## 📊 Query Classification

Every query is classified into one of five categories based on its performance bottleneck:

| Class | What It Captures | Key Threshold |
|-------|-----------------|---------------|
| **Network-bound** | COPY/UNLOAD data transfers | exec > 500ms |
| **CPU-bound** | Complex compute-heavy queries | exec/scan ratio > 20, exec > 1s |
| **IO-bound** | Large scans bottlenecked by disk | scan ≥ 2 GB, ratio < 0.061 |
| **Queue/WLM-bound** | Simple queries stuck waiting | queue ≥ exec, complexity < 4 |
| **Normal** | No dominant bottleneck | Everything else |

All thresholds are **data-derived** from behavioral regime analysis of 433M queries — not hardcoded assumptions. See **[QUERY_CLASSIFICATION.md](docs/QUERY_CLASSIFICATION.md)** for the full methodology, transition points, and validation results.

---

## 📊 Dashboard Architecture

The dashboard is a **decoupled SPA** built with a **FastAPI JSON backend** and a **Preact + HTM** client — no build tools, no bundler, no `node_modules`.

### Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Backend** | FastAPI + Uvicorn | JSON API serving dashboard data from SQLite/DuckDB |
| **Frontend** | Preact + HTM (via CDN) | Reactive UI with hooks, delivered as a single HTML file |
| **Charts** | Chart.js (via CDN) | Interactive time-series and classification charts |
| **Styling** | Vanilla CSS (custom) | Dark-mode glassmorphism design with CSS variables |

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/status` | GET | Pipeline component health status |
| `/api/instances` | GET | Available cluster instance IDs |
| `/api/instances/critical` | GET | Instances flagged as critical |
| `/api/metrics?ids=...` | GET | Aggregated performance metrics |
| `/api/classification/chart?ids=...` | GET | Query classification time-series |
| `/api/classification/table?ids=...` | GET | Query classification breakdown |
| `/api/anomalies?ids=...` | GET | Detected anomalous queries |
| `/api/critical/types?ids=...` | GET | Critical problem type distribution |
| `/api/recommendations/{type}` | GET | Auto-generated remediation advice |

### Key Features

* **Multi-instance selection** — Select/deselect individual instances, use "Select All" / "Critical Only" shortcuts
* **Time window toggle** — Switch between 24h and 1-week views
* **Real-time status indicators** — Live health checks for stream analytics, anomaly detection, and database
* **Metric cards** — Score, queue time, spillage, anomaly count with trend indicators
* **Interactive charts** — Stacked area chart for query classification over time
* **Anomaly table** — Detected anomalous queries with severity highlighting
* **Recommendations panel** — Context-aware remediation advice based on dominant problem types

---

## 📊 Performance

### Overall Pipeline

| Component | Throughput | Notes |
|-----------|-----------|-------|
| **Producer** | 31-38k msg/s | Consistent performance |
| **Consumer (with inference)** | 17,700 msg/s | 4 workers, batch 65k |
| **Consumer (write only)** | 21,600 msg/s | Excluding inference |
| **Training** | ~90s/day | 400k rows per day (async) |

**Real-time capacity:** ~36 msg/s (300x+ speedup exceeds requirements)

### Inference Parallelization Benchmark (600k messages)

| Workers | CPU Throughput | GPU Throughput |
|---------|---------------|---------------|
| **1** | 11,739 msg/s | 10,229 msg/s |
| **4** | **12,864 msg/s** ⭐ | 11,248 msg/s |
| **8** | 12,724 msg/s | — |

**Recommendation:** Use `--inference-workers 4` for optimal performance (CPU).

> ⚠️ **GPU Note:** Tested with RTX 4070. GPU is **SLOWER** than CPU for this workload! The ONNX Isolation Forest model requires CPU↔GPU data transfers (memcpy) that negate any compute gains. Use CPU.

---

## 🔧 Design Decisions & Benchmarks

### Apache Arrow Feather Format (47x Faster Writes)

| Format | Write Speed | Read Speed | File Size |
|--------|------------|-----------|-----------|
| Parquet (gzip) | 1x (baseline) | **5x faster** | **Smallest** |
| **Feather (LZ4)** | **47x faster** | 1x | 7x larger |

**Writes happen in the main consumer loop** (directly impacts throughput). **Reads happen asynchronously** in a subprocess. Therefore, write speed is critical.

### Compression Options (LZ4 vs ZSTD)

| Compression | Write Speed | File Size | Use Case |
|-------------|-----------|-----------|----------|
| **LZ4** (default) | Fastest | Larger (2.3x ratio) | Real-time streaming |
| **ZSTD** | 40% slower | **2x smaller** (5.6x ratio) | Disk-constrained, archival |

### JSON Parsing with orjson (3.6x Faster)

| Library | Speed | Improvement |
|---------|-------|-------------|
| json (stdlib) | 1x (baseline) | - |
| **orjson** | **3.6x faster** | 260% faster |

---

## 📁 Project Structure

```
Pulse/
├── README.md                       # This file
├── LICENSE                         # MIT License
├── docker-compose.yml              # Full stack Docker setup
├── Dockerfile                      # Pipeline container image
├── docker-entrypoint.sh            # Container startup script
├── requirements.txt                # Python dependencies
├── run_pipeline.bat / .sh          # Manual pipeline launchers
├── stop_pipeline.bat / .sh         # Manual pipeline stoppers
│
├── _data/                          # All runtime data (gitignored)
│   ├── input/                      # Source parquet files
│   ├── arrow/                      # Daily Arrow shard files
│   ├── models/                     # Trained ONNX + joblib models
│   ├── logs/                       # Training subprocess logs
│   ├── checkpoints/                # QuixStreams RocksDB state
│   └── store_ml/                   # ML anomalies lakehouse
│
├── docs/                           # Documentation
│   ├── DOCKER.md                   # Docker usage guide
│   ├── MANUAL_SETUP.md             # Manual (non-Docker) setup
│   ├── QUERY_CLASSIFICATION.md     # Classification methodology
│   └── QUERY_CLASSIFICATION_ANALYSIS.md  # Threshold derivation data
│
├── pipeline/                       # Core pipeline modules
│   ├── config.py                   # Shared configuration
│   ├── consumer.py                 # Main consumer with ML inference + training
│   ├── consumer_aggregate.py       # Stream analytics (classification logic)
│   ├── consumer_ml.py              # Anomaly sink for dashboard
│   ├── producer.py                 # Kafka producer with time acceleration
│   └── train_model.py              # Async training script
│
├── kafka_stream/                   # Kafka infrastructure
│   ├── docker-compose-local.yml    # Redpanda config (local)
│   ├── docker-compose-extern.yml   # Redpanda config (external network)
│   ├── arrow_writer.py             # Feather+LZ4/ZSTD writer
│   └── anomalous_query_producer.py # Anomaly output producer
│
├── outlier_tool/                   # ML library
│   ├── redset_outlier_lib.py       # Core ML service (OutlierService)
│   ├── business_features.py        # Feature engineering
│   ├── business_models.py          # Model artifacts & training
│   ├── business_io.py              # Data loading & saving
│   ├── business_pipeline.py        # ML pipeline stages
│   └── business_anomaly_logic.py   # Anomaly detection thresholds
│
├── dashboard/                      # FastAPI + Preact dashboard
│   ├── api.py                      # FastAPI backend (JSON API)
│   ├── generate_html.py            # HTML generator (builds index.html)
│   ├── index.html                  # Preact SPA (generated)
│   ├── style.css                   # Dashboard styles
│   ├── backend_connection.py       # Database connections
│   ├── db_reader.py                # Data queries
│   └── recommendations/            # Remediation advice (markdown)
│
└── assets/                         # Images and logos
```

---

## 🚨 Troubleshooting

### Training Fails
* Check `_data/logs/train_*.log` for errors
* Verify arrow files are readable: `import pyarrow.feather as feather; feather.read_feather("path")`
* Ensure training has enough data (at least N days configured)

### Low Throughput
* Use LZ4 compression (not ZSTD) for maximum write speed
* Check inference batch size (65k optimal)
* Run multiple consumer instances on partitioned topic
* Verify orjson is installed for fast JSON parsing

### Out of Memory
* Reduce inference batch size
* Reduce writer batch size
* Reduce Kafka buffer sizes

---

## 🔬 Reproducibility

> **Minimum requirement:** **8 GB of free RAM** to run the full pipeline (Redpanda + all consumers + producer + dashboard).

All development, testing, and benchmarking were performed on:

| Component | Specification |
|-----------|---------------|
| **Laptop** | ASUS ROG Zephyrus G16 (2024) — GA605WI |
| **CPU** | AMD Ryzen AI 9 HX 370 — 12 cores / 24 threads, up to 5.1 GHz |
| **RAM** | 32 GB LPDDR5X-7500 (soldered) |
| **GPU** | NVIDIA GeForce RTX 4070 Laptop — 8 GB GDDR6 |
| **Storage** | 1 TB PCIe 4.0 NVMe SSD |
| **OS** | Windows 11 Home |

---

## 📝 License

MIT License — see [LICENSE](LICENSE).

---

## 🙏 Acknowledgments

Built with:

* [QuixStreams](https://github.com/quixio/quix-streams) — Kafka streaming
* [PyArrow](https://arrow.apache.org/docs/python/) — Feather format
* [ONNX Runtime](https://onnxruntime.ai/) — ML inference
* [orjson](https://github.com/ijl/orjson) — Fast JSON parsing
* [Redpanda](https://redpanda.com/) — Kafka-compatible streaming
