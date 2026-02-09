# Dashboard

Real-time monitoring dashboard built as a decoupled SPA with a FastAPI JSON backend and a Preact + HTM client. No build tools, no bundler, no `node_modules`.

## Architecture

```
Browser (Preact + HTM via CDN)
        │  HTTP/JSON
        ▼
    api.py (FastAPI + Uvicorn)
        │
        ▼
backend_connection.py
        │
   ┌────┴────┐
   ▼         ▼
SQLite    DuckDB
(_data/)  (_data/store_ml/)
```

## Modules

| File | Role |
|------|------|
| `api.py` | FastAPI backend — serves JSON endpoints, static files, recommendation markdown |
| `backend_connection.py` | Thread-safe database connections — wraps `db_reader.py` with locking and caching |
| `db_reader.py` | Data access layer — reads metrics, classifications, anomalies from SQLite or DuckDB |
| `generate_html.py` | Builds `index.html` programmatically — avoids template escaping issues |
| `index.html` | Generated Preact SPA — reactive UI with Chart.js charts |
| `style.css` | Dark-mode glassmorphism design system with CSS variables |
| `recommendations/` | Markdown remediation advice files served as HTML cards |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/status` | GET | Pipeline component health |
| `/api/instances` | GET | Available cluster instance IDs |
| `/api/instances/critical` | GET | Instances flagged as critical |
| `/api/metrics?ids=...&window=24h` | GET | Aggregated performance metrics |
| `/api/classification/chart?ids=...` | GET | Query classification time-series |
| `/api/classification/table?ids=...` | GET | Classification breakdown table |
| `/api/anomalies?ids=...` | GET | Detected anomalous queries |
| `/api/critical/types?ids=...` | GET | Critical problem type distribution |
| `/api/recommendations/{type}` | GET | Remediation advice (markdown → HTML) |

## Running

```bash
python dashboard/api.py
```

Serves on [http://localhost:8507](http://localhost:8507).

## Data Sources

The dashboard auto-detects available backends:

| Backend | Source | Written By |
|---------|--------|------------|
| **SQLite** | `_data/stream_stats.sqlite` | `consumer_aggregate.py` |
| **DuckDB Lakehouse** | `_data/store_ml/` | `consumer_ml.py` |

SQLite is preferred when available (faster for aggregated queries). DuckDB lakehouse provides raw anomaly-level detail.
