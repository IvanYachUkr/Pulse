"""
Pulse Dashboard — FastAPI Backend
Wraps existing DashboardBackend for JSON API access.

Thread-safe: the backend is initialized once (immutable DB readers),
time windows are computed per-request via ``compute_time_window()``.
No global lock needed.
"""
import sys
from pathlib import Path

# Add parent + dashboard directories to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "dashboard"))

import math
import re
import traceback
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend_connection import DashboardBackend, compute_time_window

app = FastAPI(title="Pulse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Shared state (thread-safe — no mutable state) ────────────
backend: DashboardBackend | None = None


def get_backend() -> DashboardBackend:
    """Lazy-init the backend (creates DB readers once)."""
    global backend
    if backend is None:
        backend = DashboardBackend()
    return backend


def _sanitise(obj):
    """Replace NaN / Infinity with None for JSON serialisation."""
    if isinstance(obj, dict):
        return {k: _sanitise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitise(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def _parse_ids(ids: str) -> list[int]:
    """Parse comma-separated instance IDs."""
    return [int(x.strip()) for x in ids.split(",") if x.strip()]


# ── Endpoints ────────────────────────────────────────────────


@app.get("/api/status")
def status():
    """Probe actual database connectivity instead of returning hardcoded values."""
    result = {
        "stream_analytics": "no connection",
        "anomaly_detection": "no connection",
        "cloud_database": "running",      # local DB status (key matches frontend)
    }
    try:
        b = get_backend()
        # Probe stream DB
        try:
            b.db_stream.query(f"SELECT 1 FROM {b.db_stream.tablename} LIMIT 1")
            result["stream_analytics"] = "running"
        except Exception:
            result["stream_analytics"] = "error"
        # Probe ML DB
        try:
            b.db_ml.query(f"SELECT 1 FROM {b.db_ml.tablename} LIMIT 1")
            result["anomaly_detection"] = "running"
        except Exception:
            result["anomaly_detection"] = "error"
    except Exception:
        traceback.print_exc()
    return result


@app.get("/api/instances")
def instances(window: str = "24h"):
    """Return all known instance IDs for the active time window."""
    try:
        b = get_backend()
        tw = compute_time_window(b.db_stream, window)
        return sorted(int(x) for x in b.get_instance_ids(tw))
    except Exception as e:
        traceback.print_exc()
        return []


@app.get("/api/instances/critical")
def critical_instances(window: str = "24h"):
    """Return instance IDs that exceed critical-problem thresholds."""
    try:
        b = get_backend()
        tw = compute_time_window(b.db_stream, window)
        return sorted(int(x) for x in b.get_critical_instance_ids(tw))
    except Exception as e:
        traceback.print_exc()
        return []


@app.get("/api/metrics")
def metrics(ids: str = Query(...), window: str = "24h"):
    """Return aggregate metrics for one or more instance IDs."""
    try:
        b = get_backend()
        tw = compute_time_window(b.db_stream, window)
        result = b.get_agg_metrics(tw, _parse_ids(ids))
        return _sanitise(result)
    except Exception as e:
        traceback.print_exc()
        return {}


@app.get("/api/classification/chart")
def classification_chart(ids: str = Query(...), window: str = "24h"):
    """Return time-series query classification percentages."""
    try:
        b = get_backend()
        tw = compute_time_window(b.db_stream, window)
        df = b.get_query_classification_chart(tw, _parse_ids(ids))
        if df.empty:
            return []
        data = df.to_dict(orient="records")
        for row in data:
            ws = row.get("window_start")
            if hasattr(ws, "isoformat"):
                row["window_start"] = ws.isoformat()
            else:
                row["window_start"] = str(ws)
        return _sanitise(data)
    except Exception as e:
        traceback.print_exc()
        return []


@app.get("/api/classification/table")
def classification_table(ids: str = Query(...), window: str = "24h"):
    """Return a single-row classification summary table."""
    try:
        b = get_backend()
        tw = compute_time_window(b.db_stream, window)
        df = b.get_query_classification_table(tw, _parse_ids(ids))
        if df.empty:
            return {}
        row = df.iloc[0].to_dict()
        return _sanitise(row)
    except Exception as e:
        traceback.print_exc()
        return {}


@app.get("/api/anomalies")
def anomalies(ids: str = Query(...), window: str = "24h"):
    """Return anomaly records for the selected instances."""
    try:
        b = get_backend()
        tw = compute_time_window(b.db_stream, window)
        df = b.get_anomaly_queries(tw, _parse_ids(ids))
        data = df.to_dict(orient="records")
        for row in data:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
        return _sanitise(data)
    except Exception as e:
        traceback.print_exc()
        return []


@app.get("/api/critical/types")
def critical_types(ids: str = Query(...), window: str = "24h"):
    """Return active critical problem categories in severity order."""
    try:
        b = get_backend()
        tw = compute_time_window(b.db_stream, window)
        return b.get_critical_problem_types(tw, _parse_ids(ids))
    except Exception as e:
        traceback.print_exc()
        return []


RECOMMENDATIONS = {
    "cpu_bound": PROJECT_ROOT / "dashboard" / "recommendations" / "cpu-bound.md",
    "io_bound": PROJECT_ROOT / "dashboard" / "recommendations" / "io-bound.md",
    "network_bound": PROJECT_ROOT / "dashboard" / "recommendations" / "network-bound.md",
    "queue_wlm_bound": PROJECT_ROOT / "dashboard" / "recommendations" / "queue-wlm-bound.md",
    "normal": PROJECT_ROOT / "dashboard" / "recommendations" / "normal.md",
}


@app.get("/api/recommendations/{rec_type}")
def recommendation(rec_type: str):
    """Return markdown guidance for a recommendation category."""
    path = RECOMMENDATIONS.get(rec_type)
    if path is None or not path.exists():
        return {"text": "Unknown recommendation type."}
    raw = path.read_text(encoding="utf-8")
    raw = re.sub(r":(\w+)\[([^\]]+)\]", r"**\2**", raw)
    return {"text": raw}


# ── Static files ─────────────────────────────────────────────

STATIC_DIR = Path(__file__).parent
NO_CACHE = {"Cache-Control": "no-cache, must-revalidate"}


@app.get("/")
def index():
    """Serve the dashboard single-page app."""
    return FileResponse(STATIC_DIR / "index.html", headers=NO_CACHE)


@app.get("/style.css")
def stylesheet():
    """Serve dashboard stylesheet with no-cache headers."""
    return FileResponse(STATIC_DIR / "style.css", media_type="text/css", headers=NO_CACHE)


@app.get("/logo.png")
def logo():
    """Serve the dashboard logo asset."""
    return FileResponse(PROJECT_ROOT / "assets" / "pulse.png", media_type="image/png", headers=NO_CACHE)


@app.get("/favicon.ico")
def favicon():
    """Serve favicon image for browsers."""
    return FileResponse(
        PROJECT_ROOT / "assets" / "pulse.png", media_type="image/png"
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8507)
