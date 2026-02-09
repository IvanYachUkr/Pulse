"""Shared configuration for Lakehouse architecture.

Centralizes paths and connection logic for both consumer and dashboard.
Includes environment variable support and logging configuration.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Dict, Optional

import duckdb
from dotenv import load_dotenv

# Load environment variables from .env file (do not override existing OS env by default)
load_dotenv(override=False)

# -----------------------------------------------------------------------------
# Environment-driven configuration
# -----------------------------------------------------------------------------

# Kafka
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Base directory for lakehouse storage (DuckLake catalog + data)
_DEFAULT_BASE = (Path(__file__).parent.parent / "_data").resolve()
LAKEHOUSE_BASE_DIR: Path = Path(os.getenv("LAKEHOUSE_BASE_DIR", str(_DEFAULT_BASE))).resolve()

# Models, logging
MODEL_DIR: str = os.getenv("MODEL_DIR", "./_data/models")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# Optional DuckDB tuning (safe defaults)
DUCKDB_THREADS: Optional[int] = int(os.getenv("DUCKDB_THREADS", "0")) or None  # 0/empty => don't set
DUCKDB_MEMORY_LIMIT: Optional[str] = os.getenv("DUCKDB_MEMORY_LIMIT")  # e.g., "2GB"
DUCKDB_TEMP_DIRECTORY: Optional[str] = os.getenv("DUCKDB_TEMP_DIRECTORY")  # e.g., "/tmp/duckdb"

# Table names (keep as constants because the pipeline contract depends on them)
TABLE_STREAM_STATS: str = os.getenv("TABLE_STREAM_STATS", "minute_stats")
TABLE_ML_ANOMALIES: str = os.getenv("TABLE_ML_ANOMALIES", "anomalies")


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

def setup_logging(name: str, level: str | None = None) -> logging.Logger:
    """Setup structured logging for a module.

    Args:
        name: Logger name (typically ``__name__``).
        level: Log level override (default from ``LOG_LEVEL`` env var).

    Returns:
        Configured logger instance.
    """
    effective_level = (level or LOG_LEVEL).upper()
    log_level = getattr(logging, effective_level, logging.INFO)

    # Configure root logger once
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    return logger


_log = setup_logging(__name__)


# -----------------------------------------------------------------------------
# Lakehouse paths + connection caching
# -----------------------------------------------------------------------------

def _lakehouse_paths(name: str) -> Dict[str, Path]:
    """Compute lakehouse directories and files for a given instance name."""
    lakehouse_dir = (LAKEHOUSE_BASE_DIR / f"store_{name}").resolve()
    catalog_path = lakehouse_dir / "metadata.sqlite"
    data_dir = lakehouse_dir / "data"
    return {
        "lakehouse_dir": lakehouse_dir,
        "catalog_path": catalog_path,
        "data_dir": data_dir,
    }


def _ensure_sqlite_wal(catalog_path: Path) -> None:
    """Enable WAL mode + busy timeout for the DuckLake SQLite catalog."""
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with sqlite3.connect(str(catalog_path), timeout=10) as sl_con:
            sl_con.execute("PRAGMA journal_mode=WAL;")
            sl_con.execute("PRAGMA synchronous=NORMAL;")
            sl_con.execute("PRAGMA busy_timeout=10000;")  # ms
    except Exception as e:
        _log.warning("Could not set WAL mode on SQLite catalog '%s': %s", catalog_path, e)


def _load_or_install_extension(con: duckdb.DuckDBPyConnection, ext: str) -> None:
    """LOAD extension; if missing, INSTALL then LOAD."""
    try:
        con.execute(f"LOAD {ext};")
    except Exception:
        con.execute(f"INSTALL {ext};")
        con.execute(f"LOAD {ext};")


def _apply_duckdb_tuning(con: duckdb.DuckDBPyConnection) -> None:
    """Apply optional DuckDB PRAGMA settings from env vars."""
    if DUCKDB_THREADS:
        try:
            con.execute(f"PRAGMA threads={int(DUCKDB_THREADS)};")
        except Exception:
            pass

    if DUCKDB_MEMORY_LIMIT:
        try:
            con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}';")
        except Exception:
            pass

    if DUCKDB_TEMP_DIRECTORY:
        try:
            con.execute(f"PRAGMA temp_directory='{DUCKDB_TEMP_DIRECTORY}';")
        except Exception:
            pass


# Thread-local connection cache: safe for multi-threaded consumers and dashboards
_TLS = threading.local()


def _get_tls_cache() -> Dict[str, duckdb.DuckDBPyConnection]:
    """Return per-thread cache for DuckDB lakehouse connections."""
    cache = getattr(_TLS, "duckdb_conns", None)
    if cache is None:
        cache = {}
        _TLS.duckdb_conns = cache
    return cache


def close_lakehouse_connections() -> None:
    """Close all cached DuckDB connections for the current thread."""
    cache = _get_tls_cache()
    for k, con in list(cache.items()):
        try:
            con.close()
        except Exception:
            pass
        cache.pop(k, None)


def connect_lakehouse(name: str = "default") -> duckdb.DuckDBPyConnection:
    """Create (or reuse) a DuckDB connection to a specific Lakehouse instance.

    Uses DuckLake with a SQLite catalog and per-instance data directory.
    Connections are cached per-thread to avoid cross-thread reuse issues.

    Args:
        name: Name of the lakehouse instance (e.g., ``'stats'``, ``'ml'``).

    Returns:
        DuckDB connection with DuckLake attached as schema ``lake``
        and set as default schema.
    """
    name = str(name or "default")
    cache = _get_tls_cache()
    if name in cache:
        return cache[name]

    paths = _lakehouse_paths(name)
    lakehouse_dir: Path = paths["lakehouse_dir"]
    catalog_path: Path = paths["catalog_path"]
    data_dir: Path = paths["data_dir"]

    lakehouse_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    _ensure_sqlite_wal(catalog_path)

    con = duckdb.connect()
    _apply_duckdb_tuning(con)

    _load_or_install_extension(con, "sqlite")
    _load_or_install_extension(con, "ducklake")

    # Attach DuckLake (SQLite catalog) as schema 'lake'
    catalog_str = str(catalog_path).replace("'", "''")
    data_str = str(data_dir).replace("'", "''")

    con.execute(
        f"ATTACH 'ducklake:sqlite:{catalog_str}' AS lake (DATA_PATH '{data_str}/');"
    )
    con.execute("USE lake;")

    cache[name] = con
    return con
