"""
Pulse Dashboard — Database Readers

Provides two interchangeable readers (Lakehouse via DuckDB-Parquet, SQLite via
DuckDB sqlite_scanner) behind a common abstract interface.  A factory function
``create_stream_reader`` auto-detects which backend to use.
"""

import logging
import re
import sys
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional

import duckdb
import pandas as pd

# Add pipeline directory to path so we can import config
sys.path.append(str(Path(__file__).parent.parent / "pipeline"))
from config import connect_lakehouse

logger = logging.getLogger(__name__)


# ── Abstract interface ───────────────────────────────────────


class BaseReader(ABC):
    """Minimal contract every reader must satisfy."""

    tablename: str

    @abstractmethod
    def query(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> pd.DataFrame:
        """Execute *sql* with optional positional *params* and return a DataFrame."""


# ── Lakehouse reader (DuckDB over Parquet) ───────────────────


class ParquetReader(BaseReader):
    """Reader for Lakehouse tables using DuckDB over Parquet."""

    def __init__(self, tablename: str, lakehouse_name: str = "default"):
        self.conn = connect_lakehouse(lakehouse_name)
        self.tablename = tablename
        self._lock = threading.Lock()

    def query(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> pd.DataFrame:
        if params is None:
            params = []
        with self._lock:
            return self.conn.execute(sql, params).df()


# ── SQLite reader (DuckDB sqlite_scanner) ────────────────────


class SQLiteReader(BaseReader):
    """Reader that queries a SQLite file through DuckDB's sqlite_scanner."""

    def __init__(self, tablename: str, db_path: str):
        self.db_path = db_path
        self.tablename = tablename

        self.conn = duckdb.connect(":memory:")
        self.conn.execute("INSTALL sqlite; LOAD sqlite;")
        self._lock = threading.Lock()

    # -- internal helpers --------------------------------------------------

    def _table_ref(self) -> str:
        """Return the ``sqlite_scan(...)`` expression for this table."""
        return f"sqlite_scan('{self.db_path}', '{self.tablename}')"

    # -- public API --------------------------------------------------------

    def query(
        self, sql: str, params: Optional[List[Any]] = None
    ) -> pd.DataFrame:
        if params is None:
            params = []

        # SQLite stores timestamps as TEXT — convert datetime params to ISO
        converted = []
        for p in params:
            if hasattr(p, "isoformat"):  # datetime / pandas Timestamp
                converted.append(p.isoformat())
            else:
                converted.append(p)

        # Swap bare table name for the sqlite_scan(...) call (word-boundary safe)
        qualified_sql = re.sub(
            rf"\b{re.escape(self.tablename)}\b", self._table_ref(), sql
        )
        with self._lock:
            return self.conn.execute(qualified_sql, converted).df()


# ── Factory ──────────────────────────────────────────────────


def create_stream_reader(tablename: str = "minute_stats") -> BaseReader:
    """Create the appropriate reader for stream stats.

    Resolution order (controlled by ``DASHBOARD_BACKEND`` env-var):
      ``sqlite``     → SQLiteReader (fail if file missing)
      ``lakehouse``  → ParquetReader
      ``auto`` / unset → try SQLite first, fall back to Lakehouse
    """
    import os

    backend = os.environ.get("DASHBOARD_BACKEND", "auto").lower()
    sqlite_path = Path(__file__).parent.parent / "_data" / "stream_stats.sqlite"

    if backend == "sqlite":
        if not sqlite_path.exists():
            raise FileNotFoundError(f"SQLite file not found: {sqlite_path}")
        return SQLiteReader(tablename, str(sqlite_path))

    if backend == "lakehouse":
        return ParquetReader(tablename, "stats")

    # Auto-detect
    if sqlite_path.exists():
        try:
            reader = SQLiteReader(tablename, str(sqlite_path))
            reader.conn.execute(
                f"SELECT 1 FROM sqlite_scan('{sqlite_path}', '{tablename}') LIMIT 1"
            )
            return reader
        except Exception:
            logger.debug(
                "SQLite table '%s' not found in %s — falling back to Lakehouse",
                tablename,
                sqlite_path,
            )

    return ParquetReader(tablename, "stats")


# Backward-compatible alias
DBReader = ParquetReader
