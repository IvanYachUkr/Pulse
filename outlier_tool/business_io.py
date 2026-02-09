"""I/O utilities for parquet data, DuckDB operations, and pipeline state management."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd


# -----------------------------
# State (lightweight; pipeline keeps it for future extensions)
# -----------------------------

@dataclass
class PipelineState:
    """Lightweight state container for pipeline metadata."""
    last_updated_at: str = ""
    notes: str = ""


def load_state(path: Path) -> PipelineState:
    """Load pipeline state from JSON file, returning empty state if missing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return PipelineState()
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return PipelineState(**{k: obj.get(k, "") for k in ["last_updated_at", "notes"]})
    except Exception:
        return PipelineState()


def save_state(path: Path, state: PipelineState) -> None:
    """Save pipeline state to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_updated_at": state.last_updated_at,
        "notes": state.notes,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# -----------------------------
# DuckDB helpers
# -----------------------------

def duckdb_connect_mem() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection for read_parquet querying."""
    return duckdb.connect(database=":memory:")


def normalize_path_for_duckdb(path: str) -> str:
    """Convert Windows backslashes to forward slashes for DuckDB SQL."""
    return path.replace("\\", "/")


def parquet_schema_columns(con: duckdb.DuckDBPyConnection, parquet_path: str) -> List[str]:
    """Get column names from a parquet file without loading data."""
    parquet_path = normalize_path_for_duckdb(parquet_path)
    df0 = con.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 0").df()
    return list(df0.columns)


def list_days_in_parquet(con: duckdb.DuckDBPyConnection, parquet_path: str) -> List[date]:
    """List all distinct dates present in a parquet file based on arrival_timestamp."""
    parquet_path = normalize_path_for_duckdb(parquet_path)
    sql = f"""
        SELECT DISTINCT CAST(arrival_timestamp AS DATE) AS day
        FROM read_parquet('{parquet_path}')
        WHERE arrival_timestamp IS NOT NULL
        ORDER BY day
    """
    rows = con.execute(sql).fetchall()
    out: List[date] = []
    for (d,) in rows:
        if d is None:
            continue
        # Duckdb may return datetime.date or string depending on version
        if isinstance(d, date) and not isinstance(d, datetime):
            out.append(d)
        else:
            out.append(pd.to_datetime(d).date())
    return out


def _select_cols_sql(cols: Optional[List[str]]) -> str:
    """Build SQL column list, or '*' if no columns specified."""
    if not cols:
        return "*"
    return ", ".join([f'"{c}"' for c in cols])


def _coerce_arrival_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Convert arrival_timestamp to datetime, handling various input formats."""
    if "arrival_timestamp" in df.columns:
        df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors="coerce")
    return df


def read_day_from_parquet(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    day: date,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Read a single day's data from a parquet file."""
    parquet_path = normalize_path_for_duckdb(parquet_path)
    start = datetime(day.year, day.month, day.day)
    end = start + timedelta(days=1)

    cols_sql = _select_cols_sql(columns)
    sql = f"""
        SELECT {cols_sql}
        FROM read_parquet('{parquet_path}')
        WHERE arrival_timestamp >= TIMESTAMP '{start.isoformat(sep=" ", timespec="seconds")}'
          AND arrival_timestamp <  TIMESTAMP '{end.isoformat(sep=" ", timespec="seconds")}'
    """
    df = con.execute(sql).df()
    return _coerce_arrival_timestamp(df)


def read_daily_file_parquet(
    con: duckdb.DuckDBPyConnection,
    parquet_file: Path,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Read an entire parquet file from a daily partition."""
    p = normalize_path_for_duckdb(str(parquet_file))
    cols_sql = _select_cols_sql(columns)
    df = con.execute(f"SELECT {cols_sql} FROM read_parquet('{p}')").df()
    return _coerce_arrival_timestamp(df)


# -----------------------------
# Daily file discovery (follow mode)
# -----------------------------

_DAY_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def infer_day_from_filename(path: Path) -> Optional[date]:
    """Extract date from filename matching YYYY-MM-DD pattern."""
    m = _DAY_RE.search(path.name)
    if not m:
        return None
    try:
        return datetime.fromisoformat(m.group(1)).date()
    except Exception:
        return None


def list_daily_files(daily_dir: Path, pattern: str = "*.parquet") -> List[Tuple[date, Path]]:
    """Discover parquet files with dates in their names, sorted chronologically."""
    out: List[Tuple[date, Path]] = []
    for p in sorted(daily_dir.glob(pattern)):
        d = infer_day_from_filename(p)
        if d is not None:
            out.append((d, p))
    out.sort(key=lambda x: x[0])
    return out


# -----------------------------
# Output DB schema + append with schema alignment
# -----------------------------

def ensure_tables(out_con: duckdb.DuckDBPyConnection) -> None:
    """Create required output tables if they don't exist."""
    out_con.execute("CREATE TABLE IF NOT EXISTS run_metadata (k VARCHAR, v VARCHAR)")
    out_con.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_days(
            day DATE,
            stage VARCHAR,
            rows_scored BIGINT,
            rows_anoms BIGINT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # The wide tables (anomalies/controls/summaries) are created lazily by create_or_append_table,
    # and will auto-evolve (ALTER TABLE ADD COLUMN) when new columns appear.


def was_day_processed(out_con: duckdb.DuckDBPyConnection, day: date, stage: str) -> bool:
    """Check if a day/stage combination has already been processed."""
    q = "SELECT COUNT(*) FROM processed_days WHERE day = ? AND stage = ?"
    n = out_con.execute(q, [day.isoformat(), stage]).fetchone()[0]
    return int(n) > 0


def mark_day_processed(
    out_con: duckdb.DuckDBPyConnection,
    day: date,
    stage: str,
    rows_scored: int,
    rows_anoms: int,
) -> None:
    """Record that a day/stage has been processed with row counts."""
    out_con.execute(
        """
        INSERT INTO processed_days(day, stage, rows_scored, rows_anoms)
        VALUES (?, ?, ?, ?)
        """,
        [day.isoformat(), stage, int(rows_scored), int(rows_anoms)],
    )


def _duckdb_type_for_series(s: pd.Series) -> str:
    """Map pandas dtype to DuckDB column type."""
    dt = s.dtype
    if pd.api.types.is_bool_dtype(dt):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dt):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dt):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(dt):
        return "TIMESTAMP"
    if pd.api.types.is_datetime64tz_dtype(dt):
        return "TIMESTAMP"
    if pd.api.types.is_object_dtype(dt) or pd.api.types.is_string_dtype(dt) or isinstance(dt, pd.CategoricalDtype):
        return "VARCHAR"
    return "VARCHAR"  # Fallback


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
    return con.execute(q, [table]).fetchone()[0] > 0


def _table_columns(con: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [row[1] for row in info]


def create_or_append_table(out_con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    """
    Append df into table, auto-creating and auto-evolving schema:
    - If table doesn't exist: CREATE TABLE AS SELECT *.
    - If exists:
        * Add new columns to table (ALTER TABLE ADD COLUMN)
        * Add missing columns to df as NULL
        * Insert with explicit column list (no SELECT *)
    This prevents your "63 columns but 68 values" BinderException.
    """
    if df is None or len(df) == 0:
        return

    # make a copy so we can add columns safely
    df_local = df.copy()

    # DuckDB hates some numpy scalar objects; make strings explicit where helpful
    for c in df_local.columns:
        if pd.api.types.is_object_dtype(df_local[c].dtype):
            # leave as-is, but ensure Python objects not weird numpy scalars
            df_local[c] = df_local[c].astype(object)

    out_con.register("_tmp_df", df_local)

    try:
        if not _table_exists(out_con, table):
            out_con.execute(f"CREATE TABLE {table} AS SELECT * FROM _tmp_df")
            return

        tbl_cols = _table_columns(out_con, table)
        df_cols = list(df_local.columns)

        tbl_set = set(tbl_cols)
        df_set = set(df_cols)

        # add new columns to table
        new_in_df = [c for c in df_cols if c not in tbl_set]
        for c in new_in_df:
            col_type = _duckdb_type_for_series(df_local[c])
            out_con.execute(f'ALTER TABLE {table} ADD COLUMN "{c}" {col_type}')
            tbl_cols.append(c)
            tbl_set.add(c)

        # add missing columns to df as NULL
        missing_in_df = [c for c in tbl_cols if c not in df_set]
        for c in missing_in_df:
            df_local[c] = None

        # reorder df to match table column order
        df_local = df_local[tbl_cols]
        out_con.unregister("_tmp_df")
        out_con.register("_tmp_df", df_local)

        cols_sql = ", ".join([f'"{c}"' for c in tbl_cols])
        out_con.execute(f"INSERT INTO {table} ({cols_sql}) SELECT {cols_sql} FROM _tmp_df")

    finally:
        try:
            out_con.unregister("_tmp_df")
        except Exception:
            pass
