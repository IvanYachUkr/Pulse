"""Apache Arrow Feather writer for streaming query data to daily shard files."""
import os
import pyarrow as pa
import pyarrow.feather as feather
from typing import Optional, List, Any
from datetime import datetime, timezone


class DailyArrowWriter:
    """
    Shard-based writer: multiple files per day.
    Format: {table}_YYYY-MM-DD_{kind}_{timestamp}_{seq}.arrow
    Uses Apache Arrow Feather format with LZ4/ZSTD compression for fast writes.
    Buffers rows and writes in batches for speed.
    Skips (drops) cached/aborted rows.
    """

    def __init__(self, base_dir: str, table: str, batch_size: int = 1000, compression: str = 'lz4', consumer_id: int = 0):
        """
        Args:
            base_dir: Directory to write files to
            table: Table name for file naming
            batch_size: Number of rows to buffer before writing  
            compression: 'lz4' (faster, default) or 'zstd' (better compression)
            consumer_id: Unique consumer instance ID for multi-consumer shard isolation
        """
        self.base_dir = base_dir
        self.table = table
        self.batch_size = batch_size
        self.compression = compression
        self.consumer_id = consumer_id
        
        if compression not in ['lz4', 'zstd']:
            raise ValueError(f"Compression must be 'lz4' or 'zstd', got: {compression}")

        self.opened_day: Optional[str] = None
        self._buf: List[dict] = []
        self._shard_seq: int = 0

        # define fixed PyArrow schema to avoid schema mismatches
        # includes all columns required for feature engineering
        self.schema = pa.schema([
            ("instance_id", pa.int64()),
            ("user_id", pa.string()),
            ("database_id", pa.string()),
            ("query_id", pa.string()),
            ("query_type", pa.string()),
            ("arrival_timestamp", pa.timestamp("us")),
            ("cluster_size", pa.int64()),
            ("feature_fingerprint", pa.string()),
            ("read_table_ids", pa.list_(pa.string())),
            ("write_table_ids", pa.list_(pa.string())),
            ("num_permanent_tables_accessed", pa.int64()),
            ("num_external_tables_accessed", pa.int64()),
            ("num_system_tables_accessed", pa.int64()),
            ("num_joins", pa.int64()),
            ("num_scans", pa.int64()),
            ("num_aggregations", pa.int64()),
            ("compile_duration_ms", pa.int64()),
            ("execution_duration_ms", pa.int64()),
            ("was_aborted", pa.bool_()),
            ("was_cached", pa.bool_()),
        ])

    # ---------------------------------------------------------------------
    # Type Conversion Helpers
    # ---------------------------------------------------------------------

    def _as_str_list(self, val: Any) -> List[str]:
        """Convert value to list of strings."""
        if val is None:
            return []
        if isinstance(val, list):
            return [str(v) for v in val]
        return [str(val)]

    def _should_drop_row(self, row: dict) -> bool:
        """Check if row should be skipped (cached or aborted queries)."""
        return bool(row.get("was_cached")) or bool(row.get("was_aborted"))

    def _to_int(self, val: Any) -> Optional[int]:
        """Safely convert value to int, returning None on failure."""
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    def _to_bool(self, val: Any) -> Optional[bool]:
        """Safely convert value to bool."""
        if val is None:
            return None
        if isinstance(val, bool):
            return val
        return bool(val)

    def _parse_timestamp(self, ts_str: Any) -> Optional[datetime]:
        """Parse ISO-8601 timestamp string to datetime."""
        if ts_str is None:
            return None
        if isinstance(ts_str, datetime):
            return ts_str
        try:
            return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        except Exception:
            return None


    def _shard_path(self, day: str, shard_kind: str) -> str:
        """
        Shard filename format (multi-consumer safe):
          {table}_{YYYY-MM-DD}_{kind}_c{consumer_id}_{YYYYMMDDHHMMSS}_{seq:06d}.arrow

        The consumer_id (c0, c1, c2...) ensures unique filenames when multiple
        consumers write to the same directory.
        
        Matches TrainingManager's day prefix detection: "{table}_{day}_"
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        seq = self._shard_seq
        self._shard_seq += 1
        fname = f"{self.table}_{day}_{shard_kind}_c{self.consumer_id}_{ts}_{seq:06d}.arrow"
        return os.path.join(self.base_dir, fname)

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------

    def open_day(self, day: str):
        """Switch to a new day; flush pending rows from the previous day."""
        if self.opened_day == day:
            return

        if self.opened_day is not None:
            # Treat rollover flush like a commit (end of previous day)
            self.flush(shard_kind="commit")

        self.opened_day = day
        self._shard_seq = 0  # reset sequence per day (optional)

        
    def insert(self, row: dict) -> tuple[bool, bool]:
        """
        Returns:
        (inserted, flushed)
        """
        if self.opened_day is None:
            raise RuntimeError("No day is open. Call open_day(day) first.")

        if self._should_drop_row(row):
            return (False, False)

        self._buf.append(row)

        flushed = False
        if len(self._buf) >= self.batch_size:
            self.flush(shard_kind="batch")
            flushed = True

        return (True, flushed)

    def _normalize_batch(self, rows: List[dict]) -> pa.Table:
        """
        Vectorized batch normalization using pandas.
        Much faster than per-row normalization (runs in compiled C, not Python loops).
        """
        import pandas as pd
        import numpy as np
        
        df = pd.DataFrame(rows)
        
        # Vectorized type conversions
        int_cols = [
            "instance_id", "cluster_size",
            "num_permanent_tables_accessed", "num_external_tables_accessed", 
            "num_system_tables_accessed", "num_joins", "num_scans", 
            "num_aggregations", "compile_duration_ms", "execution_duration_ms"
        ]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        
        # String columns - ensure they are strings (data may have numeric ids)
        str_cols = ["user_id", "database_id", "query_id", "query_type", "feature_fingerprint"]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).replace("nan", None).replace("None", None)
            else:
                df[col] = None
        
        # Timestamp parsing (vectorized)
        if "arrival_timestamp" in df.columns:
            df["arrival_timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors="coerce", utc=True)
        
        # Boolean columns
        for col in ["was_aborted", "was_cached"]:
            if col in df.columns:
                df[col] = df[col].astype(bool)
            else:
                df[col] = False
        
        # List columns - convert to list of strings
        for col in ["read_table_ids", "write_table_ids"]:
            if col in df.columns:
                def to_str_list(val):
                    if val is None:
                        return []
                    if isinstance(val, list):
                        return [str(v) for v in val]
                    return [str(val)]
                df[col] = df[col].apply(to_str_list)
            else:
                df[col] = [[] for _ in range(len(df))]
        
        # Ensure all schema columns exist
        for field in self.schema:
            if field.name not in df.columns:
                df[field.name] = None
        
        # Reorder to match schema
        df = df[[field.name for field in self.schema]]
        
        # Convert to Arrow table with schema
        table = pa.Table.from_pandas(df, schema=self.schema, preserve_index=False)
        return table

    def flush(self, shard_kind: str = "batch"):
        """
        Write buffered rows to a new shard file (no append, no readback).
        Performs batch normalization for optimal performance.
        """
        if not self._buf or self.opened_day is None:
            return

        path = self._shard_path(self.opened_day, shard_kind)

        # batch normalize and convert to Arrow table
        table = self._normalize_batch(self._buf)

        # write shard
        feather.write_feather(table, path, compression=self.compression)

        # clear buffer
        self._buf.clear()

    def commit(self):
        """
        Flush buffered rows (used by hour boundary in consumer).
        Writes a shard with kind='commit' so it's easier to debug later.
        """
        self.flush(shard_kind="commit")

    def write_day_complete_marker(self, day: str) -> str:
        """
        Write a marker file indicating this consumer has finished processing the given day.
        
        Marker format: {table}_{day}_complete_c{consumer_id}.marker
        
        TrainingManager can check for these markers before starting training to ensure
        all consumers have finished writing their shards for the day.
        
        Returns: Path to the marker file
        """
        marker_name = f"{self.table}_{day}_complete_c{self.consumer_id}.marker"
        marker_path = os.path.join(self.base_dir, marker_name)
        
        # Write timestamp to marker for debugging
        with open(marker_path, 'w', encoding='utf-8') as f:
            f.write(datetime.now(timezone.utc).isoformat() + "\n")
        
        return marker_path

    def close(self, write_marker: bool = True):
        """
        Flush remaining rows and close current day.
        
        Args:
            write_marker: If True, writes a day-complete marker file for multi-consumer coordination
        """
        if self.opened_day is not None:
            self.flush(shard_kind="commit")
            if write_marker:
                self.write_day_complete_marker(self.opened_day)
        self.opened_day = None
        self._buf.clear()