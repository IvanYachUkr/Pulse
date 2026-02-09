"""Kafka consumer for aggregating query stream statistics into time windows.

Consumes Redshift query events from Kafka via QuixStreams, aggregates per instance_id
into event-time tumbling windows, classifies queries into behavioral buckets, and
writes the resulting per-window stats to a chosen storage backend:

- SQLite (WAL) for fast concurrent dashboard reads + optional retention/archival
- DuckDB/Parquet lakehouse for distributed storage
"""

import argparse
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from quixstreams import Application
from quixstreams.sinks import BatchingSink

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TABLE_STREAM_STATS,
    connect_lakehouse,
    setup_logging,
)

logger = setup_logging(__name__)


# ---------------------------------------------------------------------
# Logging noise control
# ---------------------------------------------------------------------

class SkipWindowWarningFilter(logging.Filter):
    """Rate-limits QuixStreams closed window warnings."""

    def __init__(self, show_every_n: int = 100) -> None:
        super().__init__()
        self.show_every_n = max(1, int(show_every_n))
        self._count = 0
        self._lock = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if "Skipping window processing for the closed window" not in msg:
            return True

        with self._lock:
            self._count += 1
            n = self._count

        # Show the first, then every Nth
        if n == 1 or (n % self.show_every_n) == 0:
            record.msg = f"[{n} total] Skipping closed window processing (showing every {self.show_every_n})"
            record.args = ()
            return True
        return False


logging.getLogger("quixstreams").addFilter(SkipWindowWarningFilter(show_every_n=10_000))


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _parse_iso_to_epoch_ms(ts: str) -> int:
    """Parse ISO timestamp string (possibly ending with 'Z') to epoch ms."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def _is_transient_storage_error(exc: Exception) -> bool:
    """Heuristic: decide whether an exception is worth retrying."""
    msg = str(exc).lower()
    transient_markers = (
        "locked",
        "busy",
        "timeout",
        "connection",
        "io error",
        "i/o error",
        "broken pipe",
        "temporar",
    )
    return any(m in msg for m in transient_markers)


def _with_retries(
    fn,
    *,
    max_retries: int = 5,
    base_delay_s: float = 0.1,
    on_retry=None,
) -> None:
    """Run fn() with retries + exponential backoff.

    on_retry(attempt_idx, exc) is called before sleeping (attempt_idx is 0-based).
    """
    delay = float(base_delay_s)
    for attempt in range(max_retries):
        try:
            fn()
            return
        except Exception as e:
            transient = _is_transient_storage_error(e)
            last = attempt >= (max_retries - 1)

            if transient and not last:
                if on_retry:
                    try:
                        on_retry(attempt, e)
                    except Exception:
                        pass
                time.sleep(delay)
                delay *= 2.0
                continue

            raise


# ---------------------------------------------------------------------
# Query Classification + cluster size cache
# ---------------------------------------------------------------------

# Cluster size cache for handling null values
_last_known_cluster_sizes: Dict[Any, int] = {}


def classify_query(row: Dict[str, Any]) -> str:
    """Classify a query based on behavioral regime analysis.

    Thresholds derived from 433M-row REDSET analysis. Each threshold has a
    documented behavioral transition point (see docs/QUERY_CLASSIFICATION.md).
    Evaluation order matters: first match wins.
    """
    q_type = (row.get("query_type") or "").lower()
    exec_ms = row.get("execution_duration_ms") or 0
    scanned = row.get("mbytes_scanned") or 0
    spilled = row.get("mbytes_spilled") or 0
    joins = row.get("num_joins") or 0
    aggs = row.get("num_aggregations") or 0
    scans = row.get("num_scans") or 0
    queue_ms = row.get("queue_duration_ms") or 0
    was_cached = row.get("was_cached") or False

    # ① Network-bound: COPY/UNLOAD with meaningful data transfer (>50 MB median)
    if q_type in ("copy", "unload") and exec_ms > 500:
        return "network_bound_queries"

    # Skip cached queries for resource-bottleneck classes
    if was_cached:
        return "normal_queries"

    # Avoid division by zero
    ratio = exec_ms / (scanned + 1) if scanned > 0 else 0

    # ② CPU-bound: complex, genuinely slow queries where compute dominates
    if (
        q_type not in ("copy", "unload")
        and (joins > 1 or aggs > 1)
        and scanned > 0
        and ratio > 20.0
        and exec_ms > 1000
    ):
        return "cpu_bound_queries"

    # ③ IO-bound: massive scans where disk throughput is the bottleneck
    if (
        q_type not in ("copy", "unload")
        and scanned >= 2000
        and ratio < 0.061
        and (spilled > 0 or scans > 1)
        and exec_ms > 1000
    ):
        return "io_bound_queries"

    # ④ Queue/WLM-bound: simple queries stuck waiting for cluster resources
    complexity = scans + joins + aggs
    if queue_ms >= exec_ms and queue_ms > 1000 and exec_ms > 0 and complexity < 4:
        return "queue_wlm_bound_queries"

    return "normal_queries"


def get_cluster_size(row: Dict[str, Any]) -> int:
    """Get cluster size, using last known value if current is null/NaN."""
    instance_id = row.get("instance_id")
    c_size = row.get("cluster_size")

    try:
        # NaN check: NaN != NaN
        if c_size is None or c_size != c_size:
            return _last_known_cluster_sizes.get(instance_id, -1)
        size_int = int(c_size)
    except (TypeError, ValueError):
        return _last_known_cluster_sizes.get(instance_id, -1)

    _last_known_cluster_sizes[instance_id] = size_int
    return size_int


# ---------------------------------------------------------------------
# Output contract for sinks
# ---------------------------------------------------------------------

STREAM_STATS_COLUMNS: Tuple[str, ...] = (
    "window_start",
    "instance_id",
    "cluster_size",
    "total_queries",
    "normal_queries",
    "cpu_bound_queries",
    "io_bound_queries",
    "network_bound_queries",
    "queue_wlm_bound_queries",
    "total_spilled_mb",
    "total_queue_time_ms",
    "total_exec_time_ms",
)

STREAM_STATS_PLACEHOLDERS = ", ".join(["?"] * len(STREAM_STATS_COLUMNS))


# ---------------------------------------------------------------------
# Storage Sinks
# ---------------------------------------------------------------------

class LakehouseStatsSink(BatchingSink):
    """DuckDB + Parquet lakehouse sink for distributed storage."""

    def __init__(self, batch_size: int = 500, quiet: bool = False) -> None:
        super().__init__()
        self.batch_size = int(batch_size)
        self.quiet = bool(quiet)

        self._con = None  # cached connection

        # Perf metrics
        self._write_count = 0
        self._total_rows = 0
        self._total_write_time_s = 0.0
        self._start_time_monotonic: Optional[float] = None

        self._ensure_table()

    def _get_connection(self):
        if self._con is None:
            self._con = connect_lakehouse("stats")
        return self._con

    def _close_connection(self) -> None:
        try:
            if self._con is not None:
                self._con.close()
        except Exception:
            pass
        finally:
            self._con = None

    def _reconnect(self):
        self._close_connection()
        return self._get_connection()

    def _ensure_table(self) -> None:
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_STREAM_STATS} (
                window_start TIMESTAMP,
                instance_id INTEGER,
                cluster_size INTEGER,
                total_queries INTEGER,
                normal_queries INTEGER,
                cpu_bound_queries INTEGER,
                io_bound_queries INTEGER,
                network_bound_queries INTEGER,
                queue_wlm_bound_queries INTEGER,
                total_spilled_mb DOUBLE,
                total_queue_time_ms DOUBLE,
                total_exec_time_ms DOUBLE
            )
        """.strip()

        try:
            con = self._get_connection()
            con.execute(ddl)
            if not self.quiet:
                logger.info("Lakehouse table '%s' initialized", TABLE_STREAM_STATS)
        except Exception:
            logger.error("Error initializing lakehouse", exc_info=True)
            raise

    def _batch_to_rows(self, batch) -> List[Tuple[Any, ...]]:
        rows: List[Tuple[Any, ...]] = []
        cols = STREAM_STATS_COLUMNS
        for item in batch:
            v = item.value
            rows.append(tuple(v.get(c) for c in cols))
        return rows

    def write(self, batch) -> None:
        if not batch:
            return

        t0 = time.monotonic()
        rows = self._batch_to_rows(batch)
        if not rows:
            return

        def _do_insert():
            con = self._get_connection()
            con.executemany(
                f"INSERT INTO {TABLE_STREAM_STATS} VALUES ({STREAM_STATS_PLACEHOLDERS})",
                rows,
            )

        def _on_retry(attempt: int, exc: Exception):
            logger.warning(
                "Lakehouse transient issue (attempt %d/%d), reconnecting...",
                attempt + 1, 5
            )
            try:
                self._reconnect()
            except Exception:
                pass

        try:
            _with_retries(_do_insert, max_retries=5, base_delay_s=0.1, on_retry=_on_retry)
        except Exception:
            logger.error("Error writing batch to Lakehouse", exc_info=True)
            raise

        dt = time.monotonic() - t0
        self._write_count += 1
        self._total_rows += len(rows)
        self._total_write_time_s += dt
        if self._start_time_monotonic is None:
            self._start_time_monotonic = time.monotonic()

        if not self.quiet:
            if self._write_count % 10 == 0:
                avg_ms = (self._total_write_time_s / self._write_count) * 1000.0
                elapsed = time.monotonic() - (self._start_time_monotonic or time.monotonic())
                rps = self._total_rows / elapsed if elapsed > 0 else 0.0
                logger.info("[PERF] writes=%d, rows=%d, %.0f rows/s, avg=%.1fms",
                            self._write_count, self._total_rows, rps, avg_ms)
            else:
                logger.info("Wrote %d rows (%.1fms)", len(rows), dt * 1000.0)


class SQLiteSink(BatchingSink):
    """SQLite sink with WAL mode for concurrent dashboard reads."""

    def __init__(self, db_path: str = "_data/stream_stats.sqlite", batch_size: int = 500, quiet: bool = False) -> None:
        super().__init__()
        self.db_path = db_path
        self.batch_size = int(batch_size)
        self.quiet = bool(quiet)

        self._con = None

        # Perf metrics
        self._write_count = 0
        self._total_rows = 0
        self._total_write_time_s = 0.0
        self._start_time_monotonic: Optional[float] = None

        # Retention state (optional)
        self._retention_days: Optional[int] = None
        self._archive_path: Optional[str] = None
        self._days_seen: Optional[set] = None

        self._init_db()

    def _init_db(self) -> None:
        import sqlite3
        from pathlib import Path

        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(self.db_path, check_same_thread=False)

        # WAL + pragmatic tuning for dashboard reads
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA synchronous=NORMAL")
        self._con.execute("PRAGMA cache_size=-64000")

        # Table + index
        self._con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_STREAM_STATS} (
                window_start TEXT,
                instance_id INTEGER,
                cluster_size INTEGER,
                total_queries INTEGER,
                normal_queries INTEGER,
                cpu_bound_queries INTEGER,
                io_bound_queries INTEGER,
                network_bound_queries INTEGER,
                queue_wlm_bound_queries INTEGER,
                total_spilled_mb REAL,
                total_queue_time_ms REAL,
                total_exec_time_ms REAL
            )
        """.strip())

        self._con.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_window_start
            ON {TABLE_STREAM_STATS}(window_start)
        """.strip())

        self._con.commit()
        if not self.quiet:
            logger.info("SQLite initialized: %s", self.db_path)

    def enable_retention(self, retention_days: int = 8, archive_path: str = "_data/archive") -> None:
        """Enable event-time based retention with archival to Parquet."""
        from pathlib import Path

        retention_days = int(retention_days)
        self._retention_days = retention_days
        self._archive_path = archive_path
        self._days_seen = set()

        Path(archive_path).mkdir(parents=True, exist_ok=True)
        logger.info("Retention enabled: keep %d days, archive to %s", retention_days, archive_path)

    def _rows_to_sqlite_values(self, batch) -> List[Tuple[Any, ...]]:
        values: List[Tuple[Any, ...]] = []
        cols = STREAM_STATS_COLUMNS

        for item in batch:
            rec = item.value
            ws = rec.get("window_start")
            if hasattr(ws, "isoformat"):
                ws = ws.isoformat()
            # Maintain column order
            row = tuple((ws if c == "window_start" else rec.get(c)) for c in cols)
            values.append(row)

        return values

    def write(self, batch) -> None:
        if not batch:
            return
        if self._con is None:
            raise RuntimeError("SQLite connection not initialized")

        t0 = time.monotonic()
        values = self._rows_to_sqlite_values(batch)
        if not values:
            return

        try:
            self._con.executemany(
                f"INSERT INTO {TABLE_STREAM_STATS} VALUES ({STREAM_STATS_PLACEHOLDERS})",
                values,
            )
            self._con.commit()
        except Exception:
            logger.error("Error writing to SQLite", exc_info=True)
            raise

        dt = time.monotonic() - t0
        self._write_count += 1
        self._total_rows += len(values)
        self._total_write_time_s += dt
        if self._start_time_monotonic is None:
            self._start_time_monotonic = time.monotonic()

        if not self.quiet:
            latest_window = values[-1][0] if values else "N/A"
            latest_str = str(latest_window)
            if isinstance(latest_window, str) and len(latest_str) > 16:
                latest_str = latest_str[:16].replace("T", " ")

            if self._write_count % 10 == 0:
                elapsed = time.monotonic() - (self._start_time_monotonic or time.monotonic())
                rps = self._total_rows / elapsed if elapsed > 0 else 0.0
                avg_ms = (self._total_write_time_s / self._write_count) * 1000.0
                logger.info("[PERF] rows=%d, %.0f rows/s, avg=%.1fms, at=%s",
                            self._total_rows, rps, avg_ms, latest_str)
            else:
                logger.info("Wrote %d rows (%.1fms)", len(values), dt * 1000.0)

        # Retention/archival check (event-time based)
        if self._retention_days is not None and self._days_seen is not None and values:
            self._check_and_archive(values[-1][0])

    def _check_and_archive(self, latest_window_start: str) -> None:
        if self._retention_days is None or self._archive_path is None or self._days_seen is None:
            return

        if not (isinstance(latest_window_start, str) and len(latest_window_start) >= 10):
            return

        current_date = latest_window_start[:10]
        self._days_seen.add(current_date)

        if len(self._days_seen) <= self._retention_days:
            return

        sorted_days = sorted(self._days_seen)
        cutoff_date = sorted_days[-self._retention_days]
        self._archive_before_date(cutoff_date)

        # Keep only retained days
        self._days_seen = set(sorted_days[-self._retention_days:])

    def _archive_before_date(self, cutoff_date: str) -> None:
        """Archive data before cutoff_date to Parquet, then delete from SQLite."""
        if self._con is None:
            return

        # Count rows to archive
        res = self._con.execute(
            f"SELECT COUNT(*) FROM {TABLE_STREAM_STATS} WHERE window_start < ?",
            [cutoff_date],
        ).fetchone()
        rows_to_archive = res[0] if res else 0
        if rows_to_archive == 0:
            return

        from pathlib import Path
        from datetime import datetime as dt

        try:
            import duckdb
            timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
            archive_file = Path(self._archive_path) / f"archive_before_{cutoff_date}_{timestamp}.parquet"

            duck_con = duckdb.connect()
            duck_con.execute("INSTALL sqlite; LOAD sqlite;")
            duck_con.execute(f"""
                COPY (
                    SELECT * FROM sqlite_scan('{self.db_path}', '{TABLE_STREAM_STATS}')
                    WHERE window_start < '{cutoff_date}'
                ) TO '{archive_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """.strip())
            duck_con.close()

            if not self.quiet:
                logger.info("Archived %d rows (before %s) to %s", rows_to_archive, cutoff_date, archive_file)

            self._con.execute(f"DELETE FROM {TABLE_STREAM_STATS} WHERE window_start < ?", [cutoff_date])
            self._con.commit()

        except ImportError:
            logger.warning("DuckDB not available, deleting without archiving")
            self._con.execute(f"DELETE FROM {TABLE_STREAM_STATS} WHERE window_start < ?", [cutoff_date])
            self._con.commit()


# ---------------------------------------------------------------------
# Window aggregation logic
# ---------------------------------------------------------------------

def _init_window_state(row: Dict[str, Any]) -> Dict[str, Any]:
    """Initialize window state from the first row."""
    q_class = classify_query(row)
    return {
        "instance_id": row.get("instance_id", "unknown"),
        "cluster_size": get_cluster_size(row),
        "total_queries": 1,
        "normal_queries": 1 if q_class == "normal_queries" else 0,
        "cpu_bound_queries": 1 if q_class == "cpu_bound_queries" else 0,
        "io_bound_queries": 1 if q_class == "io_bound_queries" else 0,
        "network_bound_queries": 1 if q_class == "network_bound_queries" else 0,
        "queue_wlm_bound_queries": 1 if q_class == "queue_wlm_bound_queries" else 0,
        "total_spilled_mb": row.get("mbytes_spilled") or 0,
        "total_queue_ms": row.get("queue_duration_ms") or 0,
        "total_exec_ms": row.get("execution_duration_ms") or 0,
    }


def _reduce_window_state(state: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    """Accumulate a new row into existing window state."""
    q_class = classify_query(row)
    state["total_queries"] += 1
    state[q_class] += 1
    state["cluster_size"] = get_cluster_size(row)
    state["total_spilled_mb"] += (row.get("mbytes_spilled") or 0)
    state["total_queue_ms"] += (row.get("queue_duration_ms") or 0)
    state["total_exec_ms"] += (row.get("execution_duration_ms") or 0)
    return state


def _finalize_window_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Convert closed window state into the sink output contract."""
    start_ms = result["start"]
    val = result["value"]

    window_start = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()
    return {
        "window_start": window_start,
        "instance_id": val["instance_id"],
        "cluster_size": val["cluster_size"],
        "total_queries": val["total_queries"],
        "normal_queries": val["normal_queries"],
        "cpu_bound_queries": val["cpu_bound_queries"],
        "io_bound_queries": val["io_bound_queries"],
        "network_bound_queries": val["network_bound_queries"],
        "queue_wlm_bound_queries": val["queue_wlm_bound_queries"],
        "total_spilled_mb": val["total_spilled_mb"],
        "total_queue_time_ms": val["total_queue_ms"],
        "total_exec_time_ms": val["total_exec_ms"],
    }


# ---------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------

def main() -> None:
    """Run the aggregate-consumer CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Kafka consumer with tumbling window aggregation and query classification"
    )
    parser.add_argument("--topic", default="redshift.query_events", help="Kafka topic to consume")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--group", default="redshift-stats-consumer-optimized", help="Consumer group ID")
    parser.add_argument(
        "--auto-offset-reset",
        default="latest",
        choices=["earliest", "latest"],
        help="Auto offset reset strategy",
    )
    parser.add_argument("--window-minutes", type=int, default=10, help="Tumbling window size in minutes")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress frequent output")
    parser.add_argument("--idle-timeout", type=float, default=0, help="Exit after N seconds of no messages (0=disabled)")
    parser.add_argument(
        "--backend",
        default="sqlite",
        choices=["sqlite", "lakehouse"],
        help="Storage backend: sqlite (fast reads) or lakehouse (distributed)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=8,
        help="Days to keep in SQLite (default: 8 = 7-day dashboard + 1 day buffer)",
    )
    args = parser.parse_args()

    app = Application(
        broker_address=args.bootstrap,
        consumer_group=args.group,
        auto_offset_reset=args.auto_offset_reset,
        auto_create_topics=True,
    )

    topic = app.topic(args.topic)
    sdf = app.dataframe(topic)

    sdf = sdf.set_timestamp(
        lambda row, key, ts, headers: _parse_iso_to_epoch_ms(row["arrival_timestamp"])
    )

    window = sdf.group_by(
        lambda row: str(row.get("instance_id", "unknown")),
        name="minute_stats",
    ).tumbling_window(
        duration_ms=timedelta(minutes=args.window_minutes)
    )

    window_agg = (
        window.reduce(reducer=_reduce_window_state, initializer=_init_window_state)
        .final()
        .apply(_finalize_window_result)
    )

    # Choose sink
    if args.backend == "sqlite":
        sink = SQLiteSink(db_path="_data/stream_stats.sqlite", quiet=args.quiet)
        backend_name = "SQLite WAL"
        if args.retention_days and args.retention_days > 0:
            sink.enable_retention(retention_days=args.retention_days)
    else:
        sink = LakehouseStatsSink(quiet=args.quiet)
        backend_name = "Lakehouse"

    window_agg.sink(sink)

    logger.info("Starting aggregate consumer")
    logger.info("Backend: %s", backend_name)
    logger.info("Topic: %s", args.topic)
    logger.info("Window: %d minute(s)", args.window_minutes)
    logger.info("Consumer group: %s", args.group)

    # Idle-timeout watchdog
    stop_event = threading.Event()
    last_activity = [0.0]  # monotonic timestamp of last sink write

    def request_exit() -> None:
        stop_event.set()
        stop_fn = getattr(app, "stop", None)
        if callable(stop_fn):
            try:
                stop_fn()
                return
            except Exception:
                pass
        os._exit(0)

    if args.idle_timeout and args.idle_timeout > 0:
        original_write = sink.write

        def tracked_write(batch):
            last_activity[0] = time.monotonic()
            return original_write(batch)

        sink.write = tracked_write  # type: ignore[assignment]

        def watchdog() -> None:
            while not stop_event.is_set():
                time.sleep(1)
                if last_activity[0] > 0 and (time.monotonic() - last_activity[0]) > args.idle_timeout:
                    logger.info("Idle timeout reached (%.1fs), exiting...", args.idle_timeout)
                    request_exit()

        threading.Thread(target=watchdog, daemon=True).start()

    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        stop_event.set()
        # Best-effort close of lakehouse connection
        try:
            if isinstance(sink, LakehouseStatsSink):
                sink._close_connection()
        except Exception:
            pass


if __name__ == "__main__":
    main()
