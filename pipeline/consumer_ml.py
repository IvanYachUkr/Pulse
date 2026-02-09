"""Kafka consumer for ML anomaly records, writing to Lakehouse.

Consumes anomaly records from Kafka, aggregates them into event-time
tumbling windows, adds ``window_start`` to each record, and persists
them to the DuckDB/Parquet lakehouse.
"""

import argparse
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from quixstreams import Application
from quixstreams.sinks import BatchingSink

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TABLE_ML_ANOMALIES,
    connect_lakehouse,
    setup_logging,
)

logger = setup_logging(__name__)


# ---------------------------------------------------------------------
# Logging noise control
# ---------------------------------------------------------------------

class SkipWindowWarningFilter(logging.Filter):
    """Rate-limits QuixStreams closed window warnings.

    QuixStreams can log repeated messages like:
    "Skipping window processing for the closed window ..."

    This filter allows one message every N occurrences to prevent log spam.
    """

    def __init__(self, show_every_n: int = 100) -> None:
        super().__init__()
        self.show_every_n = max(1, int(show_every_n))
        self._skip_count = 0
        self._lock = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if "Skipping window processing for the closed window" not in msg:
            return True

        with self._lock:
            self._skip_count += 1
            n = self._skip_count

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
    # QuixStreams expects event-time in milliseconds.
    # Example: "2025-01-01T12:34:56.123Z"
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def _is_transient_lakehouse_error(exc: Exception) -> bool:
    """Heuristic to decide whether an exception is worth retrying."""
    msg = str(exc).lower()
    # DuckDB/SQLite-ish "locked", plus common connection-ish failures.
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


# ---------------------------------------------------------------------
# Storage Sink
# ---------------------------------------------------------------------

class LakehouseAnomalySink(BatchingSink):
    """DuckDB + Parquet lakehouse sink for ML anomaly records.

    Expects each incoming item.value to be a list[dict] (records for the closed window).
    """

    # Keep column order explicit and centralized.
    _COLUMNS: Tuple[str, ...] = (
        "window_start",
        "instance_id",
        "user_id",
        "database_id",
        "query_id",
        "true_ms",
        "pred_ms",
        "pred_mean_ms",
        "pred_q_ms",
        "abs_err_ms",
        "signed_err_ms",
        "under_ratio",
        "anomaly_reason",
        "query_type",
        "arrival_timestamp",
        "cluster_size",
    )

    def __init__(self, batch_size: int = 100, quiet: bool = False) -> None:
        super().__init__()
        self.batch_size = int(batch_size)
        self.quiet = bool(quiet)

        self._con = None  # Cached DuckDB connection
        self._write_count = 0
        self._total_rows = 0
        self._total_write_time_s = 0.0
        self._start_time_monotonic: Optional[float] = None

        self._ensure_table()

    def _get_connection(self):
        """Get (or create) cached lakehouse connection."""
        if self._con is None:
            self._con = connect_lakehouse("ml")
        return self._con

    def _close_connection(self) -> None:
        """Close cached connection safely."""
        try:
            if self._con is not None:
                self._con.close()
        except Exception:
            pass
        finally:
            self._con = None

    def _reconnect(self):
        """Force a reconnect."""
        self._close_connection()
        return self._get_connection()

    def _ensure_table(self) -> None:
        """Create table schema if not exists."""
        # Note: the schema matches the ML pipeline output contract.
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_ML_ANOMALIES} (
                window_start TIMESTAMP,
                instance_id INTEGER,
                user_id INTEGER,
                database_id INTEGER,
                query_id INTEGER,
                true_ms DOUBLE,
                pred_ms DOUBLE,
                pred_mean_ms DOUBLE,
                pred_q_ms DOUBLE,
                abs_err_ms DOUBLE,
                signed_err_ms DOUBLE,
                under_ratio DOUBLE,
                anomaly_reason VARCHAR,
                query_type VARCHAR,
                arrival_timestamp TIMESTAMP,
                cluster_size INTEGER
            )
        """.strip()

        try:
            con = self._get_connection()
            con.execute(ddl)
            if not self.quiet:
                logger.info("Lakehouse table '%s' initialized", TABLE_ML_ANOMALIES)
        except Exception:
            logger.error("Error initializing lakehouse table", exc_info=True)
            raise

    def _flatten_batch(self, batch) -> List[Dict[str, Any]]:
        """Flatten a QuixStreams batch into a list of record dicts."""
        records: List[Dict[str, Any]] = []
        for item in batch:
            # Each item.value is expected to be a list of dict records.
            window_records = item.value
            if not window_records:
                continue
            # Be defensive: accept iterables of dicts
            for r in window_records:
                if isinstance(r, dict):
                    records.append(r)
        return records

    def _records_to_rows(self, records: Sequence[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
        """Convert record dicts to row tuples in sink column order."""
        cols = self._COLUMNS
        return [tuple(rec.get(c) for c in cols) for rec in records]

    def _insert_rows(self, rows: Sequence[Tuple[Any, ...]]) -> None:
        """Insert rows into lakehouse."""
        if not rows:
            return
        con = self._get_connection()
        placeholders = ", ".join(["?"] * len(self._COLUMNS))
        con.executemany(
            f"INSERT INTO {TABLE_ML_ANOMALIES} VALUES ({placeholders})",
            rows,
        )

    def _with_retries(self, fn, *, max_retries: int = 5, base_delay_s: float = 0.1) -> None:
        """Run fn() with retries/backoff and reconnect on transient failures."""
        delay = float(base_delay_s)
        for attempt in range(max_retries):
            try:
                fn()
                return
            except Exception as e:
                is_transient = _is_transient_lakehouse_error(e)
                is_last = attempt >= (max_retries - 1)

                if is_transient and not is_last:
                    logger.warning(
                        "Lakehouse transient issue (attempt %d/%d), reconnecting and retrying...",
                        attempt + 1, max_retries,
                    )
                    try:
                        self._reconnect()
                    except Exception:
                        # Even reconnect might fail. We'll still sleep/backoff then try again.
                        pass
                    time.sleep(delay)
                    delay *= 2.0
                    continue

                logger.error("Lakehouse write failed", exc_info=True)
                raise

    def write(self, batch) -> None:
        """Write batch of anomaly records with retry/backoff + perf tracking."""
        if not batch:
            return

        t0 = time.monotonic()

        records = self._flatten_batch(batch)
        if not records:
            return

        rows = self._records_to_rows(records)

        def _do_insert():
            self._insert_rows(rows)

        self._with_retries(_do_insert)

        write_time_s = time.monotonic() - t0
        self._write_count += 1
        self._total_rows += len(rows)
        self._total_write_time_s += write_time_s
        if self._start_time_monotonic is None:
            self._start_time_monotonic = time.monotonic()

        if not self.quiet:
            latest = rows[-1][0] if rows else "N/A"  # window_start
            if hasattr(latest, "strftime"):
                latest_str = latest.strftime("%Y-%m-%d %H:%M")
            else:
                latest_str = str(latest)
                if len(latest_str) > 16:
                    latest_str = latest_str[:16].replace("T", " ")

            # Periodic perf summary
            if self._write_count % 10 == 0:
                elapsed = time.monotonic() - (self._start_time_monotonic or time.monotonic())
                rows_per_sec = self._total_rows / elapsed if elapsed > 0 else 0.0
                avg_ms = (self._total_write_time_s / self._write_count) * 1000.0
                logger.info(
                    "[PERF] writes=%d, rows=%d, %.0f rows/s, avg=%.1fms, at=%s",
                    self._write_count, self._total_rows, rows_per_sec, avg_ms, latest_str
                )
            else:
                logger.info(
                    "Wrote %d anomaly rows (%.1fms) at=%s",
                    len(rows), write_time_s * 1000.0, latest_str
                )


# ---------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------

def main() -> None:
    """Run the ML anomaly consumer with tumbling window aggregation."""
    parser = argparse.ArgumentParser(
        description="ML Anomaly Consumer with Tumbling Window (Lakehouse sink)"
    )
    parser.add_argument("--topic", default="anomalous_queries", help="Kafka topic to consume")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--group", default="anomalous-lakehouse-sink", help="Consumer group ID")
    parser.add_argument(
        "--auto-offset-reset",
        default="latest",
        choices=["earliest", "latest"],
        help="Auto offset reset strategy",
    )
    parser.add_argument("--window-minutes", type=int, default=10, help="Tumbling window size in minutes")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress frequent output for high-speed runs")
    parser.add_argument("--idle-timeout", type=float, default=0, help="Exit after N seconds of no messages (0 = disabled)")
    args = parser.parse_args()

    app = Application(
        broker_address=args.bootstrap,
        consumer_group=args.group,
        auto_offset_reset=args.auto_offset_reset,
        auto_create_topics=True,
    )

    topic = app.topic(args.topic)
    sdf = app.dataframe(topic)

    # Event-time based windowing using arrival_timestamp
    sdf = sdf.set_timestamp(
        lambda row, key, ts, headers: _parse_iso_to_epoch_ms(row["arrival_timestamp"])
    )

    window = sdf.group_by(
        lambda row: str(row.get("instance_id", "unknown")),
        name="anomaly_windows",
    ).tumbling_window(
        duration_ms=timedelta(minutes=args.window_minutes)
    )

    # Efficiently collect records in window
    def init_state(value: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [value]

    def reduce_state(state: List[Dict[str, Any]], value: Dict[str, Any]) -> List[Dict[str, Any]]:
        state.append(value)
        return state

    window_agg = window.reduce(
        reducer=reduce_state,
        initializer=init_state,
    ).final()

    def finalize_window_result(result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Add window_start timestamp to each anomaly record (copying dicts)."""
        start_ms = result["start"]
        records: List[Dict[str, Any]] = result["value"] or []
        window_start = datetime.fromtimestamp(start_ms / 1000).isoformat()

        out: List[Dict[str, Any]] = []
        for rec in records:
            rec_copy = rec.copy()
            rec_copy["window_start"] = window_start
            out.append(rec_copy)
        return out

    window_agg = window_agg.apply(finalize_window_result)

    db_sink = LakehouseAnomalySink(quiet=args.quiet)
    window_agg.sink(db_sink)

    logger.info("Starting ML Lakehouse consumer")
    logger.info("Topic: %s", args.topic)
    logger.info("Window: %d minute(s)", args.window_minutes)
    logger.info("Lakehouse: ml")
    logger.info("Consumer group: %s", args.group)

    # -----------------------------------------------------------------
    # Idle-timeout watchdog
    # -----------------------------------------------------------------
    stop_event = threading.Event()
    last_activity = [0.0]  # 0.0 means no sink write yet

    def request_exit() -> None:
        """Try to stop cleanly, fall back to hard-exit if needed."""
        stop_event.set()
        # Prefer a graceful stop if the API exists.
        stop_fn = getattr(app, "stop", None)
        if callable(stop_fn):
            try:
                stop_fn()
                return
            except Exception:
                pass
        # If QuixStreams blocks and stop isn't supported, hard exit.
        os._exit(0)

    if args.idle_timeout and args.idle_timeout > 0:
        original_write = db_sink.write

        def tracked_write(batch):
            last_activity[0] = time.monotonic()
            return original_write(batch)

        db_sink.write = tracked_write  # type: ignore[assignment]

        def watchdog() -> None:
            while not stop_event.is_set():
                time.sleep(1)
                # Only trigger timeout if we've received at least one write
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
        # Close sink connection if it exists
        try:
            db_sink._close_connection()  # internal, but safe and intentional here
        except Exception:
            pass


if __name__ == "__main__":
    main()