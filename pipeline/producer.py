"""Kafka producer for Redshift query events.

Streams query events from a Parquet file to Kafka with configurable
speedup and resume state.
"""

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from quixstreams import Application

from config import KAFKA_BOOTSTRAP_SERVERS, setup_logging

logger = setup_logging(__name__)

# Add project root + outlier_tool to path
_project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_project_root / "outlier_tool"))
sys.path.insert(0, str(_project_root))


# ---------------------------------------------------------------------
# State handling (resume)
# ---------------------------------------------------------------------

DEFAULT_STATE_FILE = "producer_state.json"


def _load_state(state_file: Path) -> int:
    """Load resume state (processed row offset)."""
    if not state_file.exists():
        return 0
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        return int(data.get("sent_count", 0))
    except Exception:
        logger.error("Failed to load state from %s", state_file, exc_info=True)
        return 0


def _save_state_atomic(state_file: Path, count: int) -> None:
    """Save resume state atomically to avoid corruption on interruption."""
    try:
        tmp = state_file.with_suffix(state_file.suffix + ".tmp")
        tmp.write_text(json.dumps({"sent_count": int(count)}), encoding="utf-8")
        tmp.replace(state_file)
    except Exception:
        logger.error("Failed to save state to %s", state_file, exc_info=True)


# ---------------------------------------------------------------------
# Timestamp parsing + scheduling
# ---------------------------------------------------------------------

def _to_epoch_seconds(ts: Any) -> float:
    """Convert a timestamp value to epoch seconds.

    Supports datetime objects (naive assumed UTC), ISO 8601 strings,
    PyArrow scalar types, and numeric values.
    """
    if ts is None:
        raise ValueError("Timestamp is None")

    # PyArrow scalar -> Python
    if isinstance(ts, pa.Scalar):
        ts = ts.as_py()

    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.timestamp()

    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()

    # numeric fallback
    return float(ts)


@dataclass
class SpeedupClock:
    """Maps data timestamps to wall clock time with a speedup factor."""
    speedup: float
    t0_data: Optional[float] = None
    t0_wall: Optional[float] = None

    def sleep_until(self, data_ts_epoch_s: float) -> None:
        """Sleep until the wall-clock time corresponding to the data timestamp."""
        if self.speedup <= 0:
            return

        if self.t0_data is None:
            self.t0_data = data_ts_epoch_s
            self.t0_wall = time.monotonic()
            return

        assert self.t0_wall is not None
        target_wall = self.t0_wall + (data_ts_epoch_s - self.t0_data) / self.speedup
        now = time.monotonic()
        delay = target_wall - now
        if delay > 0:
            time.sleep(delay)



def _serialize_row(row: Dict[str, Any]) -> bytes:
    """Serialize row to JSON bytes (treats naive datetimes as UTC)."""
    return orjson.dumps(row, option=orjson.OPT_NAIVE_UTC)


def _producer_poll(producer, timeout_s: float = 0.0) -> None:
    """Best-effort poll for producer delivery callbacks."""
    poll = getattr(producer, "poll", None)
    if callable(poll):
        try:
            poll(timeout_s)
        except Exception:
            pass


def _producer_flush(producer, timeout_s: float = 10.0) -> None:
    """Best-effort flush of buffered messages."""
    flush = getattr(producer, "flush", None)
    if callable(flush):
        try:
            flush(timeout_s)
        except Exception:
            pass


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Kafka Producer for Redshift Query Events")
    parser.add_argument("--topic", default="redshift.query_events")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--data-file", default="_data/input/sorted_4days.parquet", help="Parquet file to stream")
    parser.add_argument("--speedup", type=float, default=60.0, help="Speedup factor (default 60x: 1 hour = 1 minute)")
    parser.add_argument("--limit", type=int, default=0, help="Limit messages (0 = no limit)")
    parser.add_argument("--batch-size", type=int, default=100000, help="Batch size for reading parquet file")
    parser.add_argument("--key-col", default="cluster_id", help="Column to use as Kafka key")
    parser.add_argument("--reset", action="store_true", help="Reset state and start from beginning")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE, help="Resume state file path")
    args = parser.parse_args()

    state_file = Path(args.state_file)

    # Reset state if requested
    if args.reset and state_file.exists():
        state_file.unlink(missing_ok=True)
        logger.info("State reset: %s", state_file)

    start_offset = _load_state(state_file)

    # Validate data file
    data_path = Path(args.data_file)
    if not data_path.is_file():
        logger.error("Data file not found: %s", data_path)
        return 1

    parquet_file = pq.ParquetFile(str(data_path))
    total_rows = parquet_file.metadata.num_rows
    total_to_send = total_rows
    if args.limit and args.limit > 0:
        total_to_send = min(total_rows, args.limit)

    logger.info("Loading data from %s", data_path)
    logger.info("Total rows in parquet: %d", total_rows)
    logger.info("Total messages to send: %d", total_to_send)
    if start_offset > 0:
        logger.info("Resuming from row offset %d", start_offset)
    logger.info("Speedup: %.1fx (1 hour -> %.1fs)", args.speedup, 3600.0 / args.speedup)

    # Kafka app + producer
    app = Application(broker_address=args.bootstrap, auto_create_topics=True)
    topic = app.topic(args.topic)

    clock = SpeedupClock(speedup=args.speedup)

    sent = 0
    processed = 0  # absolute row index in the parquet stream
    last_report = time.monotonic()
    last_save = time.monotonic()

    report_every_s = 5.0
    save_every_s = 10.0
    poll_every_n = 5000

    try:
        with app.get_producer() as producer:
            for batch in parquet_file.iter_batches(batch_size=args.batch_size):
                batch_rows = batch.num_rows

                if args.limit and args.limit > 0 and sent >= args.limit:
                    break

                # Skip whole batch if entirely before start_offset
                if processed + batch_rows <= start_offset:
                    processed += batch_rows
                    continue

                rows = batch.to_pylist()

                for row in rows:
                    if args.limit and args.limit > 0 and sent >= args.limit:
                        break

                    # Skip individual rows to align to resume offset
                    if processed < start_offset:
                        processed += 1
                        continue

                    # Speedup scheduling
                    ts_val = row.get("arrival_timestamp")
                    if ts_val is not None:
                        try:
                            data_ts = _to_epoch_seconds(ts_val)
                            clock.sleep_until(data_ts)
                        except KeyboardInterrupt:
                            raise
                        except Exception:
                            pass

                    # Key
                    key_value = row.get(args.key_col)
                    key = None if key_value is None else str(key_value).encode("utf-8")

                    # Serialize + send
                    payload = _serialize_row(row)
                    producer.produce(topic=topic.name, key=key, value=payload)

                    sent += 1
                    processed += 1

                    # Poll occasionally to avoid buffering surprises
                    if (sent % poll_every_n) == 0:
                        _producer_poll(producer, 0.0)

                    now = time.monotonic()

                    # Periodic progress
                    if now - last_report >= report_every_s:
                        elapsed = max(1e-9, now - (clock.t0_wall or now))
                        rate = sent / elapsed
                        pct = (processed / total_to_send * 100.0) if total_to_send > 0 else 0.0
                        logger.info("Sent %d/%d (%.1f%%) | %.0f msg/s", sent, total_to_send, pct, rate)
                        last_report = now

                    # Periodic state save
                    if now - last_save >= save_every_s:
                        _save_state_atomic(state_file, processed)
                        last_save = now

            # Final state save + flush
            _save_state_atomic(state_file, processed)
            _producer_flush(producer, timeout_s=10.0)

    except KeyboardInterrupt:
        _save_state_atomic(state_file, processed)
        logger.info("Interrupted. Saved state at offset %d.", processed)
        return 130

    elapsed_total = time.monotonic() - (clock.t0_wall or time.monotonic())
    rate = sent / elapsed_total if elapsed_total > 0 else 0.0
    logger.info("Finished!")
    logger.info("Sent %d messages in %.1fs (%.0f msg/s)", sent, elapsed_total, rate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
