"""Kafka consumer for Redshift query events with ML inference and async training.

Offset semantics:
- Offsets stored for handled messages (including poison pills)
- Commits only after data is durably written to disk (arrow shards)
"""

import argparse
import os
import subprocess
import sys
import time
from collections import deque
from datetime import datetime
from typing import Optional

import orjson

# add project root + outlier_tool to path for imports
_project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(_project_root, "outlier_tool"))
sys.path.insert(0, _project_root)

from quixstreams import Application
from kafka_stream.arrow_writer import DailyArrowWriter
from kafka_stream.anomalous_query_producer import AnomalousQueryProducer
from outlier_tool.redset_outlier_lib import OutlierService, InferenceConfig
from config import KAFKA_BOOTSTRAP_SERVERS


def parse_dt(ts_str: str) -> Optional[datetime]:
    """Parse ISO-8601 timestamp string to datetime."""
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None


def hour_floor(dt: datetime) -> datetime:
    """Truncate datetime to hour boundary."""
    return dt.replace(minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------
# Training Manager
# ---------------------------------------------------------------------

class TrainingManager:
    """Manages async model training in separate subprocess.
    
    Queues training jobs with dynamic multi-consumer detection.
    Only consumer 0 (leader) triggers training.
    """
    def __init__(
        self,
        trainer_script: str,
        model_dir: str,
        logs_dir: str = "./_data/logs",
        max_queue: int = 14,
        max_training_days: int = 7,
    ):
        self.trainer_script = os.path.abspath(trainer_script)
        self.model_dir = model_dir
        self.logs_dir = logs_dir
        self.max_training_days = max_training_days
        os.makedirs(self.logs_dir, exist_ok=True)

        # tracks the active subprocess (if any)
        self.proc: Optional[subprocess.Popen] = None
        self.active_day: Optional[str] = None
        self.active_log: Optional[str] = None
        self.active_t0: Optional[float] = None

        # bounded queue; if it fills up we drop the oldest jobs
        self.queue = deque(maxlen=max_queue)
        self.completed_days: list[str] = []

    def should_train(self, day: str, retrain_every_n_days: int) -> bool:
        """Check if training should run based on schedule."""
        if retrain_every_n_days <= 0:
            return False
        if day in self.completed_days:
            return False
        n = len(self.completed_days)
        return n == 0 or (n % retrain_every_n_days) == 0

    def check_day_markers(self, db_dir: str, table: str, day: str) -> tuple[int, list[int]]:
        """
        Check for day-complete marker files from consumers.
        
        Marker format: {table}_{day}_complete_c{consumer_id}.marker
        
        Returns:
            Tuple of (marker_count, list of consumer_ids that have completed)
        """
        marker_prefix = f"{table}_{day}_complete_c"
        completed_ids: list[int] = []
        
        if os.path.isdir(db_dir):
            for f in os.listdir(db_dir):
                if f.startswith(marker_prefix) and f.endswith(".marker"):
                    # extract consumer ID from filename
                    try:
                        # e.g., "query_events_2026-02-02_complete_c0.marker" -> "0"
                        id_str = f[len(marker_prefix):-len(".marker")]
                        consumer_id = int(id_str)
                        completed_ids.append(consumer_id)
                    except ValueError:
                        continue
        
        return len(completed_ids), sorted(completed_ids)

    def detect_active_consumers(self, db_dir: str, table: str, day: str) -> list[int]:
        """
        Detect which consumers have written shard files for a specific day.
        
        This enables dynamic consumer detection - we only wait for consumers
        that actually received data (partitions), not a fixed expected count.
        
        Shard format: {table}_{day}_{kind}_c{consumer_id}_{timestamp}_{seq}.arrow
        
        Returns:
            Sorted list of consumer IDs that have written at least one shard for this day
        """
        import re
        
        # pattern to match consumer ID in filename: _c{number}_
        # e.g., query_events_2024-03-02_batch_c0_20260202185833_000001.arrow
        pattern = re.compile(rf"^{re.escape(table)}_{re.escape(day)}_\w+_c(\d+)_\d+_\d+\.arrow$")
        
        active_ids: set[int] = set()
        
        if os.path.isdir(db_dir):
            for f in os.listdir(db_dir):
                match = pattern.match(f)
                if match:
                    consumer_id = int(match.group(1))
                    active_ids.add(consumer_id)
        
        return sorted(active_ids)

    def _list_files_for_day(self, db_dir: str, table: str, day: str) -> list[str]:
        prefix = f"{table}_{day}_"
        out: list[str] = []
        if not os.path.isdir(db_dir):
            return out
        for f in os.listdir(db_dir):
            if f.startswith(prefix) and f.endswith(".arrow"):
                p = os.path.join(db_dir, f)
                if os.path.isfile(p):
                    out.append(p)
        return sorted(out)

    def _select_eligible_days(self, db_dir: str, table: str) -> list[str]:
        """Get most recent N days from available shard files."""
        days: set[str] = set()
        prefix = f"{table}_"
        if not os.path.isdir(db_dir):
            return []
        for f in os.listdir(db_dir):
            if not (f.startswith(prefix) and f.endswith(".arrow")):
                continue
            remainder = f[len(prefix) :]
            candidate = remainder.split("_", 1)[0]
            if len(candidate) == 10 and candidate.count("-") == 2:
                days.add(candidate)
        return sorted(days)[-self.max_training_days :]

    def get_training_files(self, db_dir: str, table: str) -> list[str]:
        """Collect all shard files for training window."""
        files: list[str] = []
        for d in self._select_eligible_days(db_dir, table):
            files.extend(self._list_files_for_day(db_dir, table, d))
        return sorted(set(files))

    def enqueue(
        self,
        day: str,
        db_dir: str,
        table: str,
        train_window_days: int,
        max_train_rows: int,
        retrain_every_n_days: int,
    ) -> None:
        """
        Enqueue training job for a given day.
        
        Multi-consumer support: Uses dynamic detection to find which consumers
        actually wrote data for this day, then waits only for those consumers
        to write their day-complete markers.
        
        Note: `train_window_days` is still forwarded to trainer for compatibility,
        but this manager already constructs the rolling file list based on `max_training_days`.
        """
        if not self.should_train(day, retrain_every_n_days):
            print(f"[training] skipping day {day} (every {retrain_every_n_days} day(s))")
            return

        # dynamic multi-consumer detection: find which consumers actually wrote data
        active_consumers = self.detect_active_consumers(db_dir, table, day)
        
        if len(active_consumers) > 1:
            # multiple consumers wrote data - wait for all of them to signal completion
            marker_count, completed_ids = self.check_day_markers(db_dir, table, day)
            
            # check if all active consumers have completed
            missing = [c for c in active_consumers if c not in completed_ids]
            if missing:
                print(f"[training] waiting for consumers: {len(completed_ids)}/{len(active_consumers)} complete for {day}")
                print(f"[training] active consumers: {active_consumers}, completed: {completed_ids}, waiting for: {missing}")
                # queue the job to retry later (same 4-tuple shape as normal jobs)
                job = (day, None, train_window_days, max_train_rows)
                if not any(q[0] == day for q in self.queue):
                    self.queue.append(job)
                return
            else:
                print(f"[training] all {len(active_consumers)} active consumers completed for {day}")

        train_paths = self.get_training_files(db_dir, table)
        if not train_paths:
            print(f"[training] skipping {day}: no training files in {db_dir}")
            return

        job = (day, train_paths, train_window_days, max_train_rows)

        # if training is running, queue it up (and let deque(maxlen=...) drop oldest if full)
        if self.proc is not None and self.proc.poll() is None:
            if self.active_day == day or any(q_day == day for (q_day, *_rest) in self.queue):
                print(f"[training] already queued/training day={day}, skipping")
                return

            was_full = (self.queue.maxlen is not None) and (len(self.queue) == self.queue.maxlen)
            dropped_day = self.queue[0][0] if (was_full and len(self.queue) > 0) else None

            self.queue.append(job)

            if dropped_day is not None and (len(self.queue) == self.queue.maxlen):
                print(
                    f"[training] queue full, dropped oldest day={dropped_day}, "
                    f"queued day={day} (size={len(self.queue)})"
                )
            else:
                print(f"[training] queued day={day} (size={len(self.queue)})")
            return

        # otherwise start right away
        self._start(job, db_dir=db_dir, table=table)

    def _start(self, job, db_dir: str = "", table: str = "") -> None:
        """Launch training subprocess with manifest file."""
        day, train_paths, train_window_days, max_train_rows = job

        # Retry jobs have train_paths=None; recompute from disk
        if train_paths is None:
            if not db_dir or not table:
                print(f"[training] cannot retry day={day}: missing db_dir/table")
                return
            train_paths = self.get_training_files(db_dir, table)
            if not train_paths:
                print(f"[training] retry day={day}: no training files found")
                return
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(self.logs_dir, f"train_{day}_{ts}.log")
        manifest_path = os.path.join(self.logs_dir, f"train_{day}_{ts}_manifest.txt")

        # keep command lines short by passing a manifest file
        with open(manifest_path, "w", encoding="utf-8") as f:
            for p in train_paths:
                f.write(p + "\n")

        cmd = [
            sys.executable,
            self.trainer_script,
            "--train-paths-file",
            manifest_path,
            "--model-dir",
            self.model_dir,
            "--stage",
            "postcompile",
            "--target",
            "execution_duration_ms",
            "--train-window-days",
            str(train_window_days),
            "--max-train-rows",
            str(max_train_rows),
            "--model-name-prefix",
            "model",
            "--opset",
            "17",
            "--fixed-batch-size",
            "0",
        ]

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        popen_kwargs = (
            {"start_new_session": True}
            if os.name != "nt"
            else {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        )

        print(f"[training] launching subprocess for day={day} (files={len(train_paths)})")
        print(f"[training] manifest: {manifest_path}")
        print(f"[training] log: {log_path}")

        with open(log_path, "w", encoding="utf-8") as f:
            self.proc = subprocess.Popen(
                cmd, stdout=f, stderr=subprocess.STDOUT, env=env, **popen_kwargs
            )

        self.active_day = day
        self.active_log = log_path
        self.active_t0 = time.monotonic()

    def poll(self) -> None:
        """Check training subprocess status and start queued jobs."""
        if self.proc is None:
            return
        rc = self.proc.poll()
        if rc is None:
            return

        dt = time.monotonic() - (self.active_t0 or time.monotonic())
        status = "success" if rc == 0 else f"failed (rc={rc})"
        print(f"[training] finished {self.active_day} in {dt:.1f}s -> {status}")
        if self.active_log:
            print(f"[training] log: {self.active_log}")

        if rc == 0 and self.active_day:
            self.completed_days.append(self.active_day)

        self.proc = None
        self.active_day = None
        self.active_log = None
        self.active_t0 = None

        # kick off the next queued job, if any
        if self.queue:
            next_job = self.queue.popleft()
            self._start(next_job)

    def shutdown(self, wait: bool = True, timeout_s: float = 120.0) -> None:
        """Wait for training subprocess to complete on shutdown."""
        if self.proc is None or self.proc.poll() is not None:
            return
        if not wait:
            print("[training] still running at shutdown; not waiting")
            return
        print(f"[training] waiting for subprocess (timeout {timeout_s}s)...")
        try:
            self.proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            print(f"[training] still running after {timeout_s}s; leaving it")

# ---------------------------------------------------------------------
# Offset Management
# ---------------------------------------------------------------------

class OffsetManager:
    """Tracks Kafka offsets for explicit commit after durable writes.

    Note: tracks only the last message globally. Correct for single-partition
    topics. For multi-partition topics, track per (topic, partition) and
    commit all at each boundary.
    """

    def __init__(self, consumer) -> None:
        self.consumer = consumer
        self._dirty = False
        self._last_msg = None

    def store(self, msg) -> None:
        """Mark message as processed (pending commit)."""
        self._last_msg = msg
        self._dirty = True

    def commit(self, reason: str) -> None:
        """Commit stored offset to Kafka."""
        if not self._dirty:
            return
        try:
            # commit the last processed message explicitly
            self.consumer.commit(message=self._last_msg, asynchronous=False)
            self._dirty = False

        except Exception as e:
            print(f"[offsets] commit failed ({reason}): {e!r}")


# ---------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------

def main() -> None:
    """Run the Kafka consumer with ML inference pipeline."""
    p = argparse.ArgumentParser(description="kafka consumer with ml inference + async training")
    p.add_argument("--topic", default="redshift.query_events")
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP_SERVERS)
    p.add_argument("--group", default="redshift-stats-consumer")
    p.add_argument("--auto-offset-reset", default="latest", choices=["earliest", "latest"])

    p.add_argument("--db-dir", default="./_data/arrow/")
    p.add_argument("--table", default="query_events")

    p.add_argument("--model-dir", default="_data/models")
    p.add_argument("--inference-batch-size", type=int, default=8192)
    p.add_argument("--inference-workers", type=int, default=4)
    p.add_argument("--use-gpu", action="store_true")

    p.add_argument("--trainer-script", default="pipeline/train_model.py")
    p.add_argument("--training-logs-dir", default="./_data/logs")
    p.add_argument("--train-window-days", type=int, default=1)
    p.add_argument("--max-training-days", type=int, default=7)
    p.add_argument("--max-train-rows", type=int, default=400000)
    p.add_argument("--retrain-every-n-days", type=int, default=1)

    # Multi-consumer support
    p.add_argument("--consumer-id", type=int, default=0,
                   help="Consumer instance ID (0-N). Used for shard filenames and leader election. "
                        "Only consumer 0 triggers training. Active consumers are auto-detected from shard files.")

    p.add_argument("--max-days", type=int, default=0)
    p.add_argument("--idle-timeout", type=float, default=0)
    p.add_argument("--quiet", "-q", action="store_true",
                   help="Suppress frequent output (hour updates, inference batches). Critical messages still shown.")
    args = p.parse_args()

    os.makedirs(args.db_dir, exist_ok=True)

    perf = {
        "t0": time.monotonic(),
        "kafka_read": 0.0,
        "arrow_write": 0.0,
        "inference": 0.0,
        "processed": 0,
        "inf_batches": 0,
    }

    print(f"trying to connect to bootstrap: {args.bootstrap}")
    app = Application(
        broker_address=args.bootstrap,
        consumer_group=args.group,
        auto_offset_reset=args.auto_offset_reset,
        auto_create_topics=True,
        consumer_extra_config={
            "security.protocol": "PLAINTEXT",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "fetch.min.bytes": 50000,
            "fetch.max.bytes": 10485760,
            "max.partition.fetch.bytes": 5242880,
        },
    )

    # set up kafka consumer
    consumer = app.get_consumer()
    consumer.subscribe([args.topic])
    offsets = OffsetManager(consumer)
    writer = DailyArrowWriter(
        base_dir=args.db_dir,
        table=args.table,
        batch_size=50_000,
        compression="lz4",
        consumer_id=args.consumer_id,
    )

    # we publish anomalies to a separate topic; failure here shouldn't block committing offsets
    producer = AnomalousQueryProducer(bootstrap=args.bootstrap, topic="anomalous_queries")

    # set up ML-path
    # inference_batch_size (CLI) = how many rows to accumulate before calling detect_outliers
    # InferenceConfig.batch_size  = ONNX internal forward-pass batch size (smaller for GPU memory)
    providers = ("CUDAExecutionProvider", "CPUExecutionProvider") if args.use_gpu else None
    svc = OutlierService(
        model_dir=args.model_dir,
        inference=InferenceConfig(batch_size=2048, max_workers=args.inference_workers),
        providers=providers,
    )

    trainer = TrainingManager(
        trainer_script=args.trainer_script,
        model_dir=args.model_dir,
        logs_dir=args.training_logs_dir,
        max_training_days=args.max_training_days,
    )



    current_day: Optional[str] = None
    current_hour: Optional[datetime] = None
    last_hour_wall: Optional[float] = None
    days_seen: set[str] = set()

    last_msg_wall = time.monotonic()
    idle_warned = False

    pred_batch: list[dict] = []

    def run_inference_if_ready() -> None:
        nonlocal pred_batch
        if len(pred_batch) < args.inference_batch_size:
            return
        if not os.path.isfile(f"{svc.model_dir}/latest.json"):
            pred_batch = []
            return

        t0 = time.monotonic()
        hits = svc.detect_outliers(pred_batch)
        perf["inference"] += time.monotonic() - t0
        perf["inf_batches"] += 1

        if hits:
            try:
                producer.publish_many(hits)
                producer.flush()
            except Exception as exc:
                print(f"[inference] failed to publish {len(hits)} anomalies: {exc!r}")
            if not args.quiet:
                print(f"[inference] batch={len(pred_batch):,}, anomalies={len(hits)}")

        pred_batch = []

    def durable_boundary(reason: str) -> None:
        # commit offsets only after we know shards are on disk
        offsets.commit(reason)

    # Multi-consumer startup info
    print("=" * 80)
    print("kafka consumer started")
    print("=" * 80)
    print(f"consumer id: {args.consumer_id}" + (" (leader - triggers training)" if args.consumer_id == 0 else " (follower)"))
    print(f"topic: {args.topic}")
    print(f"inference batch size: {args.inference_batch_size:,}")
    print("=" * 80)

    try:
        while True:
            # get training status without blocking consumption
            trainer.poll()

            t_poll0 = time.monotonic()
            msg = consumer.poll(1.0)
            perf["kafka_read"] += time.monotonic() - t_poll0

            if msg is None:
                # optional idle exit for benchmarks or one-shot runs
                if args.idle_timeout > 0:
                    idle = time.monotonic() - last_msg_wall
                    if idle >= args.idle_timeout:
                        if not args.quiet:
                            print(f"\n[idle] no messages for {idle:.1f}s, exiting")
                        break
                    if idle >= args.idle_timeout * 0.5 and not idle_warned:
                        if not args.quiet:
                            print(f"[idle] no messages for {idle:.1f}s (timeout at {args.idle_timeout}s)...")
                        idle_warned = True
                continue

            last_msg_wall = time.monotonic()
            idle_warned = False

            if hasattr(msg, "error") and msg.error():
                raise RuntimeError(msg.error())

            try:
                row = orjson.loads(msg.value())
            except Exception:
                # poison pill: mark handled and commit immediately so we don't spin on it
                offsets.store(msg)
                durable_boundary("malformed-json")
                continue

            ts_str = row.get("arrival_timestamp") if isinstance(row, dict) else None
            dt = parse_dt(ts_str) if ts_str else None
            if not dt:
                offsets.store(msg)
                durable_boundary("bad-or-missing-timestamp")
                continue

            day = dt.date().isoformat()
            hour = hour_floor(dt)

            pred_batch.append(row)
            run_inference_if_ready()

            # day rollover is a durable boundary (we flush/close, then commit offsets)
            if current_day is None or day != current_day:
                if current_day is not None:
                    t0 = time.monotonic()
                    writer.commit()
                    writer.close()
                    perf["arrow_write"] += time.monotonic() - t0
                    durable_boundary("day-rollover")

                    print(f"[day] rollover -> {day}")

                    # leader-based training: only consumer 0 triggers async training
                    # this prevents race conditions when multiple consumers are running
                    if args.consumer_id == 0:
                        trainer.enqueue(
                            day=current_day,
                            db_dir=args.db_dir,
                            table=args.table,
                            train_window_days=args.train_window_days,
                            max_train_rows=args.max_train_rows,
                            retrain_every_n_days=args.retrain_every_n_days,
                        )
                    else:
                        print(f"[training] skipping (consumer_id={args.consumer_id}, only consumer 0 triggers)")

                current_day = day
                days_seen.add(day)
                if args.max_days > 0 and len(days_seen) >= args.max_days:
                    print(f"\n[limit] reached max days={args.max_days}, stopping")
                    break

                writer.open_day(current_day)
                current_hour = None
                last_hour_wall = None

            # hour boundary is also a durable boundary
            if current_hour is None:
                current_hour = hour
                last_hour_wall = time.monotonic()
                print(f"starting event hour: {current_hour:%Y-%m-%d %H}:00")
            elif hour != current_hour:
                wall_now = time.monotonic()
                wall_delta = wall_now - (last_hour_wall or wall_now)
                last_hour_wall = wall_now

                t0 = time.monotonic()
                writer.commit()
                perf["arrow_write"] += time.monotonic() - t0
                durable_boundary("hour-boundary")

                if not args.quiet:
                    print(f"[ok] hour passed -> {hour:%Y-%m-%d %H}:00 | wall delta {wall_delta:.1f}s")
                current_hour = hour

            # buffered write; insert returns (was_inserted, was_flushed)
            t0 = time.monotonic()
            inserted, flushed = writer.insert(row)
            perf["arrow_write"] += time.monotonic() - t0

            offsets.store(msg)
            if inserted:
                perf["processed"] += 1
            if flushed:
                durable_boundary("batch-flush")

    except KeyboardInterrupt:
        print("\n[consumer] interrupted by user")
    finally:
        # run inference on any remaining messages in buffer
        if pred_batch and os.path.isfile(f"{svc.model_dir}/latest.json"):
            try:
                t0 = time.monotonic()
                hits = svc.detect_outliers(pred_batch)
                perf["inference"] += time.monotonic() - t0
                perf["inf_batches"] += 1
                if hits:
                    producer.publish_many(hits)
                    producer.flush()
                    print(f"[inference] final batch={len(pred_batch):,}, anomalies={len(hits)}")
            except Exception:
                pass

        # flush shards to disk, then commit offsets
        try:
            t0 = time.monotonic()
            writer.close()
            perf["arrow_write"] += time.monotonic() - t0
            durable_boundary("shutdown")
        except Exception:
            pass

        try:
            consumer.close()
        except Exception:
            pass

        try:
            producer.close()
        except Exception:
            pass

        total = time.monotonic() - perf["t0"]
        print("\n" + "=" * 80)
        print("performance summary")
        print("=" * 80)
        print(f"total runtime: {total:.1f}s")
        print(f"messages processed: {perf['processed']:,}")
        if total > 0:
            print(f"throughput: {perf['processed'] / total:.0f} msg/s")

        if total > 0:
            print("\ntime breakdown:")
            print(f"  kafka read:   {perf['kafka_read']:8.2f}s ({perf['kafka_read'] / total * 100:5.1f}%)")
            print(f"  arrow write:  {perf['arrow_write']:8.2f}s ({perf['arrow_write'] / total * 100:5.1f}%)")
            print(f"  inference:    {perf['inference']:8.2f}s ({perf['inference'] / total * 100:5.1f}%)")

        if perf["inf_batches"] > 0:
            avg_ms = (perf["inference"] / perf["inf_batches"]) * 1000
            print(f"\ninference: {perf['inf_batches']} batches, avg {avg_ms:.1f}ms/batch")

        print(f"days processed: {sorted(days_seen)}")
        print(f"training sessions completed: {len(trainer.completed_days)}")
        print("=" * 80)

        trainer.shutdown(wait=True, timeout_s=60.0)


if __name__ == "__main__":
    main()
