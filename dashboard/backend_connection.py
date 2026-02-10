"""
Pulse Dashboard — Backend Connection

Data-access layer that translates dashboard requests into SQL queries
against the stream-stats and ML-anomaly databases.

Thread-safe by design: DB readers are shared (immutable after init),
time windows are computed per-request via ``compute_time_window()``.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from db_reader import DBReader, create_stream_reader

logger = logging.getLogger(__name__)

# Problem types in severity order (matches dashboard colour coding)
PROBLEM_TYPES = ("cpu_bound", "io_bound", "queue_wlm_bound", "network_bound")

WINDOW_MAP = {"24h": "24 h", "1week": "1 week"}


# ── Per-request time window (no mutable state) ──────────────

@dataclass(frozen=True)
class TimeWindow:
    """Immutable time window boundaries for a single request."""
    window_start: datetime
    prev_window_start: datetime


def compute_time_window(db_stream, window_type: str) -> TimeWindow:
    """Compute time window boundaries from the latest data timestamp.

    This is a pure function — no mutation, safe to call concurrently.
    """
    sql = f"""
        SELECT window_start
        FROM {db_stream.tablename}
        ORDER BY window_start DESC
        LIMIT 1
    """

    try:
        result = db_stream.query(sql)
        if result.empty:
            window_end = datetime.now()
        else:
            window_end = result["window_start"].iloc[0]
            if isinstance(window_end, str):
                window_end = pd.to_datetime(window_end)
    except Exception:
        logger.exception("Failed to fetch latest window_start")
        window_end = datetime.now()

    label = WINDOW_MAP.get(window_type, window_type)
    match label:
        case "24 h":
            return TimeWindow(
                window_start=window_end - timedelta(hours=24),
                prev_window_start=window_end - timedelta(hours=48),
            )
        case "1 week":
            return TimeWindow(
                window_start=window_end - timedelta(days=7),
                prev_window_start=window_end - timedelta(days=14),
            )
        case _:
            raise NotImplementedError(
                f"Window type '{window_type}' is unknown!"
            )


class DashboardBackend:
    """Serve dashboard-facing query helpers for stream and ML datasets.

    Thread-safe: holds only immutable DB readers. All time-sensitive methods
    accept a ``TimeWindow`` computed per-request by the caller.
    """

    def __init__(self):
        # Stream aggregation — auto-detects SQLite or Lakehouse
        self.db_stream = create_stream_reader("minute_stats")

        # Machine learning — 'ml' lakehouse, 'anomalies' table
        self.db_ml = DBReader("anomalies", "ml")

    # ── Helpers ──────────────────────────────────────────────

    @staticmethod
    def _id_params(instance_ids: list[int]) -> tuple[str, list[int]]:
        """Return ``(placeholders, param_list)`` for a parameterized IN clause.

        Example: ``_id_params([1, 2, 3])`` → ``("?, ?, ?", [1, 2, 3])``
        """
        ids = [int(i) for i in instance_ids]
        return ", ".join("?" for _ in ids), ids

    # ── Instance queries ─────────────────────────────────────

    def get_instance_ids(self, tw: TimeWindow) -> list[int]:
        sql = f"""
            SELECT DISTINCT instance_id
            FROM {self.db_stream.tablename}
            WHERE window_start >= ?
        """
        try:
            return self.db_stream.query(sql, [tw.window_start])["instance_id"].tolist()
        except Exception:
            logger.exception("Failed to fetch instance IDs")
            return []

    def get_critical_instance_ids(
        self, tw: TimeWindow, abs_threshold: int = 50, rel_threshold: float = 0.05
    ) -> list[int]:
        # Coerce to numeric to prevent SQL injection
        abs_threshold = int(abs_threshold)
        rel_threshold = float(rel_threshold)

        # Build HAVING conditions for each problem type
        conditions = []
        for p in PROBLEM_TYPES:
            col = f"{p}_queries"
            conditions.append(
                f"SUM({col}) > {abs_threshold} "
                f"OR SUM({col}) * 1.0 / NULLIF(SUM(total_queries),0) > {rel_threshold}"
            )
        having = " OR ".join(conditions)

        sql = f"""
            SELECT instance_id
            FROM {self.db_stream.tablename}
            WHERE window_start >= ?
            GROUP BY instance_id
            HAVING {having}
            ORDER BY instance_id DESC
        """
        try:
            return self.db_stream.query(sql, [tw.window_start])[
                "instance_id"
            ].tolist()
        except Exception:
            logger.exception("Failed to fetch critical instance IDs")
            return []

    # ── Problem-type classification ──────────────────────────

    def get_critical_problem_types(
        self,
        tw: TimeWindow,
        instance_ids: list[int],
        abs_threshold: int = 50,
        rel_threshold: float = 0.05,
    ) -> list[str]:
        # Coerce to numeric to prevent SQL injection
        abs_threshold = int(abs_threshold)
        rel_threshold = float(rel_threshold)

        ph, id_vals = self._id_params(instance_ids)

        # One sub-SELECT per problem type, UNION-ed together
        unions = []
        params: list = []
        for p in PROBLEM_TYPES:
            col = f"{p}_queries"
            unions.append(f"""
                SELECT
                    CASE WHEN SUM({col}) > {abs_threshold}
                            OR SUM({col}) * 1.0 / NULLIF(SUM(total_queries),0) > {rel_threshold}
                        THEN '{p}' ELSE NULL END AS problem
                FROM {self.db_stream.tablename}
                WHERE window_start >= ?
                AND instance_id IN ({ph})
                GROUP BY instance_id
            """)
            params.extend([tw.window_start] + id_vals)

        sql = f"""
            SELECT DISTINCT problem FROM (
                {" UNION ALL ".join(unions)}
            ) t
            WHERE problem IS NOT NULL
        """

        try:
            df = self.db_stream.query(sql, params)
            found = df["problem"].unique().tolist() if not df.empty else []
        except Exception:
            logger.exception("Failed to fetch critical problem types")
            found = []

        # Return in severity order
        return [p for p in PROBLEM_TYPES if p in found]

    # ── Aggregated metrics ───────────────────────────────────

    def get_agg_metrics(self, tw: TimeWindow, instance_ids: list[int]) -> dict:
        ph, id_vals = self._id_params(instance_ids)

        default_stream = {
            "cluster_size": 0,
            "score": 0,
            "spilled_mb": 0,
            "queue_time": 0,
            "queue_ratio": 0,
        }
        default_ml = {"anomalies_count": 0}

        stream_sql = f"""
            SELECT
                MIN(cluster_size) AS cluster_size,
                SUM(normal_queries) * 1.0 / NULLIF(SUM(total_queries),0) AS score,
                SUM(total_spilled_mb) AS spilled_mb,
                SUM(total_queue_time_ms) AS queue_time,
                SUM(total_queue_time_ms) * 1.0
                    / NULLIF(SUM(total_queue_time_ms) + SUM(total_exec_time_ms),0) AS queue_ratio
            FROM {self.db_stream.tablename}
            WHERE window_start >= ?
            AND instance_id IN ({ph})
        """

        ml_sql = f"""
            SELECT COUNT(*) AS anomalies_count
            FROM {self.db_ml.tablename}
            WHERE arrival_timestamp >= ?
            AND instance_id IN ({ph})
        """

        # ── Current window ───────────────────────────────────
        try:
            result = self.db_stream.query(
                stream_sql, [tw.window_start] + id_vals
            )
            current = (
                result.iloc[0].to_dict() if not result.empty else default_stream.copy()
            )
        except Exception:
            logger.exception("Failed to fetch current stream metrics")
            current = default_stream.copy()

        try:
            result = self.db_ml.query(
                ml_sql, [tw.window_start] + id_vals
            )
            current.update(
                result.iloc[0].to_dict() if not result.empty else default_ml.copy()
            )
        except Exception:
            logger.exception("Failed to fetch current ML metrics")
            current.update(default_ml.copy())

        # ── Previous window ──────────────────────────────────
        prev_stream_sql = f"""
            SELECT
                MIN(cluster_size) AS cluster_size,
                SUM(normal_queries) * 1.0 / NULLIF(SUM(total_queries),0) AS score,
                SUM(total_spilled_mb) AS spilled_mb,
                SUM(total_queue_time_ms) AS queue_time,
                SUM(total_queue_time_ms) * 1.0
                    / NULLIF(SUM(total_queue_time_ms) + SUM(total_exec_time_ms),0) AS queue_ratio
            FROM {self.db_stream.tablename}
            WHERE window_start >= ?
            AND window_start < ?
            AND instance_id IN ({ph})
        """
        prev_ml_sql = f"""
            SELECT COUNT(*) AS anomalies_count
            FROM {self.db_ml.tablename}
            WHERE arrival_timestamp >= ?
            AND arrival_timestamp < ?
            AND instance_id IN ({ph})
        """

        try:
            result = self.db_stream.query(
                prev_stream_sql,
                [tw.prev_window_start, tw.window_start] + id_vals,
            )
            previous = (
                result.iloc[0].to_dict() if not result.empty else default_stream.copy()
            )
        except Exception:
            logger.exception("Failed to fetch previous stream metrics")
            previous = default_stream.copy()

        try:
            result = self.db_ml.query(
                prev_ml_sql,
                [tw.prev_window_start, tw.window_start] + id_vals,
            )
            previous.update(
                result.iloc[0].to_dict() if not result.empty else default_ml.copy()
            )
        except Exception:
            logger.exception("Failed to fetch previous ML metrics")
            previous.update(default_ml.copy())

        # ── Combine current + delta ──────────────────────────
        metrics: dict = {}
        for key, cur_val in current.items():
            cur = cur_val if cur_val is not None else 0
            prev = previous.get(key, 0)
            prev = prev if prev is not None else 0
            metrics[key] = cur
            metrics[f"{key}_delta"] = cur - prev

        return metrics

    # ── Anomaly queries ──────────────────────────────────────

    def get_anomaly_queries(self, tw: TimeWindow, instance_ids: list[int]) -> pd.DataFrame:
        ph, id_vals = self._id_params(instance_ids)

        sql = f"""
            SELECT
                instance_id,
                arrival_timestamp,
                query_id,
                user_id,
                database_id,
                anomaly_reason,
                signed_err_ms AS absolute_delta,
                under_ratio  AS relative_delta
            FROM {self.db_ml.tablename}
            WHERE arrival_timestamp >= ?
            AND instance_id IN ({ph})
        """
        return self.db_ml.query(sql, [tw.window_start] + id_vals)

    # ── Classification chart / table ─────────────────────────

    def get_query_classification_chart(
        self, tw: TimeWindow, instance_ids: list[int]
    ) -> pd.DataFrame:
        ph, id_vals = self._id_params(instance_ids)

        sql = f"""
            SELECT
                window_start,
                SUM(normal_queries)          AS normal_queries,
                SUM(cpu_bound_queries)       AS cpu_bound_queries,
                SUM(io_bound_queries)        AS io_bound_queries,
                SUM(network_bound_queries)   AS network_bound_queries,
                SUM(queue_wlm_bound_queries) AS queue_wlm_bound_queries
            FROM {self.db_stream.tablename}
            WHERE window_start >= ?
            AND instance_id IN ({ph})
            GROUP BY window_start
            ORDER BY window_start
        """
        try:
            return self.db_stream.query(sql, [tw.window_start] + id_vals)
        except Exception:
            logger.exception("Failed to fetch classification chart data")
            return pd.DataFrame()

    def get_query_classification_table(
        self, tw: TimeWindow, instance_ids: list[int]
    ) -> pd.DataFrame:
        ph, id_vals = self._id_params(instance_ids)

        try:
            sql = f"""
                SELECT
                    SUM(total_queries) AS total_queries,
                    SUM(normal_queries) AS normal_queries,
                    SUM(normal_queries) * 1.0 / NULLIF(SUM(total_queries),0) AS normal_queries_pct,
                    SUM(cpu_bound_queries) AS cpu_bound_queries,
                    SUM(cpu_bound_queries) * 1.0 / NULLIF(SUM(total_queries),0) AS cpu_bound_queries_pct,
                    SUM(io_bound_queries) AS io_bound_queries,
                    SUM(io_bound_queries) * 1.0 / NULLIF(SUM(total_queries),0) AS io_bound_queries_pct,
                    SUM(network_bound_queries) AS network_bound_queries,
                    SUM(network_bound_queries) * 1.0 / NULLIF(SUM(total_queries),0) AS network_bound_queries_pct,
                    SUM(queue_wlm_bound_queries) AS queue_wlm_bound_queries,
                    SUM(queue_wlm_bound_queries) * 1.0 / NULLIF(SUM(total_queries),0) AS queue_wlm_bound_queries_pct
                FROM {self.db_stream.tablename}
                WHERE window_start >= ?
                AND instance_id IN ({ph})
            """
            stream_df = self.db_stream.query(sql, [tw.window_start] + id_vals)
        except Exception:
            logger.exception("Failed to fetch classification table (stream)")
            stream_df = pd.DataFrame({
                "total_queries": [0],
                "normal_queries": [0], "normal_queries_pct": [0],
                "cpu_bound_queries": [0], "cpu_bound_queries_pct": [0],
                "io_bound_queries": [0], "io_bound_queries_pct": [0],
                "network_bound_queries": [0], "network_bound_queries_pct": [0],
                "queue_wlm_bound_queries": [0], "queue_wlm_bound_queries_pct": [0],
            })

        try:
            ml_sql = f"""
                SELECT COUNT(*) AS anomalies_count
                FROM {self.db_ml.tablename}
                WHERE arrival_timestamp >= ?
                AND instance_id IN ({ph})
            """
            ml_df = self.db_ml.query(ml_sql, [tw.window_start] + id_vals)
        except Exception:
            logger.exception("Failed to fetch classification table (ML)")
            ml_df = pd.DataFrame({"anomalies_count": [0]})

        table_df = stream_df.join(ml_df)
        table_df["anomalies_pct"] = (
            table_df["anomalies_count"] / table_df["total_queries"]
        ).fillna(0)

        return table_df
