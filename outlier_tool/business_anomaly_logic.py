"""Anomaly detection logic for flagging query predictions that significantly underestimate execution time."""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_within_instance_ranks(
    df: pd.DataFrame, true_col: str, pred_col: str, group_col: str = "instance_id"
) -> pd.DataFrame:
    """Compute percentile ranks of true vs predicted values within each instance.
    
    Adds columns for comparing where a query ranks within its instance:
    - true_quantile, pred_quantile: percentile ranks (0-1) 
    - true_rank_desc, pred_rank_desc: ordinal ranks (1=slowest)
    - rank_gap_desc: difference in predicted vs true rank
    """
    df = df.copy()
    if group_col not in df.columns:
        df[group_col] = -1

    df["true_quantile"] = df.groupby(group_col)[true_col].transform(
        lambda s: s.rank(method="average", pct=True)
    )
    df["pred_quantile"] = df.groupby(group_col)[pred_col].transform(
        lambda s: s.rank(method="average", pct=True)
    )

    # Descending ranks: 1 = slowest query in the instance
    df["true_rank_desc"] = df.groupby(group_col)[true_col].transform(
        lambda s: (-s).rank(method="average")
    )
    df["pred_rank_desc"] = df.groupby(group_col)[pred_col].transform(
        lambda s: (-s).rank(method="average")
    )
    df["rank_gap_desc"] = df["pred_rank_desc"] - df["true_rank_desc"]
    return df


def flag_anomalies(
    df: pd.DataFrame,
    true_col: str,
    pred_col: str,
    min_long_ms: float = 60_000.0,
    underestimate_ratio: float = 10.0,
    underestimate_delta_ms: float = 120_000.0,
    true_quantile_thresh: float = 0.99,
    pred_quantile_thresh: float = 0.90,
) -> pd.DataFrame:
    """Flag queries where the model severely underestimated execution time.
    
    A query is flagged as anomaly if it's "long" (>= min_long_ms) AND meets
    at least one of:
    - underestimated_ratio: actual/predicted >= threshold (e.g., 10x underestimate)
    - underestimated_delta: actual - predicted >= threshold (e.g., 2+ minutes off)
    - misrank_quantiles: query was in top 1% actual but predicted in bottom 10%
    
    Adds columns: is_anomaly (bool), anomaly_reason (str), plus error metrics.
    """
    df = df.copy()
    eps = 1.0  # Prevent division by zero

    df["true_ms"] = pd.to_numeric(df[true_col], errors="coerce").fillna(0.0).astype(float)
    df["pred_ms_used"] = pd.to_numeric(df[pred_col], errors="coerce").fillna(0.0).astype(float)

    df["abs_err_ms"] = (df["true_ms"] - df["pred_ms_used"]).abs()
    df["signed_err_ms"] = df["pred_ms_used"] - df["true_ms"]
    df["under_ratio"] = df["true_ms"] / np.maximum(df["pred_ms_used"], eps)

    # Build anomaly masks
    long_mask = df["true_ms"] >= float(min_long_ms)
    ratio_mask = df["under_ratio"] >= float(underestimate_ratio)
    delta_mask = (df["true_ms"] - df["pred_ms_used"]) >= float(underestimate_delta_ms)
    rank_mask = (df["true_quantile"] >= float(true_quantile_thresh)) & (
        df["pred_quantile"] <= float(pred_quantile_thresh)
    )

    # Must be long AND meet at least one underestimate criterion
    df["is_anomaly"] = long_mask & (ratio_mask | delta_mask | rank_mask)

    df["anomaly_reason"] = np.select(
        [long_mask & ratio_mask, long_mask & delta_mask, long_mask & rank_mask],
        ["underestimated_ratio", "underestimated_delta", "misrank_quantiles"],
        default="",
    )
    return df


def per_key_summary(df_anom: pd.DataFrame, key: str) -> pd.DataFrame:
    """Aggregate anomaly statistics grouped by a key column (e.g., instance_id, user_id)."""
    if len(df_anom) == 0:
        return pd.DataFrame(
            columns=[key, "anoms", "true_sum_ms", "abs_err_sum_ms", "median_true_ms", "median_under_ratio"]
        )
    g = df_anom.groupby(key, dropna=False)
    out = g.agg(
        anoms=("is_anomaly", "sum"),
        true_sum_ms=("true_ms", "sum"),
        abs_err_sum_ms=("abs_err_ms", "sum"),
        median_true_ms=("true_ms", "median"),
        median_under_ratio=("under_ratio", "median"),
    ).reset_index()
    out["anoms"] = out["anoms"].astype(int)
    return out.sort_values(["anoms", "true_sum_ms"], ascending=False)

