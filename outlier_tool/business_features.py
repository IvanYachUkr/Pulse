"""Feature engineering for query execution time prediction.

Defines the postcompile stage features, column schemas, and transformation functions
for preparing Redshift query data for ML model training and inference.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Stage: postcompile (only stage used in production)
# -----------------------------
STAGE_POSTCOMPILE = "postcompile"

ALL_STAGES = [STAGE_POSTCOMPILE]


# -----------------------------
# Common columns (Redset schema)
# -----------------------------
ID_COLS = [
    "instance_id",
    "cluster_size",
    "user_id",
    "database_id",
    "query_id",
]

TIME_COLS = [
    "arrival_timestamp",
]

DURATION_COLS = [
    "compile_duration_ms",
    "queue_duration_ms",
    "execution_duration_ms",
]

CATEGORICAL_COLS_BASE = [
    "query_type",
    # feature_fingerprint is handled as frequency + rare flags; optionally also as categorical
]

BOOL_COLS = [
    "was_aborted",
    "was_cached",
]

PLAN_COUNT_COLS = [
    "num_joins",
    "num_scans",
    "num_aggregations",
]

TABLE_ACCESS_COUNT_COLS = [
    "num_permanent_tables_accessed",
    "num_external_tables_accessed",
    "num_system_tables_accessed",
]

EXEC_DERIVED_COLS = [
    "mbytes_scanned",
    "mbytes_spilled",
]

TEXT_LIST_COLS = [
    "read_table_ids",
    "write_table_ids",
    # optional: cache_source_query_id
    "cache_source_query_id",
]

# Postcompile numeric features (after compile, before execution)
POSTCOMPILE_NUMERIC_COLS: List[str] = (
    TABLE_ACCESS_COUNT_COLS
    + PLAN_COUNT_COLS
    + [
        "compile_duration_ms",
        "read_table_count",
        "write_table_count",
        "has_write",
        "arrival_hour",
        "arrival_dow",
        "cluster_size",
    ]
)

POSTCOMPILE_CATEGORICAL_COLS: List[str] = ["query_type"]


@dataclass(frozen=True)
class FeatureSpec:
    """Specification of features used for a given prediction stage.
    
    Attributes:
        stage: The prediction stage (currently only 'postcompile')
        categorical: List of categorical feature column names
        numeric: List of numeric feature column names
        target: Target column to predict (default: execution_duration_ms)
    """
    stage: str
    categorical: List[str]
    numeric: List[str]
    target: str = "execution_duration_ms"

    def all_features(self) -> List[str]:
        """Return combined list of all feature columns."""
        return self.categorical + self.numeric


# -----------------------------
# Feature engineering helpers
# -----------------------------
def _safe_to_datetime(s: pd.Series) -> pd.Series:
    """Convert to datetime, coercing invalid values to NaT."""
    return pd.to_datetime(s, errors="coerce")


def _count_csv_items(series: pd.Series) -> pd.Series:
    """
    Fast-ish count of comma separated IDs without splitting into lists.
    - NaN/empty -> 0
    - Otherwise -> number of commas + 1
    """
    s = series.fillna("").astype(str)
    s = s.str.strip()
    empty = (s == "") | (s == "nan") | (s == "None")
    # commas count +1
    counts = s.str.count(",") + 1
    counts = counts.where(~empty, 0)
    return counts.astype(np.int32)


def normalize_bool_col(df: pd.DataFrame, col: str) -> None:
    """Normalize a column to boolean, handling various input formats.
    
    Handles: bool, numeric (0/1), string ('true'/'false'/'yes'/'no'/'t'/'f').
    Missing columns are created with False. Works in-place.
    """
    if col not in df.columns:
        df[col] = False
        return
    if df[col].dtype == bool:
        df[col] = df[col].fillna(False)
        return
    if pd.api.types.is_numeric_dtype(df[col].dtype):
        df[col] = (pd.to_numeric(df[col], errors="coerce").fillna(0) != 0)
        return
    # String parsing for various true/false representations
    ss = df[col].astype(str).str.strip().str.lower()
    df[col] = ss.isin(["1", "t", "true", "y", "yes"])


def fill_missing_columns(df: pd.DataFrame, cols: List[str], fill_value) -> None:
    """Add missing columns to DataFrame with a default value."""
    for c in cols:
        if c not in df.columns:
            df[c] = fill_value


def coerce_numeric_keep_nan(df: pd.DataFrame, cols: List[str]) -> None:
    """
    Convert columns to numeric (float64), coercing errors to NaN.
    Also converts nullable Int64 dtypes (with pandas.NA) to float64 for sklearn compatibility.
    """
    for c in cols:
        if c not in df.columns:
            continue
        # Convert nullable Int64/Int32 etc to float64 first
        if pd.api.types.is_integer_dtype(df[c]) and df[c].dtype.name.startswith('Int'):
            # This is a nullable integer dtype (e.g., Int64)
            df[c] = df[c].astype('float64')
        else:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def add_time_features(df: pd.DataFrame, ts_col: str = "arrival_timestamp") -> None:
    """Extract hour and day-of-week from timestamp column."""
    if ts_col not in df.columns:
        df["arrival_hour"] = np.nan
        df["arrival_dow"] = np.nan
        return
    ts = _safe_to_datetime(df[ts_col])
    df[ts_col] = ts
    df["arrival_hour"] = ts.dt.hour.astype("float32")
    df["arrival_dow"] = ts.dt.dayofweek.astype("float32")


def add_table_list_features(df: pd.DataFrame) -> None:
    """Derive table count features from comma-separated table ID lists."""
    if "read_table_ids" in df.columns:
        df["read_table_count"] = _count_csv_items(df["read_table_ids"])
    else:
        df["read_table_count"] = 0

    if "write_table_ids" in df.columns:
        df["write_table_count"] = _count_csv_items(df["write_table_ids"])
    else:
        df["write_table_count"] = 0

    df["has_write"] = (pd.to_numeric(df["write_table_count"], errors="coerce").fillna(0) > 0).astype(np.int8)


def add_missing_and_zero_indicators(
    df: pd.DataFrame,
    numeric_cols: List[str],
    zero_indicator_cols: Optional[List[str]] = None,
    add_missing: bool = True,
    add_zero: bool = True,
) -> List[str]:
    """Add binary indicator columns for missing/zero values.
    
    Returns list of newly created indicator column names.
    """
    new_cols: List[str] = []
    zero_set = set(zero_indicator_cols or [])
    for c in numeric_cols:
        if add_missing:
            mc = f"{c}__is_missing"
            df[mc] = df[c].isna().astype(np.int8)
            new_cols.append(mc)
        if add_zero and (c in zero_set):
            zc = f"{c}__is_zero"
            df[zc] = (df[c].fillna(np.nan) == 0).fillna(False).astype(np.int8)
            new_cols.append(zc)
    return new_cols


def compute_fingerprint_stats(train_df: pd.DataFrame, col: str = "feature_fingerprint") -> Dict[str, int]:
    """
    Frequency encoding map for feature_fingerprint within the training window.
    """
    if col not in train_df.columns:
        return {}
    s = train_df[col].fillna("NA").astype(str)
    vc = s.value_counts(dropna=False)
    # store as Python ints for joblib JSON friendliness
    return {k: int(v) for k, v in vc.to_dict().items()}


def apply_fingerprint_features(
    df: pd.DataFrame,
    fingerprint_freq_map: Dict[str, int],
    rare_threshold: int = 3,
) -> None:
    """
    Adds fingerprint-based features using vectorized operations (no row iteration).
    
    Creates:
      - fp_freq: frequency count from training
      - fp_is_rare: 1 if frequency < rare_threshold, else 0
      - fp_bucket: binned frequency (common/moderate/rare)
    """
    if "feature_fingerprint" not in df.columns:
        df["fp_freq"] = 0
        df["fp_is_rare"] = 0
        df["fp_bucket"] = "unknown"
        return

    # Vectorized: map all fingerprints to frequencies at once
    df["fp_freq"] = df["feature_fingerprint"].map(fingerprint_freq_map).fillna(0).astype(np.int32)
    
    # Vectorized: compute is_rare flag
    df["fp_is_rare"] = (df["fp_freq"] < rare_threshold).astype(np.int8)
    
    # Vectorized: create frequency buckets
    def bucket_freq(freq: int) -> str:
        if freq == 0:
            return "unseen"
        elif freq < rare_threshold:
            return "rare"
        elif freq < 50:
            return "moderate"
        else:
            return "common"
    
    df["fp_bucket"] = df["fp_freq"].apply(bucket_freq).astype(str)


def build_feature_spec(stage: str = STAGE_POSTCOMPILE, target: str = "execution_duration_ms") -> FeatureSpec:
    """Build feature specification for postcompile stage."""
    if stage != STAGE_POSTCOMPILE:
        raise ValueError(f"Unknown stage: {stage}. Only '{STAGE_POSTCOMPILE}' is supported.")

    numeric = list(POSTCOMPILE_NUMERIC_COLS) + [
        # fingerprint numeric features (vectorized)
        "fp_freq",
        "fp_is_rare",
    ]
    categorical = list(POSTCOMPILE_CATEGORICAL_COLS) + [
        # fingerprint categorical features
        "fp_bucket",
    ]
    return FeatureSpec(stage=stage, categorical=categorical, numeric=numeric, target=target)


def prepare_frame_for_stage(
    df: pd.DataFrame,
    stage: str,
    target: str = "execution_duration_ms",
    fingerprint_freq_map: Optional[Dict[str, int]] = None,
    rare_threshold: int = 3,
    drop_cached: bool = False,
    drop_aborted: bool = True,
    add_indicators: bool = True,
) -> Tuple[pd.DataFrame, FeatureSpec]:
    """Transform raw query data into model-ready features.
    
    Applies the full feature engineering pipeline:
    1. Normalize boolean columns
    2. Optionally filter out aborted/cached queries
    3. Extract time features (hour, day of week)
    4. Derive table count features from ID lists
    5. Apply fingerprint frequency encoding
    6. Add missing/zero indicator columns
    7. Coerce types and fill missing values
    
    Returns:
        Tuple of (transformed DataFrame, FeatureSpec describing the columns)
    """
    df = df.copy()

    # Booleans
    for bc in BOOL_COLS:
        normalize_bool_col(df, bc)

    if drop_aborted and "was_aborted" in df.columns:
        df = df[~df["was_aborted"]].copy()

    if drop_cached and "was_cached" in df.columns:
        df = df[~df["was_cached"]].copy()

    # Time and derived features
    add_time_features(df, ts_col="arrival_timestamp")
    add_table_list_features(df)

    # Coerce numeric columns (keep NaNs)
    spec = build_feature_spec(stage=stage, target=target)
    coerce_numeric_keep_nan(df, spec.numeric + [target])

    # Categoricals
    for cc in spec.categorical:
        if cc not in df.columns:
            df[cc] = "NA"
        df[cc] = df[cc].fillna("NA").astype(str)

    # Fingerprint frequency features
    if fingerprint_freq_map is None:
        fingerprint_freq_map = {}
    apply_fingerprint_features(df, fingerprint_freq_map, rare_threshold=rare_threshold)

    # Indicator columns (missing/zero)
    if add_indicators:
        ind_cols = add_missing_and_zero_indicators(
            df,
            numeric_cols=spec.numeric,
            zero_indicator_cols=["compile_duration_ms"],
            add_missing=True,
            add_zero=True,
        )
        # extend numeric features with indicators
        spec = FeatureSpec(stage=spec.stage, categorical=spec.categorical, numeric=spec.numeric + ind_cols, target=spec.target)

    # Ensure all columns exist
    fill_missing_columns(df, spec.numeric, np.nan)
    fill_missing_columns(df, spec.categorical, "NA")

    # Target cleanup
    if target in df.columns:
        df[target] = pd.to_numeric(df[target], errors="coerce")
        df = df.dropna(subset=["arrival_timestamp", target]).copy()
        df[target] = np.clip(df[target].astype(float), 0.0, None)

    return df, spec
