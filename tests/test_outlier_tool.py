"""Tests for the outlier_tool ML library.

Tests feature engineering, model training, anomaly detection, and ONNX export
using synthetic data — no external dependencies required.
"""
import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Feature engineering (business_features)
# ---------------------------------------------------------------------------

class TestFeatureEngineering:
    """Verify feature engineering transforms work on synthetic data."""

    def test_imports(self):
        """All feature engineering modules should import cleanly."""
        from business_features import (
            FeatureSpec,
            build_feature_spec,
            prepare_frame_for_stage,
            add_time_features,
            add_table_list_features,
            add_missing_and_zero_indicators,
            compute_fingerprint_stats,
            apply_fingerprint_features,
            normalize_bool_col,
            fill_missing_columns,
            coerce_numeric_keep_nan,
            POSTCOMPILE_NUMERIC_COLS,
            POSTCOMPILE_CATEGORICAL_COLS,
            STAGE_POSTCOMPILE,
        )

    def test_build_feature_spec(self):
        """Feature spec should list categorical + numeric columns."""
        from business_features import build_feature_spec, STAGE_POSTCOMPILE

        spec = build_feature_spec(STAGE_POSTCOMPILE)
        assert len(spec.categorical) > 0
        assert len(spec.numeric) > 0
        assert spec.target == "execution_duration_ms"
        assert spec.all_features() == spec.categorical + spec.numeric

    def test_add_time_features(self):
        """Time features should produce hour (0-23) and dow (0-6)."""
        from business_features import add_time_features

        df = pd.DataFrame({
            "arrival_timestamp": pd.to_datetime(["2024-03-15 14:30:00", "2024-03-16 08:00:00"])
        })
        add_time_features(df)  # modifies in-place
        assert "arrival_hour" in df.columns
        assert "arrival_dow" in df.columns
        assert df["arrival_hour"].iloc[0] == 14
        assert 0 <= df["arrival_dow"].iloc[0] <= 6

    def test_normalize_bool_col(self):
        """Boolean normalization should handle string/numeric/bool inputs."""
        from business_features import normalize_bool_col

        df = pd.DataFrame({"flag": ["true", "false", "True", "yes", "no"]})
        normalize_bool_col(df, "flag")
        assert df["flag"].tolist() == [True, False, True, True, False]

    def test_normalize_bool_col_missing(self):
        """Missing column should be created with False default."""
        from business_features import normalize_bool_col

        df = pd.DataFrame({"other": [1, 2]})
        normalize_bool_col(df, "flag")
        assert "flag" in df.columns
        assert df["flag"].tolist() == [False, False]

    def test_fill_missing_columns(self):
        """Missing columns should be filled with default value."""
        from business_features import fill_missing_columns

        df = pd.DataFrame({"a": [1, 2]})
        fill_missing_columns(df, ["a", "b", "c"], 0)
        assert "b" in df.columns
        assert "c" in df.columns
        assert df["b"].tolist() == [0, 0]

    def test_add_table_list_features(self):
        """CSV table ID lists should produce count columns."""
        from business_features import add_table_list_features

        df = pd.DataFrame({
            "read_table_ids": ["1,2,3", "", None],
            "write_table_ids": ["5", "1,2", None],
            "cache_source_query_id": [None, None, None],
        })
        add_table_list_features(df)  # modifies in-place
        assert "read_table_count" in df.columns
        assert "write_table_count" in df.columns
        assert "has_write" in df.columns
        assert df["read_table_count"].iloc[0] == 3

    def test_compute_fingerprint_stats(self):
        """Fingerprint frequency map should count occurrences."""
        from business_features import compute_fingerprint_stats

        df = pd.DataFrame({
            "feature_fingerprint": ["aaa", "bbb", "aaa", "aaa", "ccc"]
        })
        freq_map = compute_fingerprint_stats(df)
        assert freq_map["aaa"] == 3
        assert freq_map["bbb"] == 1

    def test_apply_fingerprint_features(self):
        """Fingerprint features should produce fp_freq, fp_is_rare, fp_bucket."""
        from business_features import apply_fingerprint_features

        df = pd.DataFrame({"feature_fingerprint": ["aaa", "bbb", "ccc"]})
        freq_map = {"aaa": 100, "bbb": 1, "ccc": 5}
        apply_fingerprint_features(df, freq_map, rare_threshold=3)  # modifies in-place
        assert "fp_freq" in df.columns
        assert "fp_is_rare" in df.columns
        assert "fp_bucket" in df.columns
        assert df["fp_is_rare"].iloc[1] == 1  # bbb has freq 1 < 3


# ---------------------------------------------------------------------------
# Anomaly detection (business_anomaly_logic)
# ---------------------------------------------------------------------------

class TestAnomalyDetection:
    """Verify anomaly flagging logic with controlled inputs."""

    def test_imports(self):
        from business_anomaly_logic import (
            flag_anomalies,
            compute_within_instance_ranks,
            per_key_summary,
        )

    def test_flag_anomalies_basic(self):
        """A query that is long and greatly underestimated should be flagged."""
        from business_anomaly_logic import flag_anomalies, compute_within_instance_ranks

        df = pd.DataFrame({
            "instance_id": [1, 1, 1],
            "true_ms_col": [100_000.0, 500.0, 200.0],        # first is very long
            "pred_ms_col": [5_000.0, 400.0, 250.0],          # first is wildly underestimated
        })
        df = compute_within_instance_ranks(df, "true_ms_col", "pred_ms_col")
        result = flag_anomalies(
            df, "true_ms_col", "pred_ms_col",
            min_long_ms=60_000, underestimate_ratio=10,
            underestimate_delta_ms=50_000,
        )
        assert result["is_anomaly"].iloc[0] == True
        assert result["is_anomaly"].iloc[1] == False
        assert result["anomaly_reason"].iloc[0] != ""

    def test_flag_anomalies_short_queries_never_flagged(self):
        """Short queries should never be flagged even if ratio is high."""
        from business_anomaly_logic import flag_anomalies, compute_within_instance_ranks

        df = pd.DataFrame({
            "instance_id": [1],
            "true_ms_col": [1000.0],    # short query (< 60s)
            "pred_ms_col": [1.0],       # wildly underestimated
        })
        df = compute_within_instance_ranks(df, "true_ms_col", "pred_ms_col")
        result = flag_anomalies(df, "true_ms_col", "pred_ms_col")
        assert result["is_anomaly"].iloc[0] == False

    def test_per_key_summary_empty(self):
        """Empty anomaly DataFrame should return empty summary."""
        from business_anomaly_logic import per_key_summary

        empty_df = pd.DataFrame(columns=["instance_id", "is_anomaly", "true_ms", "abs_err_ms", "under_ratio"])
        result = per_key_summary(empty_df, "instance_id")
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Model training (business_models)
# ---------------------------------------------------------------------------

class TestModels:
    """Verify model building and prediction with synthetic data."""

    def test_imports(self):
        from business_models import (
            build_mean_regressor,
            build_quantile_regressor,
            build_explode_classifier,
            build_quartile_classifier,
            train_stage_models,
            predict_stage,
        )

    def test_build_regressors(self):
        """Regressors should build without errors."""
        from business_models import build_mean_regressor, build_quantile_regressor

        mean_reg = build_mean_regressor()
        assert mean_reg is not None
        q_reg = build_quantile_regressor(0.9)
        assert q_reg is not None

    def test_train_and_predict_roundtrip(self):
        """Full train → predict cycle with synthetic data."""
        from business_features import build_feature_spec, STAGE_POSTCOMPILE
        from business_models import train_stage_models, predict_stage

        spec = build_feature_spec(STAGE_POSTCOMPILE)
        rng = np.random.default_rng(42)
        n = 200

        # Build synthetic DataFrame with all required features
        data = {}
        for col in spec.numeric:
            data[col] = rng.uniform(0, 100, n).astype(float)
        for col in spec.categorical:
            data[col] = rng.choice(["SELECT", "INSERT", "UPDATE"], n)
        data[spec.target] = rng.uniform(100, 10000, n).astype(float)
        data["feature_fingerprint"] = rng.choice(["fp_a", "fp_b", "fp_c"], n)

        df = pd.DataFrame(data)

        from business_features import compute_fingerprint_stats
        freq_map = compute_fingerprint_stats(df)

        models = train_stage_models(df, spec, freq_map, quantile=0.9)
        assert models.mean_model is not None
        assert models.quantile_model is not None

        preds = predict_stage(models, df[:10])
        assert "pred_mean_ms" in preds
        assert "pred_q_ms" in preds
        assert len(preds["pred_mean_ms"]) == 10
        assert all(p > 0 for p in preds["pred_mean_ms"])


# ---------------------------------------------------------------------------
# I/O utilities (business_io)
# ---------------------------------------------------------------------------

class TestIO:
    """Verify I/O utility functions."""

    def test_imports(self):
        from business_io import (
            PipelineState,
            load_state,
            save_state,
            duckdb_connect_mem,
            normalize_path_for_duckdb,
            infer_day_from_filename,
        )

    def test_normalize_path_for_duckdb(self):
        from business_io import normalize_path_for_duckdb

        assert normalize_path_for_duckdb("C:\\data\\file.parquet") == "C:/data/file.parquet"
        assert normalize_path_for_duckdb("/unix/path/file.parquet") == "/unix/path/file.parquet"

    def test_infer_day_from_filename(self):
        from business_io import infer_day_from_filename
        from pathlib import Path
        from datetime import date

        assert infer_day_from_filename(Path("queries_2024-03-15_shard.parquet")) == date(2024, 3, 15)
        assert infer_day_from_filename(Path("no_date_here.parquet")) is None

    def test_pipeline_state_roundtrip(self, tmp_path):
        """State should survive save → load cycle."""
        from business_io import PipelineState, save_state, load_state

        state = PipelineState(last_updated_at="2024-01-01", notes="test")
        path = tmp_path / "state.json"
        save_state(path, state)
        loaded = load_state(path)
        assert loaded.last_updated_at == "2024-01-01"
        assert loaded.notes == "test"

    def test_load_state_missing_file(self, tmp_path):
        """Missing state file should return empty state."""
        from business_io import load_state

        state = load_state(tmp_path / "nonexistent.json")
        assert state.last_updated_at == ""


# ---------------------------------------------------------------------------
# OutlierService (redset_outlier_lib) — core service class
# ---------------------------------------------------------------------------

class TestOutlierService:
    """Smoke-test the main OutlierService class."""

    def test_imports(self):
        from redset_outlier_lib import (
            OutlierService,
            AnomalyThresholds,
            TrainingConfig,
            OnnxExportConfig,
            InferenceConfig,
            OutlierHit,
            ModelStore,
        )

    def test_dataclass_defaults(self):
        """Config dataclasses should instantiate with sensible defaults."""
        from redset_outlier_lib import AnomalyThresholds, TrainingConfig, InferenceConfig

        at = AnomalyThresholds()
        assert at.min_long_ms == 60_000.0
        assert at.underestimate_ratio == 10.0

        tc = TrainingConfig()
        assert tc.target == "execution_duration_ms"
        assert tc.quantile == 0.90

        ic = InferenceConfig()
        assert ic.batch_size == 2048

    def test_model_store_init(self, tmp_path):
        """ModelStore should initialize with the given directory."""
        from redset_outlier_lib import ModelStore

        store = ModelStore(str(tmp_path))
        assert store is not None
