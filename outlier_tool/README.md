# Outlier Tool

Query execution time prediction and anomaly detection library for Redshift-style workloads. Trains sklearn models, exports to ONNX, and runs high-throughput parallel inference.

---

## Architecture

```
outlier_tool/
├── redset_outlier_lib.py    # Main API: OutlierService, training, ONNX inference
├── business_features.py     # Feature engineering, column schemas
├── business_models.py       # Model building (HistGradientBoosting)
├── business_anomaly_logic.py# Anomaly flagging rules
├── business_io.py           # DuckDB/parquet I/O utilities
└── business_pipeline.py     # Batch pipeline (simulate/follow modes)
```

---

## Quick Start

### Training

```python
from outlier_tool.redset_outlier_lib import (
    OutlierService, TrainingConfig, OnnxExportConfig
)
from pathlib import Path

svc = OutlierService(model_dir="_data/models")

# Collect training files (Arrow/Feather shards or DuckDB files)
train_files = list(Path("daily_shards").glob("*.arrow"))

artifact = svc.retrain(
    train_paths=train_files,
    training=TrainingConfig(
        train_window_days=14,
        max_train_rows=400_000,
    ),
    onnx=OnnxExportConfig(opset=17),
)
print(f"Model: {artifact.tag}")
```

### Inference

```python
from outlier_tool.redset_outlier_lib import OutlierService, InferenceConfig

svc = OutlierService(
    model_dir="_data/models",
    inference=InferenceConfig(batch_size=2048, max_workers=8),
)

# batch = list of dicts or DataFrame
hits = svc.detect_outliers(batch)
# Returns: list of anomaly dicts
```

---

## Module Reference

### `redset_outlier_lib.py`

Main entry point containing high-level classes:

| Class | Purpose |
|-------|---------|
| `OutlierService` | Unified API for training + inference with auto-reload |
| `OutlierTrainer` | Trains sklearn models from Arrow/Feather/DuckDB files |
| `OutlierEngine` | ONNX inference with parallel batch processing |
| `ModelStore` | Artifact management (bundles, ONNX files, latest.json) |

**Configuration dataclasses:**

| Dataclass | Key Fields |
|-----------|------------|
| `TrainingConfig` | `train_window_days`, `max_train_rows`, `quantile`, `rare_threshold` |
| `InferenceConfig` | `batch_size`, `max_workers`, `conservative`, `min_pred_ms` |
| `OnnxExportConfig` | `opset`, `dtype`, `providers`, `fixed_batch_size` |
| `AnomalyThresholds` | `min_long_ms`, `underestimate_ratio`, `underestimate_delta_ms` |

---

### `business_features.py`

Feature engineering for the **postcompile** prediction stage.

**Column Schema Constants:**

| Constant | Columns |
|----------|---------|
| `ID_COLS` | instance_id, cluster_size, user_id, database_id, query_id |
| `TIME_COLS` | arrival_timestamp |
| `DURATION_COLS` | compile_duration_ms, queue_duration_ms, execution_duration_ms |
| `BOOL_COLS` | was_aborted, was_cached |
| `PLAN_COUNT_COLS` | num_joins, num_scans, num_aggregations |
| `TABLE_ACCESS_COUNT_COLS` | num_permanent_tables_accessed, num_external_tables_accessed, num_system_tables_accessed |
| `TEXT_LIST_COLS` | read_table_ids, write_table_ids, cache_source_query_id |

**Key Functions:**

| Function | Purpose |
|----------|---------|
| `prepare_frame_for_stage()` | Full feature pipeline: normalization → time features → fingerprint encoding |
| `compute_fingerprint_stats()` | Build frequency map for fingerprint encoding |
| `apply_fingerprint_features()` | Add fp_freq, fp_is_rare, fp_bucket columns |
| `build_feature_spec()` | Return `FeatureSpec` with categorical/numeric column lists |

**Derived Features Added:**
- `arrival_hour`, `arrival_dow` (from timestamp)
- `read_table_count`, `write_table_count`, `has_write` (from table ID lists)
- `fp_freq`, `fp_is_rare`, `fp_bucket` (fingerprint encoding)
- Missing/zero indicator columns (optional)

---

### `business_models.py`

sklearn model building using **HistGradientBoosting** regressors.

| Function | Purpose |
|----------|---------|
| `build_mean_regressor()` | Mean prediction (squared error loss) |
| `build_quantile_regressor(q)` | Quantile prediction (default q=0.90) |
| `train_stage_models()` | Train both regressors, return `StageModels` |
| `predict_stage()` | Generate predictions from trained models |
| `save_model_bundle()` / `load_model_bundle()` | Joblib serialization |

**Model Architecture:**
- Log-transform target (`log1p`) during training
- OrdinalEncoder for categoricals (handles unknown values)
- HistGradientBoostingRegressor with early stopping
- Hyperparameters: `max_depth=6`, `learning_rate=0.05`, `l2_regularization=0.1`

---

### `business_anomaly_logic.py`

Anomaly detection rules with three criteria.

| Function | Purpose |
|----------|---------|
| `compute_within_instance_ranks()` | Rank queries within each instance |
| `flag_anomalies()` | Apply anomaly criteria, set `is_anomaly` flag |
| `per_key_summary()` | Aggregate statistics by instance/user/database |

**Anomaly Criteria** (query must be "long" AND meet ≥1):

| Criterion | Default Threshold | Meaning |
|-----------|-------------------|---------|
| `underestimated_ratio` | actual/predicted ≥ 10 | 10x underestimate |
| `underestimated_delta` | actual - predicted ≥ 120,000ms | 2+ minutes off |
| `misrank_quantiles` | top 1% actual, bottom 10% predicted | Rank mismatch |

---

### `business_io.py`

I/O utilities for data loading and output.

| Function | Purpose |
|----------|---------|
| `duckdb_connect_mem()` | In-memory DuckDB connection |
| `read_day_from_parquet()` | Read single day from parquet file |
| `list_daily_files()` | Discover dated parquet files in directory |
| `create_or_append_table()` | Schema-evolution-safe table append |
| `infer_day_from_filename()` | Extract YYYY-MM-DD from filename |

---

### `business_pipeline.py`

Batch pipeline for offline processing (CLI).

**Modes:**
- `simulate`: Day-by-day backtesting from single parquet file
- `follow`: Incremental processing of daily parquet directory

```bash
python business_pipeline.py simulate \
  --parquet data.parquet \
  --out-db results.duckdb \
  --train-window 14 \
  --stages postcompile
```

---

## Required Input Schema

Queries must contain these columns for training/inference:

| Category | Columns |
|----------|---------|
| **Categorical** | query_type, feature_fingerprint |
| **Timestamp** | arrival_timestamp |
| **Numeric** | cluster_size, compile_duration_ms, execution_duration_ms, num_joins, num_scans, num_aggregations, num_permanent_tables_accessed, num_external_tables_accessed, num_system_tables_accessed |
| **Table Lists** | read_table_ids, write_table_ids |
| **IDs (recommended)** | instance_id, user_id, database_id, query_id |
| **Flags (optional)** | was_aborted, was_cached |

---

## Output Schema

Each anomaly hit contains:

```python
{
    "instance_id": "i-123",
    "user_id": "u-7",
    "database_id": "db-2", 
    "query_id": "q-999",
    "true_ms": 250000.0,           # Actual execution time
    "pred_ms": 15000.0,            # Final prediction (conservative)
    "pred_mean_ms": 12000.0,       # Mean model prediction
    "pred_q_ms": 15000.0,          # Quantile model prediction
    "abs_err_ms": 235000.0,        # |actual - predicted|
    "signed_err_ms": -235000.0,    # predicted - actual
    "under_ratio": 16.67,          # actual / predicted
    "anomaly_reason": "underestimated_ratio",
    "query_type": "SELECT",
    "arrival_timestamp": "2026-01-28T12:34:56",
    "cluster_size": 8
}
```

---

## Artifact Layout

```
_data/models/
├── bundles/
│   └── model_postcompile_2026-01-28.joblib   # sklearn bundle
├── onnx/
│   ├── model_postcompile_2026-01-28_mean.onnx
│   └── model_postcompile_2026-01-28_q.onnx
└── latest.json                                # Points to current model
```

**Auto-Cleanup:** Keeps only 3 most recent models to prevent unbounded disk usage.

**Hot Reload:** `detect_outliers()` checks `latest.json` mtime and reloads ONNX sessions when a new model is trained.

---

## Performance

| Component | Optimization |
|-----------|-------------|
| Training | Capped at `max_train_rows` (default 400K) with random sampling |
| ONNX Export | Dynamic batch size, float32 precision |
| Inference | Parallel batch processing (`max_workers=8`), configurable batch size |
| I/O | Arrow/Feather format with LZ4 compression, column projection |

---

## Dependencies

```
numpy pandas pyarrow duckdb scikit-learn joblib onnxruntime skl2onnx tqdm
```
