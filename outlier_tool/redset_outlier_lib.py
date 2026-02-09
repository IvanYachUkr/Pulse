"""Core library for query outlier detection using ML models and ONNX inference.

Provides:
- Configuration dataclasses (AnomalyThresholds, TrainingConfig, etc.)
- ModelStore for artifact management
- OutlierTrainer for model training from parquet/DuckDB files
- OutlierEngine for ONNX-based real-time inference
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import duckdb

from business_features import (
    STAGE_POSTCOMPILE,
    compute_fingerprint_stats,
    prepare_frame_for_stage,
)
from business_models import train_stage_models, save_model_bundle, load_model_bundle
from business_anomaly_logic import compute_within_instance_ranks, flag_anomalies


# ---------------------------------------------------------------------
# Public dataclasses (configs + outputs)
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class AnomalyThresholds:
    """Configuration for anomaly detection thresholds.
    
    min_long_ms: Minimum actual duration to consider for anomaly flagging
    underestimate_ratio: Flag when actual/predicted exceeds this ratio
    underestimate_delta_ms: Flag when actual-predicted exceeds this delta
    """
    min_long_ms: float = 60_000.0
    underestimate_ratio: float = 10.0
    underestimate_delta_ms: float = 120_000.0
    true_quantile: float = 0.99
    pred_quantile: float = 0.90


@dataclass(frozen=True)
class TrainingConfig:
    """Configuration for model training parameters."""
    stage: str = STAGE_POSTCOMPILE
    target: str = "execution_duration_ms"
    quantile: float = 0.90
    explode_threshold_ms: float = 600_000.0
    rare_threshold: int = 3
    drop_cached_train: bool = False
    drop_aborted: bool = True
    add_indicators: bool = True
    train_window_days: int = 14
    max_train_rows: int = 400_000
    min_train_rows: int = 500
    model_name_prefix: str = "model"


@dataclass(frozen=True)
class OnnxExportConfig:
    """Configuration for ONNX model export."""
    opset: int = 17
    dtype: str = "float32"
    providers: Tuple[str, ...] = ("CPUExecutionProvider",)
    fixed_batch_size: Optional[int] = None


@dataclass(frozen=True)
class InferenceConfig:
    """Configuration for ONNX inference."""
    dtype: str = "float32"
    conservative: bool = True
    min_pred_ms: float = 1.0
    batch_size: int = 2048
    max_workers: int = 8


@dataclass(frozen=True)
class OutlierHit:
    """Detected anomaly with prediction details."""
    instance_id: Any
    user_id: Any
    database_id: Any
    query_id: Any
    true_ms: float
    pred_ms: float
    pred_mean_ms: float
    pred_q_ms: float
    abs_err_ms: float
    signed_err_ms: float
    under_ratio: float
    anomaly_reason: str
    query_type: Any
    arrival_timestamp: Any
    cluster_size: Any


# ---------------------------------------------------------------------
# Internal utilities
# ---------------------------------------------------------------------

_DAY_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _infer_day_from_filename(p: Path) -> Optional[date]:
    """Extract YYYY-MM-DD date from filename."""
    m = _DAY_RE.search(p.name)
    if not m:
        return None
    try:
        return datetime.fromisoformat(m.group(1)).date()
    except Exception:
        return None




def _ensure_dir(p: Union[str, Path]) -> Path:
    """Create directory if it doesn't exist."""
    pp = Path(p)
    pp.mkdir(parents=True, exist_ok=True)
    return pp


def _safe_json_dump(path: Path, obj: Dict[str, Any]) -> None:
    """
    Atomically write JSON to file using temp file + rename pattern.
    
    This prevents corruption when multiple consumers/training processes
    attempt to write to the same file (e.g., latest.json).
    
    The rename operation is atomic on POSIX systems and mostly-atomic on Windows.
    """
    import tempfile
    
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to temporary file in same directory (ensures same filesystem for atomic rename)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".tmp",
        prefix=path.stem + "_",
        dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(obj, f, indent=2, default=str)
        
        # Atomic rename (replace if exists)
        # On Windows, we need to remove the target first if it exists
        if os.name == 'nt' and path.exists():
            try:
                path.unlink()
            except OSError:
                pass  # Race condition: another process may have removed it
        
        os.rename(tmp_path, str(path))
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _safe_json_load(path: Path) -> Optional[Dict[str, Any]]:
    """Load JSON from disk, returning ``None`` when unavailable."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _normalize_raw_batch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make incoming Kafka-ish payloads less annoying:
    - arrival_timestamp: parseable
    - read_table_ids/write_table_ids: accept list/tuple -> comma-separated string
    - feature_fingerprint/query_type: force string if present
    """
    out = df.copy()

    if "arrival_timestamp" in out.columns:
        out["arrival_timestamp"] = pd.to_datetime(out["arrival_timestamp"], errors="coerce")

    for c in ["read_table_ids", "write_table_ids"]:
        if c in out.columns:
            def _to_csv(x: Any) -> str:
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return ""
                if isinstance(x, (list, tuple, set)):
                    return ",".join(str(v) for v in x)
                return str(x)
            out[c] = out[c].map(_to_csv)

    for c in ["feature_fingerprint", "query_type"]:
        if c in out.columns:
            out[c] = out[c].fillna("NA").astype(str)

    return out


def _required_raw_cols_for_postcompile(target: str) -> List[str]:
    """Build list of required columns using shared schema constants.
    
    This matches what prepare_frame_for_stage will use for postcompile feature engineering.
    IDs are optional but strongly recommended for useful output.
    """
    from business_features import (
        BOOL_COLS,
        ID_COLS,
        PLAN_COUNT_COLS,
        TABLE_ACCESS_COUNT_COLS,
        TEXT_LIST_COLS,
        TIME_COLS,
    )
    
    return (
        ID_COLS
        + TIME_COLS
        + ["query_type", "feature_fingerprint"]
        + TEXT_LIST_COLS[:2]  # read_table_ids, write_table_ids (not cache_source_query_id)
        + TABLE_ACCESS_COUNT_COLS
        + PLAN_COUNT_COLS
        + ["compile_duration_ms", target]
        + BOOL_COLS
    )


def _coerce_feature_dtypes_for_onnx(df: pd.DataFrame, cat_cols: List[str], num_cols: List[str], dtype: str) -> pd.DataFrame:
    """Cast feature columns to ONNX-compatible string and float dtypes."""
    out = df.copy()

    for c in cat_cols:
        if c in out.columns:
            out[c] = out[c].astype(str)

    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    cast = np.float32 if dtype == "float32" else np.float64
    for c in num_cols:
        if c in out.columns:
            out[c] = out[c].astype(cast, copy=False)

    return out


def _ensure_2d_col(a: np.ndarray) -> np.ndarray:
    """Ensure a vector uses two dimensions for model input tensors."""
    a = np.asarray(a)
    if a.ndim == 1:
        return a.reshape((-1, 1))
    return a


def _predict_ms_from_regressor_output(pred_log: np.ndarray, min_pred_ms: float) -> np.ndarray:
    """Convert clipped log predictions into millisecond values."""
    pred_log = np.asarray(pred_log, dtype=np.float64).reshape((-1,))
    pred_ms = np.expm1(np.clip(pred_log, -50.0, 50.0))
    pred_ms = np.maximum(pred_ms, float(min_pred_ms))
    return pred_ms.astype(np.float64)


# ---------------------------------------------------------------------
# ONNX support (export + runtime)
# ---------------------------------------------------------------------


def _export_pipeline_to_onnx(
    pipeline: Any,
    cat_cols: List[str],
    num_cols: List[str],
    opset: int,
    fixed_batch_size: Optional[int] = None,
) -> bytes:
    """Export a fitted sklearn pipeline to serialized ONNX bytes."""
    from skl2onnx import to_onnx
    from skl2onnx.common.data_types import FloatTensorType, StringTensorType

    bdim = fixed_batch_size if fixed_batch_size is not None else None

    initial_types = []
    for c in cat_cols:
        initial_types.append((c, StringTensorType([bdim, 1])))
    for c in num_cols:
        initial_types.append((c, FloatTensorType([bdim, 1])))

    onx = to_onnx(pipeline, initial_types=initial_types, target_opset=int(opset))
    return onx.SerializeToString()


def _make_ort_session(onnx_bytes: bytes, providers: Sequence[str]) -> Any:
    """Create an ONNX Runtime session with the configured providers."""
    import onnxruntime as ort
    so = ort.SessionOptions()
    return ort.InferenceSession(onnx_bytes, sess_options=so, providers=list(providers))


def _ort_inputs_from_df(sess: Any, df: pd.DataFrame, dtype: str) -> Dict[str, np.ndarray]:
    """Build a feed dictionary from prepared feature data."""
    feeds: Dict[str, np.ndarray] = {}

    for inp in sess.get_inputs():
        name = inp.name
        if name not in df.columns:
            raise RuntimeError(f"ONNX input '{name}' not found in dataframe columns. Missing: {name}")

        col = df[name]
        
        # ONNX type determines string vs numeric handling
        onnx_type_str = str(inp.type)
        
        if 'string' in onnx_type_str.lower() or col.dtype == object or pd.api.types.is_string_dtype(col):
            arr = col.astype(str).to_numpy()
        else:
            arr = col.to_numpy()
            arr = arr.astype(np.float32 if dtype == "float32" else np.float64, copy=False)

        feeds[name] = _ensure_2d_col(arr)

    return feeds


# ---------------------------------------------------------------------
# Model store: save/load "latest" artifacts
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class ModelArtifact:
    """Persisted model artifact metadata and file pointers."""
    tag: str
    stage: str
    trained_on: str

    bundle_path: str
    onnx_mean_path: str
    onnx_q_path: str

    spec: Dict[str, Any]
    fingerprint_freq_map: Dict[str, int]

    training_config: Dict[str, Any]
    anomaly_thresholds: Dict[str, Any]
    onnx_export_config: Dict[str, Any]


class ModelStore:
    """
    Layout:
      model_dir/
        bundles/
          model_postcompile_2026-01-28.joblib
        onnx/
          model_postcompile_2026-01-28_mean.onnx
          model_postcompile_2026-01-28_q.onnx
        latest.json
    """

    def __init__(self, model_dir: Union[str, Path]) -> None:
        self.model_dir = Path(model_dir)
        self.bundles_dir = _ensure_dir(self.model_dir / "bundles")
        self.onnx_dir = _ensure_dir(self.model_dir / "onnx")
        self.latest_path = self.model_dir / "latest.json"

    def save_artifacts(self, art: ModelArtifact) -> None:
        _safe_json_dump(self.latest_path, asdict(art))
        # Clean up old models after saving new one
        self._cleanup_old_models(keep_n=3)
    
    def _cleanup_old_models(self, keep_n: int = 3) -> None:
        """
        Keep only the N most recent model artifacts (by mtime).
        Removes old bundles and ONNX files to prevent unbounded disk usage.
        
        Uses a lock file to prevent race conditions when multiple consumers
        might be loading models while cleanup is running.
        
        Args:
            keep_n: Number of most recent models to keep (default 3: current + 2 backups)
        """
        import time
        
        lock_path = self.model_dir / ".cleanup.lock"
        
        # Try to acquire lock (simple file-based lock)
        try:
            if lock_path.exists():
                # Check if lock is stale (older than 60 seconds)
                lock_age = time.time() - lock_path.stat().st_mtime
                if lock_age < 60:
                    print(f"[Cleanup] Skipping: another cleanup in progress (lock age: {lock_age:.1f}s)")
                    return
                else:
                    print(f"[Cleanup] Removing stale lock (age: {lock_age:.1f}s)")
            
            # Create lock file
            lock_path.write_text(str(time.time()), encoding="utf-8")
        except Exception as e:
            print(f"[Cleanup] Could not acquire lock: {e}")
            return
        
        try:
            # Get all bundle files sorted by modification time (newest first)
            bundles = sorted(
                self.bundles_dir.glob("*.joblib"),
                key=lambda p: p.stat().st_mtime,
                reverse=True
            )
            
            if len(bundles) <= keep_n:
                return  # Nothing to clean up
            
            # Delete old bundles
            for old_bundle in bundles[keep_n:]:
                tag = old_bundle.stem  # e.g., "model_postcompile_2024-03-02"
                
                # Delete bundle
                try:
                    old_bundle.unlink()
                    print(f"[Cleanup] Deleted old bundle: {old_bundle.name}")
                except Exception as e:
                    print(f"[Cleanup] Warning: Could not delete {old_bundle.name}: {e}")
                
                # Delete associated ONNX files
                onnx_mean = self.onnx_dir / f"{tag}_mean.onnx"
                onnx_q = self.onnx_dir / f"{tag}_q.onnx"
                
                for onnx_file in [onnx_mean, onnx_q]:
                    if onnx_file.exists():
                        try:
                            onnx_file.unlink()
                            print(f"[Cleanup] Deleted old ONNX: {onnx_file.name}")
                        except Exception as e:
                            print(f"[Cleanup] Warning: Could not delete {onnx_file.name}: {e}")
        finally:
            # Release lock
            try:
                lock_path.unlink()
            except Exception:
                pass

    def load_latest(self) -> Optional[ModelArtifact]:
        obj = _safe_json_load(self.latest_path)
        if not obj:
            return None
        try:
            return ModelArtifact(**obj)
        except Exception:
            return None

    def find_latest_by_glob(self, stage: str) -> Optional[ModelArtifact]:
        """Fallback when latest.json is missing: pick newest bundle by mtime."""
        bundles = sorted(self.bundles_dir.glob(f"*{stage}*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not bundles:
            return None
        bundle_path = bundles[0]
        # derive onnx filenames
        stem = bundle_path.stem  # e.g., model_postcompile_2026-01-28
        onnx_mean = self.onnx_dir / f"{stem}_mean.onnx"
        onnx_q = self.onnx_dir / f"{stem}_q.onnx"

        b = load_model_bundle(str(bundle_path))
        spec = b.get("spec", {})
        fp = b.get("fingerprint_freq_map", {})

        return ModelArtifact(
            tag=stem,
            stage=stage,
            trained_on=str(b.get("day", "")) or datetime.utcnow().date().isoformat(),
            bundle_path=str(bundle_path),
            onnx_mean_path=str(onnx_mean),
            onnx_q_path=str(onnx_q),
            spec=spec if isinstance(spec, dict) else {},
            fingerprint_freq_map=fp if isinstance(fp, dict) else {},
            training_config={},
            anomaly_thresholds={},
            onnx_export_config={},
        )


# ---------------------------------------------------------------------
# Trainer
# ---------------------------------------------------------------------

class OutlierTrainer:
    """Train, validate, and export outlier-detection artifacts."""

    def __init__(self, model_dir: Union[str, Path]) -> None:
        self.store = ModelStore(model_dir)

    # Train from explicit list of shard files
    def train_from_files(
        self,
        train_paths: Sequence[Union[str, Path]],
        *,
        training: TrainingConfig = TrainingConfig(),
        anomaly_thresholds: AnomalyThresholds = AnomalyThresholds(),
        onnx: OnnxExportConfig = OnnxExportConfig(),
        duckdb_table: Optional[str] = None,
    ) -> ModelArtifact:
        """
        Train from an explicit list of files (typically Arrow/Feather shards).
        Includes timing breakdowns for benchmarking.
        """
        t_total0 = time.perf_counter()
        timings: Dict[str, float] = {}

        # -------------------------
        # Load data (includes shard IO + normalization breakdown inside loader)
        # -------------------------
        t0 = time.perf_counter()
        df_train_raw, trained_on_day = self._load_training_data_from_files(
            train_paths=train_paths,
            training=training,
            duckdb_table=duckdb_table,
        )
        timings["load_total"] = time.perf_counter() - t0

        if df_train_raw is None or len(df_train_raw) < training.min_train_rows:
            raise ValueError(
                f"Not enough training data. Got {0 if df_train_raw is None else len(df_train_raw)} rows."
            )

        print(f"[Trainer][Data] raw_rows={len(df_train_raw):,} trained_on_day={trained_on_day}")

        # -------------------------
        # Fingerprint stats
        # -------------------------
        t0 = time.perf_counter()
        fp_map = compute_fingerprint_stats(df_train_raw, col="feature_fingerprint")
        timings["fingerprint_stats"] = time.perf_counter() - t0
        print(f"[Trainer][Data] fingerprint_unique={len(fp_map):,}")

        # -------------------------
        # Feature engineering
        # -------------------------
        t0 = time.perf_counter()
        df_train, spec_obj = prepare_frame_for_stage(
            df_train_raw,
            stage=training.stage,
            target=training.target,
            fingerprint_freq_map=fp_map,
            rare_threshold=training.rare_threshold,
            drop_cached=training.drop_cached_train,
            drop_aborted=training.drop_aborted,
            add_indicators=training.add_indicators,
        )
        timings["feature_prep"] = time.perf_counter() - t0

        if len(df_train) < training.min_train_rows:
            raise ValueError(f"Not enough usable rows after feature prep. Got {len(df_train)} rows.")

        print(
            f"[Trainer][Feat] rows={len(df_train):,} "
            f"cat={len(getattr(spec_obj, 'categorical', []))} "
            f"num={len(getattr(spec_obj, 'numeric', []))} "
            f"target={getattr(spec_obj, 'target', training.target)}"
        )

        # -------------------------
        # Train models
        # -------------------------
        t0 = time.perf_counter()
        models = train_stage_models(
            df_train=df_train,
            spec=spec_obj,
            fingerprint_freq_map=fp_map,
            quantile=training.quantile,
            explode_threshold_ms=training.explode_threshold_ms,
        )
        timings["train_models"] = time.perf_counter() - t0

        # -------------------------
        # Save sklearn bundle
        # -------------------------
        t0 = time.perf_counter()
        tag = f"{training.model_name_prefix}_{training.stage}_{trained_on_day}"
        bundle_path = self.store.bundles_dir / f"{tag}.joblib"

        bundle_payload = {
            "stage": training.stage,
            "day": trained_on_day,
            "spec": {
                "stage": spec_obj.stage,
                "categorical": list(spec_obj.categorical),
                "numeric": list(spec_obj.numeric),
                "target": spec_obj.target,
            },
            "fingerprint_freq_map": fp_map,
            "explode_threshold_ms": float(training.explode_threshold_ms),
            "quantile": float(training.quantile),
            "models": models,
        }
        save_model_bundle(str(bundle_path), bundle_payload)
        timings["save_bundle"] = time.perf_counter() - t0

        # -------------------------
        # Export ONNX
        # -------------------------
        t0 = time.perf_counter()
        timings["onnx_import"] = time.perf_counter() - t0

        cat_cols = list(bundle_payload["spec"]["categorical"])
        num_cols = list(bundle_payload["spec"]["numeric"])

        onnx_mean_path = self.store.onnx_dir / f"{tag}_mean.onnx"
        onnx_q_path = self.store.onnx_dir / f"{tag}_q.onnx"

        t0 = time.perf_counter()
        mean_bytes = _export_pipeline_to_onnx(
            pipeline=models.mean_model,
            cat_cols=cat_cols,
            num_cols=num_cols,
            opset=onnx.opset,
            fixed_batch_size=onnx.fixed_batch_size,
        )
        timings["onnx_export_mean"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        q_bytes = _export_pipeline_to_onnx(
            pipeline=models.quantile_model,
            cat_cols=cat_cols,
            num_cols=num_cols,
            opset=onnx.opset,
            fixed_batch_size=onnx.fixed_batch_size,
        )
        timings["onnx_export_quantile"] = time.perf_counter() - t0

        t0 = time.perf_counter()
        onnx_mean_path.write_bytes(mean_bytes)
        onnx_q_path.write_bytes(q_bytes)
        timings["onnx_write"] = time.perf_counter() - t0

        # -------------------------
        # Save artifact metadata
        # -------------------------
        t0 = time.perf_counter()
        art = ModelArtifact(
            tag=tag,
            stage=training.stage,
            trained_on=trained_on_day,
            bundle_path=str(bundle_path),
            onnx_mean_path=str(onnx_mean_path),
            onnx_q_path=str(onnx_q_path),
            spec=bundle_payload["spec"],
            fingerprint_freq_map=fp_map,
            training_config=asdict(training),
            anomaly_thresholds=asdict(anomaly_thresholds),
            onnx_export_config=asdict(onnx),
        )
        self.store.save_artifacts(art)
        timings["save_artifacts"] = time.perf_counter() - t0

        timings["total"] = time.perf_counter() - t_total0

        # -------------------------
        # Print timing summary
        # -------------------------
        print("[Trainer][Perf] breakdown:")
        for k in [
            "load_total",
            "fingerprint_stats",
            "feature_prep",
            "train_models",
            "save_bundle",
            "onnx_import",
            "onnx_export_mean",
            "onnx_export_quantile",
            "onnx_write",
            "save_artifacts",
            "total",
        ]:
            if k in timings:
                print(f"[Trainer][Perf]   {k:18s} {timings[k]:8.3f}s")

        return art


    def _load_training_data_from_files(
        self,
        *,
        train_paths: Sequence[Union[str, Path]],
        training: TrainingConfig,
        duckdb_table: Optional[str],
    ) -> Tuple[pd.DataFrame, str]:
        """
        Load training data from explicit file list.
        Applies training.train_window_days (keep last N days), then reads all files for those days.
        """

        target = training.target
        wanted = _required_raw_cols_for_postcompile(target)

        t_all0 = time.perf_counter()

        # --- selection / grouping ---
        t0 = time.perf_counter()
        paths = [Path(p) for p in train_paths]
        by_day = {}
        for p in paths:
            d = _infer_day_from_filename(p) or datetime.utcfromtimestamp(p.stat().st_mtime).date()
            by_day.setdefault(d, []).append(p)

        days_sorted = sorted(by_day.keys())
        if training.train_window_days > 0:
            days_sorted = days_sorted[-training.train_window_days:]

        selected_files = []
        for d in days_sorted:
            selected_files.extend(sorted(by_day[d]))
        t_select = time.perf_counter() - t0

        # --- read files ---
        t0 = time.perf_counter()
        read_times = []
        bytes_total = 0
        dfs = []

        for p in selected_files:
            t_file0 = time.perf_counter()
            size = p.stat().st_size
            bytes_total += size

            suf = p.suffix.lower()
            if suf in [".arrow", ".feather"]:
                df = self._read_arrow_file(p, wanted_cols=wanted)
            elif suf in [".duckdb", ".db"]:
                df = self._read_duckdb_file(p, wanted_cols=wanted, table=duckdb_table)
            else:
                raise ValueError(f"Unsupported training file type: {p}")

            dt_file = time.perf_counter() - t_file0
            read_times.append((dt_file, size, str(p), len(df)))
            dfs.append(df)

        t_read = time.perf_counter() - t0

        # --- concat ---
        t0 = time.perf_counter()
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=wanted)
        t_concat = time.perf_counter() - t0


        # Cap/sample FIRST (before normalizing) to avoid wasted work
        t0 = time.perf_counter()
        df = self._cap_rows(df, training.max_train_rows)
        t_cap = time.perf_counter() - t0

        # Normalize only kept rows
        t0 = time.perf_counter()
        df = _normalize_raw_batch(df)
        t_norm = time.perf_counter() - t0


        t_all = time.perf_counter() - t_all0

        # Print summary (top slow files)
        read_times.sort(reverse=True, key=lambda x: x[0])
        slow = read_times[:10]
        mb_total = bytes_total / (1024 * 1024)

        print(f"[Trainer][IO] files_selected={len(selected_files)} days={len(days_sorted)} bytes={mb_total:.1f}MB")
        print(f"[Trainer][IO] select={t_select:.3f}s read={t_read:.3f}s concat={t_concat:.3f}s norm={t_norm:.3f}s cap={t_cap:.3f}s total_load={t_all:.3f}s")

        if slow:
            print("[Trainer][IO] slowest files:")
            for dt_file, size, path_s, nrows in slow:
                print(f"  {dt_file:.3f}s  {size/(1024*1024):6.1f}MB  rows={nrows:7}  {path_s}")

        trained_on = days_sorted[-1] if days_sorted else datetime.utcnow().date()
        return df, trained_on.isoformat()

    def _cap_rows(self, df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
        if max_rows <= 0 or len(df) <= max_rows:
            return df
        return df.sample(n=max_rows, random_state=0)

    def _read_arrow_file(self, arrow_path: Path, wanted_cols: List[str]) -> pd.DataFrame:
        """Read Apache Arrow Feather file."""
        import pyarrow.feather as feather
        df = feather.read_feather(arrow_path, columns=wanted_cols)
        return df

    def _read_duckdb_file(self, db_path: Path, wanted_cols: List[str], table: Optional[str]) -> pd.DataFrame:
        con = duckdb.connect(database=str(db_path), read_only=True)
        try:
            tname = table or self._auto_pick_table(con, wanted_cols)
            cols_in_table = [r[0] for r in con.execute(f"DESCRIBE {self._quote_ident(tname)}").fetchall()]
            select_cols = [c for c in wanted_cols if c in cols_in_table]
            if not select_cols:
                raise ValueError(f"No wanted columns found in {db_path}::{tname}")

            sel = ", ".join(self._quote_ident(c) for c in select_cols)
            df = con.execute(f"SELECT {sel} FROM {self._quote_ident(tname)}").df()
            return df
        finally:
            con.close()

    def _auto_pick_table(self, con: duckdb.DuckDBPyConnection, wanted_cols: List[str]) -> str:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if not tables:
            raise ValueError("DuckDB file has no tables.")

        wanted = set(wanted_cols)
        best = None
        best_score = -1

        for t in tables:
            try:
                cols = [r[0] for r in con.execute(f"DESCRIBE {self._quote_ident(t)}").fetchall()]
                score = len(wanted.intersection(cols))
                if score > best_score:
                    best_score = score
                    best = t
            except Exception:
                continue

        if best is None:
            raise ValueError("Could not inspect tables in DuckDB.")
        return best

    def _quote_ident(self, s: str) -> str:
        return '"' + s.replace('"', '""') + '"'


# ---------------------------------------------------------------------
# ONNX inference engine (Kafka-friendly)
# ---------------------------------------------------------------------

class OutlierEngine:
    """Execute ONNX inference and anomaly classification for query batches."""

    def __init__(
        self,
        artifact: ModelArtifact,
        *,
        anomaly_thresholds: Optional[AnomalyThresholds] = None,
        inference: InferenceConfig = InferenceConfig(),
        providers: Optional[Sequence[str]] = None,
    ) -> None:
        self.artifact = artifact
        self.inference = inference
        self.thresholds = anomaly_thresholds or AnomalyThresholds(**(artifact.anomaly_thresholds or {}))  # type: ignore[arg-type]

        self._latest_mtime: float = 0.0
        self._sess_mean = None
        self._sess_q = None

        self._providers = tuple(providers) if providers is not None else tuple(
            (artifact.onnx_export_config or {}).get("providers", ("CPUExecutionProvider",))
        )

        self._load_sessions()

    @property
    def stage(self) -> str:
        return self.artifact.stage

    def _load_sessions(self) -> None:
        mean_bytes = Path(self.artifact.onnx_mean_path).read_bytes()
        q_bytes = Path(self.artifact.onnx_q_path).read_bytes()
        self._sess_mean = _make_ort_session(mean_bytes, providers=self._providers)
        self._sess_q = _make_ort_session(q_bytes, providers=self._providers)

    def predict_and_flag_outliers(
        self,
        batch: Union[pd.DataFrame, Sequence[Dict[str, Any]]],
        *,
        anomaly_thresholds: Optional[AnomalyThresholds] = None,
    ) -> List[OutlierHit]:
        """
        Input: batch of raw queries (Kafka message payloads). Must include at least:
          query_type, arrival_timestamp, cluster_size, feature_fingerprint,
          read_table_ids, write_table_ids,
          num_permanent_tables_accessed, num_external_tables_accessed, num_system_tables_accessed,
          num_joins, num_scans, num_aggregations,
          compile_duration_ms, execution_duration_ms

        Optional but strongly recommended for good output:
          instance_id, user_id, database_id, query_id

        Output: compact list of OutlierHit for rows flagged as anomalies.
        """
        thr = anomaly_thresholds or self.thresholds

        if isinstance(batch, pd.DataFrame):
            df_raw = batch.copy()
        else:
            df_raw = pd.DataFrame(list(batch))

        if len(df_raw) == 0:
            return []

        df_raw = _normalize_raw_batch(df_raw)

        # feature prep needs the fingerprint freq map from training
        fp_map = self.artifact.fingerprint_freq_map or {}
        stage = self.artifact.stage
        target = (self.artifact.spec or {}).get("target", "execution_duration_ms")

        df_prep, _spec_obj = prepare_frame_for_stage(
            df_raw,
            stage=stage,
            target=target,
            fingerprint_freq_map=fp_map,
            rare_threshold=int((self.artifact.training_config or {}).get("rare_threshold", 3)),
            drop_cached=False,
            drop_aborted=bool((self.artifact.training_config or {}).get("drop_aborted", True)),
            add_indicators=bool((self.artifact.training_config or {}).get("add_indicators", True)),
        )

        if len(df_prep) == 0:
            return []

        # model expects EXACT spec columns used during training
        spec = self.artifact.spec or {}
        cat_cols = list(spec.get("categorical", []))
        num_cols = list(spec.get("numeric", []))
        needed = cat_cols + num_cols

        # ensure columns exist
        for c in needed:
            if c not in df_prep.columns:
                df_prep[c] = "NA" if c in cat_cols else np.nan

        df_feat = df_prep[needed].copy()
        df_feat = _coerce_feature_dtypes_for_onnx(df_feat, cat_cols=cat_cols, num_cols=num_cols, dtype=self.inference.dtype)

        # Parallel batch processing for ONNX inference
        import concurrent.futures
        import os
        
        pred_mean_all: List[np.ndarray] = []
        pred_q_all: List[np.ndarray] = []

        bs = max(1, int(self.inference.batch_size))
        max_workers = self.inference.max_workers
        
        # Helper function to process a single batch
        def _process_batch(batch_df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
            """Process one batch through both ONNX models in parallel."""
            feeds_m = _ort_inputs_from_df(self._sess_mean, batch_df, dtype=self.inference.dtype)
            feeds_q = _ort_inputs_from_df(self._sess_q, batch_df, dtype=self.inference.dtype)
            
            # Run both models (mean + quantile) in parallel within batch
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                future_mean = executor.submit(self._sess_mean.run, None, feeds_m)
                future_q = executor.submit(self._sess_q.run, None, feeds_q)
                out_m = future_mean.result()[0]
                out_q = future_q.result()[0]
            
            pred_mean = _predict_ms_from_regressor_output(out_m, min_pred_ms=self.inference.min_pred_ms)
            pred_q = _predict_ms_from_regressor_output(out_q, min_pred_ms=self.inference.min_pred_ms)
            return pred_mean, pred_q
        
        # Create batches
        batches = [df_feat.iloc[i : i + bs].copy() for i in range(0, len(df_feat), bs)]
        
        # Process batches in parallel if max_workers > 1
        if max_workers > 1:
            # Parallel execution across batches
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_process_batch, batch) for batch in batches]
                
                # Collect results in order (maintains batch order)
                for future in futures:
                    pred_mean, pred_q = future.result()
                    pred_mean_all.append(pred_mean)
                    pred_q_all.append(pred_q)
        else:
            # Sequential execution (fallback for max_workers=0 or 1)
            for batch in batches:
                pred_mean, pred_q = _process_batch(batch)
                pred_mean_all.append(pred_mean)
                pred_q_all.append(pred_q)

        pred_mean = np.concatenate(pred_mean_all, axis=0)
        pred_q = np.concatenate(pred_q_all, axis=0)

        pred_used = np.maximum(pred_mean, pred_q) if self.inference.conservative else pred_mean

        df_scored = df_prep.copy()
        df_scored["pred_mean_ms"] = pred_mean
        df_scored["pred_q_ms"] = pred_q
        df_scored["pred_execution_ms"] = pred_used

        # ranks + anomaly flags
        if "instance_id" not in df_scored.columns:
            df_scored["instance_id"] = -1
        df_scored = compute_within_instance_ranks(
            df_scored,
            true_col=target,
            pred_col="pred_execution_ms",
            group_col="instance_id",
        )
        df_scored = flag_anomalies(
            df_scored,
            true_col=target,
            pred_col="pred_execution_ms",
            min_long_ms=thr.min_long_ms,
            underestimate_ratio=thr.underestimate_ratio,
            underestimate_delta_ms=thr.underestimate_delta_ms,
            true_quantile_thresh=thr.true_quantile,
            pred_quantile_thresh=thr.pred_quantile,
        )

        df_anom = df_scored[df_scored["is_anomaly"]].copy()
        if len(df_anom) == 0:
            return []

        # compact output
        hits: List[OutlierHit] = []
        for _, r in df_anom.iterrows():
            hits.append(
                OutlierHit(
                    instance_id=r.get("instance_id", None),
                    user_id=r.get("user_id", None),
                    database_id=r.get("database_id", None),
                    query_id=r.get("query_id", None),
                    true_ms=float(r.get("true_ms", 0.0)),
                    pred_ms=float(r.get("pred_ms_used", r.get("pred_execution_ms", 0.0))),
                    pred_mean_ms=float(r.get("pred_mean_ms", 0.0)),
                    pred_q_ms=float(r.get("pred_q_ms", 0.0)),
                    abs_err_ms=float(r.get("abs_err_ms", 0.0)),
                    signed_err_ms=float(r.get("signed_err_ms", 0.0)),
                    under_ratio=float(r.get("under_ratio", 0.0)),
                    anomaly_reason=str(r.get("anomaly_reason", "")),
                    query_type=r.get("query_type", None),
                    arrival_timestamp=r.get("arrival_timestamp", None),
                    cluster_size=r.get("cluster_size", None),
                )
            )
        return hits


# ---------------------------------------------------------------------
# High-level service wrapper: retrain + auto-load latest for inference
# ---------------------------------------------------------------------


class OutlierService:
    """
    Intended usage:
      svc = OutlierService(model_dir="_data/models")

      # NEW API (sharded / multi-file training):
      svc.retrain(train_paths=[...])          # when you want

      hits = svc.detect_outliers(batch_records)  # in Kafka consumer
    """

    def __init__(
        self,
        model_dir: Union[str, Path],
        *,
        inference: InferenceConfig = InferenceConfig(),
        providers: Optional[Sequence[str]] = None,
    ) -> None:
        self.model_dir = Path(model_dir)
        self.store = ModelStore(self.model_dir)
        self.trainer = OutlierTrainer(self.model_dir)

        self.inference = inference
        self._providers = tuple(providers) if providers else None  # None = use artifact defaults
        self._engine: Optional[OutlierEngine] = None
        self._latest_json_mtime: float = 0.0

    def retrain(
        self,
        train_paths: Sequence[Union[str, Path]],
        *,
        training: TrainingConfig = TrainingConfig(),
        anomaly_thresholds: AnomalyThresholds = AnomalyThresholds(),
        onnx: OnnxExportConfig = OnnxExportConfig(),
        duckdb_table: Optional[str] = None,
    ) -> ModelArtifact:
        """
        NEW API: Train from an explicit list of Arrow/Feather shard files.

        `train_paths` is expected to contain 1..N file paths (e.g., *.arrow shards),
        typically covering a rolling N-day window.

        Notes:
          - This method no longer accepts `daily_path` (folder or single file).
          - The trainer is expected to implement: train_from_files(train_paths, ...)
        """
        # Normalize + validate paths early to fail fast
        paths: list[Path] = []
        for p in train_paths:
            pp = Path(p)
            paths.append(pp)

        if not paths:
            raise ValueError("retrain(train_paths=...): got empty train_paths list")

        missing = [str(p) for p in paths if not p.is_file()]
        if missing:
            raise FileNotFoundError(
                "retrain(train_paths=...): some training files do not exist:\n  - "
                + "\n  - ".join(missing[:50])
                + (f"\n  ... ({len(missing) - 50} more)" if len(missing) > 50 else "")
            )

        # Delegate to new trainer API
        train_from_files = getattr(self.trainer, "train_from_files", None)
        if train_from_files is None:
            raise TypeError(
                "OutlierTrainer is missing required method `train_from_files(train_paths, ...)` "
                "to support sharded/multi-file training. Next step: update OutlierTrainer."
            )

        art = train_from_files(
            paths,
            training=training,
            anomaly_thresholds=anomaly_thresholds,
            onnx=onnx,
            duckdb_table=duckdb_table,
        )

        # force reload next detect
        self._engine = None
        self._latest_json_mtime = 0.0
        return art

    def detect_outliers(
        self,
        batch: Union[pd.DataFrame, Sequence[Dict[str, Any]]],
        *,
        anomaly_thresholds: Optional[AnomalyThresholds] = None,
    ) -> List[Dict[str, Any]]:
        eng = self._get_or_reload_engine()
        hits = eng.predict_and_flag_outliers(batch, anomaly_thresholds=anomaly_thresholds)
        # return JSON-friendly dicts for easy Kafka downstream handling
        return [asdict(h) for h in hits]

    def _get_or_reload_engine(self) -> OutlierEngine:
        latest_path = self.store.latest_path
        mtime = latest_path.stat().st_mtime if latest_path.exists() else 0.0

        if self._engine is None or (mtime > 0 and mtime != self._latest_json_mtime):
            art = self.store.load_latest() or self.store.find_latest_by_glob(stage=STAGE_POSTCOMPILE)
            if art is None:
                raise RuntimeError(f"No model found in {self.model_dir}. Train first with retrain().")

            # Use provided providers or fall back to artifact defaults
            providers = self._providers or tuple(
                (art.onnx_export_config or {}).get("providers", ("CPUExecutionProvider",))
            )
            self._engine = OutlierEngine(
                art,
                inference=self.inference,
                providers=providers,
            )
            self._latest_json_mtime = mtime

        return self._engine



# ---------------------------------------------------------------------
# Minimal example
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # This is here so you can quickly sanity test it; it's still a library.
    svc = OutlierService(model_dir="_data/models", inference=InferenceConfig(batch_size=512))

    # 1) Train (once in a while, triggered manually or by automation)
    # art = svc.retrain(
    #     daily_path="daily_duckdbs/",
    #     training=TrainingConfig(train_window_days=14, max_train_rows=400_000),
    #     onnx=OnnxExportConfig(opset=17, dtype="float32"),
    # )

    # 2) Inference in a Kafka consumer: you get only the batch payload
    example_batch = [
        {
            "instance_id": "i-123",
            "user_id": "u-7",
            "database_id": "db-2",
            "query_id": "q-999",
            "query_type": "SELECT",
            "arrival_timestamp": "2026-01-28 12:34:56",
            "cluster_size": 8,
            "feature_fingerprint": "abc123",
            "read_table_ids": ["t1", "t2"],
            "write_table_ids": [],
            "num_permanent_tables_accessed": 2,
            "num_external_tables_accessed": 0,
            "num_system_tables_accessed": 0,
            "num_joins": 3,
            "num_scans": 4,
            "num_aggregations": 1,
            "compile_duration_ms": 250.0,
            "execution_duration_ms": 250_000.0,
        }
    ]

    # hits = svc.detect_outliers(example_batch)
    # print(json.dumps(hits, indent=2, default=str))
    pass
