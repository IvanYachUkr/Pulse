"""Batch pipeline for training and scoring query execution time models.

Provides simulate mode (day-by-day backtesting) and follow mode (incremental
processing of daily parquet files).
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

from business_anomaly_logic import compute_within_instance_ranks, flag_anomalies, per_key_summary
from business_features import (
    ALL_STAGES,
    BOOL_COLS,
    DURATION_COLS,
    EXEC_DERIVED_COLS,
    ID_COLS,
    PLAN_COUNT_COLS,
    TABLE_ACCESS_COUNT_COLS,
    TEXT_LIST_COLS,
    TIME_COLS,
    compute_fingerprint_stats,
    prepare_frame_for_stage,
)
from business_io import (
    create_or_append_table,
    duckdb_connect_mem,
    ensure_tables,
    list_daily_files,
    list_days_in_parquet,
    load_state,
    mark_day_processed,
    normalize_path_for_duckdb,
    read_day_from_parquet,
    read_daily_file_parquet,
    save_state,
    was_day_processed,
)
from business_models import predict_stage, save_model_bundle, train_stage_models


def parse_day(s: str) -> date:
    """Parse ISO format date string."""
    return datetime.fromisoformat(s).date()


def day_range(days: List[date], start: Optional[date], end: Optional[date]) -> List[date]:
    """Filter days to those within optional start/end bounds."""
    if start is not None:
        days = [d for d in days if d >= start]
    if end is not None:
        days = [d for d in days if d <= end]
    return days


def choose_training_days(all_days: List[date], idx: int, window_days: int) -> List[date]:
    """Select training window days before the current day index."""
    start = max(0, idx - window_days)
    return all_days[start:idx]


def sample_cap(df: pd.DataFrame, max_rows: int, seed: int = 0) -> pd.DataFrame:
    """Randomly sample DataFrame if it exceeds max_rows."""
    if max_rows <= 0 or len(df) <= max_rows:
        return df
    return df.sample(n=max_rows, random_state=seed)


def infer_columns_needed(stages: List[str], target: str) -> List[str]:
    """Build list of columns needed for training/inference using shared schema constants."""
    cols = set(
        ID_COLS
        + TIME_COLS
        + DURATION_COLS
        + BOOL_COLS
        + PLAN_COUNT_COLS
        + TABLE_ACCESS_COUNT_COLS
        + EXEC_DERIVED_COLS
        + TEXT_LIST_COLS
        + [
            target,
            "query_type",
            "feature_fingerprint",
        ]
    )
    return sorted(cols)



def write_run_metadata(out_con: duckdb.DuckDBPyConnection, meta: Dict) -> None:
    """Write run metadata to output database."""
    out_con.execute("CREATE TABLE IF NOT EXISTS run_metadata (k VARCHAR, v VARCHAR)")
    df = pd.DataFrame([(k, json.dumps(v, default=str)) for k, v in meta.items()], columns=["k", "v"])
    create_or_append_table(out_con, "run_metadata", df)


def process_day_for_stage(
    *,
    stage: str,
    day_eval: date,
    df_train_raw: Optional[pd.DataFrame],
    df_eval_raw: pd.DataFrame,
    out_con: duckdb.DuckDBPyConnection,
    out_dir: Path,
    target: str,
    quantile: float,
    explode_threshold_ms: float,
    conservative: bool,
    min_pred_ms: float,
    drop_cached_train: bool,
    drop_aborted: bool,
    rare_threshold: int,
    max_train_rows: int,
    anomaly_thresholds: Dict[str, float],
    control_sample: int,
    rng: np.random.Generator,
    reuse_bundle: Optional[Dict] = None,
) -> Dict:
    """Train model (or reuse existing) and score a single day's queries.
    
    Returns model bundle dict that can be reused for subsequent days.
    If reuse_bundle is provided, skips training and uses existing model.
    """

    if reuse_bundle is None:
        if df_train_raw is None or len(df_train_raw) == 0:
            return {}

        # optional cap early (raw cap reduces prep cost)
        df_train_raw = sample_cap(df_train_raw, max_train_rows, seed=0)

        fp_map = compute_fingerprint_stats(df_train_raw, col="feature_fingerprint")

        df_train, spec = prepare_frame_for_stage(
            df_train_raw,
            stage=stage,
            target=target,
            fingerprint_freq_map=fp_map,
            rare_threshold=rare_threshold,
            drop_cached=drop_cached_train,
            drop_aborted=drop_aborted,
            add_indicators=True,
        )

        if len(df_train) < 500:
            return {}

        models = train_stage_models(
            df_train=df_train,
            spec=spec,
            fingerprint_freq_map=fp_map,
            quantile=quantile,
            explode_threshold_ms=explode_threshold_ms,
        )

        # Save model bundle occasionally is fine; saving every day is optional overhead,
        # but we keep it since you wanted reproducibility.
        models_dir = out_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)
        model_path = models_dir / f"model_{stage}_{day_eval.isoformat()}.joblib"
        save_model_bundle(
            str(model_path),
            {
                "stage": stage,
                "day": day_eval.isoformat(),
                "spec": {"stage": spec.stage, "categorical": spec.categorical, "numeric": spec.numeric, "target": spec.target},
                "fingerprint_freq_map": fp_map,
                "explode_threshold_ms": explode_threshold_ms,
                "quantile": quantile,
                "models": models,
            },
        )

        bundle = {"models": models, "spec": spec, "fp_map": fp_map, "trained_on_day": day_eval.isoformat()}
    else:
        bundle = reuse_bundle
        models = bundle["models"]
        spec = bundle["spec"]
        fp_map = bundle["fp_map"]

    # Prepare eval with the training fingerprint map
    df_eval, _ = prepare_frame_for_stage(
        df_eval_raw,
        stage=stage,
        target=target,
        fingerprint_freq_map=fp_map,
        rare_threshold=rare_threshold,
        drop_cached=False,  # keep cached in eval
        drop_aborted=drop_aborted,
        add_indicators=True,
    )

    if len(df_eval) == 0:
        return bundle

    preds = predict_stage(models, df_eval, min_pred_ms=min_pred_ms, conservative=conservative)
    df_scored = df_eval.copy()
    df_scored["stage"] = stage
    df_scored["pred_mean_ms"] = preds["pred_mean_ms"]
    df_scored["pred_q_ms"] = preds["pred_q_ms"]
    df_scored["pred_execution_ms"] = preds["pred_ms"]
    df_scored["p_explode"] = preds["p_explode"]
    df_scored["pred_quartile"] = preds["pred_quartile"]

    df_scored = compute_within_instance_ranks(df_scored, true_col=target, pred_col="pred_execution_ms", group_col="instance_id")
    df_scored = flag_anomalies(
        df_scored,
        true_col=target,
        pred_col="pred_execution_ms",
        min_long_ms=anomaly_thresholds["min_long_ms"],
        underestimate_ratio=anomaly_thresholds["underestimate_ratio"],
        underestimate_delta_ms=anomaly_thresholds["underestimate_delta_ms"],
        true_quantile_thresh=anomaly_thresholds["true_quantile"],
        pred_quantile_thresh=anomaly_thresholds["pred_quantile"],
    )
    df_scored["day"] = day_eval.isoformat()

    df_anom = df_scored[df_scored["is_anomaly"]].copy()
    if len(df_anom) > 0:
        create_or_append_table(out_con, "anomalies", df_anom)

        by_instance = per_key_summary(df_anom, "instance_id")
        by_instance["day"] = day_eval.isoformat()
        by_instance["stage"] = stage
        create_or_append_table(out_con, "daily_instance_anomaly_summary", by_instance)

        by_user = per_key_summary(df_anom, "user_id") if "user_id" in df_anom.columns else pd.DataFrame()
        if len(by_user) > 0:
            by_user["day"] = day_eval.isoformat()
            by_user["stage"] = stage
            create_or_append_table(out_con, "daily_user_anomaly_summary", by_user)

    if control_sample > 0:
        non = df_scored[~df_scored["is_anomaly"]].copy()
        if len(non) > 0:
            k = min(control_sample, len(non))
            idx = rng.choice(len(non), size=k, replace=False)
            ctrl = non.iloc[idx].copy()
            create_or_append_table(out_con, "controls", ctrl)

    mark_day_processed(out_con, day_eval, stage, rows_scored=int(len(df_scored)), rows_anoms=int(len(df_anom)))
    return bundle


def run_simulation(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    state_path = out_dir / "state" / "state.json"
    state = load_state(state_path)

    read_con = duckdb_connect_mem()
    all_days = list_days_in_parquet(read_con, args.input_parquet)
    all_days = day_range(all_days, args.start_day, args.end_day)

    if len(all_days) < 2:
        raise SystemExit("Not enough days in selected range to do train->eval iteration.")

    out_db = out_dir / "db" / "anomalies.duckdb"
    out_db.parent.mkdir(parents=True, exist_ok=True)
    out_con = duckdb.connect(database=str(out_db))
    ensure_tables(out_con)

    write_run_metadata(
        out_con,
        {
            "mode": "simulate",
            "input_parquet": normalize_path_for_duckdb(args.input_parquet),
            "created_at": datetime.now().isoformat(),
            "stages": args.stages,
            "train_window_days": args.train_window_days,
            "min_train_days": args.min_train_days,
            "target": args.target,
            "quantile": args.quantile,
            "explode_threshold_ms": args.explode_threshold_ms,
            "conservative": args.conservative,
            "max_train_rows": args.train_max_rows,
            "retrain_every_days": args.retrain_every_days,
            "cache_raw_days": bool(args.cache_raw_days),
            "cache_max_days": int(args.cache_max_days),
            "anomaly_thresholds": {
                "min_long_ms": args.min_long_ms,
                "underestimate_ratio": args.underestimate_ratio,
                "underestimate_delta_ms": args.underestimate_delta_ms,
                "true_quantile": args.true_quantile,
                "pred_quantile": args.pred_quantile,
            },
        },
    )

    cols = infer_columns_needed(args.stages, target=args.target)
    rng = np.random.default_rng(args.seed)

    # raw-day cache to avoid re-reading parquet repeatedly
    raw_day_cache: Dict[date, pd.DataFrame] = {}

    def get_day_df(d: date) -> pd.DataFrame:
        if args.cache_raw_days and d in raw_day_cache:
            return raw_day_cache[d]
        df = read_day_from_parquet(read_con, args.input_parquet, d, columns=cols)
        if args.cache_raw_days:
            raw_day_cache[d] = df
            # crude cache eviction
            if len(raw_day_cache) > int(args.cache_max_days):
                # drop oldest cached day
                oldest = sorted(raw_day_cache.keys())[0]
                raw_day_cache.pop(oldest, None)
        return df

    # stage model reuse: {stage: (last_trained_idx, bundle)}
    stage_state: Dict[str, Tuple[int, Dict]] = {}

    # Iterate days; for day i we score day i using model trained on prior window
    for i in tqdm(range(1, len(all_days)), desc="days"):
        d_eval = all_days[i]

        df_eval_raw = get_day_df(d_eval)
        if len(df_eval_raw) == 0:
            continue

        for stage in args.stages:
            if was_day_processed(out_con, d_eval, stage):
                continue

            # decide whether to retrain
            last = stage_state.get(stage, None)
            need_retrain = True
            if last is not None:
                last_idx, _bundle = last
                if args.retrain_every_days > 1 and (i - last_idx) < int(args.retrain_every_days):
                    need_retrain = False

            bundle_to_use: Optional[Dict] = None
            df_train_raw: Optional[pd.DataFrame] = None

            if need_retrain:
                train_days = choose_training_days(all_days, i, args.train_window_days)
                if len(train_days) < args.min_train_days:
                    # not enough history yet
                    continue

                train_frames: List[pd.DataFrame] = []
                for td in train_days:
                    df_td = get_day_df(td)
                    if len(df_td) > 0:
                        train_frames.append(df_td)
                if not train_frames:
                    continue
                df_train_raw = pd.concat(train_frames, ignore_index=True)
            else:
                bundle_to_use = last[1]

            bundle = process_day_for_stage(
                stage=stage,
                day_eval=d_eval,
                df_train_raw=df_train_raw,
                df_eval_raw=df_eval_raw,
                out_con=out_con,
                out_dir=out_dir,
                target=args.target,
                quantile=args.quantile,
                explode_threshold_ms=args.explode_threshold_ms,
                conservative=args.conservative,
                min_pred_ms=args.min_pred_ms,
                drop_cached_train=args.drop_cached_train,
                drop_aborted=args.drop_aborted,
                rare_threshold=args.rare_threshold,
                max_train_rows=args.train_max_rows,
                anomaly_thresholds={
                    "min_long_ms": args.min_long_ms,
                    "underestimate_ratio": args.underestimate_ratio,
                    "underestimate_delta_ms": args.underestimate_delta_ms,
                    "true_quantile": args.true_quantile,
                    "pred_quantile": args.pred_quantile,
                },
                control_sample=args.control_sample,
                rng=rng,
                reuse_bundle=bundle_to_use,
            )

            # if we retrained successfully, update state
            if need_retrain and bundle:
                stage_state[stage] = (i, bundle)

    out_con.close()
    read_con.close()
    save_state(state_path, state)


def run_follow(args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    daily_dir = Path(args.daily_dir)
    if not daily_dir.exists():
        raise SystemExit(f"daily_dir does not exist: {daily_dir}")

    state_path = out_dir / "state" / "state.json"
    state = load_state(state_path)

    out_db = out_dir / "db" / "anomalies.duckdb"
    out_db.parent.mkdir(parents=True, exist_ok=True)
    out_con = duckdb.connect(database=str(out_db))
    ensure_tables(out_con)

    write_run_metadata(
        out_con,
        {
            "mode": "follow",
            "daily_dir": str(daily_dir),
            "pattern": args.pattern,
            "created_at": datetime.now().isoformat(),
            "stages": args.stages,
            "train_window_days": args.train_window_days,
            "min_train_days": args.min_train_days,
            "target": args.target,
            "quantile": args.quantile,
            "explode_threshold_ms": args.explode_threshold_ms,
            "conservative": args.conservative,
            "max_train_rows": args.train_max_rows,
        },
    )

    rng = np.random.default_rng(args.seed)
    read_con = duckdb_connect_mem()

    daily_files = list_daily_files(daily_dir, pattern=args.pattern)
    if not daily_files:
        raise SystemExit("No daily files found with a parsable YYYY-MM-DD in the filename.")

    days = [d for d, _ in daily_files]
    day_to_path = {d: p for d, p in daily_files}

    cols = infer_columns_needed(args.stages, target=args.target)

    # In follow mode we do a single pass; your streaming system can run this daily.
    stage_state: Dict[str, Tuple[int, Dict]] = {}

    for i, d_eval in enumerate(days):
        df_eval_raw = read_daily_file_parquet(read_con, day_to_path[d_eval], columns=cols)
        if len(df_eval_raw) == 0:
            continue

        for stage in args.stages:
            if was_day_processed(out_con, d_eval, stage):
                continue

            # follow-mode retrain policy: still supported, but window is small anyway
            last = stage_state.get(stage, None)
            need_retrain = True
            if last is not None:
                last_idx, _bundle = last
                if args.retrain_every_days > 1 and (i - last_idx) < int(args.retrain_every_days):
                    need_retrain = False

            bundle_to_use: Optional[Dict] = None
            df_train_raw: Optional[pd.DataFrame] = None

            if need_retrain:
                train_days = days[max(0, i - args.train_window_days) : i]
                if len(train_days) < args.min_train_days:
                    continue

                train_frames: List[pd.DataFrame] = []
                for td in train_days:
                    df_td = read_daily_file_parquet(read_con, day_to_path[td], columns=cols)
                    if len(df_td) > 0:
                        train_frames.append(df_td)
                if not train_frames:
                    continue
                df_train_raw = pd.concat(train_frames, ignore_index=True)
            else:
                bundle_to_use = last[1]

            bundle = process_day_for_stage(
                stage=stage,
                day_eval=d_eval,
                df_train_raw=df_train_raw,
                df_eval_raw=df_eval_raw,
                out_con=out_con,
                out_dir=out_dir,
                target=args.target,
                quantile=args.quantile,
                explode_threshold_ms=args.explode_threshold_ms,
                conservative=args.conservative,
                min_pred_ms=args.min_pred_ms,
                drop_cached_train=args.drop_cached_train,
                drop_aborted=args.drop_aborted,
                rare_threshold=args.rare_threshold,
                max_train_rows=args.train_max_rows,
                anomaly_thresholds={
                    "min_long_ms": args.min_long_ms,
                    "underestimate_ratio": args.underestimate_ratio,
                    "underestimate_delta_ms": args.underestimate_delta_ms,
                    "true_quantile": args.true_quantile,
                    "pred_quantile": args.pred_quantile,
                },
                control_sample=args.control_sample,
                rng=rng,
                reuse_bundle=bundle_to_use,
            )

            if need_retrain and bundle:
                stage_state[stage] = (i, bundle)

    out_con.close()
    read_con.close()
    save_state(state_path, state)


def main() -> None:
    ap = argparse.ArgumentParser(description="Daily training + anomaly extraction pipeline (simulation or folder-follow).")

    ap.add_argument("--mode", choices=["simulate", "follow"], required=True)

    # simulate mode: one big parquet
    ap.add_argument("--input-parquet", type=str, default="", help="Path to full parquet (simulate mode).")
    ap.add_argument("--start-day", type=parse_day, default=None)
    ap.add_argument("--end-day", type=parse_day, default=None)

    # follow mode: daily parquet dumps
    ap.add_argument("--daily-dir", type=str, default="", help="Folder with daily dumps (follow mode).")
    ap.add_argument("--pattern", type=str, default="*.parquet", help="Glob pattern, filenames should contain YYYY-MM-DD.")

    ap.add_argument("--out-dir", type=str, default="out_business", help="Output folder created in CWD.")
    ap.add_argument("--stages", type=str, default="admission,postcompile,full")

    ap.add_argument("--target", type=str, default="execution_duration_ms")

    # rolling training
    ap.add_argument("--train-window-days", type=int, default=7)
    ap.add_argument("--min-train-days", type=int, default=2)
    ap.add_argument("--train-max-rows", type=int, default=700_000)

    # speed knobs (optional)
    ap.add_argument("--retrain-every-days", type=int, default=1, help="Reuse models for K days (simulate/follow). 1 = retrain every day.")
    ap.add_argument("--cache-raw-days", action="store_true", default=False, help="Cache raw per-day frames in simulate mode.")
    ap.add_argument("--cache-max-days", type=int, default=16, help="Max days to keep in raw cache (simulate mode).")

    # models
    ap.add_argument("--quantile", type=float, default=0.90)
    ap.add_argument("--explode-threshold-ms", type=float, default=600_000.0)
    ap.add_argument("--conservative", action="store_true", default=True)
    ap.add_argument("--no-conservative", action="store_false", dest="conservative")
    ap.add_argument("--min-pred-ms", type=float, default=1.0)
    ap.add_argument("--rare-threshold", type=int, default=3)

    ap.add_argument("--drop-cached-train", action="store_true", default=True)
    ap.add_argument("--keep-cached-train", action="store_false", dest="drop_cached_train")
    ap.add_argument("--drop-aborted", action="store_true", default=True)
    ap.add_argument("--keep-aborted", action="store_false", dest="drop_aborted")

    # anomaly thresholds
    ap.add_argument("--min-long-ms", type=float, default=60_000.0)
    ap.add_argument("--underestimate-ratio", type=float, default=10.0)
    ap.add_argument("--underestimate-delta-ms", type=float, default=120_000.0)
    ap.add_argument("--true-quantile", type=float, default=0.99)
    ap.add_argument("--pred-quantile", type=float, default=0.90)

    # optional controls
    ap.add_argument("--control-sample", type=int, default=0)

    ap.add_argument("--seed", type=int, default=0)

    args = ap.parse_args()

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    for s in stages:
        if s not in ALL_STAGES:
            raise SystemExit(f"Unknown stage '{s}'. Allowed: {ALL_STAGES}")
    args.stages = stages

    if args.retrain_every_days < 1:
        raise SystemExit("--retrain-every-days must be >= 1")

    if args.mode == "simulate":
        if not args.input_parquet:
            raise SystemExit("--input-parquet is required for simulate mode.")
        run_simulation(args)
    else:
        if not args.daily_dir:
            raise SystemExit("--daily-dir is required for follow mode.")
        run_follow(args)


if __name__ == "__main__":
    main()
