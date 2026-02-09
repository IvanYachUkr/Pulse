"""ML model building, training, and prediction for query execution time estimation.

Uses HistGradientBoosting models with log-transformed targets for both mean 
and quantile regression.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from joblib import dump, load
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from business_features import FeatureSpec


def _safe_expm1(x: np.ndarray) -> np.ndarray:
    """Safely compute expm1 with clipping to prevent overflow."""
    x = np.clip(x, -50.0, 50.0)
    return np.expm1(x)


def _log1p_clip(y: np.ndarray) -> np.ndarray:
    """Log-transform target values, handling NaN and negative values."""
    y = np.nan_to_num(y, nan=0.0)
    y = np.clip(y, 0.0, None)
    return np.log1p(y)


def _build_preprocessor(spec: FeatureSpec) -> ColumnTransformer:
    """Build sklearn preprocessor for categorical encoding and numeric passthrough."""
    return ColumnTransformer(
        transformers=[
            ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), spec.categorical),
            ("num", "passthrough", spec.numeric),
        ],
        remainder="drop",
        sparse_threshold=0.0,
    )


def build_mean_regressor() -> HistGradientBoostingRegressor:
    """
    Mean regressor with optimized hyperparameters.
    
    Key improvements over previous config:
    - early_stopping=True: Stops training when validation loss plateaus
    - max_depth=6: Shallower trees reduce overfitting (was 10)
    - learning_rate=0.05: Lower rate for better generalization
    - l2_regularization=0.1: Meaningful regularization (was 1e-6)
    - max_iter=500: Higher ceiling, but early stopping will halt earlier
    """
    return HistGradientBoostingRegressor(
        loss="squared_error",
        max_depth=6,
        learning_rate=0.05,
        max_iter=500,
        min_samples_leaf=50,
        l2_regularization=0.1,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=15,
        random_state=0,
    )


def build_quantile_regressor(q: float) -> HistGradientBoostingRegressor:
    """
    Quantile regressor (default 90th percentile) with optimized hyperparameters.
    
    Same improvements as mean regressor - early stopping, shallower trees,
    and meaningful regularization for better generalization.
    """
    return HistGradientBoostingRegressor(
        loss="quantile",
        quantile=float(q),
        max_depth=6,
        learning_rate=0.05,
        max_iter=500,
        min_samples_leaf=50,
        l2_regularization=0.1,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=15,
        random_state=0,
    )


def build_explode_classifier() -> HistGradientBoostingClassifier:
    """Build classifier for predicting extremely long queries (>10min)."""
    return HistGradientBoostingClassifier(
        loss="log_loss",
        max_depth=8,
        learning_rate=0.08,
        max_iter=250,
        min_samples_leaf=80,
        l2_regularization=1e-6,
        random_state=0,
    )


def build_quartile_classifier() -> HistGradientBoostingClassifier:
    """Build classifier for predicting query duration quartile."""
    return HistGradientBoostingClassifier(
        loss="log_loss",
        max_depth=8,
        learning_rate=0.08,
        max_iter=250,
        min_samples_leaf=80,
        l2_regularization=1e-6,
        random_state=0,
    )


def _fit_regressor(df: pd.DataFrame, spec: FeatureSpec, reg: HistGradientBoostingRegressor) -> Pipeline:
    """Fit a regressor pipeline on log-transformed target."""
    X = df[spec.all_features()].copy()
    y = pd.to_numeric(df[spec.target], errors="coerce").to_numpy(dtype=float)
    ylog = _log1p_clip(y)
    model = Pipeline([("pre", _build_preprocessor(spec)), ("reg", reg)])
    model.fit(X, ylog)
    return model


def _predict_regressor_ms(model: Pipeline, df: pd.DataFrame, spec: FeatureSpec, min_pred_ms: float) -> np.ndarray:
    """Predict milliseconds from a log-trained regressor model."""
    X = df[spec.all_features()].copy()
    pred_log = model.predict(X)
    pred_ms = _safe_expm1(pred_log)
    pred_ms = np.clip(pred_ms, float(min_pred_ms), None)
    return pred_ms.astype(np.float64)


def _fit_classifier(df: pd.DataFrame, spec: FeatureSpec, clf: HistGradientBoostingClassifier, y_cls: np.ndarray) -> Pipeline:
    """Fit a classifier pipeline."""
    X = df[spec.all_features()].copy()
    model = Pipeline([("pre", _build_preprocessor(spec)), ("clf", clf)])
    model.fit(X, y_cls)
    return model


def _predict_proba_pos(model: Pipeline, df: pd.DataFrame, spec: FeatureSpec) -> np.ndarray:
    """Predict probability of positive class."""
    X = df[spec.all_features()].copy()
    proba = model.predict_proba(X)
    if proba.shape[1] == 1:
        return np.zeros(len(df), dtype=np.float64)
    return proba[:, 1].astype(np.float64)


def _predict_quartile(model: Pipeline, df: pd.DataFrame, spec: FeatureSpec) -> np.ndarray:
    """Predict quartile class label."""
    X = df[spec.all_features()].copy()
    return model.predict(X).astype(np.int64)


@dataclass
class StageModels:
    """Container for trained models and metadata for a prediction stage.
    
    The mean and quantile models are always trained. The optional explode and
    quartile classifiers are only trained when train_classifiers=True.
    """
    stage: str
    spec: FeatureSpec
    fingerprint_freq_map: Dict[str, int]

    mean_model: Pipeline
    quantile_model: Pipeline

    # Optional classifiers
    explode_model: Optional[Pipeline] = None
    quartile_model: Optional[Pipeline] = None

    explode_threshold_ms: float = 600_000.0
    quartile_edges_ms: Optional[Tuple[float, float, float]] = None


def train_stage_models(
    df_train: pd.DataFrame,
    spec: FeatureSpec,
    fingerprint_freq_map: Dict[str, int],
    quantile: float = 0.90,
    explode_threshold_ms: float = 600_000.0,
    train_classifiers: bool = False,
) -> StageModels:
    """Train mean and quantile regressors, plus optional classifiers.
    
    Args:
        df_train: Training DataFrame with features and target
        spec: Feature specification
        fingerprint_freq_map: Frequency encoding for fingerprints
        quantile: Target quantile for quantile regressor (default 0.90)
        explode_threshold_ms: Threshold for "exploding" query classification
        train_classifiers: Whether to train explode/quartile classifiers
    """
    y = pd.to_numeric(df_train[spec.target], errors="coerce").to_numpy(dtype=float)
    y = np.nan_to_num(y, nan=0.0)
    y = np.clip(y, 0.0, None)

    # Always train regressors
    mean_model = _fit_regressor(df_train, spec, build_mean_regressor())
    q_model = _fit_regressor(df_train, spec, build_quantile_regressor(quantile))

    # Optional classifiers
    explode_model = None
    quartile_model = None
    edges = None

    if train_classifiers:
        y_explode = (y >= float(explode_threshold_ms)).astype(np.int8)
        explode_model = _fit_classifier(df_train, spec, build_explode_classifier(), y_explode)

        q25, q50, q75 = np.quantile(y, [0.25, 0.50, 0.75]).tolist()
        edges = (float(q25), float(q50), float(q75))
        y_quart = np.digitize(y, bins=[edges[0], edges[1], edges[2]], right=True).astype(np.int8)
        quartile_model = _fit_classifier(df_train, spec, build_quartile_classifier(), y_quart)

    return StageModels(
        stage=spec.stage,
        spec=spec,
        fingerprint_freq_map=fingerprint_freq_map,
        mean_model=mean_model,
        quantile_model=q_model,
        explode_model=explode_model,
        quartile_model=quartile_model,
        explode_threshold_ms=float(explode_threshold_ms),
        quartile_edges_ms=edges,
    )


def predict_stage(
    models: StageModels,
    df_eval: pd.DataFrame,
    min_pred_ms: float = 1.0,
    conservative: bool = True,
) -> Dict[str, np.ndarray]:
    """Generate predictions from trained models.
    
    Returns dict with pred_mean_ms, pred_q_ms, pred_ms (combined),
    p_explode, and pred_quartile arrays.
    """
    pred_mean = _predict_regressor_ms(models.mean_model, df_eval, models.spec, min_pred_ms=min_pred_ms)
    pred_q = _predict_regressor_ms(models.quantile_model, df_eval, models.spec, min_pred_ms=min_pred_ms)
    if conservative:
        pred = np.maximum(pred_mean, pred_q)
    else:
        pred = pred_mean

    if models.explode_model is not None:
        p_explode = _predict_proba_pos(models.explode_model, df_eval, models.spec)
    else:
        p_explode = np.zeros(len(df_eval), dtype=np.float64)

    if models.quartile_model is not None:
        pred_quart = _predict_quartile(models.quartile_model, df_eval, models.spec)
    else:
        pred_quart = np.zeros(len(df_eval), dtype=np.int64)

    return {
        "pred_mean_ms": pred_mean,
        "pred_q_ms": pred_q,
        "pred_ms": pred,
        "p_explode": p_explode,
        "pred_quartile": pred_quart,
    }


def save_model_bundle(path: str, bundle: Dict) -> None:
    """Save model bundle to disk using joblib."""
    dump(bundle, path)


def load_model_bundle(path: str) -> Dict:
    """Load model bundle from disk."""
    return load(path)
