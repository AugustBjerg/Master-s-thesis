import os
import sys
import joblib
import shap
import numpy as np
import pandas as pd
from pathlib import Path
from loguru import logger
from gam import SklearnGAM

# ---------------------------------------------------------------------------
# Add cleaning-scripts to path so we can import config
# ---------------------------------------------------------------------------
_script_dir = os.path.dirname(os.path.abspath(__file__))
_cleaning_scripts_dir = os.path.join(_script_dir, "..", "data", "cleaning-scripts")
sys.path.insert(0, os.path.abspath(_cleaning_scripts_dir))

from config import WINDOW_LENGTH, INCLUDE_VOYAGE_DUMMIES, VOYAGE_DUMMY_PREFIX, WEATHER_FEATURES, NON_WEATHER_FEATURES, KERNEL_BACKGROUND_SIZE  # noqa: E402

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_data_dir = os.path.join(_script_dir, "..", "data")
_BASE_FEATURES = NON_WEATHER_FEATURES + WEATHER_FEATURES

NPZ_PATH = os.path.join(_data_dir, "train-and-test", f"train_test_splits_{WINDOW_LENGTH}.npz")
TIMESTAMPS_PATH = os.path.join(_data_dir, "train-and-test", f"train_test_timestamps_{WINDOW_LENGTH}.npz")
MODELS_DIR = os.path.join(_script_dir, "..", "outputs", "models")
SHAP_OUTPUT_DIR = Path(_script_dir) / ".." / "outputs" / "shap"

DEBUG_SHAP = False          # set to False when running full SHAP
DEBUG_SHAP_N = 200 

MODEL_NAMES = [
    "ridge_regression", 
    "GAM",
    "random_forest", 
    "NN"]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_train_data() -> tuple[pd.DataFrame, np.ndarray, pd.DatetimeIndex]:
    """Load X_train, y_train, and timestamps from the pre-computed split."""
    data = np.load(NPZ_PATH, allow_pickle=True)
    feature_cols = data["X_columns"].tolist()

    if INCLUDE_VOYAGE_DUMMIES:
        voyage_cols = sorted([c for c in feature_cols if c.startswith(VOYAGE_DUMMY_PREFIX)])
    else:
        voyage_cols = []

    all_features = _BASE_FEATURES + voyage_cols
    X_train = pd.DataFrame(data["X_train"], columns=feature_cols)[all_features]
    y_train = data["y_train"].ravel()

    ts = np.load(TIMESTAMPS_PATH)
    train_timestamps = pd.to_datetime(ts["train_timestamps"])

    logger.info(f"Loaded X_train ({X_train.shape}) and timestamps ({len(train_timestamps)})")
    return X_train, y_train, train_timestamps


def load_pipeline(model_name: str):
    """Load a fitted pipeline from disk."""
    path = os.path.join(MODELS_DIR, f"{model_name}_pipeline_incl_fouling_{WINDOW_LENGTH}.joblib")
    pipeline = joblib.load(path)
    logger.info(f"Loaded pipeline from {path}")
    return pipeline

# ---------------------------------------------------------------------------
# SHAP computation
# ---------------------------------------------------------------------------

def compute_shap_values(pipeline, X_train: pd.DataFrame, model_name: str) -> tuple[np.ndarray, float]:
    """Compute SHAP values, choosing the right explainer per model type.

    Returns (shap_values, base_value) where shap_values has shape (n_samples, n_features).
    """
    if model_name == "random_forest":
        # TreeExplainer: fast, exact — operates on the raw tree model with pre-scaled data
        preprocessor = pipeline.named_steps["preprocessor"]
        rf_model = pipeline.named_steps["model"]
        X_scaled = preprocessor.transform(X_train)
        explainer = shap.TreeExplainer(rf_model)
        shap_values = explainer.shap_values(X_scaled)
        base_value = float(np.squeeze(explainer.expected_value))

    else:
        # KernelExplainer: model-agnostic — works on the full pipeline.predict.
        # Wrap in a lambda so SHAP cannot access (and try to set) pipeline attributes.
        predict_fn = lambda X: pipeline.predict(pd.DataFrame(X, columns=X_train.columns))  # noqa: E731
        background = X_train.sample(n=KERNEL_BACKGROUND_SIZE, random_state=42)
        explainer = shap.KernelExplainer(predict_fn, background)
        shap_values = explainer.shap_values(X_train)
        base_value = float(explainer.expected_value)

    logger.info(f"  SHAP values shape: {shap_values.shape}, base value: {base_value:.4f}")
    return shap_values, base_value

# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def build_wide_df(shap_values: np.ndarray, X_train: pd.DataFrame, train_timestamps: pd.DatetimeIndex) -> pd.DataFrame:
    """Wide SHAP matrix (n_samples x n_features) with timestamps."""
    df = pd.DataFrame(shap_values, columns=X_train.columns, index=X_train.index)
    df.insert(0, "timestamp", train_timestamps.values[: len(df)])
    return df


def build_long_df(
    shap_values: np.ndarray,
    X_train: pd.DataFrame,
    train_timestamps: pd.DatetimeIndex,
    model_name: str,
    base_value: float,
) -> pd.DataFrame:
    """Long / tidy SHAP table: one row per (sample, feature)."""
    shap_wide = pd.DataFrame(shap_values, columns=X_train.columns, index=X_train.index)

    shap_long = (
        shap_wide
        .reset_index(names="row_id")
        .melt(id_vars="row_id", var_name="feature_name", value_name="shap_value")
    )

    feature_long = (
        X_train
        .reset_index(names="row_id")
        .melt(id_vars="row_id", var_name="feature_name", value_name="feature_value")
    )

    shap_long = shap_long.merge(feature_long, on=["row_id", "feature_name"], how="left")
    shap_long["timestamp"] = shap_long["row_id"].map(
        dict(enumerate(train_timestamps.values[: len(X_train)]))
    )
    shap_long["model_name"] = model_name
    shap_long["base_value"] = base_value

    return shap_long

# ---------------------------------------------------------------------------
# Saving
# ---------------------------------------------------------------------------

def save_shap(wide_df: pd.DataFrame, long_df: pd.DataFrame, model_name: str) -> None:
    """Write wide and long SHAP CSV files."""
    out_dir = SHAP_OUTPUT_DIR.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    wide_path = out_dir / f"{model_name}_incl_foul_shap_wide_{WINDOW_LENGTH}.csv"
    long_path = out_dir / f"{model_name}_incl_foul_shap_long_{WINDOW_LENGTH}.csv"

    wide_df.to_csv(wide_path, index=False)
    long_df.to_csv(long_path, index=False)

    logger.info(f"  Saved {wide_path.name}  ({wide_df.shape})")
    logger.info(f"  Saved {long_path.name}  ({long_df.shape})")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    logger.info("=== SHAP computation started ===")
    logger.info(f"Window length: {WINDOW_LENGTH}")
    logger.info(f"Models: {MODEL_NAMES}")

    X_train, y_train, train_timestamps = load_train_data()

    if DEBUG_SHAP:
        logger.warning(f"DEBUG_SHAP=True → subsampling to {DEBUG_SHAP_N} rows")
        # keep timestamps aligned with the sampled rows
        sample_idx = X_train.sample(n=min(DEBUG_SHAP_N, len(X_train)),
                                    random_state=42).index
        X_train = X_train.loc[sample_idx].reset_index(drop=True)
        y_train = y_train[sample_idx]
        train_timestamps = train_timestamps[sample_idx]
    else:
        X_train = X_train.reset_index(drop=True)

    for model_name in MODEL_NAMES:
        logger.info(f"Processing {model_name} …")
        pipeline = load_pipeline(model_name)
        shap_values, base_value = compute_shap_values(pipeline, X_train, model_name)

        wide_df = build_wide_df(shap_values, X_train, train_timestamps)
        long_df = build_long_df(shap_values, X_train, train_timestamps, model_name, base_value)
        save_shap(wide_df, long_df, model_name)

    logger.info("=== SHAP computation finished ===")


if __name__ == "__main__":
    main()
