import os
import sys
import warnings
import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from loguru import logger
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.exceptions import ConvergenceWarning
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)
from sklearn.model_selection import KFold, cross_validate, train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Add cleaning-scripts to path to import config
# ---------------------------------------------------------------------------
_script_dir = os.path.dirname(os.path.abspath(__file__))
_cleaning_scripts_dir = os.path.join(_script_dir, "..", "data", "cleaning-scripts")
sys.path.insert(0, os.path.abspath(_cleaning_scripts_dir))

patience = 5

from config import (  # noqa: E402
    FOULING_PROXY_VAR_NAME_WITH_UNIT,
    INCLUDE_VOYAGE_DUMMIES,
    TARGET_VARIABLE,
    WEATHER_FEATURES,
    NON_WEATHER_FEATURES,
    VOYAGE_DUMMY_PREFIX,
    WINDOW_LENGTH,
    N_CV_SPLITS,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RANDOM_SEED = 42
_BASE_FEATURES = NON_WEATHER_FEATURES + WEATHER_FEATURES

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_data_dir = os.path.join(_script_dir, "..", "data")
NPZ_PATH = os.path.join(_data_dir, "train-and-test", f"train_test_splits_{WINDOW_LENGTH}.npz")
MODELS_OUTPUT_DIR = os.path.join(_script_dir, "..", "outputs", "models")

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(npz_path: str) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    """Load the pre-computed train/test split from an .npz file."""
    data = np.load(npz_path, allow_pickle=True)
    feature_cols = data["X_columns"].tolist()
    if INCLUDE_VOYAGE_DUMMIES:
        voyage_cols = sorted([c for c in feature_cols if c.startswith(VOYAGE_DUMMY_PREFIX)])
    else:
        voyage_cols = []
        logger.info("Voyage dummies disabled; excluding voyage dummy columns.")
    all_features = _BASE_FEATURES + voyage_cols

    X_train = pd.DataFrame(data["X_train"], columns=feature_cols)[all_features]
    X_test = pd.DataFrame(data["X_test"], columns=feature_cols)[all_features]
    y_train = pd.Series(data["y_train"].ravel(), name=TARGET_VARIABLE)
    y_test = pd.Series(data["y_test"].ravel(), name=TARGET_VARIABLE)

    logger.info(f"Loaded train/test split from {npz_path}")
    logger.info(f"Train rows: {len(X_train):,} | Test rows: {len(X_test):,}")
    logger.info(f"Voyage dummy columns ({len(voyage_cols)}): {voyage_cols}")
    return X_train, X_test, y_train, y_test

# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline(feature_list: list[str], patience: int) -> Pipeline:
    """Assemble the full sklearn Pipeline (scaler → NN)."""
    preprocessor = ColumnTransformer(
        transformers=[
            ("scaler", StandardScaler(), feature_list),
        ]
    )

    # Hyperparameters
    nn_model = MLPRegressor(
        hidden_layer_sizes=(128, 64),
        activation="relu",
        alpha=0.01,
        learning_rate_init=0.001,
        max_iter=1000,
        early_stopping=True,
        n_iter_no_change=patience,
        validation_fraction=0.1,
        random_state=RANDOM_SEED,
        verbose=False,
    )

    return Pipeline([
        ("preprocessor", preprocessor),
        ("model", nn_model),
    ])

# ---------------------------------------------------------------------------
# Training curves
# ---------------------------------------------------------------------------

def plot_training_curves(
    pipeline: Pipeline,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    model_name: str,
    output_dir: str,
    patience: int,
) -> None:
    """Plot one curve chart with training and validation RMSE by iteration.

    A cloned pipeline is trained with max_iter=1 and warm_start=True so we can
    record both training and validation RMSE at each optimization step.
    """
    curve_pipeline = clone(pipeline)
    mlp: MLPRegressor = curve_pipeline.named_steps["model"]

    n_iterations = int(mlp.max_iter)
    validation_fraction = float(getattr(mlp, "validation_fraction", 0.1))
    if not 0 < validation_fraction < 1:
        validation_fraction = 0.1

    X_tr, X_val, y_tr, y_val = train_test_split(
        X_train,
        y_train,
        test_size=validation_fraction,
        random_state=RANDOM_SEED,
    )

    curve_pipeline.set_params(
        model__max_iter=1,
        model__warm_start=True,
        model__early_stopping=False,
        model__verbose=False,
    )

    logger.info(f"Generating RMSE training curves for {model_name} ({n_iterations} iterations)")
    patience = patience
    tol = 0.1
    best_val_rmse = np.inf
    no_improve_count = 0
    best_iter = 0
    train_rmse, val_rmse = [], []

    for i in range(n_iterations):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConvergenceWarning)
            curve_pipeline.fit(X_tr, y_tr)

        current_train_rmse = root_mean_squared_error(y_tr, curve_pipeline.predict(X_tr))
        current_val_rmse = root_mean_squared_error(y_val, curve_pipeline.predict(X_val))
        train_rmse.append(current_train_rmse)
        val_rmse.append(current_val_rmse)

        if current_val_rmse < best_val_rmse - tol:
            best_val_rmse = current_val_rmse
            no_improve_count = 0
            best_iter = i
        else:
            no_improve_count += 1
            if no_improve_count >= patience:
                logger.info(f"Early stopping curves at iteration {i + 1} (best: {best_iter + 1})")
                break

    iterations = np.arange(1, len(train_rmse) + 1)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(iterations, train_rmse, color="steelblue", linewidth=1.5, label="Training RMSE")
    ax.plot(iterations, val_rmse, color="coral", linewidth=1.5, label="Validation RMSE")

    ax.axvline(
        best_iter + 1,
        color="gray",
        linestyle="--",
        linewidth=1.2,
        label=f"Best val iter: {best_iter + 1} (RMSE={val_rmse[best_iter]:.4f})",
    )
    ax.set_xlabel("Iteration")
    ax.set_ylabel("RMSE")
    ax.set_yscale("log")
    ax.set_title(f"{model_name} - Training vs Validation RMSE")
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.suptitle(model_name, fontsize=11, fontweight="bold", y=1.02)
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    save_path = os.path.join(output_dir, f"{model_name}_training_curves.png")
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Training curves saved to {save_path}")

# ---------------------------------------------------------------------------
# Training and evaluation
# ---------------------------------------------------------------------------

def train_and_evaluate(
    pipeline: Pipeline,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    patience: int,
    model_name: str = "NN",
) -> tuple[Pipeline, tuple[str, dict]]:
    """Cross-validate, fit on full training set, and compute holdout metrics."""
    cv = KFold(n_splits=N_CV_SPLITS, shuffle=True, random_state=RANDOM_SEED)

    scoring = {
        "mape": "neg_mean_absolute_percentage_error",
        "rmse": "neg_root_mean_squared_error",
        "mae": "neg_mean_absolute_error",
    }

    logger.info("Fitting on full training set …")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConvergenceWarning)
        pipeline.fit(X_train, y_train)

    # Plot training curves right after fitting
    plot_training_curves(pipeline, X_train, y_train, model_name, MODELS_OUTPUT_DIR, patience=patience)

    y_pred_train = pipeline.predict(X_train)
    y_pred_test = pipeline.predict(X_test)

    metrics = {
        "train_rmse": root_mean_squared_error(y_train, y_pred_train),
        "cv_rmse_mean": 0,
        "cv_rmse_std": 0,
        "test_rmse": root_mean_squared_error(y_test, y_pred_test),
        "train_mape": mean_absolute_percentage_error(y_train, y_pred_train),
        "cv_mape_mean": 0,
        "cv_mape_std": 0,
        "test_mape": mean_absolute_percentage_error(y_test, y_pred_test),
        "train_mae": mean_absolute_error(y_train, y_pred_train),
        "cv_mae_mean": 0,
        "cv_mae_std": 0,
        "test_mae": mean_absolute_error(y_test, y_pred_test),
    }

    logger.info(
        f"{model_name} results | "
        f"Train RMSE: {metrics['train_rmse']:.2f} | "
        f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f} | "
        f"Test RMSE: {metrics['test_rmse']:.2f} | "
        f"Test MAPE: {metrics['test_mape']:.4f}"
    )

    return pipeline, (model_name, metrics)

def save_metrics(metrics_names_and_metrics: list[tuple[str, dict]]):
    """Save the metrics for multiple models to a single CSV file."""
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(MODELS_OUTPUT_DIR, exist_ok=True)
    results_path = os.path.join(MODELS_OUTPUT_DIR, f"NN_training_{timestamp}.csv")
    rows = []
    for model_name, metrics in metrics_names_and_metrics:
        rows.append({"model": model_name, **metrics})
    pd.DataFrame(rows).to_csv(results_path, index=False)
    logger.info(f"Comparison metrics saved to {results_path}")

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_pipeline(pipeline: Pipeline, output_dir: str, incl_fouling: bool) -> str:
    """Save the fitted pipeline with joblib; create the directory if needed."""
    os.makedirs(output_dir, exist_ok=True)
    if incl_fouling:
        save_path = os.path.join(output_dir, f"nn_pipeline_incl_fouling_{WINDOW_LENGTH}.joblib")
    elif not incl_fouling:
        save_path = os.path.join(output_dir, f"nn_pipeline_excl_fouling_{WINDOW_LENGTH}.joblib")
    joblib.dump(pipeline, save_path)
    logger.info(f"Pipeline saved to {save_path}")
    return save_path

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    np.random.seed(RANDOM_SEED)

    logger.info("=== NN training pipeline started ===")
    logger.info(f"Window length: {WINDOW_LENGTH}")
    logger.info(f"Target variable: {TARGET_VARIABLE}")

    X_train, X_test, y_train, y_test = load_data(NPZ_PATH)

    all_features = list(X_train.columns)
    features_excl_fouling = [f for f in all_features if f != FOULING_PROXY_VAR_NAME_WITH_UNIT]
    logger.info(f"Features ({len(all_features)}): {all_features}")

    # Build pipelines
    pipeline_excl_fouling = build_pipeline(features_excl_fouling, patience=patience)
    pipeline_incl_fouling = build_pipeline(all_features, patience=patience)

    # Train and evaluate
    pipeline_excl_fouling, name_n_metrics_excl_fouling = train_and_evaluate(
        pipeline_excl_fouling,
        X_train[features_excl_fouling],
        y_train,
        X_test[features_excl_fouling],
        y_test,
        model_name="NN_excl_fouling",
        patience=patience
    )
    pipeline_incl_fouling, name_n_metrics_incl_fouling = train_and_evaluate(
        pipeline_incl_fouling,
        X_train[all_features],
        y_train,
        X_test[all_features],
        y_test,
        model_name="NN_incl_fouling",
        patience=patience
    )

    # Save
    save_pipeline(pipeline_excl_fouling, MODELS_OUTPUT_DIR, incl_fouling=False)
    save_pipeline(pipeline_incl_fouling, MODELS_OUTPUT_DIR, incl_fouling=True)
    save_metrics([name_n_metrics_incl_fouling, name_n_metrics_excl_fouling])

    logger.info("=== Done ===")

if __name__ == "__main__":
    main()