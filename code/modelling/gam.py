import os
import sys
import joblib
import numpy as np
import pandas as pd
import copy
from loguru import logger
from pygam import LinearGAM, s, te, f
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)
from sklearn.model_selection import KFold, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.utils.validation import check_is_fitted

# ---------------------------------------------------------------------------
# Add cleaning-scripts to path so we can import config
# ---------------------------------------------------------------------------
_script_dir = os.path.dirname(os.path.abspath(__file__))
_cleaning_scripts_dir = os.path.join(_script_dir, "..", "data", "cleaning-scripts")
sys.path.insert(0, os.path.abspath(_cleaning_scripts_dir))

from config import (  # noqa: E402
    FOULING_PROXY_VAR_NAME_WITH_UNIT,
    TARGET_VARIABLE,
    WEATHER_FEATURES,
    NON_WEATHER_FEATURES,
    SPEED_VARIABLE,
    WINDOW_LENGTH,
    N_CV_SPLITS,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RANDOM_SEED = 42
ALL_FEATURES = NON_WEATHER_FEATURES + WEATHER_FEATURES
NON_FOULING_FEATURES = [f for f in ALL_FEATURES if f != FOULING_PROXY_VAR_NAME_WITH_UNIT]

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
    """Load the pre-computed train/test split from an .npz file.

    Column names are stored inside the npz as X_columns.

    Returns
    -------
    X_train, X_test, y_train, y_test
    """
    data = np.load(npz_path, allow_pickle=True)
    feature_cols = data["X_columns"].tolist()

    X_train = pd.DataFrame(data["X_train"], columns=feature_cols)[ALL_FEATURES]
    X_test  = pd.DataFrame(data["X_test"],  columns=feature_cols)[ALL_FEATURES]
    y_train = pd.Series(data["y_train"].ravel(), name=TARGET_VARIABLE)
    y_test  = pd.Series(data["y_test"].ravel(),  name=TARGET_VARIABLE)

    logger.info(f"Loaded train/test split from {npz_path}")
    logger.info(f"Train rows: {len(X_train):,} | Test rows: {len(X_test):,}")
    return X_train, X_test, y_train, y_test

# ---------------------------------------------------------------------------
# GAM formula builder
# ---------------------------------------------------------------------------

def build_gam_formula(
    feature_list: list[str],
    include_fouling_proxy: bool = True,
    speed_fouling_interaction: bool = False,
) -> object:
    """Construct a pyGAM formula from the feature list."""

    # Pre-find indices needed for the optional tensor product term
    speed_idx = next(
        (i for i, v in enumerate(feature_list) if v == SPEED_VARIABLE), None
    )
    fouling_idx = next(
        (i for i, v in enumerate(feature_list) if v == FOULING_PROXY_VAR_NAME_WITH_UNIT),
        None,
    )

    use_interaction = (
        speed_fouling_interaction
        and include_fouling_proxy
        and speed_idx is not None
        and fouling_idx is not None
    )

    # Indices absorbed into te() — skip their individual s() terms in the loop
    interaction_indices = {speed_idx, fouling_idx} if use_interaction else set()

    # Seed formula with interaction term if applicable
    formula = te(speed_idx, fouling_idx, n_splines=[15, 15]) if use_interaction else None

    for i, var_name in enumerate(feature_list):

        if i in interaction_indices:
            continue  # Already handled by te()

        if var_name == FOULING_PROXY_VAR_NAME_WITH_UNIT and include_fouling_proxy:
            term = s(i, constraints="monotonic_inc", n_splines=15)

        elif var_name == SPEED_VARIABLE:
            term = s(i, constraints="monotonic_inc", n_splines=15)

        elif var_name in (
            "Speed Through Water^3 (m/s)",
            "Speed^3 x DSC (calculated)",
            "Speed x DSC (calculated)",
        ):
            term = s(i, constraints="monotonic_inc", n_splines=15)

        elif var_name == "Avg Draft (Calculated)":
            term = s(i, constraints="monotonic_inc", n_splines=10)

        elif var_name == "Vessel External Conditions Sea Water Temperature (Provider S)":
            term = s(i, constraints="monotonic_dec", n_splines=10)

        elif var_name in (
            "Vessel External Conditions Wave Significant Height (Provider MB)",
            "Vessel External Conditions Swell Significant Height (Provider MB)",
        ):
            term = s(i, constraints="monotonic_inc", n_splines=10)

        elif var_name == "Vessel External Conditions Wind Relative Angle (degrees)":
            term = s(i, basis="cp", n_splines=10, edge_knots=[0, 360])

        elif var_name == "Vessel External Conditions Wind Relative Speed (knots)":
            term = s(i, constraints="monotonic_inc", n_splines=15)

        else:
            term = s(i, n_splines=10)

        formula = term if formula is None else formula + term

    return formula

# ---------------------------------------------------------------------------
# sklearn-compatible GAM wrapper
# ---------------------------------------------------------------------------

class SklearnGAM(BaseEstimator, RegressorMixin):
    """Thin sklearn wrapper around pyGAM's LinearGAM with optional gridsearch."""

    def __init__(self, formula, auto_tune: bool = True):
        self.formula = formula
        self.auto_tune = auto_tune

    def fit(self, X, y):
        self.gam_model_ = LinearGAM(copy.deepcopy(self.formula))  # ← deepcopy here
        if self.auto_tune:
            lam_grid = np.logspace(-6, 3, 20)
            self.gam_model_.gridsearch(X, y, lam=lam_grid)
        else:
            self.gam_model_.fit(X, y)
        self.is_fitted_ = True
        return self

    def predict(self, X):
        check_is_fitted(self, "is_fitted_")
        return self.gam_model_.predict(X)

# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline(formula, feature_list: list[str]) -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[("scaler", StandardScaler(), feature_list)]
    )
    return Pipeline([
        ("preprocessor", preprocessor),
        ("model", SklearnGAM(formula=formula, auto_tune=True)),
    ])

# ---------------------------------------------------------------------------
# Training and evaluation
# ---------------------------------------------------------------------------

def train_and_evaluate(
    pipeline: Pipeline,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    model_name: str,
) -> dict:
    """Cross-validate, fit on full training set, and compute holdout metrics."""
    cv = KFold(n_splits=N_CV_SPLITS, shuffle=True, random_state=RANDOM_SEED)

    scoring = {
        "mape": "neg_mean_absolute_percentage_error",
        "rmse": "neg_root_mean_squared_error",
        "mae":  "neg_mean_absolute_error",
    }

    logger.info("Running cross-validation …")
    cv_scores = cross_validate(
        pipeline, X_train, y_train,
        cv=cv, scoring=scoring,
        n_jobs=-1, return_train_score=True,
    )

    logger.info("Fitting on full training set …")
    pipeline.fit(X_train, y_train)

    y_pred_train = pipeline.predict(X_train)
    y_pred_test  = pipeline.predict(X_test)

    metrics = {
        # RMSE
        "train_rmse":   root_mean_squared_error(y_train, y_pred_train),
        "cv_rmse_mean": -cv_scores["test_rmse"].mean(),
        "cv_rmse_std":  cv_scores["test_rmse"].std(),
        "test_rmse":    root_mean_squared_error(y_test, y_pred_test),
        # MAPE
        "train_mape":   mean_absolute_percentage_error(y_train, y_pred_train),
        "cv_mape_mean": -cv_scores["test_mape"].mean(),
        "cv_mape_std":  cv_scores["test_mape"].std(),
        "test_mape":    mean_absolute_percentage_error(y_test, y_pred_test),
        # MAE
        "train_mae":    mean_absolute_error(y_train, y_pred_train),
        "cv_mae_mean":  -cv_scores["test_mae"].mean(),
        "cv_mae_std":   cv_scores["test_mae"].std(),
        "test_mae":     mean_absolute_error(y_test, y_pred_test),
    }

    logger.info(
        f"GAM results  |  "
        f"Train RMSE: {metrics['train_rmse']:.2f}  |  "
        f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}  |  "
        f"Test RMSE: {metrics['test_rmse']:.2f}  |  "
        f"Test MAPE: {metrics['test_mape']:.4f}"
    )

    return pipeline, (model_name, metrics)

def save_metrics(metrics_names_and_metrics: list[tuple[str, dict]]):
    """Save the metrics for multiple models to a single CSV file."""
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    results_path = os.path.join(MODELS_OUTPUT_DIR, f"GAM_training_{timestamp}.csv")
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
        save_path = os.path.join(output_dir, f"gam_pipeline_incl_fouling_{WINDOW_LENGTH}.joblib")
    elif not incl_fouling:
        save_path = os.path.join(output_dir, f"gam_pipeline_excl_fouling_{WINDOW_LENGTH}.joblib")
    joblib.dump(pipeline, save_path)
    logger.info(f"Pipeline saved to {save_path}")
    return save_path

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    np.random.seed(RANDOM_SEED)

    logger.info("=== GAM training pipeline started ===")
    logger.info(f"Window length: {WINDOW_LENGTH}")
    logger.info(f"Target variable: {TARGET_VARIABLE}")
    logger.info(f"Features ({len(ALL_FEATURES)}): {ALL_FEATURES}")

    # 1. Load data
    X_train, X_test, y_train, y_test = load_data(NPZ_PATH)

    # 2. Build GAM formulas
    formula_excl_fouling = build_gam_formula(NON_FOULING_FEATURES, include_fouling_proxy=False, speed_fouling_interaction=False)
    formula_incl_fouling = build_gam_formula(ALL_FEATURES, include_fouling_proxy=True, speed_fouling_interaction=True)
    logger.info("GAM formula constructed.")

    # 3. Build pipelines
    pipeline_excl_fouling = build_pipeline(formula_excl_fouling, feature_list=NON_FOULING_FEATURES)
    pipeline_incl_fouling = build_pipeline(formula_incl_fouling, feature_list=ALL_FEATURES)

    # 4. Train and evaluate
    pipeline_excl_fouling, name_n_metrics_excl_fouling = train_and_evaluate(pipeline_excl_fouling, X_train, y_train, X_test, y_test, model_name="GAM_excl_fouling")
    pipeline_incl_fouling, name_n_metrics_incl_fouling = train_and_evaluate(pipeline_incl_fouling, X_train, y_train, X_test, y_test, model_name="GAM_incl_fouling")
    
    # 5. Save
    save_pipeline(pipeline_excl_fouling, MODELS_OUTPUT_DIR, incl_fouling=False)
    save_pipeline(pipeline_incl_fouling, MODELS_OUTPUT_DIR, incl_fouling=True)
    save_metrics([name_n_metrics_incl_fouling, name_n_metrics_excl_fouling])

    logger.info("=== Done ===")

if __name__ == "__main__":
    main()