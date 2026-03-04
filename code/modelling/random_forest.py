import os
import sys
import joblib
import numpy as np
import pandas as pd
from loguru import logger
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import (
	mean_absolute_error,
	mean_absolute_percentage_error,
	root_mean_squared_error,
)
from sklearn.model_selection import KFold, GridSearchCV, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Add cleaning-scripts to path so we can import config
# ---------------------------------------------------------------------------
_script_dir = os.path.dirname(os.path.abspath(__file__))
_cleaning_scripts_dir = os.path.join(_script_dir, "..", "data", "cleaning-scripts")
sys.path.insert(0, os.path.abspath(_cleaning_scripts_dir))

from config import (  # noqa: E402
	TARGET_VARIABLE,
	WEATHER_FEATURES,
	NON_WEATHER_FEATURES,
	WINDOW_LENGTH,
	N_CV_SPLITS,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RANDOM_SEED = 42
ALL_FEATURES = NON_WEATHER_FEATURES + WEATHER_FEATURES

# Notebook RF grid
RF_PARAM_GRID = {
	"model__max_depth": [8],
	"model__min_samples_leaf": [20],
	"model__min_samples_split": [20],
	"model__n_estimators": [300],
	"model__max_features": [0.75],
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_data_dir = os.path.join(_script_dir, "..", "data")
NPZ_PATH = os.path.join(_data_dir, "train-and-test", "train_test_splits.npz")
MODELS_OUTPUT_DIR = os.path.join(_script_dir, "..", "outputs", "models")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(npz_path: str) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
	"""Load the pre-computed train/test split from an .npz file."""
	data = np.load(npz_path, allow_pickle=True)
	feature_cols = data["X_columns"].tolist()

	X_train = pd.DataFrame(data["X_train"], columns=feature_cols)[ALL_FEATURES]
	X_test = pd.DataFrame(data["X_test"], columns=feature_cols)[ALL_FEATURES]
	y_train = pd.Series(data["y_train"].ravel(), name=TARGET_VARIABLE)
	y_test = pd.Series(data["y_test"].ravel(), name=TARGET_VARIABLE)

	logger.info(f"Loaded train/test split from {npz_path}")
	logger.info(f"Train rows: {len(X_train):,} | Test rows: {len(X_test):,}")
	return X_train, X_test, y_train, y_test


# ---------------------------------------------------------------------------
# Pipeline and search builders
# ---------------------------------------------------------------------------

def build_pipeline() -> Pipeline:
	"""Assemble the full sklearn Pipeline (scaler → random forest)."""
	preprocessor = ColumnTransformer(
		transformers=[
			("scaler", StandardScaler(), ALL_FEATURES),
		]
	)
	return Pipeline([
		("preprocessor", preprocessor),
		("model", RandomForestRegressor(random_state=RANDOM_SEED)),
	])


def build_grid_search(pipeline: Pipeline, cv) -> GridSearchCV:
	"""Create GridSearchCV exactly as in the notebook RF setup."""
	return GridSearchCV(
		estimator=pipeline,
		param_grid=RF_PARAM_GRID,
		cv=cv,
		scoring="neg_root_mean_squared_error",
		refit=True,
		n_jobs=-1,
		verbose=1,
	)


# ---------------------------------------------------------------------------
# Training and evaluation
# ---------------------------------------------------------------------------

def train_and_evaluate(
	estimator,
	X_train: pd.DataFrame,
	y_train: pd.Series,
	X_test: pd.DataFrame,
	y_test: pd.Series,
	model_name: str = "RandomForest",
) -> tuple[dict, object]:
	"""Cross-validate, fit on full training set, and compute holdout metrics."""
	cv = KFold(n_splits=N_CV_SPLITS, shuffle=True, random_state=RANDOM_SEED)

	scoring = {
		"mape": "neg_mean_absolute_percentage_error",
		"rmse": "neg_root_mean_squared_error",
		"mae": "neg_mean_absolute_error",
	}

	logger.info("Running cross-validation …")
	cv_scores = cross_validate(
		estimator,
		X_train,
		y_train,
		cv=cv,
		scoring=scoring,
		n_jobs=-1,
		return_train_score=True,
	)

	logger.info("Fitting on full training set …")
	estimator.fit(X_train, y_train)

	fitted_pipeline = estimator.best_estimator_ if hasattr(estimator, "best_estimator_") else estimator
	y_pred_train = fitted_pipeline.predict(X_train)
	y_pred_test = fitted_pipeline.predict(X_test)

	metrics = {
		"train_rmse": root_mean_squared_error(y_train, y_pred_train),
		"cv_rmse_mean": -cv_scores["test_rmse"].mean(),
		"cv_rmse_std": cv_scores["test_rmse"].std(),
		"test_rmse": root_mean_squared_error(y_test, y_pred_test),
		"train_mape": mean_absolute_percentage_error(y_train, y_pred_train),
		"cv_mape_mean": -cv_scores["test_mape"].mean(),
		"cv_mape_std": cv_scores["test_mape"].std(),
		"test_mape": mean_absolute_percentage_error(y_test, y_pred_test),
		"train_mae": mean_absolute_error(y_train, y_pred_train),
		"cv_mae_mean": -cv_scores["test_mae"].mean(),
		"cv_mae_std": cv_scores["test_mae"].std(),
		"test_mae": mean_absolute_error(y_test, y_pred_test),
	}

	logger.info(
		f"{model_name} results  |  "
		f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}  |  "
		f"Test RMSE: {metrics['test_rmse']:.2f}  |  "
		f"Test MAPE: {metrics['test_mape']:.4f}"
	)

	if hasattr(estimator, "best_params_"):
		logger.info(f"Best CV RMSE: {-estimator.best_score_:.4f}")
		logger.info(f"Best parameters: {estimator.best_params_}")

	timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
	os.makedirs(MODELS_OUTPUT_DIR, exist_ok=True)
	results_path = os.path.join(MODELS_OUTPUT_DIR, f"{model_name}_training_{timestamp}.csv")
	pd.DataFrame([{"model": model_name, **metrics}]).to_csv(results_path, index=False)
	logger.info(f"Metrics saved to {results_path}")

	return metrics, fitted_pipeline


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_pipeline(pipeline: Pipeline, output_dir: str) -> str:
	"""Save the fitted best pipeline with joblib; create the directory if needed."""
	os.makedirs(output_dir, exist_ok=True)
	save_path = os.path.join(output_dir, f"random_forest_pipeline_{WINDOW_LENGTH}.joblib")
	joblib.dump(pipeline, save_path)
	logger.info(f"Pipeline saved to {save_path}")
	return save_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
	np.random.seed(RANDOM_SEED)

	logger.info("=== Random Forest training pipeline started ===")
	logger.info(f"Window length: {WINDOW_LENGTH}")
	logger.info(f"Target variable: {TARGET_VARIABLE}")
	logger.info(f"Features ({len(ALL_FEATURES)}): {ALL_FEATURES}")

	X_train, X_test, y_train, y_test = load_data(NPZ_PATH)

	cv = KFold(n_splits=N_CV_SPLITS, shuffle=True, random_state=RANDOM_SEED)
	pipeline = build_pipeline()
	grid_search = build_grid_search(pipeline, cv)

	metrics, fitted_pipeline = train_and_evaluate(
		grid_search,
		X_train,
		y_train,
		X_test,
		y_test,
		model_name="RandomForest",
	)

	save_pipeline(fitted_pipeline, MODELS_OUTPUT_DIR)

	logger.info("=== Done ===")
	return fitted_pipeline, metrics


if __name__ == "__main__":
	main()
