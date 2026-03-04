import os
import sys
import joblib
import numpy as np
import pandas as pd
from loguru import logger
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
	mean_absolute_error,
	mean_absolute_percentage_error,
	root_mean_squared_error,
)
from sklearn.model_selection import KFold, cross_validate
from sklearn.neural_network import MLPRegressor
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
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline() -> Pipeline:
	"""Assemble the full sklearn Pipeline (scaler → NN)."""
	preprocessor = ColumnTransformer(
		transformers=[
			("scaler", StandardScaler(), ALL_FEATURES),
		]
	)

	# Hyperparameters mirror the notebook's NN setup
	nn_model = MLPRegressor(
		hidden_layer_sizes=(128, 64),
		activation="relu",
		alpha=0.005,
		learning_rate_init=0.001,
		max_iter=200,
		early_stopping=True,
		validation_fraction=0.1,
		random_state=RANDOM_SEED,
		verbose=False,
	)

	return Pipeline([
		("preprocessor", preprocessor),
		("model", nn_model),
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
	model_name: str = "NN",
) -> dict:
	"""Cross-validate, fit on full training set, and compute holdout metrics."""
	cv = KFold(n_splits=N_CV_SPLITS, shuffle=True, random_state=RANDOM_SEED)

	scoring = {
		"mape": "neg_mean_absolute_percentage_error",
		"rmse": "neg_root_mean_squared_error",
		"mae": "neg_mean_absolute_error",
	}

	logger.info("Running cross-validation …")
	cv_scores = cross_validate(
		pipeline,
		X_train,
		y_train,
		cv=cv,
		scoring=scoring,
		n_jobs=-1,
		return_train_score=True,
	)

	logger.info("Fitting on full training set …")
	pipeline.fit(X_train, y_train)

	y_pred_train = pipeline.predict(X_train)
	y_pred_test = pipeline.predict(X_test)

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
		f"Train RMSE: {metrics['train_rmse']:.2f}  |  "
		f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}  |  "
		f"Test RMSE: {metrics['test_rmse']:.2f}  |  "
		f"Test MAPE: {metrics['test_mape']:.4f}"
	)

	timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
	os.makedirs(MODELS_OUTPUT_DIR, exist_ok=True)
	results_path = os.path.join(MODELS_OUTPUT_DIR, f"{model_name}_training_{timestamp}.csv")
	pd.DataFrame([{"model": model_name, **metrics}]).to_csv(results_path, index=False)
	logger.info(f"Metrics saved to {results_path}")

	return metrics


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_pipeline(pipeline: Pipeline, output_dir: str) -> str:
	"""Save the fitted pipeline with joblib; create the directory if needed."""
	os.makedirs(output_dir, exist_ok=True)
	save_path = os.path.join(output_dir, f"nn_pipeline_{WINDOW_LENGTH}.joblib")
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
	logger.info(f"Features ({len(ALL_FEATURES)}): {ALL_FEATURES}")

	X_train, X_test, y_train, y_test = load_data(NPZ_PATH)
	pipeline = build_pipeline()
	metrics = train_and_evaluate(
		pipeline,
		X_train,
		y_train,
		X_test,
		y_test,
		model_name="NN",
	)

	save_pipeline(pipeline, MODELS_OUTPUT_DIR)

	logger.info("=== Done ===")
	return pipeline, metrics


if __name__ == "__main__":
	main()
