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

def build_pipeline(feature_list: list[str]) -> Pipeline:
	"""Assemble the full sklearn Pipeline (scaler → NN)."""
	preprocessor = ColumnTransformer(
		transformers=[
			("scaler", StandardScaler(), feature_list),
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
) -> tuple[Pipeline, tuple[str, dict]]:
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
	pipeline_excl_fouling = build_pipeline(features_excl_fouling)
	pipeline_incl_fouling = build_pipeline(all_features)

	# Train and evaluate
	
	pipeline_excl_fouling, name_n_metrics_excl_fouling = train_and_evaluate(
		pipeline_excl_fouling,
		X_train[features_excl_fouling],
		y_train,
		X_test[features_excl_fouling],
		y_test,
		model_name="NN_excl_fouling",
	)
	pipeline_incl_fouling, name_n_metrics_incl_fouling = train_and_evaluate(
		pipeline_incl_fouling,
		X_train[all_features],
		y_train,
		X_test[all_features],
		y_test,
		model_name="NN_incl_fouling",
	)

	# Save
	save_pipeline(pipeline_excl_fouling, MODELS_OUTPUT_DIR, incl_fouling=False)
	save_pipeline(pipeline_incl_fouling, MODELS_OUTPUT_DIR, incl_fouling=True)
	save_metrics([name_n_metrics_incl_fouling, name_n_metrics_excl_fouling])

	logger.info("=== Done ===")


if __name__ == "__main__":
	main()
