import pandas as pd
import numpy as np
from numpy import linspace
import os
import re
import itertools
from pathlib import Path
from sklearn.model_selection import train_test_split
from config import WINDOW_LENGTH, TRAIN_RATIO, TARGET_VARIABLE, FOULING_PROXY_VAR_NAME, FOULING_PROXY_VAR_NAME_WITH_UNIT, FOULING_PROXY_CONTROLLED_VARIABLE_RANGE, SPEED_CONTROLLED_VARIABLE_RANGE, SPEED_CONTROLLED_VARIABLE_NAME, DRAFT_CONTROLLED_VARIABLE_NAME, DRAFT_CONTROLLED_VARIABLE_RANGE, WEATHER_FEATURES, NON_WEATHER_FEATURES
from typing import List, Optional
from datetime import datetime
from loguru import logger

random_seed = 42
np.random.seed(random_seed)

# define paths
script_dir = os.path.dirname(os.path.abspath(__file__))
engineered_dir = os.path.join(script_dir, '..', 'engineered')
engineered_data_path = os.path.join(engineered_dir, f'engineered_features_{WINDOW_LENGTH}.csv')
train_test_dir = os.path.join(script_dir, '..', 'train-and-test')
out_path = Path(train_test_dir) / "train_test_splits.npz"

# Create the train-and-test directory if it doesn't exist
if not os.path.exists(train_test_dir):
    os.makedirs(train_test_dir)
    logger.info(f'Created train-and-test directory: {train_test_dir}')
else:
    logger.info(f'Train-and-test directory already exists: {train_test_dir}')

# load data
df = pd.read_csv(engineered_data_path)

# Ensure datetime datatypes
df["window_start"] = pd.to_datetime(df["window_start"], format="ISO8601", utc=True)

logger.info(f'Loaded data from {engineered_data_path} with shape {df.shape}')

def create_controlled_var_df(
    window_length: str,
    variable_dict: dict,
    columns: Optional[List[str]] = None,
    X_train: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Creates a synthetic dataframe for a controlled variable experiment.

    Parameters
    ----------
    window_length : str
        Window length identifier used in the output filename (e.g. '5min').
    variable_dict : dict
        Mapping of variable name -> list of values that variable should take.
        One row is produced for every combination (Cartesian product) of these values.
    columns : list of str, optional
        Columns to include in the output dataframe.
        Defaults to all columns of X_train (if provided) or the keys of variable_dict.
    X_train : pd.DataFrame, optional
        Training data used to compute median values for columns not listed in
        variable_dict.  When omitted, those columns are filled with NaN.

    Returns
    -------
    pd.DataFrame
        The synthetic dataframe, also saved to the train-and-test directory as
        'controlled_var_<window_length>_<var1>_<var2>_....csv'.
    """
    if columns is None:
        columns = X_train.columns.tolist() if X_train is not None else list(variable_dict.keys())

    # Median values for columns that are not being varied
    median_values: dict = {}
    for col in columns:
        if col not in variable_dict:
            if X_train is not None and col in X_train.columns:
                median_values[col] = X_train[col].median()
            else:
                median_values[col] = np.nan

    # Cartesian product of all varied values
    var_names = list(variable_dict.keys())
    combinations = list(itertools.product(*variable_dict.values()))

    rows = []
    for combo in combinations:
        row = {col: median_values.get(col, np.nan) for col in columns}
        for var_name, val in zip(var_names, combo):
            row[var_name] = val
        rows.append(row)

    synthetic_df = pd.DataFrame(rows, columns=columns)

    # Build filename and save
    var_names_str = '_'.join(var_names)
    filename = f'controlled_var_{window_length}_{var_names_str}.csv'
    save_path = os.path.join(train_test_dir, filename)
    synthetic_df.to_csv(save_path, index=False)
    logger.info(f'Saved controlled variable dataframe to {save_path} with shape {synthetic_df.shape}')

    return synthetic_df

if __name__ == "__main__":

    features = WEATHER_FEATURES + NON_WEATHER_FEATURES
    all_column_names = features + [TARGET_VARIABLE]

    all_columns_in_df = df.columns.tolist()
    logger.info(f'All columns in the loaded dataframe: {all_columns_in_df}')

    filtered_df = df[all_column_names]

    # drop NaN values for the fouling proxy
    filtered_df = filtered_df.dropna(subset=[FOULING_PROXY_VAR_NAME_WITH_UNIT])

    # Make a randomized test and training split
    X_train, X_test, y_train, y_test = train_test_split(
        filtered_df.drop(columns=[TARGET_VARIABLE]), 
        filtered_df[TARGET_VARIABLE], 
        test_size=1-TRAIN_RATIO, 
        random_state=random_seed
    )

    # Store as a npz files
    np.savez_compressed(
        out_path,
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        X_columns=X_train.columns.to_numpy(),
    )

    # create a fouling controlled variable dataframe for the median case
    if FOULING_PROXY_VAR_NAME_WITH_UNIT == "Days Since Last Cleaning":
        full_controlled_fouling_proxy_range = list(range(FOULING_PROXY_CONTROLLED_VARIABLE_RANGE[0], FOULING_PROXY_CONTROLLED_VARIABLE_RANGE[1] + 1))
    else:
        max_value_of_fouling_proxy = int(X_train[FOULING_PROXY_VAR_NAME_WITH_UNIT].max())
        min_value_of_fouling_proxy = int(X_train[FOULING_PROXY_VAR_NAME_WITH_UNIT].min())
        full_controlled_fouling_proxy_range = list(linspace(min_value_of_fouling_proxy, max_value_of_fouling_proxy, 150))
    
    fouling_proxy_df = create_controlled_var_df(
        window_length=WINDOW_LENGTH,
        variable_dict={FOULING_PROXY_VAR_NAME_WITH_UNIT: full_controlled_fouling_proxy_range},
        columns=all_column_names,
        X_train=X_train
    )

    # create a fouling controlled variable dataframe, but with varying speeds
    fouling_proxy_and_speed_df = create_controlled_var_df(
        window_length=WINDOW_LENGTH,
        variable_dict={
            FOULING_PROXY_VAR_NAME_WITH_UNIT: full_controlled_fouling_proxy_range,
            SPEED_CONTROLLED_VARIABLE_NAME: SPEED_CONTROLLED_VARIABLE_RANGE
        },
        columns=all_column_names,
        X_train=X_train
    )

    # create a fouling controlled variable dataframe, but with varying drafts
    fouling_proxy_and_draft_df = create_controlled_var_df(
        window_length=WINDOW_LENGTH,
        variable_dict={
            FOULING_PROXY_VAR_NAME_WITH_UNIT: full_controlled_fouling_proxy_range,
            DRAFT_CONTROLLED_VARIABLE_NAME: DRAFT_CONTROLLED_VARIABLE_RANGE
        },
        columns=all_column_names,
        X_train=X_train
    )
