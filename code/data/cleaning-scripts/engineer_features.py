import pandas as pd
import numpy as np
import os
import re
from config import WINDOW_LENGTH, JULY_CLEANING_DATE, JANUARY_CLEANING_DATE
from typing import List
from datetime import datetime
from loguru import logger

# define paths
script_dir = os.path.dirname(os.path.abspath(__file__))
aggregated_dir = os.path.join(script_dir, '..', 'aggregated')
aggregated_data_path = os.path.join(aggregated_dir, f'aggregated_{WINDOW_LENGTH}.csv')
engineered_dir = os.path.join(script_dir, '..', 'engineered')
feature_engineering_output_dir = os.path.join(script_dir, '..', '..', 'outputs', 'feature-engineering')

# Create the aggregated directory if it doesn't exist
if not os.path.exists(aggregated_dir):
    os.makedirs(aggregated_dir)
    logger.info(f'Created aggregated directory: {aggregated_dir}')
else:
    logger.info(f'Aggregated directory already exists: {aggregated_dir}')

# start logger

# Create the filtering output directory for filtering results if it doesn't exist
if not os.path.exists(feature_engineering_output_dir):
    os.makedirs(feature_engineering_output_dir)
    logger.info(f'Created feature engineering output directory: {feature_engineering_output_dir}')
else:
    logger.info(f'Feature engineering output directory already exists: {feature_engineering_output_dir}')

log_path = os.path.join(feature_engineering_output_dir, f'pre_agg_cleaning_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logger.add(
    log_path,
    level='INFO',
    format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}'
)

# load data
df = pd.read_csv(aggregated_data_path)

# Ensure datetime datatypes
df["window_start"] = pd.to_datetime(df["window_start"], format="ISO8601", utc=True)

logger.info(f'Loaded data from {aggregated_data_path} with shape {df.shape}')

# --- Functions ---

# --- Necessary Features ---

def add_days_since_cleaning(df, new_column_name: str, cleaning_dates: List):
    if "window_start" not in df.columns:
        raise KeyError("Column 'window_start' is required to calculate days since cleaning.")

    cleaning_ts = pd.to_datetime(cleaning_dates, utc=True, errors="coerce")
    cleaning_ts = pd.Series(cleaning_ts).dropna().drop_duplicates().sort_values()

    if cleaning_ts.empty:
        raise ValueError("No valid cleaning dates were provided.")

    timestamps = pd.to_datetime(df["window_start"], utc=True, errors="coerce")

    if timestamps.isna().any():
        raise ValueError("Column 'window_start' contains invalid datetime values.")

    timestamps_df = pd.DataFrame(
        {
            "window_start": timestamps,
            "_row_order": np.arange(len(df)),
        }
    ).sort_values("window_start")

    cleaning_df = pd.DataFrame({"cleaning_date": cleaning_ts}).sort_values("cleaning_date")

    merged = pd.merge_asof(
        timestamps_df,
        cleaning_df,
        left_on="window_start",
        right_on="cleaning_date",
        direction="backward",
    )

    merged[new_column_name] = (
        (merged["window_start"] - merged["cleaning_date"]).dt.total_seconds() / 86400
    )

    df[new_column_name] = (
        merged.sort_values("_row_order")[new_column_name].to_numpy()
    )

    return df

def add_mid_draft(df, new_column_name: str, fore_draft_col_name: str, aft_draft_col_name: str):

    df[new_column_name] = (df[fore_draft_col_name] + df[aft_draft_col_name]) / 2

    return df

# --- Fringe Features ---

# --- Executions ---

df = add_days_since_cleaning(df, "Days Since Last Cleaning", [JANUARY_CLEANING_DATE, JULY_CLEANING_DATE])
df = add_mid_draft(df, "Vessel Mid Draft (m)", "Vessel Fore Draft (m)", "Vessel Aft Draft (m)")

# get the first value of every day in january to check if the feature is correct
first_values_january = df[df["window_start"].dt.month == 1].groupby(df["window_start"].dt.date).first()[["window_start", "Days Since Last Cleaning"]]

logger.info("Added 'Days Since Last Cleaning' feature. First values in January:")
logger.info(first_values_january.head(31))