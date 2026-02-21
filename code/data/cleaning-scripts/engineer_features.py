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
feature_engineering_output_dir = os.path.join(script_dir, '..', '..', 'outputs', 'aggregation')


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
df["utc_timestamp"] = pd.to_datetime(df["utc_timestamp"], format="ISO8601", utc=True)

logger.info(f'Loaded data from {aggregated_data_path} with shape {df.shape}')

# --- Functions ---

# --- Necessary Features ---

def _add_days_since_cleaning(df, cleaning_dates: List):

    return df

# --- Fringe Features ---

# --- Executions ---

