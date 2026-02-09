import pandas as pd
import numpy as np
import os
from config import INTENDED_SAMPLING_INTERVALS_SECONDS, THRESHOLD_FACTOR
from loguru import logger

script_dir = os.path.dirname(os.path.abspath(__file__))
appended_data_dir = os.path.join(script_dir, '..', 'appended')
synchronized_data_dir = os.path.join(script_dir, '..', 'synchronized')

# create synchronized data directory if it does not exist
if not os.path.exists(synchronized_data_dir):
    os.makedirs(synchronized_data_dir)
else:
    logger.info(f'synchronized data directory already exists: {synchronized_data_dir}')

# load the appended dataframe
df = pd.read_csv(os.path.join(appended_data_dir, 'appended_data.csv'), parse_dates=['utc_timestamp'])

logger.info(f'Synchronizing dataframe with shape: {df.shape}')
logger.info(f'')
logger.info(f'distribution of intended sampling intervals: {pd.Series(INTENDED_SAMPLING_INTERVALS_SECONDS).value_counts()}')

# TODO: create a boolean mask representing clean stretches of data 