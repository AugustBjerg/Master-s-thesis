import os
import glob
import pandas as pd
import numpy as np
from loguru import logger
from config import EXPECTED_SENSOR_OBSERVATIONS
from multiprocessing import Pool, cpu_count

# Get the directory where THIS script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# -- STEP 1: append all monthly observation files into a single dataframe   --

# Defining prerequisites for appending
columns = [
    'utc_timestamp',
    'qid_mapping',
    'value',
]
parent_dir = os.path.dirname(os.getcwd())
appended_data_dir = os.path.join(script_dir, '..', 'appended')
sensor_dictionary_path = os.path.join(script_dir, '..', 'metadata', 'Metrics registration.csv')

# Parallel file reading function
def read_csv_file(file_path):
    df = pd.read_csv(file_path, names=columns, parse_dates=['utc_timestamp'], date_format='ISO8601')
    logger.info(f'Read file: {file_path} with shape: {df.shape}')
    return df

# Get all CSV files from month directories (1-12 only)
all_files = []
for month in range(1, 13):
    input_pattern = os.path.join(script_dir, '..', 'raw', 'unzipped', str(month), '*.csv')
    all_files.extend(glob.glob(input_pattern))
logger.info(f'Found {len(all_files)} files to process')

# Read files in parallel using all CPU cores minus 1
with Pool(cpu_count() - 1) as pool:
    dfs = pool.map(read_csv_file, all_files)

# Concatenate all dataframes at once (much faster than iterative concat)
appended_df = pd.concat(dfs, ignore_index=True)
logger.info(f'Successfully appended all files. Total shape: {appended_df.shape}')

# check if there is the right number of sensor observations
if appended_df.shape[0] != EXPECTED_SENSOR_OBSERVATIONS or appended_df.shape[1] != len(columns):
    logger.error(f'dataframe shape ({appended_df.shape[0]}) does not match expected ({EXPECTED_SENSOR_OBSERVATIONS},{len(columns)})')
else:
    logger.info(f'dataframe shape ({appended_df.shape[0]}) is as expected: ({EXPECTED_SENSOR_OBSERVATIONS},{len(columns)})')

# -- STEP 2: Add columns from metrics registration file --
sensor_dict_df = pd.read_csv(sensor_dictionary_path)

# set the value for "unit" of Vessel Propeller Shaft Revolutions to "revs", because it was missing in the original file from Mærsk
sensor_dict_df.loc[sensor_dict_df['quantity_name'] == 'Vessel Propeller Shaft Revolutions', 'unit'] = 'revs'

# save it to a csv file again to keep the correction
sensor_dict_df.to_csv(sensor_dictionary_path, index=False)

logger.info(f'number of variables in sensor dictionary: {sensor_dict_df["qid_mapping"].nunique()}')
logger.info(f' is 2::0::25::0_1::2::0::3::0_1::0::6::0_8 in sensor dictionary? {"2::0::25::0_1::2::0::3::0_1::0::6::0_8" in sensor_dict_df["qid_mapping"].values}')

# Merge sensor metadata onto the appended dataframe
appended_df = appended_df.merge(
    sensor_dict_df[['qid_mapping', 'quantity_name', 'source_name', 'unit']], 
    on='qid_mapping', 
    how='left'
)

logger.info(f'number of variables after merge: {appended_df["qid_mapping"].nunique()}')
logger.info(f'Added sensor metadata columns. Final shape: {appended_df.shape}')

# convert uct_timestamp to datetime if not already
appended_df['utc_timestamp'] = pd.to_datetime(appended_df['utc_timestamp']).dt.tz_convert('UTC')

# Sort by timestamp for consistency
appended_df = appended_df.sort_values(by='utc_timestamp').reset_index(drop=True)

# add a column for time delta between observations for each variable (measuring only the difference between a given observation and the last observation of that qid_mapping)
logger.info(f'shape before adding time_delta: {appended_df.shape}')
appended_df['time_delta_sec'] = appended_df.groupby('qid_mapping')['utc_timestamp'].diff().dt.total_seconds()
logger.info(f'Added time_delta column to appended dataframe. Shape is now: {appended_df.shape}')

# Save this version of the appended df (excl. noon report data) to the folder
os.makedirs(appended_data_dir, exist_ok=True)
appended_df.to_csv(os.path.join(appended_data_dir, 'excl_noon_reports.csv'), index=False)
