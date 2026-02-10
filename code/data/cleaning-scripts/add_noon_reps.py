import os
import glob
import pandas as pd
import numpy as np
from loguru import logger
from config import NOON_REPORT_QIDS, NOON_REPORT_UNITS
from multiprocessing import Pool, cpu_count

# Defining prerequisites for appending loop
# Get the directory where THIS script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Defining prerequisites for appending
columns = [
    'utc_timestamp',
    'qid_mapping',
    'value',
    'quantity_name',
]

# Define paths relative to script location
appended_data_dir = os.path.join(script_dir, '..', 'appended')
noon_rep_qid_dict = NOON_REPORT_QIDS
noon_rep_units_dict = NOON_REPORT_UNITS
raw_noon_reports_dir = os.path.join(script_dir, '..', 'raw', 'unzipped', 'Noon Reports')

# Parallel file reading and processing function
def process_noon_report_file(file_path):
    # Read the file
    df = pd.read_csv(file_path)
    logger.info(f'Reading noon report file: {file_path} with shape: {df.shape}')
    
    # Rewrite the "date" column to be called "utc_timestamp" and parsed as datetime objects with time set to noon/12PM
    df['utc_timestamp'] = pd.to_datetime(df['Date'], format='%d/%m/%Y') + pd.Timedelta(hours=12)
    df['utc_timestamp'] = df['utc_timestamp'].dt.tz_localize('UTC')
    df = df.drop(columns=['Date'])
    
    # Melt the dataframe so that each column (except utc_timestamp) is turned into a row
    melted_df = df.melt(id_vars=['utc_timestamp'], var_name='quantity_name', value_name='value')
    
    # Add a column called "qid_mapping" that contains maps the quantity names to their QID dummies
    melted_df['qid_mapping'] = melted_df['quantity_name'].map(noon_rep_qid_dict)
    
    # Arrange the columns in the same order as specified in the columns list
    melted_df = melted_df[['utc_timestamp', 'qid_mapping', 'value', 'quantity_name']]
    
    return melted_df

# Get all noon report files
all_files = glob.glob(os.path.join(raw_noon_reports_dir, '*.csv'))
logger.info(f'Found {len(all_files)} noon report files to process')

# Read and process files in parallel
with Pool(min(cpu_count() - 1, len(all_files))) as pool:
    dfs = pool.map(process_noon_report_file, all_files)

# Concatenate all dataframes at once (much faster than iterative concat)
appended_df = pd.concat(dfs, ignore_index=True)
logger.info(f'Successfully processed all noon reports. Total shape: {appended_df.shape}')    

# drop rows where the value is NaN
logger.info(f'Shape before dropping NaN values: {appended_df.shape}')
appended_df = appended_df.dropna(subset=['value'])
logger.info(f'Dropped NaN values. Shape is now: {appended_df.shape}')

# add needed columns to match excl. noon reports dataframe

# Change utc_timestamp column to datetime with UTC (for consistency), although timezones are not known
appended_df['utc_timestamp'] = pd.to_datetime(appended_df['utc_timestamp']).dt.tz_convert('UTC')

# add a "source_name" column with value "Noon Report"
appended_df['source_name'] = 'Noon Report'

# add a "unit" column by mapping the quantity_name column to the units in the NOON_REPORT_UNITS dictionary
appended_df['unit'] = appended_df['quantity_name'].map(noon_rep_units_dict)

# add a column for time delta between observations for each variable (measuring only the difference between a given observation and the last observation of that qid_mapping)
logger.info(f'shape before adding time_delta: {appended_df.shape}')
appended_df['time_delta_sec'] = appended_df.groupby('qid_mapping')['utc_timestamp'].diff().dt.total_seconds()
logger.info(f'Added time_delta column to noon reports dataframe. Shape is now: {appended_df.shape}')

# save the noon report data only as a separate csv file for reference
appended_df.to_csv(os.path.join(appended_data_dir, 'noon_reports_only.csv'), index=False)

# load the dataframe containing all the appended sensor observations
excl_noon_reps_df = pd.read_csv(os.path.join(appended_data_dir, 'excl_noon_reports.csv'))
logger.info(f'Loaded appended sensor observations excl. noon reports with shape: {excl_noon_reps_df.shape}')

# set the column "utc_timestamp" to datetime with UTC timezone if not already
excl_noon_reps_df['utc_timestamp'] = pd.to_datetime(excl_noon_reps_df['utc_timestamp'], format='ISO8601').dt.tz_convert('UTC')

# append all the noon report observations to the dataframe containing all the sensor observations
full_appended_df = pd.concat([excl_noon_reps_df, appended_df], ignore_index=True)
logger.info(f'Appended noon reports. New shape: {full_appended_df.shape}')
logger.info(f'Columns in appended dataframe excl. sensors: {appended_df.columns.tolist()}')
logger.info(f'Columns in appended dataframe excl. noon reports: {excl_noon_reps_df.columns.tolist()}')
logger.info(f'Columns in full appended dataframe: {full_appended_df.columns.tolist()}')
logger.info(f'distribution of dtypes in utc_timestamp columns: {full_appended_df["utc_timestamp"].apply(type).value_counts()}')

# sort by utc_timestamp for consistency
full_appended_df = full_appended_df.sort_values(by='utc_timestamp').reset_index(drop=True)
logger.info(f'Sorted full appended dataframe by utc_timestamp.')

# save the full appended dataframe to the appended data folder
full_appended_df.to_csv(os.path.join(appended_data_dir, 'incl_noon_reports.csv'), index=False)
logger.info(f'Saved full appended dataframe with noon reports to CSV.')


