import os
import glob
import pandas as pd
import numpy as np
from loguru import logger
from config import NOON_REPORT_QIDS, NOON_REPORT_UNITS

# Defining prerequisites for appending loop
# Get the directory where THIS script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Defining prerequisites for appending loop
columns = [
    'utc_timestamp',
    'qid_mapping',
    'value',
    'quantity_name',
]
appended_df = pd.DataFrame(columns=columns)

# Define paths relative to script location
appended_data_dir = os.path.join(script_dir, '..', 'appended')
noon_rep_qid_dict = NOON_REPORT_QIDS
noon_rep_units_dict = NOON_REPORT_UNITS
raw_noon_reports_dir = os.path.join(script_dir, '..', 'raw', 'unzipped', 'Noon Reports')

# write a loop that: 
# 1. loops through all the files in the noon reports folder
for i, file in enumerate(glob.glob(os.path.join(raw_noon_reports_dir, '*.csv'))):
    # read the file
    df = pd.read_csv(file)
    logger.info(f'Reading noon reports for month ({i+1}) with shape: {df.shape}')
    
    # rewrite the "date" column to be called "utc_timestamp" and parsed as datetime objects with time set to noon/12PM
    df['utc_timestamp'] = pd.to_datetime(df['Date'], format='%d/%m/%Y') + pd.Timedelta(hours=12, microseconds=0)
    df['utc_timestamp'] = df['utc_timestamp'].dt.tz_localize('UTC')
    df = df.drop(columns=['Date'])
    
    # melt the dataframe so that each column (except utc_timestamp) is turned into a row
    melted_df = df.melt(id_vars=['utc_timestamp'], var_name='quantity_name', value_name='value')
    
    # add a column called "qid_mapping" that contains maps the quantity names to their QID dummies
    melted_df['qid_mapping'] = melted_df['quantity_name'].map(noon_rep_qid_dict)
    
    # Arrange the columns in the same order as specified in the columns list
    melted_df = melted_df[['utc_timestamp', 'qid_mapping', 'value', 'quantity_name']]
    
    # append that to the running dataframe
    appended_df = pd.concat([appended_df, melted_df], ignore_index=True)    

# add needed columns to match excl. noon reports dataframe

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

# Parse sensor data timestamps to preserve microsecond precision
excl_noon_reps_df['utc_timestamp'] = pd.to_datetime(excl_noon_reps_df['utc_timestamp'], format='ISO8601')

# append all the noon report observations to the dataframe containing all the sensor observations
full_appended_df = pd.concat([excl_noon_reps_df, appended_df], ignore_index=True)
logger.info(f'Appended noon reports. New shape: {full_appended_df.shape}')

# sort by utc_timestamp for consistency
full_appended_df = full_appended_df.sort_values(by='utc_timestamp').reset_index(drop=True)
logger.info(f'Sorted full appended dataframe by utc_timestamp.')

# save the full appended dataframe to the appended data folder
full_appended_df.to_csv(os.path.join(appended_data_dir, 'incl_noon_reports.csv'), index=False)
logger.info(f'Saved full appended dataframe with noon reports to CSV.')


