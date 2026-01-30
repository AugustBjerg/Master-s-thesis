import os
import glob
import pandas as pd
import numpy as np
from loguru import logger
from config import EXPECTED_SENSOR_OBSERVATIONS

# -- STEP 1: append all monthly observation files into a single dataframe   --

# Defining prerequisites for appending loop
columns = [
    'utc_timestamp',
    'qid_mapping',
    'value',
]
appended_df = pd.DataFrame(columns=columns)
parent_dir = os.path.dirname(os.getcwd())
appended_data_dir = os.path.join(parent_dir, 'Master\'s Thesis', 'code', 'data', 'appended')
sensor_dictionary_path = os.path.join(parent_dir, 'Master\'s Thesis', 'code', 'data', 'metadata', 'Metrics registration.csv')

# Loop itself
for month in range(1, 13):
    input_pattern = os.path.join(parent_dir, 'Master\'s Thesis', 'code', 'data', 'raw', 'unzipped', str(month), str(month) + '.csv')

    for file_path in glob.glob(input_pattern):
        df = pd.read_csv(file_path, names=columns, parse_dates=['utc_timestamp'])
        appended_df = pd.concat([appended_df, df], ignore_index=True)
        logger.info(f'Appended file: {file_path} with shape: {df.shape}')

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

# Merge sensor metadata onto the appended dataframe
appended_df = appended_df.merge(
    sensor_dict_df[['qid_mapping', 'quantity_name', 'source_name', 'unit']], 
    on='qid_mapping', 
    how='left'
)

logger.info(f'Added sensor metadata columns. Final shape: {appended_df.shape}')

# Sort by timestamp for consistency
appended_df = appended_df.sort_values(by='utc_timestamp').reset_index(drop=True)

# Save this version of the appended df (excl. noon report data) to the folder
appended_df.to_csv(os.path.join(appended_data_dir, 'excl_noon_reports.csv'), index=False)
