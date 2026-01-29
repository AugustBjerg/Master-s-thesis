import os
import glob
import pandas as pd
import numpy as np
from loguru import logger

# -- STEP 1: append all monthly observation files into a single dataframe --

# Defining prerequisites for appending loop
columns = [
    'utc_timestamp',
    'qid_mapping',
    'value',
]
appended_df = pd.DataFrame(columns=columns)
parent_dir = os.path.dirname(os.getcwd())
sensor_dictionary_path = os.path.join(parent_dir, 'Master\'s Thesis', 'code', 'data', 'metadata', 'Metrics registration.csv')

# Loop itself
for month in range(1, 13):
    input_pattern = os.path.join(parent_dir, 'Master\'s Thesis', 'code', 'data', 'raw', 'unzipped', str(month), str(month) + '.csv')
    logger.info(f'Processing month: {month}, pattern: {input_pattern}')

    for file_path in glob.glob(input_pattern):
        logger.info(f'Appending file: {file_path}')
        df = pd.read_csv(file_path, names=columns, parse_dates=['utc_timestamp'])
        logger.info(f'File shape: {df.shape}')
        appended_df = pd.concat([appended_df, df], ignore_index=True)
        logger.info(f'Appended DataFrame shape: {appended_df.shape}')

logger.info(f'Final appended DataFrame shape: {appended_df.shape}')

# -- STEP 2: Add columns from metrics registration file --

sensor_dict_df = pd.read_csv(sensor_dictionary_path)

# set the value for "unit" of Vessel Propeller Shaft Revolutions to "revs", because it was missing in the original file from Mærsk
sensor_dict_df.loc[sensor_dict_df['quantity_name'] == 'Vessel Propeller Shaft Revolutions', 'unit'] = 'revs'

# save it to a csv file again to keep the correction
sensor_dict_df.to_csv(sensor_dictionary_path, index=False)



# step 2: include observations from noon reports as observations
# step 3: check that it was done correctly

