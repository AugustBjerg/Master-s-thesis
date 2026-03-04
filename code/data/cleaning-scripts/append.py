import os
import glob
import pandas as pd
import numpy as np
from loguru import logger
from config import EXPECTED_SENSOR_OBSERVATIONS, SPEED_THROUGH_WATER_THRESHOLD, DELTA_FPI_QID, FPI_QID
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

def _stw_weight_term(stw, maneuvering_threshold_knots=SPEED_THROUGH_WATER_THRESHOLD, epsilon=0.1):
    # divide the maneuvering threshold by 2 to get the v_0 value
    pass

def _water_temp_term(water_temp, water_temp_threshold_degrees=10):
    pass

def _fpi_change(stw, water_temp, delta_t, maneuvering_threshold, water_temp_threshold_degrees=10, epsilon=0.1):
    # combine the two functions above, and check if the value is positive. If it is not positive, throw an error
    # Inside here we multiply by delta t to give the correct "time elapsed" weight
    pass

def add_fouling_penalty_index(df, water_temp_qid, stw_qid, epsilon=0.1, water_temp_threshold_degrees=10, fouling_index_name="fouling_penalty_index", stw_staleness_threhsold_sec=300, water_temp_staleness_threshold_sec=21600): 
    """
    
    keep in mind that the df is structure in a way where each row is an independent measurement of of a given variable with the columns ['utc_timestamp', 'qid_mapping', 'value', 'quantity_name', 'source_name', 'unit', 'time_delta_sec']

    Adds a fouling penalty index that is based on idle time and water temperature. It starts by calculating the change in fouling pressure for each passing timestamp:
    1. Choose the speed through water (STW) column as the base series (these are the time stamps we will be following)
    2. for Each row that is an observation of STW, get the latest value for water temp, relative to that utc_timestamp, as well as the time elapsed
    3. for each row that is an observation of STW, make a new row that contains the calculated change in fouling pressure (_fpi_change) based on the stw weight term and the water temp weight term
         For each new observatio of fpi_change, the columns should be populated as follows:
            - utc_timestamp: same as the corresponding STW observation
            - qid_mapping: DELTA_FPI_QID (from config)
            - value: the calculated change in fouling pressure
            - quantity_name: "fouling_pressure_change"
            - source_name: "calculated"
            - unit: "calculated"
            - time_delta_sec: same as the corresponding STW observation
    4. for each row created, create another row that contains the cumulative sum of the change in fouling pressure 
        For each new observation of cumulative fpi, the columns should be populated as follows:
            - utc_timestamp: same as the corresponding STW observation
            - qid_mapping: FPI_QID (from config)
            - value: the cumulative sum of the change in fouling pressure
            - quantity_name: "fouling_penalty_index"
            - source_name: "calculated"
            - unit: "calculated"
            - time_delta_sec: same as the corresponding STW observation

    # Finally, log the number of rows that were added, and return the new dataframe with the added rows for fouling penalty index and change in fouling pressure

    """
    return df

if __name__ == "__main__":
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

    # -- Step 3: Add a column for the fouling penalty index
    logger.info(f'columns in the dataframe at this point: {appended_df.columns.tolist()}')

    # Save this version of the appended df (excl. noon report data) to the folder
    os.makedirs(appended_data_dir, exist_ok=True)
    appended_df.to_csv(os.path.join(appended_data_dir, 'excl_noon_reports.csv'), index=False)
