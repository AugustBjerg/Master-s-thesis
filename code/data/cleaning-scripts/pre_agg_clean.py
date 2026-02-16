# --- TODO: Keep in mind - WHAT SHOULD BE DONE NOW AND WHAT SHOULD BE DONE AFTER AGGREGATION? ---   

import pandas as pd
import numpy as np
import os
import json
import time
from multiprocessing import Pool
from loguru import logger

script_dir = os.path.dirname(os.path.abspath(__file__))
synchronized_data_dir = os.path.join(script_dir, '..', 'synchronized')
cleaned_not_aggregated_data_dir = os.path.join(script_dir, '..', 'cleaned-not-aggregated')
pre_agg_clean_output_dir = os.path.join(script_dir, '..', '..', 'outputs', 'pre-agg-cleaning')
meta_data_dir = os.path.join(script_dir, '..', 'metadata')

script_start = time.perf_counter()

def setup_output_directories(output_dir):
    """
    Create output directory if it doesn't exist, otherwise log that it already exists.
    
    Args:
        output_dir: Path to the output directory.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f'Created output directory: {output_dir}')
    else:
        logger.info(f'Output directory already exists: {output_dir}')

def _read_synchronized_file(file_path):
    """Helper function for parallel file reading."""
    df = pd.read_csv(file_path)
    logger.info(f'Read {file_path} — shape: {df.shape}')
    return df

def load_synchronized_data(data_dir, column_metadata_df, test_n=None):
    """
    Loads all synchronized CSV files from data_dir into a single DataFrame.
    
    Args:
        data_dir: Path to the directory containing synchronized CSV files.
        column_metadata_df: DataFrame containing metadata about columns (e.g. qid to name mappings and units).
        test_n: If provided, only load the first n files (for faster testing).
    
    Returns:
        A single concatenated DataFrame containing all loaded files.
    """
    all_files = sorted(
        [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    )
    logger.info(f'Found {len(all_files)} synchronized CSV files in {data_dir}')

    if test_n is not None:
        all_files = all_files[:test_n]
        logger.info(f'Test mode: loading only the first {test_n} file(s)')

    with Pool(min(os.cpu_count() - 1, len(all_files))) as pool:
        dfs = pool.map(_read_synchronized_file, all_files)

    combined_df = pd.concat(dfs, ignore_index=True)

    # rename all the columns from their qids (qid_mapping) to their real names (quantity_name) using the column_metadata_df
    qid_to_name = dict(zip(column_metadata_df['qid_mapping'], column_metadata_df['quantity_name']))
    combined_df.rename(columns=qid_to_name, inplace=True)

    # reformat the utc_timestamp column to datetime (utc, ISO 8601 format)
    combined_df['utc_timestamp'] = pd.to_datetime(combined_df['utc_timestamp'], format='ISO8601',utc=True)

    logger.info(f'Combined DataFrame shape: {combined_df.shape}')
    return combined_df

def load_column_metadata(file_path):
    metadata = pd.read_csv(file_path)
    return metadata

def _drop_zero_only_columns(df):
    """ This function removes columns that only have zero or NaN values."""
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    logger.info(f'Checking {len(numeric_cols)} numeric columns for all-zero values')
    zero_only_cols = [col for col in numeric_cols if (df[col].dropna().values == 0).all()]
    logger.info(f'Found {len(zero_only_cols)} columns with all zero values: {zero_only_cols}')
    if zero_only_cols:
        df.drop(columns=zero_only_cols, inplace=True)
        logger.info(f'Dropped {len(zero_only_cols)} columns with all zero values: {zero_only_cols}')
    else:
        logger.info(f'No columns with all zero values found')
    return df

def deal_with_dropouts(df):
    
    df_no_zero_only = _drop_zero_only_columns(df)

    return df_no_zero_only

setup_output_directories(pre_agg_clean_output_dir)

column_metadata = load_column_metadata(os.path.join(meta_data_dir, 'Metrics registration.csv'))

df = load_synchronized_data(synchronized_data_dir, column_metadata, test_n=10)

logger.info(f'DataFrame loaded with shape: {df.shape}')
logger.info(f'Dataframe info:\n{df.info()}')

# --- Sentinel / dropout /invalid values (no valid measurement. SHOULD THESE BE DROPPED OR REPLACED WITH NaN?)) ---
# TODO: remove (or replace with NaN - TBD) impossible / inconsistent data points (based on single values)
    # Drop draft columns with all 0 measurements
     # 1. rows where propeller shaft rotational speed is 0 AND main engine rotational speed is above 0
     # 2. rows where propeller RPM and Shaft power have different sign (must have the same sign)
    # 3. rows where wave height, wave period, wind speed are negative 
    # 4. Headings / angles outside their defined ranges
    # 5. replace negative hull over ground speed values with Nan
    # 6. replace negative values of main engine rotation with NaN (it cannot be negative)
     # 1. replace sea temp = exactly 6 or below with NaN (obvious sensor dropout - see line graph)
    # Insert NaN values for cumulative revs that decrease (by making a new column with delta, and replacing both cumulative and delta with NaN where delta is negative)

df_no_dropouts = deal_with_dropouts(df)

logger.info(f'After dealing with dropouts, DataFrame shape: {df_no_dropouts.shape}')

# TODO: (optional - if noon report data is included)clean value column on noon report data from scale or unit-related contamination
# TODO: Make sure this plan is represented in onenote
# TODO: make the below as functions and apply them to each synchronized time segment separately, either as seperated files or as gruops in the pd.dataframe (to avoid interpolating across intervals)
# TODO: every time something is removed, make sure to log the amount of N and % of both the original dataframe length and the current dataframe length that is removed





# Optional
    # 7. rows where Vessel External Conditions Eastward AND northward (2 variables) Sea Water Velocity (provider MB) is 0. Consider whether this is likely to just be a "near 0" value
    # 8. rows where Vessel External Conditions wave signfiicant height (provider S) is 0. Consider whether this is likely to just be a "near 0" value
    # 9. rows where Vessel External Conditions Eastward AND northward (2 variables) Sea Water Velocity (provider S) is -0.4. Consider whether this is likely to just be a "near 0" value
    # 10. Swell significant height of 0

# --- NaN removal where appropriate ---

# --- CLEANING of "undesirable" data ---
# TODO: remove any signs of a ship "in reverse" or maneuvering
    # 1. Start with DNVs method for rolling-window variance for stw and hull heading (include boundaries as config variable for tweaking). This should remove most "unsteady" operations
    # 2. continue with removing too low speed (below 4 knots according to Dalheim & Stein). A ship could be steady in this region without it being of interest
    # 3. negative propeller shaft rotational speed (remove rows)
    # 4. rows where propeller shaft power are negative (is either reversing/maneuvering or a bad measurement - remove). This is done because i want to document a more steady state

# TODO: optional
    # 1. (optional) remove rows where the ship is "cruising" (propeller turned off / 0 but still moving)
    # 2. heavy/violent weather (i think i will keep this for now in case there is some variance to feed the model with)
    # 3. Anything where the ship is accerelating very fast (not yet sure if this might actually be useful information to the model, so keep for now)

# TODO: deal with observations with incongruent main engine fuel load and shaft power

# --- Outlier removal ---
# TODO: for every column, have chat pick obvious (for physical/practical reasons) thresholds that a no-brainer outliers
# TODO: after that, consider adding additional outlier removal based on statistical methods (e.g. IQR method, z-score method, etc.)

    # 1. Propeller shaft power and propeller shaft rotational speed has some pretty clear outliers with negative values (remove)

# --- NaN imputation ---
# TODO: when imputing, keep a dummy column that flags "imputed" so i retain the information that this was a bad measurement
# TODO: TBD - maybe impute weather data now - maybe wait until aggregation step

# TODO: for sea temp: 
    # if less than 4 in a row and not in the end of a time segment, use linear interpolation
    # if 4 or more, impute with the median for that time segment (Note in report that it is model convenience / judgment call, not strict practice)

# --- Formatting --- 
    # 1. Change the sign on thrust force (currently negative)
    # 2. Rename columns to their real names instead of qids