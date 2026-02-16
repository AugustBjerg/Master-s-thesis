# --- TODO: Keep in mind - WHAT SHOULD BE DONE NOW AND WHAT SHOULD BE DONE AFTER AGGREGATION? ---   

import pandas as pd
import numpy as np
import os
import json
import time
from typing import Dict
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

    # rename all the columns from their qids (qid_mapping) to their real names (quantity_name) using the column_metadata_df. If the qid starts with anything else than 4, it should just replace it, if it starts with 4, it should append the provider in brackets
    qid_to_name = {}
    for _, row in column_metadata_df.iterrows():
        qid = row['qid_mapping']
        name = row['quantity_name']
        provider = row['source_name']
        if qid.startswith('4'):
            qid_to_name[qid] = f"{name} ({provider})"
        else:
            qid_to_name[qid] = name
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

def _replace_inconsistent_propeller_and_engine_rpm(df, flag_columns: Dict):
    """ This function replaces rows where propeller shaft rotational speed is 0 AND main engine rotational speed is above 0 with NaN, as this is an inconsistent state.
    In non-techincal terms: if the engine is running, the propeller should be as well.
    negative values for propeller rpm is kept for now since it means the ship is in reverse (not an inconsistent/impossible state)
    """
    condition = (df['Vessel Propeller Shaft Rotational Speed'] == 0) & (df['Main Engine Rotational Speed'] > 0)
    num_inconsistent_rows = condition.sum()
    df.loc[condition, ['Vessel Propeller Shaft Rotational Speed', 'Main Engine Rotational Speed']] = np.nan
    logger.info(f'Replaced {num_inconsistent_rows} ({num_inconsistent_rows / len(df) * 100:.5f}% of df) inconsistent rows with NaN in propeller shaft rotational speed and main engine rotational speed')
    flag_column_name = 'Inconsistent Engine and Propeller RPM'
    df[flag_column_name] = condition.astype(int) # create a flag column for inconsistent rows (1 if inconsistent, 0 if not)
    flag_columns[flag_column_name] = num_inconsistent_rows # add the number of inconsistent rows to the flag_columns dict for logging later
    return df

def _replace_inconsistent_propeller_rpm_and_shaft_power(df, flag_columns: Dict):
    """rows where propeller RPM and Shaft power have different sign (must have the same sign) are replaced with NaN, as this is an inconsistent state."""
    condition = (df['Vessel Propeller Shaft Rotational Speed'] > 0) & (df['Vessel Propeller Shaft Mechanical Power'] < 0) | (df['Vessel Propeller Shaft Rotational Speed'] < 0) & (df['Vessel Propeller Shaft Mechanical Power'] > 0)
    num_inconsistent_rows = condition.sum()
    df.loc[condition, ['Vessel Propeller Shaft Rotational Speed', 'Vessel Propeller Shaft Mechanical Power']] = np.nan
    logger.info(f'Replaced {num_inconsistent_rows} ({num_inconsistent_rows / len(df) * 100:.5f}% of df) inconsistent rows with NaN in propeller shaft rotational speed and shaft power')
    flag_column_name = 'Inconsistent Propeller RPM and Shaft Power'
    flag_columns[flag_column_name] = num_inconsistent_rows # add the number of inconsistent rows to the flag_columns dict for logging later
    df[flag_column_name] = condition.astype(int) # create a flag column for inconsistent rows (1 if inconsistent, 0 if not)
    return df

def _replace_impossible_weather_values(df, flag_columns: Dict):
    """ This function replaces impossible weather values (e.g. negative wave height, negative wind speed, etc.) with NaN and creates flag columns for each type of impossible value."""
    # Define conditions for impossible weather values
    column_mapping = {
        'Negative Wave Height (Provider MB)': 'Vessel External Conditions Wave Significant Height (Provider MB)',
        'Negative Wave Height (Provider S)': 'Vessel External Conditions Wave Significant Height (Provider S)',
        'Negative Wind Speed': 'Vessel External Conditions Wind True Speed (Provider MB)',
        'Negative Wave Period': 'Vessel External Conditions Wave Period (Provider S)',
        'Negative Sea Temperature': 'Vessel External Conditions Sea Water Temperature (Provider S)',
    }
    
    for flag_column_name, column_name in column_mapping.items():
        condition = df[column_name] < 0
        num_impossible_rows = condition.sum()
        df.loc[condition, column_name] = np.nan  # Replace only the specific column values
        logger.info(f'Replaced {num_impossible_rows} ({num_impossible_rows / len(df) * 100:.5f}% of df) impossible rows with NaN for {flag_column_name}')
        df[flag_column_name] = condition.astype(int)  # Create a flag column for impossible rows (1 if impossible, 0 if not)
        flag_columns[flag_column_name] = num_impossible_rows  # Add the number of impossible rows to the flag_columns dict for logging later
    
    return df

def _replace_negative_hull_over_ground_speed(df, flag_columns: Dict):
    """ This function replaces negative hull over ground speed values with NaN, as this is an impossible state."""
    condition = df['Vessel Hull Over Ground Speed'] < 0
    num_impossible_rows = condition.sum()
    df.loc[condition, 'Vessel Hull Over Ground Speed'] = np.nan
    logger.info(f'Replaced {num_impossible_rows} ({num_impossible_rows / len(df) * 100:.5f}% of df) impossible rows with NaN in Vessel Hull Over Ground Speed')
    flag_column_name = 'Negative Hull Over Ground Speed'
    df[flag_column_name] = condition.astype(int) # create a flag column for impossible rows (1 if impossible, 0 if not)
    flag_columns[flag_column_name] = num_impossible_rows # add the number of impossible rows to the flag_columns dict for logging later
    return df

def _replace_negative_main_engine_rotation(df, flag_columns: Dict):
    """ This function replaces negative main engine rotational speed values with NaN, as this is an impossible state."""
    condition = df['Main Engine Rotational Speed'] < 0
    num_impossible_rows = condition.sum()
    df.loc[condition, 'Main Engine Rotational Speed'] = np.nan
    logger.info(f'Replaced {num_impossible_rows} ({num_impossible_rows / len(df) * 100:.5f}% of df) impossible rows with NaN in Main Engine Rotational Speed')
    flag_column_name = 'Negative Main Engine Rotational Speed'
    df[flag_column_name] = condition.astype(int) # create a flag column for impossible rows (1 if impossible, 0 if not)
    flag_columns[flag_column_name] = num_impossible_rows # add the number of impossible rows to the flag_columns dict for logging later
    return df
    
def _replace_sea_temp_dropouts(df, flag_columns: Dict):
    """ This function replaces sea temperature values that are exactly 6 or below with NaN, as this is an obvious sensor dropout (based on line graph). It also creates a flag column for sea temperature dropouts."""
    condition = df['Vessel External Conditions Sea Water Temperature (Provider S)'] <= 6
    num_dropout_rows = condition.sum()
    df.loc[condition, 'Vessel External Conditions Sea Water Temperature (Provider S)'] = np.nan
    logger.info(f'Replaced {num_dropout_rows} ({num_dropout_rows / len(df) * 100:.5f}% of df) sea temperature dropout rows with NaN')
    flag_column_name = 'Sea Temperature Dropout'
    df[flag_column_name] = condition.astype(int) # create a flag column for sea temperature dropouts (1 if dropout, 0 if not)
    flag_columns[flag_column_name] = num_dropout_rows # add the number of sea temperature dropout rows to the flag_columns dict for logging later
    return df

# --- Sentinel / dropout /invalid values (no valid measurement. SHOULD THESE BE DROPPED OR REPLACED WITH NaN?)) ---
# TODO: replace with NaN any impossible / inconsistent data points (based on single values). Remember to include flags for any inconsistent values
    # 4. Headings / angles outside their defined ranges
    # replace rows with more than 2% deviation from calculated shaft power (from rpm and torque) with NaN. Include the difference as a variable for good measure
    # Insert NaN values for cumulative revs that decrease (by making a new column with delta, and replacing both cumulative and delta with NaN where delta is negative)


def deal_with_dropouts(df, flag_columns: Dict = {}):
    
    # Apply all the wrapped functions in sequence to deal with dropout values
    df = _drop_zero_only_columns(df)
#    df = _replace_inconsistent_propeller_and_engine_rpm(df, flag_columns=flag_columns)
#    df = _replace_inconsistent_propeller_rpm_and_shaft_power(df, flag_columns=flag_columns)
#    df = _replace_impossible_weather_values(df, flag_columns=flag_columns)
#    df = _replace_negative_hull_over_ground_speed(df, flag_columns=flag_columns)
#    df = _replace_negative_main_engine_rotation(df, flag_columns=flag_columns)
    df = _replace_sea_temp_dropouts(df, flag_columns=flag_columns)
    
    return df, flag_columns

setup_output_directories(pre_agg_clean_output_dir)

column_metadata = load_column_metadata(os.path.join(meta_data_dir, 'Metrics registration.csv'))

df = load_synchronized_data(synchronized_data_dir, column_metadata, test_n=25)

logger.info(f'DataFrame loaded with shape: {df.shape}')
# logger.info(f'Dataframe info:\n{df.info()}')


df_no_dropouts, flag_columns = deal_with_dropouts(df, flag_columns={})

logger.info(f'After dealing with dropouts, DataFrame shape: {df_no_dropouts.shape}')
logger.info(f'Flag (dropout/sentinel/invalid) columns and counts of inconsistent rows: {flag_columns}')

# TODO: (optional - if noon report data is included) clean value column on noon report data from scale or unit-related contamination
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
    # 1. Start with DNVs method for rolling-window variance for stw and hull heading (include boundaries as config variable for tweaking). This should remove most "unsteady" operations. The threshold for each should be set in config for later variation
    # 2. continue with removing too low speed (below 4 knots according to Dalheim & Stein). A ship could be steady in this region without it being of interest
    # 3. negative propeller shaft rotational speed (remove rows)
    # 4. rows where propeller shaft power are 0 or negative (is either reversing/maneuvering or a bad measurement - remove). This is done because i want to document a more steady state

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