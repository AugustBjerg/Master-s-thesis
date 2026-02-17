# --- TODO: Keep in mind - WHAT SHOULD BE DONE NOW AND WHAT SHOULD BE DONE AFTER AGGREGATION? ---   

import pandas as pd
import numpy as np
import os
import json
import time
from typing import Dict, List
from multiprocessing import Pool
from loguru import logger
from config import SHAFT_POWER_MAX_DEVIATION, REQUIRED_SENSOR_VARIABLES, REQUIRED_WEATHER_VARIABLES, ROLLING_STD_THRESHOLDS, ROLLING_STD_WINDOW_SIZE, ROLLING_STD_MIN_PERIODS, SPEED_THROUGH_WATER_THRESHOLD

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

def _drop_propeller_shaft_revolutions_column(df):
    """ This function drops the column 'Vessel Propeller Shaft Revolutions (cumulative)' since it is not reliable"""
    if 'Vessel Propeller Shaft Revolutions (cumulative) (Instrument Torquemeter)' in df.columns:
        df.drop(columns=['Vessel Propeller Shaft Revolutions (cumulative)'], inplace=True)
        logger.info('Dropped column Vessel Propeller Shaft Revolutions (cumulative) due to redundancy and many dropouts')
    else:
        logger.info('Column Vessel Propeller Shaft Revolutions (cumulative) not found, skipping drop')
    return df

def drop_columns(df):
    """ This function drops columns that are not needed for the analysis, such as columns with all zero values, redundant columns, etc. It also logs the number of columns dropped and the new shape of the dataframe."""
    df = _drop_zero_only_columns(df)
    df = _drop_propeller_shaft_revolutions_column(df)
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

def _replace_impossible_angles(df, flag_columns: Dict):
    """ This function replaces impossible heading and angle values with NaN, as these are inconsistent states. It also creates flag columns for each type of impossible value."""
    column_mapping = {
        'Impossible Wind Relative Angle': 'Vessel External Conditions Wind Relative Angle',
        'Impossible Vessel Heading': 'Vessel Hull Heading True Angle',
        'Impossible Vessel External Conditions Wind True Angle (Provivider MB)': 'Vessel External Conditions Wind True Angle (Provider MB)',
        'Vessel External Conditions Wave True Angle (Provider S)': 'Vessel External Conditions Wave True Angle (Provider S)',
    }       
    
    for flag_column_name, column_name in column_mapping.items():
        condition = (df[column_name] < 0) | (df[column_name] > 360)
        num_impossible_rows = condition.sum()
        df.loc[condition, column_name] = np.nan  # Replace only the specific column values that are impossible
        logger.info(f'Replaced {num_impossible_rows} ({num_impossible_rows / len(df) * 100:.5f}% of df) impossible rows with NaN for {flag_column_name}')
        df[flag_column_name] = condition.astype(int)  # Create a flag column for impossible rows (1 if impossible, 0 if not)
        flag_columns[flag_column_name] = num_impossible_rows  # Add the number of impossible rows to the flag_columns dict for logging later

    return df

def _replace_unreliable_shaft_power(df, flag_columns: Dict):
    """ This function compares shaft power measurements to the ones that are calculated from propeller rpm and torque
    Using the formula: """

    df['Calculated Shaft Power'] = (df['Vessel Propeller Shaft Torque'] * df['Vessel Propeller Shaft Rotational Speed'] * 2 * np.pi) / 60
    condition = (df['Vessel Propeller Shaft Mechanical Power'] - df['Calculated Shaft Power']).abs() > (SHAFT_POWER_MAX_DEVIATION * df['Calculated Shaft Power'].abs())
    num_unreliable_rows = condition.sum()

    # For the rows where the condition is true, replace the measure shaft power, torque and rotational speed with NaN
    df.loc[condition, 'Vessel Propeller Shaft Mechanical Power'] = np.nan
    df.loc[condition, 'Vessel Propeller Shaft Torque'] = np.nan
    df.loc[condition, 'Vessel Propeller Shaft Rotational Speed'] = np.nan

    logger.info(f'Replaced {num_unreliable_rows} ({num_unreliable_rows / len(df) * 100:.5f}% of df) unreliable shaft power rows with NaN based on {SHAFT_POWER_MAX_DEVIATION*100:.2f}% deviation from calculated shaft power')
    flag_column_name = 'Unreliable Shaft / Torque / RPM relation'
    df[flag_column_name] = condition.astype(int) # create a flag column for unreliable shaft power (1 if unreliable, 0 if not)
    flag_columns[flag_column_name] = num_unreliable_rows # add the number of unreliable shaft power rows to the flag_columns dict for logging later
    
    return df

def _replace_seawater_velocity_dropouts(df, flag_columns: Dict):
    """ This function replaces seawater velocity values that are exactly 0 with NaN, as this is an obvious sensor dropout (based on line graph). It also creates a flag column for seawater velocity dropouts."""
    column_mapping = {
        'Vessel External Conditions Eastward Sea Water Velocity (Provider S)': 'Vessel External Conditions Eastward Sea Water Velocity (Provider S)',
        'Vessel External Conditions Northward Sea Water Velocity (Provider S)': 'Vessel External Conditions Northward Sea Water Velocity (Provider S)',
    }
    
    for flag_column_name, column_name in column_mapping.items():
        condition = df[column_name] == -0.4  # Based on histogram, -0.4 seems to be the value that is given during dropouts, rather than 0. This is likely because the sensor can measure slightly below 0 when the ship is moving very slowly, but during dropouts it gives a value of -0.4 (based on line graph). Therefore, we will consider -0.4 as the dropout value for seawater velocity.
        num_dropout_rows = condition.sum()
        df.loc[condition, column_name] = np.nan  # Replace only the specific column values that are dropouts
        logger.info(f'Replaced {num_dropout_rows} ({num_dropout_rows / len(df) * 100:.5f}% of df) seawater velocity dropout rows with NaN for {flag_column_name}')
        df[flag_column_name] = condition.astype(int)  # Create a flag column for seawater velocity dropouts (1 if dropout, 0 if not)
        flag_columns[flag_column_name] = num_dropout_rows  # Add the number of seawater velocity dropout rows to the flag_columns dict for logging later
    
    return df

def deal_with_dropouts(df, flag_columns: Dict = {}):
    
    logger.info(f'Dataframe shape before dealing with dropouts: {df.shape}')

    num_rows_before = len(df)
    num_columns_before = len(df.columns)

    # Apply all the wrapped functions in sequence to deal with dropout values
    df = _replace_inconsistent_propeller_and_engine_rpm(df, flag_columns=flag_columns)
    df = _replace_inconsistent_propeller_rpm_and_shaft_power(df, flag_columns=flag_columns)
    df = _replace_impossible_weather_values(df, flag_columns=flag_columns)
    df = _replace_negative_hull_over_ground_speed(df, flag_columns=flag_columns)
    df = _replace_negative_main_engine_rotation(df, flag_columns=flag_columns)
    df = _replace_sea_temp_dropouts(df, flag_columns=flag_columns)
    df = _replace_impossible_angles(df, flag_columns=flag_columns)
    df = _replace_unreliable_shaft_power(df, flag_columns=flag_columns)
    df = _replace_seawater_velocity_dropouts(df, flag_columns=flag_columns)

    # Calculate the total number of NaN values added to the dataframe from the values in the flag_columns object
    total_added_NaNs = sum(flag_columns.values())
    total_added_NaNs_percentage = total_added_NaNs / (num_rows_before * num_columns_before) * 100

    logger.info(f'Dataframe shape after dealing with dropouts: {df.shape}. Replaced {total_added_NaNs} values with NaN ({total_added_NaNs_percentage:.2f}% of values in original dataframe) and {len(flag_columns)} flag columns.')

    return df, flag_columns

def _trim_segment_boundary(segment_df, required_weather_variables: List, required_sensor_variables: List):
    """
    Trims the first and last row of a single segment if they have NaN values in any
    required sensor variables. Before removing, any non-NaN weather values are moved
    to the adjacent row (next for first, previous for last) so hourly weather data
    is preserved.

    Args:
        segment_df: DataFrame slice for a single seg_id (must have at least 3 rows).
        required_weather_variables: Weather columns whose values should be preserved.
        required_sensor_variables: Sensor columns that must be non-NaN to keep a boundary row.

    Returns:
        Trimmed segment DataFrame.
    """
    if len(segment_df) < 3:
        return segment_df  # too short to trim safely

    idx = segment_df.index

    # --- First row ---
    first_idx = idx[0]
    second_idx = idx[1]
    first_row_has_sensor_nan = segment_df.loc[first_idx, required_sensor_variables].isna().any()

    if first_row_has_sensor_nan:
        # Move non-NaN weather values from the first row to the second row
        for col in required_weather_variables:
            if col in segment_df.columns and pd.notna(segment_df.loc[first_idx, col]) and pd.isna(segment_df.loc[second_idx, col]):
                segment_df.loc[second_idx, col] = segment_df.loc[first_idx, col]
        segment_df = segment_df.drop(index=first_idx)

    # --- Last row ---
    idx = segment_df.index  # refresh after potential drop
    if len(segment_df) < 2:
        return segment_df

    last_idx = idx[-1]
    second_last_idx = idx[-2]
    last_row_has_sensor_nan = segment_df.loc[last_idx, required_sensor_variables].isna().any()

    if last_row_has_sensor_nan:
        # Move non-NaN weather values from the last row to the second-to-last row
        for col in required_weather_variables:
            if col in segment_df.columns and pd.notna(segment_df.loc[last_idx, col]) and pd.isna(segment_df.loc[second_last_idx, col]):
                segment_df.loc[second_last_idx, col] = segment_df.loc[last_idx, col]
        segment_df = segment_df.drop(index=last_idx)

    return segment_df

def _filter_segment_start_and_ends(df, required_weather_variables: List, required_sensor_variables: List):
    """
    This function groups the dataframe by seg_id and trims the first and last row of these segments as these contain many NaN values that should be imputed. 

    It does the following for each first and last row of a given segment:
    - If any of the required weather variables are not NaN, move that value to the next row (for the first row) or previous row (for the last row). This is because we want to preserve this information as much as possible, and moving it 15 seconds is a minor imputation compared to losing 1hr of weather data.
    - remove the row (first or last) if there is a NaN value in any of the required sensor variables.

    """
    rows_before = len(df)
    
    # Filter lists to only include columns actually present in the dataframe
    weather_vars = [c for c in required_weather_variables if c in df.columns]
    sensor_vars = [c for c in required_sensor_variables if c in df.columns]

    trimmed_segments = []
    for seg_id, segment in df.groupby('seg_id', sort=False):
        trimmed_segments.append(
            _trim_segment_boundary(segment, weather_vars, sensor_vars)
        )

    df = pd.concat(trimmed_segments, ignore_index=True)
    rows_removed = rows_before - len(df)
    logger.info(f'Filtered segment boundaries: removed {rows_removed} rows ({rows_removed / rows_before * 100:.4f}% of df) that had NaN in required sensor variables at segment starts/ends')
    return df

def _remove_required_sensor_nans(df, required_sensor_variables: List):
    """ Removes rows where any of the required sensor variables have a NaN value, as these are not useful for the analysis."""
    sensor_vars = [c for c in required_sensor_variables if c in df.columns]
    condition = df[sensor_vars].isna().any(axis=1)
    num_rows_removed = condition.sum()
    df = df[~condition]
    logger.info(f'Removed {num_rows_removed} rows with NaN in at least one required sensor variable, resulting in a new shape of {df.shape}')
    return df

def filter_nans(df, required_weather_variables=REQUIRED_WEATHER_VARIABLES, required_sensor_variables=REQUIRED_SENSOR_VARIABLES):
    """ This function removes rows where key columns have NaN values, as these are not useful for the analysis and we want to focus on documenting a more steady state of the ship. The key columns are: Vessel Propeller Shaft Mechanical Power, Vessel Hull Over Ground Speed, Vessel Speed Through Water."""
    
    rows_before = len(df)
    logger.info(f'number of observations before NaN filtering: {len(df)}')
    
    df = _filter_segment_start_and_ends(df, required_weather_variables=required_weather_variables, required_sensor_variables=required_sensor_variables)
    df = _remove_required_sensor_nans(df, required_sensor_variables=required_sensor_variables)
    
    logger.info(f'removed {rows_before - len(df)} rows ({(rows_before - len(df)) / rows_before * 100:.2f}% of original observations) after NaN filtering')
    return df

def _filter_by_rolling_stds(df, rolling_std_thresholds=ROLLING_STD_THRESHOLDS, rolling_std_window_size=ROLLING_STD_WINDOW_SIZE, rolling_std_min_periods=ROLLING_STD_MIN_PERIODS):
    """ This function identifies steady states by calculating a rolling window variance for speed through water, speed over ground and hull heading.
    if the rolling standard deviation is too high (thresholds defined in config file), these rows are removed for being "unsteady".
    """
    rows_before = len(df)
    
    # Filter to only columns present in the dataframe
    thresholds = {col: thresh for col, thresh in rolling_std_thresholds.items() if col in df.columns}
    
    if not thresholds:
        logger.warning('No rolling std threshold columns found in dataframe, skipping rolling std filtering')
        return df

    # Compute rolling standard deviation per segment for each variable
    unsteady_mask = pd.Series(False, index=df.index)
    
    for col, threshold in thresholds.items():
        rolling_std = (
            df.groupby('seg_id')[col]
            .transform(lambda x: x.rolling(window=rolling_std_window_size, min_periods=rolling_std_min_periods).std())
        )
        exceeds = rolling_std > threshold
        valid = rolling_std.notna()
        n_exceeds = exceeds.sum()
        n_valid = valid.sum()
        logger.info(f'Rolling std filter for {col}: {n_exceeds:,} / {n_valid:,} ({n_exceeds / n_valid * 100:.2f}%) observations exceed threshold {threshold}')
        
        unsteady_mask = unsteady_mask | exceeds
    
    # Also flag rows where rolling std could not be calculated (insufficient data in window) as unsteady,
    # but only if they are NaN for ALL variables (avoid removing too many at segment starts)
    all_rolling_nan = pd.Series(True, index=df.index)
    for col in thresholds:
        rolling_std = (
            df.groupby('seg_id')[col]
            .transform(lambda x: x.rolling(window=rolling_std_window_size, min_periods=rolling_std_min_periods).std())
        )
        all_rolling_nan = all_rolling_nan & rolling_std.isna()
    
    # Don't remove rows where the rolling std is NaN — these are just early-window rows
    # Only remove rows that actively exceeded a threshold
    df = df[~unsteady_mask]
    
    rows_removed = rows_before - len(df)
    logger.info(f'Rolling std filtering: removed {rows_removed} rows ({rows_removed / rows_before * 100:.2f}% of df) that exceeded at least one rolling std threshold')
    return df

def _filter_low_speed_rows(df, speed_threshold=SPEED_THROUGH_WATER_THRESHOLD):
    """ This function removes rows where the speed through water is below a certain threshold, as these are not of interest for the analysis and could represent the ship being steady in a low-speed region."""
    if 'Vessel Hull Through Water Longitudinal Speed' not in df.columns:
        logger.warning('Column Vessel Hull Through Water Longitudinal Speed not found in dataframe, skipping low speed filtering')
        return df
    
    rows_before = len(df)
    condition = df['Vessel Hull Through Water Longitudinal Speed'] < speed_threshold
    df = df[~condition]
    rows_removed = condition.sum()
    logger.info(f'Low speed filtering: removed {rows_removed} rows ({rows_removed / rows_before * 100:.2f}% of df) with speed through water below {speed_threshold} knots')
    return df

def _filter_negative_propeller_shaft_rpm(df):
    """ This function removes rows where the propeller shaft rotational speed is negative, as this indicates the ship is in reverse or maneuvering, which is not of interest for the analysis."""
    if 'Vessel Propeller Shaft Rotational Speed' not in df.columns:
        logger.warning('Column Vessel Propeller Shaft Rotational Speed not found in dataframe, skipping negative propeller shaft rpm filtering')
        return df
    
    rows_before = len(df)
    condition = df['Vessel Propeller Shaft Rotational Speed'] < 0
    df = df[~condition]
    rows_removed = condition.sum()
    logger.info(f'Negative propeller shaft RPM filtering: removed {rows_removed} rows ({rows_removed / rows_before * 100:.2f}% of df) with negative propeller shaft rotational speed')
    return df

def _filter_neg_or_zero_shaft_power(df):
    """ This function removes rows where the propeller shaft mechanical power is negative or zero, as this indicates the ship is in reverse, maneuvering, or a bad measurement, which are not of interest for the analysis."""
    if 'Vessel Propeller Shaft Mechanical Power' not in df.columns:
        logger.warning('Column Vessel Propeller Shaft Mechanical Power not found in dataframe, skipping negative/zero shaft power filtering')
        return df
    
    rows_before = len(df)
    condition = df['Vessel Propeller Shaft Mechanical Power'] <= 0
    df = df[~condition]
    rows_removed = condition.sum()
    logger.info(f'Negative/zero shaft power filtering: removed {rows_removed} rows ({rows_removed / rows_before * 100:.2f}% of df) with non-positive propeller shaft mechanical power')
    return df

def filter_undesired_rows(df, rolling_std_thresholds=ROLLING_STD_THRESHOLDS, rolling_std_window_size=ROLLING_STD_WINDOW_SIZE, rolling_std_min_periods=ROLLING_STD_MIN_PERIODS):
    """ This function applies various filters to remove "undesirable" rows from the dataframe, such as rows where the ship is in reverse or maneuvering, rows with heavy/violent weather, etc. The specific filters applied are based on the thresholds defined in the config file and the judgment of what constitutes "undesirable" data for the analysis."""
    df = _filter_by_rolling_stds(df, rolling_std_thresholds=rolling_std_thresholds, rolling_std_window_size=rolling_std_window_size, rolling_std_min_periods=rolling_std_min_periods)
    df = _filter_low_speed_rows(df, speed_threshold=SPEED_THROUGH_WATER_THRESHOLD)
    df = _filter_negative_propeller_shaft_rpm(df)
    df = _filter_neg_or_zero_shaft_power(df)

    return df

# Load the dataframe and metadata
setup_output_directories(pre_agg_clean_output_dir)

column_metadata = load_column_metadata(os.path.join(meta_data_dir, 'Metrics registration.csv'))

df = load_synchronized_data(
    synchronized_data_dir, column_metadata, 
    test_n=25
    )

logger.info(f'DataFrame loaded with shape: {df.shape}')

# Execute the functions in sequence

# --- Dropping of undesired columns ---
df = drop_columns(df)

# -- Replace dropouts and inconsistent values with NaN and create flag columns for them ---
df, flag_columns = deal_with_dropouts(df, flag_columns={})

logger.info(f'Flag (dropout/sentinel/invalid) columns and counts of inconsistent rows: {flag_columns}')

nan_percentages = df.isna().mean() * 100
nan_percentages = nan_percentages[nan_percentages > 0].sort_values(ascending=False)
logger.info(f'Percentage of NaN values per column after dealing with dropouts:\n{nan_percentages}')

# --- Remove rows with NaN in required Sensor columns --- 
df = filter_nans(df)

# Make the same log again but after NaN filtering, to see the impact of this step on the dataframe
nan_percentages_after_filtering = df.isna().mean() * 100
nan_percentages_after_filtering = nan_percentages_after_filtering[nan_percentages_after_filtering > 0].sort_values(ascending=False)
logger.info(f'Percentage of NaN values per column with NaN filtering:\n{nan_percentages_after_filtering}')

# --- Filtering undesired (non-steady) state rows ---
df = filter_undesired_rows(df)

# Save the final df to a csv file in the pre_agg_clean_output_dir
output_file_path = os.path.join(cleaned_not_aggregated_data_dir, 'pre_agg_cleaned_data.csv')
df.to_csv(output_file_path, index=False)
logger.info(f'Saved pre-agg cleaned data to {output_file_path}')

logger.info(f'Final shape so far: {df.shape}')

# --- Repeated values ---
# Flag repeated values for all weather variables (ignoreing NaN)
# Flag repeated values for relevant sensor variables (only if they are present in the dataframe)
    # (start by making a function that just flags the suspicious values and prints them in the log. Then i will decide what action to take)
    # 1. Scavenging Air Pressure
    # 2. Fuel Oil inlet mass flow
    # 3. Shaft Torque
    # 4. Shaft thrust force
    # 5. Shaft mechanical power

# --- Outlier removal ---
# TODO: for every column, have chat pick obvious (for physical/practical reasons) thresholds that a no-brainer outliers
# TODO: after that, consider adding additional outlier removal based on statistical methods (e.g. IQR method, z-score method, etc.)

    # 1. Propeller shaft power and propeller shaft rotational speed has some pretty clear outliers with negative values (remove)

# TODO: add required Noon Report data and decide on imputation strategy

# --- Formatting --- 
    # 1. Change the sign on thrust force (currently negative)
    # 2. Rename columns to their real names instead of qids

# TODO: (optional - if noon report data is included) clean value column on noon report data from scale or unit-related contamination

# TODO: include something that saves the logs to the output folder (so i can ask chatgpt to make a table of it in LateX)

# TODO: optional
    # Remove rows where the ship is "cruising" (propeller turned off / 0 but still moving)
    # Make a function that removes all columns not included as "required" columns in the config file
