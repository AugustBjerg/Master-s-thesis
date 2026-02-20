import pandas as pd
import numpy as np
import os
import json
import time
from datetime import datetime
from typing import Dict, List
from multiprocessing import Pool
from loguru import logger
from config import SHAFT_POWER_MAX_DEVIATION, REQUIRED_SENSOR_VARIABLES, REQUIRED_WEATHER_VARIABLES, ROLLING_STD_THRESHOLDS, ROLLING_STD_WINDOW_SIZE, ROLLING_STD_MIN_PERIODS, SPEED_THROUGH_WATER_THRESHOLD, NO_REPETITION_SENSOR_VARIABLES, SENSOR_SPIKE_THRESHOLDS, LOW_PASS_MIN_PERIODS, LOW_PASS_WINDOW_SIZE_SECONDS, MAX_CONSECUTIVE_SPIKES

script_dir = os.path.dirname(os.path.abspath(__file__))
synchronized_data_dir = os.path.join(script_dir, '..', 'synchronized')
filtered_data_dir = os.path.join(script_dir, '..', 'filtered')
filtering_output_dir = os.path.join(script_dir, '..', '..', 'outputs', 'filtering')
meta_data_dir = os.path.join(script_dir, '..', 'metadata')

script_start = time.perf_counter()

# Create the filtering output directory for filtering results if it doesn't exist
if not os.path.exists(filtering_output_dir):
    os.makedirs(filtering_output_dir)
    logger.info(f'Created filtering output directory: {filtering_output_dir}')
else:
    logger.info(f'Filtering output directory already exists: {filtering_output_dir}')

log_path = os.path.join(filtering_output_dir, f'pre_agg_cleaning_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logger.add(
    log_path,
    level='INFO',
    format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}'
)

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
        condition = df[column_name] == -0.4  # Based on histogram, -0.4 seems to be the value that is given during dropouts, rather than 0.
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

def _filter_low_propeller_shaft_rpm(df):
    """ This function removes rows where the propeller shaft rotational speed is below a certain threshold, as this indicates the ship is in reverse or maneuvering, which is not of interest for the analysis."""
    if 'Vessel Propeller Shaft Rotational Speed' not in df.columns:
        logger.warning('Column Vessel Propeller Shaft Rotational Speed not found in dataframe, skipping low propeller shaft rpm filtering')
        return df
    
    rows_before = len(df)
    condition = df['Vessel Propeller Shaft Rotational Speed'] < 50
    df = df[~condition]
    rows_removed = condition.sum()
    logger.info(f'Low propeller shaft RPM filtering: removed {rows_removed} rows ({rows_removed / rows_before * 100:.2f}% of df) with low propeller shaft rotational speed')
    return df

def _filter_low_engine_rpm(df):
    """ This function removes rows where the main engine rotational speed is below a certain threshold, as this indicates the ship is in reverse or maneuvering, which is not of interest for the analysis."""
    if 'Main Engine Rotational Speed' not in df.columns:
        logger.warning('Column Main Engine Rotational Speed not found in dataframe, skipping low engine rpm filtering')
        return df
    
    rows_before = len(df)
    condition = df['Main Engine Rotational Speed'] < 50
    df = df[~condition]
    rows_removed = condition.sum()
    logger.info(f'Low engine RPM filtering: removed {rows_removed} rows ({rows_removed / rows_before * 100:.2f}% of df) with low main engine rotational speed')
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
    df = _filter_low_propeller_shaft_rpm(df)
    df = _filter_low_engine_rpm(df)
    df = _filter_neg_or_zero_shaft_power(df)

    return df

def _mark_repeated_weather_values(df, repeated_values_flag_columns: Dict):
    """ This function flags repeated values for all weather variables (ignoring NaN) and creates flag columns for them."""
    weather_cols = [col for col in df.columns if any(var in col for var in REQUIRED_WEATHER_VARIABLES)]
    
    for col in weather_cols:
        condition = df[col].notna() & (df[col] == df.groupby('seg_id')[col].shift())
        num_observations = df[col].notna().sum()
        num_repeated = condition.sum()
        percentage = (num_repeated / num_observations * 100) if num_observations > 0 else 0
        logger.info(f'Flagged {num_repeated} ({percentage:.2f} %) repeated values in weather variable {col}')
        flag_column_name = f'Repeated Values in {col}'
        df[flag_column_name] = condition.astype(int)  # Create a flag column for repeated values (1 if repeated, 0 if not)
        repeated_values_flag_columns[flag_column_name] = num_repeated  # Add the number of repeated values to the flag_columns dict for logging later
    
    return df

def _mark_repeated_sensor_values(df, repeated_values_flag_columns: Dict, no_repetition_sensor_variables=NO_REPETITION_SENSOR_VARIABLES):
    """ This function flags repeated values for relevant sensor variables (only if they are present in the dataframe) and creates flag columns for them. The specific sensor variables to check for repetitions are defined in the config file as no_repetition_sensor_variables, as these are variables that we would not expect to have the same value in consecutive rows during steady states."""
    sensor_cols = [col for col in df.columns if any(var in col for var in no_repetition_sensor_variables)]
    
    for col in sensor_cols:
        condition = df[col].notna() & (df[col] == df.groupby('seg_id')[col].shift())
        num_observations = df[col].notna().sum()
        num_repeated = condition.sum()
        percentage = (num_repeated / num_observations * 100) if num_observations > 0 else 0
        logger.info(f'Flagged {num_repeated} ({percentage:.2f} %) repeated values in sensor variable {col}')
        flag_column_name = f'Repeated Values in {col}'
        df[flag_column_name] = condition.astype(int)  # Create a flag column for repeated values (1 if repeated, 0 if not)
        repeated_values_flag_columns[flag_column_name] = num_repeated  # Add the number of repeated values to the flag_columns dict for logging later
    
    return df

def flag_repeated_values(df, repeated_values_flag_columns: Dict, no_repetition_sensor_variables=NO_REPETITION_SENSOR_VARIABLES):
        """ This function applies the repeated values flagging for both weather and sensor variables."""
        df = _mark_repeated_weather_values(df, repeated_values_flag_columns=repeated_values_flag_columns)
        df = _mark_repeated_sensor_values(df, repeated_values_flag_columns=repeated_values_flag_columns, no_repetition_sensor_variables=no_repetition_sensor_variables)
        return df

def _mark_spikes(df, spike_columns: Dict, spike_thresholds=SENSOR_SPIKE_THRESHOLDS, rolling_window_size=LOW_PASS_WINDOW_SIZE_SECONDS, rolling_min_periods=LOW_PASS_MIN_PERIODS):
    """ This function marks spikes in the data based on a median + MAD method. It creates flag columns for the spikes and counts how many observations were marked as spikes for each variable."""
    for col, threshold in spike_thresholds.items():
        if col not in df.columns:
            logger.warning(f'Column {col} not found in dataframe, skipping spike detection for this variable')
            continue
        
        # Calculate rolling median per segment
        rolling_median = df.groupby('seg_id')[col].transform(
            lambda x: x.rolling(window=rolling_window_size, min_periods=rolling_min_periods).median()
        )

        # Vectorized MAD: take rolling median of absolute deviations from rolling_median
        # This avoids the extremely slow .apply(lambda) that ran Python per window
        abs_deviation = (df[col] - rolling_median).abs()
        mad = abs_deviation.groupby(df['seg_id']).transform(
            lambda x: x.rolling(window=rolling_window_size, min_periods=rolling_min_periods).median()
        )
        
        # Identify spikes
        condition = (df[col] - rolling_median).abs() > (threshold * mad)
        num_spikes = condition.sum()
        num_observations = df[col].notna().sum()
        percentage = (num_spikes / num_observations * 100) if num_observations > 0 else 0
        
        flag_column_name = f'Spike in {col}'
        df[flag_column_name] = condition.astype(int)
        spike_columns[flag_column_name] = num_spikes

    # Log summary
    spike_summary = {name.replace('Spike in ', ''): f'{count:,} ({count / len(df) * 100:.2f}%)' for name, count in spike_columns.items()}
    logger.info(f'Spike detection summary: {json.dumps(spike_summary, indent=2)}')
    
    return df

def _mark_consecutive_spikes(df, spike_columns: Dict):
    """ This function marks consecutive spikes for each variable in the data based on the spike flag columns."""
    max_consec_summary = {}
    for flag_col_name in spike_columns.keys():
        if flag_col_name not in df.columns:
            logger.warning(f'Spike flag column {flag_col_name} not found in dataframe, skipping consecutive spike counting')
            continue

        consec_col_name = f'Consecutive {flag_col_name}'
        spike_flags = df[flag_col_name]

        # Vectorized consecutive count: cumsum resets at each 0
        cumsum = spike_flags.groupby(df['seg_id']).cumsum()
        reset = cumsum.where(spike_flags == 0).groupby(df['seg_id']).ffill().fillna(0)
        df[consec_col_name] = (cumsum - reset).astype(int)

        max_consec_summary[flag_col_name.replace('Spike in ', '')] = int(df[consec_col_name].max())

    logger.info(f'Max consecutive spike runs: {json.dumps(max_consec_summary, indent=2)}')
    return df

def _impute_and_reject_spikes(df, spike_columns: Dict, max_consecutive_spikes=MAX_CONSECUTIVE_SPIKES):
    """ This function imputes spikes with linear interpolation if they are in runs of less than max_consecutive_spikes, and rejects them (replace with NaN) if they are in runs of max_consecutive_spikes or more."""
    impute_summary = {}
    reject_summary = {}
    n = len(df)
    new_flag_cols = {}

    # Pre-compute segment boundary mask once (used to prevent cross-segment interpolation)
    seg_boundary = df['seg_id'] != df['seg_id'].shift()

    for flag_col_name in spike_columns.keys():
        if flag_col_name not in df.columns:
            continue

        consec_col_name = f'Consecutive {flag_col_name}'
        if consec_col_name not in df.columns:
            continue

        var_col = flag_col_name.replace('Spike in ', '')
        if var_col not in df.columns:
            continue

        # Identify which spikes to impute vs reject before modifying any values
        is_spike = df[flag_col_name] == 1
        consec = df[consec_col_name]
        to_impute = is_spike & (consec < max_consecutive_spikes)
        to_reject = is_spike & (consec >= max_consecutive_spikes)

        n_imputed = to_impute.sum()
        n_rejected = to_reject.sum()

        # Reject first: set long-run spike values to NaN so they don't act as interpolation anchors
        df.loc[to_reject, var_col] = np.nan
        new_flag_cols[f'Rejected Spike in {var_col}'] = to_reject.astype(int)

        # Impute: set short-run spike values to NaN, then linearly interpolate
        # Use segment-boundary-aware interpolation without per-variable groupby
        df.loc[to_impute, var_col] = np.nan
        boundary_vals = df.loc[seg_boundary, var_col].copy()  # save boundary values
        df.loc[seg_boundary, var_col] = np.nan  # temporarily break cross-segment interpolation
        df[var_col] = df[var_col].interpolate(method='linear', limit=max_consecutive_spikes - 1)
        df.loc[seg_boundary, var_col] = boundary_vals  # restore boundary values

        new_flag_cols[f'Imputed Spike in {var_col}'] = to_impute.astype(int)

        if n_imputed > 0:
            impute_summary[var_col] = f'{n_imputed:,} ({n_imputed / n * 100:.2f}%)'
        if n_rejected > 0:
            reject_summary[var_col] = f'{n_rejected:,} ({n_rejected / n * 100:.2f}%)'

    # Join all new flag columns in one operation to avoid DataFrame fragmentation
    df = pd.concat([df, pd.DataFrame(new_flag_cols, index=df.index)], axis=1)

    logger.info(f'Spike imputation summary (linear interpolation, runs < {max_consecutive_spikes}): {json.dumps(impute_summary, indent=2)}')
    logger.info(f'Spike rejection summary (replaced with NaN, runs >= {max_consecutive_spikes}): {json.dumps(reject_summary, indent=2)}')

    return df

def deal_with_spikes(df, 
    spike_columns: Dict = {},
    spike_thresholds=SENSOR_SPIKE_THRESHOLDS,
    rolling_window_size=LOW_PASS_WINDOW_SIZE_SECONDS,
    rolling_min_periods=LOW_PASS_MIN_PERIODS,
    max_consecutive_spikes=MAX_CONSECUTIVE_SPIKES):
    """ This function applies the spike marking and then imputes the spikes based on the number of consecutive spikes. If there are less than max_consecutive_spikes, the spike values are imputed with linear interpolation using the nearest non-spike values. If there are max_consecutive_spikes or more, the spike values are replaced with NaN, as these are likely not imputable."""
    
    df = _mark_spikes(df, spike_columns=spike_columns, spike_thresholds=spike_thresholds, rolling_window_size=rolling_window_size, rolling_min_periods=rolling_min_periods)
    df = _mark_consecutive_spikes(df, spike_columns=spike_columns)
    df = _impute_and_reject_spikes(df, spike_columns=spike_columns, max_consecutive_spikes=max_consecutive_spikes)
    return df

def _change_thrust_force_sign(df):
    """ This function changes the sign of the thrust force column 'Vessel Propeller Shaft Thrust Force', as it is currently negative but should be positive."""
    col = 'Vessel Propeller Shaft Thrust Force'
    if col in df.columns:
        df[col] = -df[col]
        logger.info(f'Changed sign of thrust force column {col}')
    else:
        logger.warning(f'Column {col} not found in dataframe, skipping sign change')
    return df

def _add_units(df, metadata_df):
    """ This function adds the units in parenthesis after the column names based on the metadata dataframe."""
    column_unit_mapping = dict(zip(metadata_df['quantity_name'], metadata_df['unit']))
    new_columns = []
    for col in df.columns:
        if col in column_unit_mapping and pd.notna(column_unit_mapping[col]):
            new_col = f"{col} ({column_unit_mapping[col]})"
        else:
            new_col = col
        new_columns.append(new_col)
    df.columns = new_columns
    return df

def format_data(df, metadata_df):
    """ This function applies the formatting functions to the dataframe."""
    df = _change_thrust_force_sign(df)
    df = _add_units(df, metadata_df)
    return df

# Load the dataframe and metadata
setup_output_directories(filtering_output_dir)

column_metadata = load_column_metadata(os.path.join(meta_data_dir, 'Metrics registration.csv'))

df = load_synchronized_data(
    synchronized_data_dir, column_metadata, 
#    test_n=25
    )

logger.info(f'DataFrame loaded with shape: {df.shape}')
logger.info(f'percentage of non-NaN values per column: {df.count() / len(df) * 100}')

# Execute the functions in sequence

# --- Dropping of useless columns ---
df = drop_columns(df)

# -- Replace dropouts and inconsistent values with NaN and create flag columns for them ---
df, flag_columns = deal_with_dropouts(df, flag_columns={})

logger.info(f'Flag (dropout/sentinel/invalid) columns and counts of inconsistent rows: {flag_columns}')

nan_percentages = df.isna().mean() * 100
nan_percentages = nan_percentages[nan_percentages > 0].sort_values(ascending=False)
logger.info(f'Percentage of NaN values per column after dealing with dropouts:\n{nan_percentages}')

# --- Remove rows with NaN in required Sensor columns --- 
df = filter_nans(df)

# --- Flag repeated values in weather and sensor variables ---
repeated_values_flag_columns = {}
df = flag_repeated_values(df, repeated_values_flag_columns=repeated_values_flag_columns)

# --- Detect and impute spikes ---
df = deal_with_spikes(df, spike_columns={}, spike_thresholds=SENSOR_SPIKE_THRESHOLDS, rolling_window_size=LOW_PASS_WINDOW_SIZE_SECONDS, rolling_min_periods=LOW_PASS_MIN_PERIODS, max_consecutive_spikes=MAX_CONSECUTIVE_SPIKES)

# Make the same log again but after spike marking/removal
nan_percentages_after_spike_removal = df.isna().mean() * 100
nan_percentages_after_spike_removal = nan_percentages_after_spike_removal[nan_percentages_after_spike_removal > 0].sort_values(ascending=False)
logger.info(f'Percentage of NaN values per column with spike filtering:\n{nan_percentages_after_spike_removal}')

# Filter NaNs again (remaining NaNs are values with more than 10 consecutive spikes)
df = filter_nans(df)

nan_percentages_after_spike_removal = df.isna().mean() * 100
nan_percentages_after_spike_removal = nan_percentages_after_spike_removal[nan_percentages_after_spike_removal > 0].sort_values(ascending=False)
logger.info(f'Percentage of NaN values per column with spike filtering:\n{nan_percentages_after_spike_removal}')

# --- Filtering undesired (non-steady) state rows ---
df = filter_undesired_rows(df)

# --- Drop all the TRULY unneccessary columns (some of the added columns might be used for modelling - TBD)
# columns starting with "Rejected", "Consecutive", "Spike", "Negative", "Impossible" or "Repeated" are all flag columns that are deemed irrelevant (the others I will try to use for modelling)
columns_to_drop = [col for col in df.columns if col.startswith('Rejected') or col.startswith('Consecutive') or col.startswith('Spike') or col.startswith('Repeated') or col.startswith('Negative') or col.startswith('Impossible')]
df.drop(columns=columns_to_drop, inplace=True)
logger.info(f'Dropped {len(columns_to_drop)} flag columns: {columns_to_drop}')

# Any columns that contain only 0 or only 1
for col in df.columns:
    if set(df[col].dropna().unique()) <= {0}:
        df.drop(columns=[col], inplace=True)
        logger.info(f'Dropped column {col} since it only contains 0 values')
    elif set(df[col].dropna().unique()) <= {1}:
        df.drop(columns=[col], inplace=True)
        logger.info(f'Dropped column {col} since it only contains 1 values')

# Also drop "Vessel External Conditions Eastward Sea Water Velocity (Provider S)", since provider MB is used for this (somehow provider S snuck in)
if 'Vessel External Conditions Eastward Sea Water Velocity (Provider S)' in df.columns:
    df.drop(columns=['Vessel External Conditions Eastward Sea Water Velocity (Provider S)'], inplace=True)
    logger.info('Dropped column Vessel External Conditions Eastward Sea Water Velocity (Provider S) since provider MB is used for this')

# --- Formatting --- 
df = format_data(df, column_metadata)

# Save the final df to a csv file in the filtered_data_dir
filtered_file_path = os.path.join(filtered_data_dir, 'filtered.csv')

# create output directory if it doesn't exist
output_dir = os.path.dirname(filtered_file_path)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

df.to_csv(filtered_file_path, index=False)
logger.info(f'Saved filtered data to {filtered_file_path}')

logger.info(f'Final shape so far: {df.shape}')