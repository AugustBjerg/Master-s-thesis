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

# Parallel file reading function
def read_csv_file(file_path):
    df = pd.read_csv(file_path, names=columns, parse_dates=['utc_timestamp'], date_format='ISO8601')
    logger.info(f'Read file: {file_path} with shape: {df.shape}')
    return df


def _stw_weight_term(stw, maneuvering_threshold_knots=SPEED_THROUGH_WATER_THRESHOLD, epsilon=0.1):
    """
    w(v) = epsilon + (1-epsilon)*exp(-v/v0)
    Here v0 is set to maneuvering_threshold / 2 by convention (tunable).
    """
    v0 = maneuvering_threshold_knots / 2.0
    if v0 <= 0:
        raise ValueError("maneuvering_threshold_knots must be > 0")

    stw = np.asarray(stw, dtype=float)
    w = epsilon + (1.0 - epsilon) * np.exp(-stw / v0)
    return w


def _water_temp_term(water_temp, water_temp_threshold_degrees=10):
    """
    g(T) = max(0, T - T0)
    """
    water_temp = np.asarray(water_temp, dtype=float)
    return np.maximum(0.0, water_temp - float(water_temp_threshold_degrees))


def _fpi_change(stw, water_temp, delta_t, maneuvering_threshold, water_temp_threshold_degrees=10, epsilon=0.1):
    """
    ΔFPI = w(STW) * g(T) * Δt_hours
    delta_t is expected in seconds.
    """
    delta_t = np.asarray(delta_t, dtype=float)
    delta_t_hours = delta_t / 3600.0

    w = _stw_weight_term(stw, maneuvering_threshold_knots=maneuvering_threshold, epsilon=epsilon)
    g = _water_temp_term(water_temp, water_temp_threshold_degrees=water_temp_threshold_degrees)

    d = w * g * delta_t_hours

    if np.any(d < -1e-12):
        raise ValueError("ΔFPI became negative; check inputs (STW, water_temp, delta_t).")

    d = np.maximum(d, 0.0)
    return d

def add_fouling_penalty_index(
    df,
    water_temp_qid,
    stw_qid,
    epsilon=0.1,
    water_temp_threshold_degrees=10,
    fouling_index_name="fouling_penalty_index",
    stw_staleness_threhsold_sec=300,
    water_temp_staleness_threshold_sec=21600,
    water_temp_failed_default=6.0,
    water_temp_failed_tol=0.05,   # treat ~6.0 as failed
):
    """
    Expects df long format with at least:
    ['utc_timestamp','qid_mapping','value','quantity_name','source_name','unit','time_delta_sec'].

    Adds two calculated variables as new rows:
      - DELTA_FPI_QID: 'fouling_pressure_change'
      - FPI_QID: cumulative fouling_penalty_index

    Computed on STW observations timeline using latest (as-of) valid water temp,
    with staleness checks and explicit handling of a known failed-default value.
    """
    required_cols = {
        'utc_timestamp', 'qid_mapping', 'value',
        'quantity_name', 'source_name', 'unit', 'time_delta_sec'
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"df missing required columns: {missing}")

    if df.empty:
        logger.warning("add_fouling_penalty_index called with empty df")
        return df

    # Work on a view of the original df (no big copy), but we will only
    # add temporary columns to stw_df/wt_df, not to df itself.
    work = df
    work['utc_timestamp'] = pd.to_datetime(work['utc_timestamp'], utc=True, errors='coerce')
    if work['utc_timestamp'].isna().any():
        bad = work['utc_timestamp'].isna().sum()
        raise ValueError(f"Found {bad} rows with non-parsable utc_timestamp")

    # --- base series: STW observations ---
    stw_df = work.loc[
        work['qid_mapping'] == stw_qid,
        ['utc_timestamp', 'qid_mapping', 'value', 'time_delta_sec']
    ].copy()
    if stw_df.empty:
        logger.warning(f"No STW rows found for qid {stw_qid}; skipping FPI creation.")
        return df

    stw_df = stw_df.sort_values('utc_timestamp').reset_index(drop=True)
    stw_df.rename(columns={'value': 'stw'}, inplace=True)

    # Use time_delta_sec already computed per qid_mapping; fill first delta with 0
    stw_df['delta_t_sec'] = stw_df['time_delta_sec'].fillna(0.0)
    stw_df['delta_t_sec'] = stw_df['delta_t_sec'].clip(lower=0.0)

    # --- water temp series for asof join ---
    wt_df = work.loc[
        work['qid_mapping'] == water_temp_qid,
        ['utc_timestamp', 'value']
    ].copy()

    if wt_df.empty:
        logger.warning(f"No water temp rows found for qid {water_temp_qid}; FPI will be all zeros.")
        stw_df['water_temp'] = np.nan
        stw_df['water_temp_age_sec'] = np.inf
    else:
        wt_df = wt_df.sort_values('utc_timestamp').reset_index(drop=True)
        wt_df.rename(columns={'value': 'water_temp'}, inplace=True)

        # 1) Flag sensor-failure default and treat it as invalid for as-of usage
        wt_df['water_temp'] = pd.to_numeric(wt_df['water_temp'], errors='coerce')
        wt_df['wt_is_failed_default'] = (
            (wt_df['water_temp'] - float(water_temp_failed_default)).abs()
            <= float(water_temp_failed_tol)
        )

        # Create a "valid water temp" series where failed defaults are set to NaN
        wt_df['water_temp_valid'] = wt_df['water_temp'].where(
            ~wt_df['wt_is_failed_default'], np.nan
        )

        # Keep only rows that can serve as valid sources for as-of temperature
        wt_valid = wt_df.loc[
            wt_df['water_temp_valid'].notna(),
            ['utc_timestamp', 'water_temp_valid']
        ].copy()

        if wt_valid.empty:
            logger.warning(
                f"Water temp series exists for qid {water_temp_qid}, but all values look like "
                f"failed default (~{water_temp_failed_default}±{water_temp_failed_tol}). "
                f"FPI increments will be zero."
            )
            stw_df['water_temp'] = np.nan
            stw_df['water_temp_age_sec'] = np.inf
        else:
            # Rename timestamp for clarity and single as-of join
            wt_valid.rename(columns={'utc_timestamp': 'wt_obs_ts'}, inplace=True)

            # 2) As-of join: last VALID water temp at or before STW timestamp
            stw_df = stw_df.sort_values('utc_timestamp')
            wt_valid = wt_valid.sort_values('wt_obs_ts')

            stw_df = pd.merge_asof(
                stw_df,
                wt_valid,
                left_on='utc_timestamp',
                right_on='wt_obs_ts',
                direction='backward',
                allow_exact_matches=True
            )

            # After merge: stw_df has columns ['water_temp_valid', 'wt_obs_ts']
            stw_df.rename(columns={'water_temp_valid': 'water_temp'}, inplace=True)

            # 3) Compute staleness age relative to timestamp of matched VALID temp observation
            stw_df['water_temp_age_sec'] = (
                stw_df['utc_timestamp'] - stw_df['wt_obs_ts']
            ).dt.total_seconds()
            stw_df.drop(columns=['wt_obs_ts'], inplace=True)

    # STW staleness: since STW is the base series, we only need to guard against huge gaps in STW itself
    stw_df['stw_gap_too_large'] = stw_df['delta_t_sec'] > float(stw_staleness_threhsold_sec)

    # Water temp staleness
    stw_df['wt_stale'] = stw_df['water_temp_age_sec'] > float(water_temp_staleness_threshold_sec)

    # Determine where we have valid inputs for ΔFPI
    valid = (
        stw_df['stw'].notna()
        & stw_df['water_temp'].notna()
        & (~stw_df['stw_gap_too_large'])
        & (~stw_df['wt_stale'])
    )

    d_fpi = np.zeros(len(stw_df), dtype=float)
    if valid.any():
        d_fpi[valid.values] = _fpi_change(
            stw=stw_df.loc[valid, 'stw'].values,
            water_temp=stw_df.loc[valid, 'water_temp'].values,
            delta_t=stw_df.loc[valid, 'delta_t_sec'].values,
            maneuvering_threshold=SPEED_THROUGH_WATER_THRESHOLD,
            water_temp_threshold_degrees=water_temp_threshold_degrees,
            epsilon=epsilon
        )

    stw_df['delta_fpi'] = d_fpi
    stw_df['fpi'] = stw_df['delta_fpi'].cumsum()

    # Build new rows (long format)
    delta_rows = pd.DataFrame({
        'utc_timestamp': stw_df['utc_timestamp'],
        'qid_mapping': DELTA_FPI_QID,
        'value': stw_df['delta_fpi'],
        'quantity_name': 'fouling_pressure_change',
        'source_name': 'calculated',
        'unit': 'calculated',
        'time_delta_sec': stw_df['delta_t_sec']
    })

    fpi_rows = pd.DataFrame({
        'utc_timestamp': stw_df['utc_timestamp'],
        'qid_mapping': FPI_QID,
        'value': stw_df['fpi'],
        'quantity_name': fouling_index_name,
        'source_name': 'calculated',
        'unit': 'calculated',
        'time_delta_sec': stw_df['delta_t_sec']
    })

    out = pd.concat([df, delta_rows, fpi_rows], ignore_index=True)

    logger.info(
        f"Added fouling rows: delta={len(delta_rows)}, fpi={len(fpi_rows)} "
        f"(stw points={len(stw_df)}, valid_increments={int(valid.sum())}). "
        f"New shape: {out.shape}"
    )

    return out

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

    # Add a column for the fouling penalty index
    appended_df = add_fouling_penalty_index(
        appended_df,
        water_temp_qid='4::0::8::0_1::1::0::7::0_4::0::12::0_8',
        stw_qid='2::0::7::0_1::1::0::2::0_1::0::5::11_8',
        epsilon=0.1,
        water_temp_threshold_degrees=10,
        fouling_index_name="fouling_penalty_index",
        stw_staleness_threhsold_sec=300,
        water_temp_staleness_threshold_sec=21600,
        water_temp_failed_default=6.0,
        water_temp_failed_tol=0.05
    )
    logger.info(f'Added fouling penalty index columns to appended dataframe. Shape is now: {appended_df.shape}')
    logger.info(f'columns in the dataframe at this point: {appended_df.columns.tolist()}')

    # Sort by timestamp again since new rows were added
    appended_df = appended_df.sort_values(by='utc_timestamp').reset_index(drop=True)

    # Save this version of the appended df (excl. noon report data) to the folder
    os.makedirs(appended_data_dir, exist_ok=True)
    appended_df.to_csv(os.path.join(appended_data_dir, 'excl_noon_reports.csv'), index=False)
