import os
import glob
import pandas as pd
import numpy as np
from typing import List
from loguru import logger
from config import (EXPECTED_SENSOR_OBSERVATIONS, DELTA_FPI_QID, FPI_QID,
    JANUARY_CLEANING_DATE, JULY_CLEANING_DATE, FOULING_PROXY_V_0, FOULING_PROXY_EPSILON,
    IS_IN_VOYAGE_QID, TEMPORARY_VOYAGE_ID_QID, VOYAGE_DURATION_QID, ACTUAL_VOYAGE_ID_QID,
    VOYAGE_SPEED_THRESHOLD, VOYAGE_DURATION_THRESHOLD_HOURS)
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


def _stw_weight_term(stw, v_0=FOULING_PROXY_V_0, epsilon=FOULING_PROXY_EPSILON):
    """
    w(v) = epsilon + (1-epsilon)*exp(-v/v0)
    """
    if v_0 <= 0:
        raise ValueError("v_0 must be > 0")

    stw = np.asarray(stw, dtype=float)
    w = epsilon + (1.0 - epsilon) * np.exp(-stw / v_0)
    return w

def _water_temp_term(water_temp, water_temp_threshold_degrees=10):
    """
    g(T) = max(0, T - T0)
    """
    water_temp = np.asarray(water_temp, dtype=float)
    return np.maximum(0.0, water_temp - float(water_temp_threshold_degrees))

def _fpi_change(stw, water_temp, delta_t, v_0=FOULING_PROXY_V_0, water_temp_threshold_degrees=10, epsilon=FOULING_PROXY_EPSILON):
    """
    ΔFPI = w(STW) * g(T) * Δt_hours
    delta_t is expected in seconds.
    """
    delta_t = np.asarray(delta_t, dtype=float)
    delta_t_hours = delta_t / 3600.0

    w = _stw_weight_term(stw, v_0=FOULING_PROXY_V_0, epsilon=epsilon)
    g = _water_temp_term(water_temp, water_temp_threshold_degrees=water_temp_threshold_degrees)

    d = w * g * delta_t_hours

    if np.any(d < -1e-12):
        raise ValueError("ΔFPI became negative; check inputs (STW, water_temp, delta_t).")

    d = np.maximum(d, 0.0)
    return d

def add_voyage_dummies(
    df,
    sog_qid="2::0::6::1_1::1::0::2::0_1::0::1::0_8",
    voyage_speed_threshold=VOYAGE_SPEED_THRESHOLD,
    voyage_duration_threshold_hours=VOYAGE_DURATION_THRESHOLD_HOURS,
    voyage_merge_gap_hours=12,
):
    """
    Adds voyage-related calculated rows based on Speed Over Ground (SOG) timeline.

    New rows added for each SOG observation:
      1. is_in_voyage: binary, 1 if SOG > threshold, 0 otherwise
      2. temporary_voyage_id: incremented each 0->1 transition, NaN when not in voyage
      3. voyage_duration_hours: hours since current voyage start, NaN when not in voyage
      4. actual_voyage_id: same as temporary but NaN if voyage duration < threshold
    """
    required_cols = {
        'utc_timestamp', 'qid_mapping', 'value',
        'quantity_name', 'source_name', 'unit', 'time_delta_sec'
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"df missing required columns: {missing}")

    if df.empty:
        logger.warning("add_voyage_dummies called with empty df")
        return df

    # Extract SOG rows
    sog_df = df.loc[
        df['qid_mapping'] == sog_qid,
        ['utc_timestamp', 'value', 'time_delta_sec']
    ].copy()

    if sog_df.empty:
        logger.warning(f"No SOG rows found for qid {sog_qid}; skipping voyage dummies.")
        return df

    sog_df = sog_df.sort_values('utc_timestamp').reset_index(drop=True)
    sog_df['utc_timestamp'] = pd.to_datetime(sog_df['utc_timestamp'], errors='coerce')
    sog_df['sog'] = pd.to_numeric(sog_df['value'], errors='coerce')

    # --- Step 1: is_in_voyage ---
    sog_df['is_in_voyage'] = (sog_df['sog'] > voyage_speed_threshold).astype(int)

    n_total = len(sog_df)
    n_in_voyage = int(sog_df['is_in_voyage'].sum())
    n_not_in_voyage = n_total - n_in_voyage
    logger.info(
        f"is_in_voyage: {n_in_voyage} ({100*n_in_voyage/n_total:.1f}%) are 1, "
        f"{n_not_in_voyage} ({100*n_not_in_voyage/n_total:.1f}%) are 0"
    )

    # --- Step 2: temporary_voyage_id ---
    # New voyage starts when is_in_voyage transitions 0->1 (first obs being 1 counts as start)
    voyage_starts = (sog_df['is_in_voyage'] == 1) & (
        sog_df['is_in_voyage'].shift(1, fill_value=0) == 0
    )
    sog_df['temporary_voyage_id'] = np.where(
        sog_df['is_in_voyage'] == 1,
        voyage_starts.cumsum(),
        np.nan
    )

    n_unique_voyages = int(sog_df['temporary_voyage_id'].dropna().nunique())
    logger.info(f"temporary_voyage_id: {n_unique_voyages} unique voyages identified")

    # --- Step 3: voyage_duration_hours ---
    sog_df['voyage_duration_hours'] = np.nan
    voyage_mask = sog_df['temporary_voyage_id'].notna()
    if voyage_mask.any():
        voyage_start_ts = sog_df.loc[voyage_mask].groupby('temporary_voyage_id')['utc_timestamp'].transform('first')
        sog_df.loc[voyage_mask, 'voyage_duration_hours'] = (
            (sog_df.loc[voyage_mask, 'utc_timestamp'] - voyage_start_ts).dt.total_seconds() / 3600.0
        )

    if n_unique_voyages > 0:
        avg_duration = sog_df.loc[voyage_mask].groupby('temporary_voyage_id')['voyage_duration_hours'].max().mean()
        logger.info(f"Average voyage duration: {avg_duration:.2f} hours")

    # --- Step 4: actual_voyage_id ---
    # NaN if is_in_voyage == 0 or if the voyage's max duration < threshold
    sog_df['actual_voyage_id'] = np.nan
    if voyage_mask.any():
        max_durations = sog_df.loc[voyage_mask].groupby('temporary_voyage_id')['voyage_duration_hours'].transform('max')
        sog_df.loc[voyage_mask, 'actual_voyage_id'] = np.where(
            max_durations >= voyage_duration_threshold_hours,
            sog_df.loc[voyage_mask, 'temporary_voyage_id'],
            np.nan
        )

    n_voyages_after = int(sog_df['actual_voyage_id'].dropna().nunique())
    n_filtered = n_unique_voyages - n_voyages_after
    pct_filtered = 100 * n_filtered / n_unique_voyages if n_unique_voyages > 0 else 0
    logger.info(
        f"actual_voyage_id: {n_filtered} ({pct_filtered:.1f}%) voyages set to NaN "
        f"due to duration threshold ({voyage_duration_threshold_hours}h). "
        f"{n_voyages_after} voyages remain."
    )

    # --- Step 5: merge actual voyages with small gaps ---
    n_before_merge = n_voyages_after
    if n_voyages_after > 1:
        actual_mask = sog_df['actual_voyage_id'].notna()
        # Get end/start timestamps per actual voyage
        voyage_bounds = sog_df.loc[actual_mask].groupby('actual_voyage_id')['utc_timestamp'].agg(['min', 'max'])
        voyage_bounds = voyage_bounds.sort_values('min')

        # Build a mapping: old_id -> merged_id
        merge_map = {}
        current_group_id = voyage_bounds.index[0]
        current_end = voyage_bounds.loc[current_group_id, 'max']
        merge_map[current_group_id] = current_group_id

        for vid in voyage_bounds.index[1:]:
            gap_hours = (voyage_bounds.loc[vid, 'min'] - current_end).total_seconds() / 3600.0
            if gap_hours <= voyage_merge_gap_hours:
                merge_map[vid] = current_group_id
                current_end = max(current_end, voyage_bounds.loc[vid, 'max'])
            else:
                current_group_id = vid
                current_end = voyage_bounds.loc[vid, 'max']
                merge_map[vid] = current_group_id

        # Apply merge mapping
        sog_df.loc[actual_mask, 'actual_voyage_id'] = (
            sog_df.loc[actual_mask, 'actual_voyage_id'].map(merge_map)
        )

        # Also fill in the gap rows between merged voyages
        merged_ids = set(merge_map.values())
        for mid in merged_ids:
            member_ids = [k for k, v in merge_map.items() if v == mid]
            if len(member_ids) <= 1:
                continue
            overall_start = voyage_bounds.loc[member_ids, 'min'].min()
            overall_end = voyage_bounds.loc[member_ids, 'max'].max()
            gap_mask = (
                (sog_df['utc_timestamp'] >= overall_start)
                & (sog_df['utc_timestamp'] <= overall_end)
                & sog_df['actual_voyage_id'].isna()
            )
            sog_df.loc[gap_mask, 'actual_voyage_id'] = mid

        n_after_merge = int(sog_df['actual_voyage_id'].dropna().nunique())
        n_merged = n_before_merge - n_after_merge
        logger.info(
            f"Voyage merging: {n_merged} voyages merged into neighbours "
            f"(gap <= {voyage_merge_gap_hours}h). {n_after_merge} voyages remain."
        )

    # --- Build new rows (long format) ---
    is_in_voyage_rows = pd.DataFrame({
        'utc_timestamp': sog_df['utc_timestamp'],
        'qid_mapping': IS_IN_VOYAGE_QID,
        'value': sog_df['is_in_voyage'],
        'quantity_name': 'is_in_voyage',
        'source_name': 'calculated',
        'unit': 'calculated',
        'time_delta_sec': sog_df['time_delta_sec']
    })

    temp_voyage_id_rows = pd.DataFrame({
        'utc_timestamp': sog_df['utc_timestamp'],
        'qid_mapping': TEMPORARY_VOYAGE_ID_QID,
        'value': sog_df['temporary_voyage_id'],
        'quantity_name': 'temporary_voyage_id',
        'source_name': 'calculated',
        'unit': 'calculated',
        'time_delta_sec': sog_df['time_delta_sec']
    })

    voyage_duration_rows = pd.DataFrame({
        'utc_timestamp': sog_df['utc_timestamp'],
        'qid_mapping': VOYAGE_DURATION_QID,
        'value': sog_df['voyage_duration_hours'],
        'quantity_name': 'voyage_duration_hours',
        'source_name': 'calculated',
        'unit': 'calculated',
        'time_delta_sec': sog_df['time_delta_sec']
    })

    actual_voyage_id_rows = pd.DataFrame({
        'utc_timestamp': sog_df['utc_timestamp'],
        'qid_mapping': ACTUAL_VOYAGE_ID_QID,
        'value': sog_df['actual_voyage_id'],
        'quantity_name': 'actual_voyage_id',
        'source_name': 'calculated',
        'unit': 'calculated',
        'time_delta_sec': sog_df['time_delta_sec']
    })

    out = pd.concat(
        [df, is_in_voyage_rows, temp_voyage_id_rows, voyage_duration_rows, actual_voyage_id_rows],
        ignore_index=True
    )

    logger.info(f"Added voyage dummy rows: {4 * len(sog_df)} new rows. New shape: {out.shape}")

    return out

def add_fouling_penalty_index(
    df,
    water_temp_qid,
    stw_qid,
    cleaning_dates: List = [JANUARY_CLEANING_DATE, JULY_CLEANING_DATE],
    epsilon=FOULING_PROXY_EPSILON,
    water_temp_threshold_degrees=10,
    stw_staleness_threhsold_sec=300,
    water_temp_staleness_threshold_sec=21600,
    water_temp_failed_default=6.0,
    water_temp_failed_tol=0.01,   # treat ~6.0 as failed
):
    """
    Expects df long format with at least:
    ['utc_timestamp','qid_mapping','value','quantity_name','source_name','unit','time_delta_sec'].

    Adds two calculated variables as new rows:
      - DELTA_FPI_QID: 'delta_fouling_penalty_index'
      - FPI_QID: cumulative cumulative_fouling_penalty_index

    Computed on STW observations timeline using latest (as-of) valid water temp,
    with staleness checks, explicit handling of a known failed-default value,
    and resetting of the cumulative index at specified cleaning dates.

    For timestamps before the first cleaning date, ΔFPI and FPI are set to NaN.
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

    # ---- Normalize timestamps in df to timezone-aware UTC ----
    work = df
    work['utc_timestamp'] = pd.to_datetime(work['utc_timestamp'], errors='coerce')

    if work['utc_timestamp'].dt.tz is None:
        work['utc_timestamp'] = work['utc_timestamp'].dt.tz_localize('UTC')
    else:
        work['utc_timestamp'] = work['utc_timestamp'].dt.tz_convert('UTC')

    if work['utc_timestamp'].isna().any():
        bad = work['utc_timestamp'].isna().sum()
        raise ValueError(f"Found {bad} rows with non-parsable utc_timestamp")

    # ---- Prepare cleaning dates (sorted, timezone-aware UTC) ----
    cleaning_ts = pd.to_datetime(cleaning_dates)
    if cleaning_ts.tz is None:
        cleaning_ts = cleaning_ts.tz_localize('UTC')
    else:
        cleaning_ts = cleaning_ts.tz_convert('UTC')
    cleaning_ts = cleaning_ts.sort_values()
    cleaning_ts_array = cleaning_ts.values

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

        wt_df['water_temp'] = pd.to_numeric(wt_df['water_temp'], errors='coerce')
        wt_df['wt_is_failed_default'] = (
            (wt_df['water_temp'] - float(water_temp_failed_default)).abs()
            <= float(water_temp_failed_tol)
        )
        wt_df['water_temp_valid'] = wt_df['water_temp'].where(
            ~wt_df['wt_is_failed_default'], np.nan
        )

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
            wt_valid.rename(columns={'utc_timestamp': 'wt_obs_ts'}, inplace=True)
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

            stw_df.rename(columns={'water_temp_valid': 'water_temp'}, inplace=True)
            stw_df['water_temp_age_sec'] = (
                stw_df['utc_timestamp'] - stw_df['wt_obs_ts']
            ).dt.total_seconds()
            stw_df.drop(columns=['wt_obs_ts'], inplace=True)

    # STW staleness (gaps)
    stw_df['stw_gap_too_large'] = stw_df['delta_t_sec'] > float(stw_staleness_threhsold_sec)

    # Water temp staleness
    stw_df['wt_stale'] = stw_df['water_temp_age_sec'] > float(water_temp_staleness_threshold_sec)

    # Valid increments (ignoring cleaning logic for now)
    base_valid = (
        stw_df['stw'].notna()
        & stw_df['water_temp'].notna()
        & (~stw_df['stw_gap_too_large'])
        & (~stw_df['wt_stale'])
    )

    # --- assign cleaning segments ---
    # Segment indices:
    #   -1: before first cleaning (unknown prior cleaning, should become NaN)
    #    0: between first and second cleaning
    #    1: between second and third cleaning, etc.
    stw_times = stw_df['utc_timestamp'].values
    pos = cleaning_ts_array.searchsorted(stw_times, side='right') - 1
    stw_df['segment_idx'] = pos

    # Initialize arrays
    d_fpi = np.full(len(stw_df), np.nan, dtype=float)
    fpi = np.full(len(stw_df), np.nan, dtype=float)

    # Compute ΔFPI and cumulative FPI separately per segment (segment_idx >= 0)
    for seg in np.unique(stw_df['segment_idx']):
        if seg < 0:
            # Before first known cleaning: remain NaN by requirement
            continue

        seg_mask = stw_df['segment_idx'] == seg
        seg_valid = base_valid & seg_mask

        if not seg_valid.any():
            continue

        idx = np.where(seg_valid)[0]
        d_seg = _fpi_change(
            stw=stw_df.loc[seg_valid, 'stw'].values,
            water_temp=stw_df.loc[seg_valid, 'water_temp'].values,
            delta_t=stw_df.loc[seg_valid, 'delta_t_sec'].values,
            v_0=FOULING_PROXY_V_0,
            water_temp_threshold_degrees=water_temp_threshold_degrees,
            epsilon=epsilon
        )
        d_fpi[idx] = d_seg

        seg_indices = np.where(seg_mask)[0]
        seg_d = d_fpi[seg_indices]
        seg_d_filled = np.where(np.isnan(seg_d), 0.0, seg_d)
        seg_cum = np.cumsum(seg_d_filled)
        fpi[seg_indices] = seg_cum

    stw_df['delta_fpi'] = d_fpi
    stw_df['fpi'] = fpi

    # Build new rows (long format)
    delta_rows = pd.DataFrame({
        'utc_timestamp': stw_df['utc_timestamp'],
        'qid_mapping': DELTA_FPI_QID,
        'value': stw_df['delta_fpi'],
        'quantity_name': 'delta_fouling_penalty_index',
        'source_name': 'calculated',
        'unit': 'calculated',
        'time_delta_sec': stw_df['delta_t_sec']
    })

    fpi_rows = pd.DataFrame({
        'utc_timestamp': stw_df['utc_timestamp'],
        'qid_mapping': FPI_QID,
        'value': stw_df['fpi'],
        'quantity_name': 'cumulative_fouling_penalty_index',
        'source_name': 'calculated',
        'unit': 'calculated',
        'time_delta_sec': stw_df['delta_t_sec']
    })

    out = pd.concat([df, delta_rows, fpi_rows], ignore_index=True)

    logger.info(
        f"Added fouling rows: delta={len(delta_rows)}, fpi={len(fpi_rows)} "
        f"(stw points={len(stw_df)}, valid_increments={int(np.isfinite(d_fpi).sum())}). "
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
        epsilon=FOULING_PROXY_EPSILON,
        water_temp_threshold_degrees=10,
        stw_staleness_threhsold_sec=300,
        water_temp_staleness_threshold_sec=21600,
        water_temp_failed_default=6.0,
        water_temp_failed_tol=0.05
    )

    # Sort by timestamp again since new rows were added
    appended_df = appended_df.sort_values(by='utc_timestamp').reset_index(drop=True)

    # Add voyage dummy variables
    appended_df = add_voyage_dummies(appended_df)

    # Sort by timestamp again since new rows were added
    appended_df = appended_df.sort_values(by='utc_timestamp').reset_index(drop=True)
    os.makedirs(appended_data_dir, exist_ok=True)
    appended_df.to_csv(os.path.join(appended_data_dir, 'excl_noon_reports.csv'), index=False)
