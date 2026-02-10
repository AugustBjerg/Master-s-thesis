import pandas as pd
import numpy as np
import os
import json
import time
from config import INTENDED_SAMPLING_INTERVALS_SECONDS, THRESHOLD_FACTOR, MIN_SEGMENT_LENGTH_SECONDS, DROP_TRANDUCER_DEPTH
from loguru import logger

script_dir = os.path.dirname(os.path.abspath(__file__))
appended_data_dir = os.path.join(script_dir, '..', 'appended')
synchronized_data_dir = os.path.join(script_dir, '..', 'synchronized')
sync_output_dir = os.path.join(script_dir, '..', '..', 'outputs', 'synchronization')

script_start = time.perf_counter()

# create synchronized data directory if it does not exist
if not os.path.exists(synchronized_data_dir):
    os.makedirs(synchronized_data_dir)
else:
    logger.info(f'synchronized data directory already exists: {synchronized_data_dir}')

    # clear the directory
    for filename in os.listdir(synchronized_data_dir):
        file_path = os.path.join(synchronized_data_dir, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
                logger.info(f'Deleted file: {file_path}')
        except Exception as e:
            logger.error(f'Error deleting file {file_path}: {e}')
    logger.info(f'Cleared synchronized data directory: {synchronized_data_dir}')

# create output directory if it doesnt already exist
if not os.path.exists(sync_output_dir_dir):
    os.makedirs(sync_output_dir_dir)
else:
    logger.info(f'synchronized data directory already exists: {sync_output_dir_dir}')

# load the appended dataframe
df = pd.read_csv(
    os.path.join(appended_data_dir, 'excl_noon_reports.csv'),
    parse_dates=['utc_timestamp'],
#    nrows=10000000 # for testing, remove this line for full dataset
    )

# find the start and end time of the dataset
df['utc_timestamp'] = pd.to_datetime(df['utc_timestamp'], format='ISO8601')
df_start_time = df['utc_timestamp'].min()
df_end_time = df['utc_timestamp'].max()
total_duration = df_end_time - df_start_time
logger.info(f'Dataset time range: {df_start_time} to {df_end_time} (duration: {total_duration})')

# If chosen in config file, drop all transducer depth variables (this is way more unreliable and creates a lot of unnecessary time gaps, for a relatively low return in terms of data value)
if DROP_TRANDUCER_DEPTH:
    df = df.drop(df[df['qid_mapping'].str.contains('"2::0::4::0_1::1::0::2::0_37::0::2::0_8"')]['qid_mapping'].unique(), axis=0)
    logger.info(f'Dropped transducer depth variable')
else:
    logger.info(f'Keeping transducer depth variable. Variables: {df["qid_mapping"].unique()}')


logger.info(f'Synchronizing dataframe with shape: {df.shape}')
logger.info(f'number of unique time stamps: {df["utc_timestamp"].nunique()}')
logger.info(f'Threshold factor for synchronization: {THRESHOLD_FACTOR}')
logger.info(f'distribution of intended sampling intervals: {pd.Series(INTENDED_SAMPLING_INTERVALS_SECONDS).value_counts()}')

# -- PART 1 -- identify time observations within gaps based on intended sampling intervals and a tolerance threshold

# Map each qid to its intended sampling interval
df['nominal_dt'] = df['qid_mapping'].map(INTENDED_SAMPLING_INTERVALS_SECONDS)

# Calculate the tolerance threshold using Dalheim & Steen's method:
# Flag as time gap if |Δt_i - Δt_nominal| > (1/2) * Δt_nominal
df['tolerance_threshold'] = THRESHOLD_FACTOR * df['nominal_dt']

# Create boolean mask identifying time gaps
# True indicates a time gap (violation of the intended sampling interval)
df['is_time_gap'] = np.abs(df['time_delta_sec'] - df['nominal_dt']) > df['tolerance_threshold']

# -- PART 2 -- create continuous windows of unobstructed data 

# Collapse to one row per timestamp: True if ANY variable has a time jump at that time
time_gap_mask = df.groupby('utc_timestamp')['is_time_gap'].any().rename('is_gap_any').sort_index()

logger.info(f'created mask with shape: {time_gap_mask.shape}. ')
logger.info(f'Number of obs marked as time gaps: {time_gap_mask.sum()}')

# use time gap mask to create segments (for every TRUE value, there is a time gap)
# Boolean: this timestamp starts a new segment (first row or follows a gap)
seg_start = time_gap_mask.shift(fill_value=True)

# Assign segment ids using cumulative sum
seg_id = seg_start.cumsum()

# Create segments dataframe with gap info and segment ids
gap_segments = time_gap_mask.to_frame().assign(seg_id=seg_id)

# Create an object that stores the beginning and end of each valid segment
valid_segments_info = gap_segments.groupby('seg_id').agg(
    start_time=('is_gap_any', 'idxmin'), 
    end_time=('is_gap_any', 'idxmax')
)

# convert start_time and end_time to datetime
valid_segments_info['start_time'] = pd.to_datetime(valid_segments_info['start_time'],format='ISO8601')
valid_segments_info['end_time'] = pd.to_datetime(valid_segments_info['end_time'], format='ISO8601')

# Filter out segments that are too short (i.e. shorter than the minimum segment length defined in config file)
segment_sizes = valid_segments_info['end_time'] - valid_segments_info['start_time']
valid_segments_mask = segment_sizes >= pd.Timedelta(seconds=MIN_SEGMENT_LENGTH_SECONDS)

# Apply the filter to keep only valid segments
valid_segments_info = valid_segments_info[valid_segments_mask]

# calculate the total duration of valid segments
total_valid_duration = (valid_segments_info['end_time'] - valid_segments_info['start_time']).sum()


logger.info(f'valid segments info (shape: {valid_segments_info.shape}):\n{valid_segments_info.head()}')
logger.info(f'Minimum segment length: {MIN_SEGMENT_LENGTH_SECONDS} seconds')
logger.info(f'Segments after filtering: {valid_segments_mask.sum()}/{len(valid_segments_mask)} ({valid_segments_mask.sum()/len(valid_segments_mask)*100:.4f}%)')
logger.info(f'Total duration of valid segments: {total_valid_duration} (seconds) ({total_valid_duration/total_duration*100:.4f}% of total duration)')

# For each valid time segment, create 2 dataframes with "empty" time grids: one with 15second-intervals and one with 1-hour intervals

# Get unique qids from the dataset and filter by intended sampling interval
unique_qids = df['qid_mapping'].unique()
logger.info(f'Unique qids in dataset: {len(unique_qids)}')

# Filter qids by their intended sampling interval
qids_15s = [qid for qid in unique_qids if INTENDED_SAMPLING_INTERVALS_SECONDS.get(qid) == 15]
qids_1h = [qid for qid in unique_qids if INTENDED_SAMPLING_INTERVALS_SECONDS.get(qid) == 3600]

logger.info(f'QIDs with 15s sampling interval: {len(qids_15s)}')
logger.info(f'QIDs with 1h sampling interval: {len(qids_1h)}')

# Pre-define column order for reindexing
all_columns_15s = ['utc_timestamp', 'seg_id'] + list(qids_15s)
all_columns_1h = ['utc_timestamp', 'seg_id'] + list(qids_1h)

valid_segment_dataframes = []

for seg_id, segment in valid_segments_info.iterrows():
    start_time = segment['start_time']
    end_time = segment['end_time']
    
    # Create a time grid with 15-second intervals
    time_grid_15s = pd.date_range(start=start_time, end=end_time, freq='15s')
    df_15s = pd.DataFrame({'utc_timestamp': time_grid_15s, 'seg_id': seg_id})
    df_15s = df_15s.reindex(columns=all_columns_15s, fill_value=np.nan)
    
    # Create a time grid with 1-hour intervals
    time_grid_1h = pd.date_range(start=start_time, end=end_time, freq='1h')
    df_1h = pd.DataFrame({'utc_timestamp': time_grid_1h, 'seg_id': seg_id})
    df_1h = df_1h.reindex(columns=all_columns_1h, fill_value=np.nan)
    
    valid_segment_dataframes.append((df_15s, df_1h))

# Check that there is the right amount of dataframes and that the time grids are correct
logger.info(f'Created {len(valid_segment_dataframes)} valid segment dataframes with 15s and 1h time grids. shapes of list items: {[ (df_15s.shape, df_1h.shape) for df_15s, df_1h in valid_segment_dataframes[:5] ]}')

# check that the date format is correct (utc timestamps should be in datetime format)
logger.info(f'utc_timestamp column data type in first 15s and 1h dataframe: {valid_segment_dataframes[0][0]["utc_timestamp"].dtype} and {valid_segment_dataframes[0][1]["utc_timestamp"].dtype}')

# -- PART 3 -- Linear interpolation for each segment

for i, (grid_15s, grid_1h) in enumerate(valid_segment_dataframes):
    seg_id = grid_15s['seg_id'].iloc[0]
    seg_start_time = valid_segments_info.loc[seg_id, 'start_time']
    seg_end_time = valid_segments_info.loc[seg_id, 'end_time']
    seg_duration = seg_end_time - seg_start_time

    logger.info(f'\n--- Segment {i+1}/{len(valid_segment_dataframes)} (ID: {seg_id}, duration: {seg_duration}) ---')

    # -- 15s interpolation --

    # Filter original data to this segment's time range and only 15s qids
    df_segment_15s = df.query(
        'utc_timestamp >= @seg_start_time and utc_timestamp <= @seg_end_time and qid_mapping in @qids_15s'
    )

    # Pivot the segment data so each qid is a column with timestamp as index
    if df_segment_15s.duplicated(subset=['utc_timestamp', 'qid_mapping']).any():
        pivot_15s = df_segment_15s.pivot_table(
            index='utc_timestamp', columns='qid_mapping', values='value', aggfunc='first')
    else:
        pivot_15s = df_segment_15s.pivot(
            index='utc_timestamp', columns='qid_mapping', values='value')

    # Combine actual observation timestamps with the grid, interpolate, extract grid points
    combined_15s = pivot_15s.reindex(pivot_15s.index.union(grid_15s['utc_timestamp'])).sort_index()
    combined_15s.interpolate(method='linear', limit_area='inside', inplace=True)
    combined_15s = (
        combined_15s
        .loc[grid_15s['utc_timestamp']]
        .reset_index()
        .assign(seg_id=seg_id)
        .reindex(columns=all_columns_15s)
    )

    logger.info(f'15s interpolation: {combined_15s.shape[0]} rows, {combined_15s.isna().sum().sum()} total NaNs')

    # -- 1h interpolation --

    # Filter original data to this segment's time range and only 1h qids
    df_segment_1h = df.query(
        'utc_timestamp >= @seg_start_time and utc_timestamp <= @seg_end_time and qid_mapping in @qids_1h'
    )

    # Pivot the segment data so each qid is a column with timestamp as index
    if df_segment_1h.duplicated(subset=['utc_timestamp', 'qid_mapping']).any():
        pivot_1h = df_segment_1h.pivot_table(
            index='utc_timestamp', columns='qid_mapping', values='value', aggfunc='first')
    else:
        pivot_1h = df_segment_1h.pivot(
            index='utc_timestamp', columns='qid_mapping', values='value')

    # Combine actual observation timestamps with the grid, interpolate, extract grid points
    combined_1h = pivot_1h.reindex(pivot_1h.index.union(grid_1h['utc_timestamp'])).sort_index()
    combined_1h.interpolate(method='linear', limit_area='inside', inplace=True)
    combined_1h = (
        combined_1h
        .loc[grid_1h['utc_timestamp']]
        .reset_index()
        .assign(seg_id=seg_id)
        .reindex(columns=all_columns_1h)
    )

    # -- Combine 15s and 1h dataframes into one segment dataframe --

    df_segment_combined = pd.merge(
        combined_15s,
        combined_1h,
        on=['utc_timestamp', 'seg_id'],
        how='outer'
    )

    # Save the combined segment dataframe
    start_str = seg_start_time.strftime('%Y-%m-%d_%H-%M-%S')
    end_str = seg_end_time.strftime('%Y-%m-%d_%H-%M-%S')
    segment_filepath = os.path.join(synchronized_data_dir, f'synced_{start_str}_to_{end_str}.csv')
    df_segment_combined.to_csv(segment_filepath, index=False)

    logger.info(f'Saved synchronized segment dataframe number {i+1}/{len(valid_segment_dataframes)} to: {segment_filepath}')

logger.info(f'\nFinished processing all {len(valid_segment_dataframes)} segments')


# Save all relevant metadata/statistics that has been collected during synchronization to a json file for later reference
metadata = {
    'total_observations': len(df),
    'unique_timestamps': df['utc_timestamp'].nunique(),
    'intended_sampling_intervals_distribution': pd.Series(INTENDED_SAMPLING_INTERVALS_SECONDS).value_counts().to_dict(),
    'number_of_time_gaps': time_gap_mask.sum(),
    'tolerance_factor': THRESHOLD_FACTOR,
    'valid_segments_info': valid_segments_info.to_dict(orient='records'),
    'total_duration': str(total_duration),
    'total_valid_duration': str(total_valid_duration)
}

# save metadata to json file
metadata_filepath = os.path.join(sync_output_dir, f'synchronization_metadata_{script_start}.json') # script start time is close enough
with open(metadata_filepath, 'w') as f:
    json.dump(metadata, f)
logger.info(f'Saved metadata to: {metadata_filepath}')

script_end = time.perf_counter()
elapsed_time = script_end - script_start
logger.info(f'Total synchronization time: {elapsed_time:.2f} seconds')