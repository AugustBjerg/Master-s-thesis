import pandas as pd
import numpy as np
import os
import json
from config import INTENDED_SAMPLING_INTERVALS_SECONDS, THRESHOLD_FACTOR, MIN_SEGMENT_LENGTH
from loguru import logger

script_dir = os.path.dirname(os.path.abspath(__file__))
appended_data_dir = os.path.join(script_dir, '..', 'appended')
synchronized_data_dir = os.path.join(script_dir, '..', 'synchronized')

# create synchronized data directory if it does not exist
if not os.path.exists(synchronized_data_dir):
    os.makedirs(synchronized_data_dir)
else:
    logger.info(f'synchronized data directory already exists: {synchronized_data_dir}')

# load the appended dataframe
df = pd.read_csv(
    os.path.join(appended_data_dir, 'excl_noon_reports.csv'),
    parse_dates=['utc_timestamp'],
    nrows=100000 # for testing, remove this line for full dataset
    )

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

logger.info(f'created mask with shape: {time_gap_mask.shape}; number of obs marked as time gaps: {time_gap_mask.sum()} and head:\n{time_gap_mask.head()}')

# use time gap mask to create segments (for every TRUE value, there is a time gap)
# Boolean: this timestamp starts a new segment (first row or follows a gap)
seg_start = time_gap_mask.shift(fill_value=True)

# Assign segment ids using cumulative sum
seg_id = seg_start.cumsum()

# Create segments dataframe with gap info and segment ids
gap_segments = time_gap_mask.to_frame().assign(seg_id=seg_id)

logger.info(f'Created {seg_id.max()} continuous segments before filtering')

# Filter segments by minimum length
segment_sizes = gap_segments.groupby('seg_id').size()
valid_segments = segment_sizes[segment_sizes >= MIN_SEGMENT_LENGTH].index

# Create an object that stores the beginning and end of each valid segment
valid_segments_info = gap_segments[gap_segments['seg_id'].isin(valid_segments)].groupby('seg_id').agg(start_time=('is_gap_any', 'idxmin'), end_time=('is_gap_any', 'idxmax'))

logger.info(f'gap segment info (shape: {gap_segments.shape}):\n{gap_segments.head()}')
logger.info(f'valid segments info (shape: {valid_segments_info.shape}):\n{valid_segments_info.head()}')
logger.info(f'Minimum segment length: {MIN_SEGMENT_LENGTH} timestamps / observations')
logger.info(f'Segments after filtering: {len(valid_segments)}/{len(segment_sizes)} ({len(valid_segments)/len(segment_sizes)*100:.4f}%)')
logger.info(f'Valid segment size distribution:\n{segment_sizes[valid_segments].describe()}')

# For each valid time segment, create 2 dataframes with "empty" time grids: one with 15second-intervals and one with 1-hour intervals

valid_segment_dataframes = []

for id, segment in valid_segments_info.iterrows():
    start_time = segment['start_time']
    end_time = segment['end_time']
    
    # Create a time grid with 15-second intervals
    time_grid_15s = pd.date_range(start=start_time, end=end_time, freq='15S')
    df_15s = pd.DataFrame({'utc_timestamp': time_grid_15s})
    
    # Create a time grid with 1-hour intervals
    time_grid_1h = pd.date_range(start=start_time, end=end_time, freq='1H')
    df_1h = pd.DataFrame({'utc_timestamp': time_grid_1h})
    
    valid_segment_dataframes.append((df_15s, df_1h))

# check that the date format is correct




# Save the synchronized dataframe with time gap flags
output_file = os.path.join(synchronized_data_dir, 'synchronized.csv')
df_filtered.to_csv(output_file, index=False)
logger.info(f'Synchronized dataframe saved to: {output_file}') 
