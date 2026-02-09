import pandas as pd
import numpy as np
import os
import json
from config import INTENDED_SAMPLING_INTERVALS_SECONDS, THRESHOLD_FACTOR, MIN_SEGMENT_LENGTH_SECONDS, DROP_TRANDUCER_DEPTH
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
    nrows=1000000 # for testing, remove this line for full dataset
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

valid_segment_dataframes = []

for id, segment in valid_segments_info.iterrows():
    start_time = segment['start_time']
    end_time = segment['end_time']
    
    # Create a time grid with 15-second intervals
    time_grid_15s = pd.date_range(start=start_time, end=end_time, freq='15s')
    df_15s = pd.DataFrame({'utc_timestamp': time_grid_15s})
    
    # Create a time grid with 1-hour intervals
    time_grid_1h = pd.date_range(start=start_time, end=end_time, freq='1h')
    df_1h = pd.DataFrame({'utc_timestamp': time_grid_1h})
    
    valid_segment_dataframes.append((df_15s, df_1h))

# Check that there is the right amount of dataframes and that the time grids are correct
logger.info(f'Created {len(valid_segment_dataframes)} valid segment dataframes with 15s and 1h time grids. shapes of list items: {[ (df_15s.shape, df_1h.shape) for df_15s, df_1h in valid_segment_dataframes[:5] ]}')

# check that the date format is correct (utc timestamps should be in datetime format)
logger.info(f'utc_timestamp column data type in first 15s and 1h dataframe: {valid_segment_dataframes[0][0]["utc_timestamp"].dtype} and {valid_segment_dataframes[0][1]["utc_timestamp"].dtype}')




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

# Save the synchronized dataframe with time gap flags
output_file = os.path.join(synchronized_data_dir, 'synchronized.csv')
# df_filtered.to_csv(output_file, index=False)
logger.info(f'Synchronized dataframe saved to: {output_file}') 
