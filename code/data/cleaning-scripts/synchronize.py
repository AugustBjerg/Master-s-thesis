import pandas as pd
import numpy as np
import os
from config import INTENDED_SAMPLING_INTERVALS_SECONDS, THRESHOLD_FACTOR
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
#    nrows=100000 # for testing, remove this line for full dataset
    )

logger.info(f'Synchronizing dataframe with shape: {df.shape}')
logger.info(f'Threshold factor for synchronization: {THRESHOLD_FACTOR}')
logger.info(f'distribution of intended sampling intervals: {pd.Series(INTENDED_SAMPLING_INTERVALS_SECONDS).value_counts()}')

# Map each qid to its intended sampling interval
df['nominal_dt'] = df['qid_mapping'].map(INTENDED_SAMPLING_INTERVALS_SECONDS)

# Calculate the tolerance threshold using Dalheim & Steen's method:
# Flag as time gap if |Δt_i - Δt_nominal| > (1/2) * Δt_nominal
df['tolerance_threshold'] = THRESHOLD_FACTOR * df['nominal_dt']

# Create boolean mask identifying time gaps
# True indicates a time gap (violation of the intended sampling interval)
df['is_time_gap'] = np.abs(df['time_delta_sec'] - df['nominal_dt']) > df['tolerance_threshold']

# Log statistics about time gaps
logger.info(f'Total observations: {len(df)}')
logger.info(f'Time gaps identified: {df["is_time_gap"].sum()} ({df["is_time_gap"].sum() / len(df) * 100:.2f}%)')
logger.info(f'Time gap distribution by sensor (qid_mapping):')
for qid in df['qid_mapping'].unique():
    qid_mask = df['qid_mapping'] == qid
    qid_time_gaps = df.loc[qid_mask, 'is_time_gap'].sum()
    qid_total = qid_mask.sum()
    logger.info(f'  {qid}: {qid_time_gaps}/{qid_total} ({qid_time_gaps / qid_total * 100:.2f}%)')

# Create a boolean mask representing clean stretches of data
# True indicates clean data (no time gap), False indicates a time gap

# Save the synchronized dataframe with time gap flags
output_file = os.path.join(synchronized_data_dir, 'synchronized.csv')
df.to_csv(output_file, index=False)
logger.info(f'Synchronized dataframe saved to: {output_file}') 
