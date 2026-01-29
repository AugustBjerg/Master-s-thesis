import os
import glob
import pandas as pd
import numpy as np
from loguru import logger
from config import NOON_REPORT_QIDS, NOON_REPORT_UNITS

# Defining prerequisites for appending loop
columns = [
    'utc_timestamp',
    'qid_mapping',
    'value',
    'quantity_name',
]
appended_df = pd.DataFrame(columns=columns)
parent_dir = os.path.dirname(os.getcwd())
appended_data_dir = os.path.join(parent_dir, 'Master\'s Thesis', 'code', 'data', 'appended')
noon_rep_qid_dict = NOON_REPORT_QIDS
noon_rep_units_dict = NOON_REPORT_UNITS
raw_noon_reports_dir = os.path.join(parent_dir, 'Master\'s Thesis', 'code', 'data', 'raw', 'unzipped', 'Noon Reports')

# write a loop that: 
# 1. loops through all the files in the noon reports folder
for i, file in enumerate(glob.glob(os.path.join(raw_noon_reports_dir, '*.csv'))):
    # read the file
    df = pd.read_csv(file)
    logger.info(f'Reading noon reports for month ({i+1}) with shape: {df.shape}')
    
    # rewrite the "date" column to be called "utc_timestamp" and parsed as datetime objects with time set to noon/12PM
    df['utc_timestamp'] = pd.to_datetime(df['Date'], format='%d/%m/%Y') + pd.Timedelta(hours=12)
    df = df.drop(columns=['Date'])
    
    # melt the dataframe so that each column (except utc_timestamp) is turned into a row
    melted_df = df.melt(id_vars=['utc_timestamp'], var_name='quantity_name', value_name='value')
    
    # add a column called "qid_mapping" that contains maps the quantity names to their QID dummies
    melted_df['qid_mapping'] = melted_df['quantity_name'].map(noon_rep_qid_dict)
    
    # Arrange the columns in the same order as specified in the columns list
    melted_df = melted_df[['utc_timestamp', 'qid_mapping', 'value', 'quantity_name']]
    
    # append that to the running dataframe
    appended_df = pd.concat([appended_df, melted_df], ignore_index=True)    

# add needed columns to match excl. noon reports dataframe

# add a "source_name" column with value "Noon Report"
appended_df['source_name'] = 'Noon Report'

# TODO: add a unit (use dict but check that each of them make sense)
appended_df['unit'] = appended_df['quantity_name'].map(noon_rep_units_dict)

# load the dataframe containing all the appended sensor observations
excl_noon_reps_df = pd.read_csv(os.path.join(appended_data_dir, 'excl_noon_reports.csv'))

