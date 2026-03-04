import pandas as pd
import numpy as np
import os
import re
from sklearn.model_selection import train_test_split
from config import WINDOW_LENGTH, TRAIN_RATIO, TARGET_VARIABLE
from typing import List
from datetime import datetime
from loguru import logger

random_seed = 42
np.random.seed(random_seed)

# define paths
script_dir = os.path.dirname(os.path.abspath(__file__))
engineered_dir = os.path.join(script_dir, '..', 'engineered')
engineered_data_path = os.path.join(engineered_dir, f'engineered_features_{WINDOW_LENGTH}.csv')
train_test_dir = os.path.join(script_dir, '..', 'train-and-test')

# Create the train-and-test directory if it doesn't exist
if not os.path.exists(train_test_dir):
    os.makedirs(train_test_dir)
    logger.info(f'Created train-and-test directory: {train_test_dir}')
else:
    logger.info(f'Train-and-test directory already exists: {train_test_dir}')

# start logger

# load data
df = pd.read_csv(engineered_data_path)

# Ensure datetime datatypes
df["window_start"] = pd.to_datetime(df["window_start"], format="ISO8601", utc=True)

logger.info(f'Loaded data from {engineered_data_path} with shape {df.shape}')

# Make a randomized test and training split
X_train, X_test, y_train, y_test = train_test_split(
    df.drop(columns=[TARGET_VARIABLE]), 
    df[TARGET_VARIABLE], 
    test_size=1-TRAIN_RATIO, 
    random_state=random_seed
)

# Store as a npz files
np.savez_compressed(
    train_test_dir / "train_test_splits.npz",
    X_train=X_train,
    X_test=X_test,
    y_train=y_train,
    y_test=y_test,
)

# Use overall or train median for controlled variable experiments?