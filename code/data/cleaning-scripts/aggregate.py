import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- Specs ---

# TODO: put these in config file when code is ready

WINDOW_LENGTH = 

WINDOW_ANCHOR =

WINDOW_SIDE =

WINDOW_LABEL = 

DATA_AGGREGATION_METHODS = {
    "instantaneous continuous":[], #
    "cumulative counters":[],
    "rate":[],
    "angle":[],
    "boolean":[]
}

# --- Aggregate ---

# 1. For each window, compute the following: value (based on agg function), sensor data coverage (proportion of values vs. expected), as well as std and similar for important / relevant variables
# 2. Include error handling if a variable is not present in the DATA_AGGREGATION_METHODS dict
# 
# 4.

# TODO: Include error handling if a variable is not present in the DATA AGGREGATION METHODS dict




# set aggregation functions for each statistic

# Handle NaN values in metocean data:
    # What only some values are NaN?
    # What if all values are NaN?

# What happens when there are not observations in that time period?

# TODO: NaN imputationfor sea temp: 
    # if less than 4 in a row and not in the end of a time segment, use linear interpolation
    # if 4 or more, impute with the median for that time segment (Note in report that it is model convenience / judgment call, not strict practice)

# TODO: add draft data from noon reports (literature recommends doing this after filtering)
# - consider adding an average draft column

# Do any feature engineering here (after adding and aggregating - maybe in a separate script)


