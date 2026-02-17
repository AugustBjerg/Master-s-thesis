import pandas as pd
import numpy as np
import os
from datetime import datetime


# TODO: add draft data from noon reports (deemed easier to do after the synced tables area appended to one long table)
# Determine the granularity

# set aggregation functions for each statistic

# Handle NaN values
    # What only some values are NaN?
    # What if all values are NaN?

# What happens when there are not observations in that time period?

# TODO: NaN imputationfor sea temp: 
    # if less than 4 in a row and not in the end of a time segment, use linear interpolation
    # if 4 or more, impute with the median for that time segment (Note in report that it is model convenience / judgment call, not strict practice)


