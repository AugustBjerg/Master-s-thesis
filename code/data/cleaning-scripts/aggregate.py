import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- Specs ---

# TODO: put these in config file when code is ready

WINDOW_LENGTH = "15min"

WINDOW_COVERAGE = 0.9

WINDOW_ANCHOR =

WINDOW_SIDE =

WINDOW_LABEL = 

SENSOR_DATA_AGGREGATION_METHODS = {
    "Vessel Hull Over Ground Speed (knots)": "mean",
    "Vessel Hull Through Water Longitudinal Speed (knots)": "mean",
    "Vessel External Conditions Wind Relative Speed (knots)": "mean",
    "Vessel External Conditions Wind Relative Angle (degrees)": "mean",
    "Vessel Hull Heading True Angle (degrees)": "mean",
    "Vessel Hull Heading Turn Rate (deg/min)": "mean",
    "Main Engine Turbocharger Rotational Speed (rpm)": "mean",
    "Main Engine Scavenging Air Pressure (bar)": "mean",
    "Main Engine Fuel Load % (%)": "mean",
    "Main Engine Rotational Speed (rpm)": "mean",
    "Vessel Propeller Shaft Torque (N*m)": "mean",
    "Vessel Propeller Shaft Mechanical Power (KW)": "mean",
    "Main Engine Fuel Oil Inlet Mass Flow (kg/hr)": "mean",
    "Vessel Propeller Shaft Mechanical Energy (KWh)": "sum",
    "Vessel Propeller Shaft Thrust Force (KN)": "mean",
    "Vessel Propeller Shaft Rotational Speed (rpm)": "mean",
    "Vessel Propeller Shaft Revolutions (cumulative) (revs)": "sum",
    "Vessel External Conditions Northward Sea Water Velocity (Provider MB)": "mean",
    "Vessel External Conditions Wave Significant Height (Provider MB)": "mean",
    "Vessel External Conditions Eastward Wind Velocity (Provider S)": "mean",
    "Vessel External Conditions Wind True Angle (Provider MB)": "mean",
    "Vessel External Conditions Swell Significant Height (Provider MB)": "mean",
    "Vessel External Conditions Sea Water Temperature (Provider S)": "mean",
    "Vessel External Conditions Eastward Sea Water Velocity (Provider MB)": "mean",
    "Vessel External Conditions Wave Period (Provider S)": "mean",
    "Vessel External Conditions Wind True Speed (Provider MB)": "mean",
    "Vessel External Conditions Northward Sea Water Velocity (Provider S)": "mean",
    "Vessel External Conditions Wave Significant Height (Provider S)": "mean",
    "Vessel External Conditions Northward Wind Velocity (Provider S)": "mean",
    "Sea Temperature Dropout": "sum",
    "Calculated Shaft Power": "mean",
    "Imputed Spike in Main Engine Rotational Speed": "sum",
    "Imputed Spike in Vessel External Conditions Wind Relative Speed": "sum",
    "Imputed Spike in Vessel External Conditions Wind Relative Angle": "sum",
    "Imputed Spike in Vessel Hull Over Ground Speed": "sum",
    "Imputed Spike in Vessel Hull Heading Turn Rate": "sum",
    "Imputed Spike in Vessel Hull Heading True Angle": "sum",
    "Imputed Spike in Main Engine Turbocharger Rotational Speed": "sum",
    "Imputed Spike in Vessel Hull Through Water Longitudinal Speed": "sum",
    "Imputed Spike in Main Engine Fuel Oil Inlet Mass Flow": "sum",
    "Imputed Spike in Vessel Propeller Shaft Mechanical Power": "sum",
    "Imputed Spike in Vessel Propeller Shaft Rotational Speed": "sum",
    "Imputed Spike in Vessel Propeller Shaft Torque": "sum",
    "Imputed Spike in Vessel Propeller Shaft Thrust Force": "sum",
    "Imputed Spike in Main Engine Fuel Load %": "sum",
    "Imputed Spike in Main Engine Scavenging Air Pressure": "sum",
}

# Ensure datetime type
df["utc_timestamp"] = pd.to_datetime(df["utc_timestamp"], utc=True)

# --- Aggregate ---

# TODO: remember not to aggregate across time windows (group by seg_id)

# 1. Create a data "backbone" with the window index as the index (columns for seg_id and label)
# 2. For each window, compute the following: value (based on agg function), sensor data coverage (proportion of values vs. expected), as well as std, min and max for important / relevant variables
# 3. Include error handling if a variable is not present in the SENSOR_DATA_AGGREGATION_METHODS dict



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


