import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from config import WINDOW_LENGTH, MIN_WINDOW_COVERAGE, WINDOW_SIDE, WINDOW_LABEL, SENSOR_DATA_AGGREGATION_METHODS, ANGLE_COLUMNS, CUMULATIVE_COLS
from loguru import logger

_num_re = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")

# define paths
script_dir = os.path.dirname(os.path.abspath(__file__))
appended_dir = os.path.join(script_dir, '..', 'appended')
filtered_dir = os.path.join(script_dir, '..', 'filtered')
aggregated_dir = os.path.join(script_dir, '..', 'aggregated')
aggregation_output_dir = os.path.join(script_dir, '..', '..', 'outputs', 'aggregation')

# Create the aggregated directory if it doesn't exist
if not os.path.exists(aggregated_dir):
    os.makedirs(aggregated_dir)
    logger.info(f'Created aggregated directory: {aggregated_dir}')
else:
    logger.info(f'Aggregated directory already exists: {aggregated_dir}')

# start logger

# Create the filtering output directory for filtering results if it doesn't exist
if not os.path.exists(aggregation_output_dir):
    os.makedirs(aggregation_output_dir)
    logger.info(f'Created aggregation output directory: {aggregation_output_dir}')
else:
    logger.info(f'Aggregation output directory already exists: {aggregation_output_dir}')

log_path = os.path.join(aggregation_output_dir, f'pre_agg_cleaning_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logger.add(
    log_path,
    level='INFO',
    format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}'
)

# load dataframes
df = pd.read_csv(
    os.path.join(filtered_dir, 'filtered.csv'),
#    nrows=20000
    )

mixed_long = pd.read_csv(
    os.path.join(appended_dir, 'incl_noon_reports.csv'),
#    nrows=1000000
    )


# Ensure datetime datatypes
mixed_long["utc_timestamp"] = pd.to_datetime(mixed_long["utc_timestamp"], format="ISO8601", utc=True)
df["utc_timestamp"] = pd.to_datetime(df["utc_timestamp"], format="ISO8601", utc=True)

logger.info(f'Loaded filtered data with shape: {df.shape} and raw appended data with shape: {mixed_long.shape}')

logger.info(f'Aggregating with window length: {WINDOW_LENGTH}, minimum coverage: {MIN_WINDOW_COVERAGE}, window side: {WINDOW_SIDE}, and label: {WINDOW_LABEL}...')

# functions
def circular_mean_deg(x):
    x = pd.Series(x).dropna().to_numpy(dtype=float)
    if x.size == 0:
        return np.nan
    r = np.deg2rad(x)
    s = np.mean(np.sin(r))
    c = np.mean(np.cos(r))
    ang = np.rad2deg(np.arctan2(s, c)) % 360.0
    return ang

def counter_increase(x):
    """
    Robust to resets: sums only positive increments.
    Good if the meter can reset/roll over.
    """
    x = pd.Series(x).dropna()
    if len(x) < 2:
        return np.nan
    d = x.diff()
    return d[d > 0].sum()

def parse_numeric_value(x):
    """Extract first numeric token from strings like '%:  -3.85'."""
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    m = _num_re.search(str(x))
    return float(m.group(0)) if m else np.nan

def add_var_full(long_df,
                 quantity_col="quantity_name",
                 source_col="source_name",
                 out_col="var_full"):
    long2 = long_df.copy()
    long2[out_col] = long2[quantity_col].astype(str).str.strip() + " (" + long2[source_col].astype(str).str.strip() + ")"
    return long2

def join_long_vars_asof(
    out_df,
    long_df,
    vars_full,                     # list of column names exactly as you want them in out
    out_time_col="window_start",
    long_time_col="utc_timestamp",
    long_var_col="var_full",
    long_value_col="value",
    direction="backward",
    tolerance="1h",
    collapse_duplicates="mean",
):
    out2 = out_df.copy()

    # If out uses MultiIndex (seg_id, window_start), bring to columns
    if isinstance(out2.index, pd.MultiIndex) and out_time_col not in out2.columns:
        out2 = out2.reset_index()

    out2[out_time_col] = pd.to_datetime(out2[out_time_col], utc=True)
    out2 = out2.sort_values(out_time_col)

    long2 = long_df.copy()
    long2[long_time_col] = pd.to_datetime(long2[long_time_col], utc=True)
    long2[long_value_col] = long2[long_value_col].map(parse_numeric_value)
    long2 = long2.sort_values(long_time_col)

    tol = pd.Timedelta(tolerance)

    for v in vars_full:
        tmp = long2.loc[long2[long_var_col] == v, [long_time_col, long_value_col]].dropna()
        if tmp.empty:
            out2[v] = np.nan
            continue

        # If you have multiple rows at the exact same timestamp for a variable, collapse them
        if collapse_duplicates == "mean":
            tmp = tmp.groupby(long_time_col, as_index=False)[long_value_col].mean()
        elif collapse_duplicates == "last":
            tmp = tmp.groupby(long_time_col, as_index=False)[long_value_col].last()

        tmp = tmp.rename(columns={long_value_col: v})

        out2 = pd.merge_asof(
            out2,
            tmp,
            left_on=out_time_col,
            right_on=long_time_col,
            direction=direction,
            tolerance=tol,
        ).drop(columns=[long_time_col])

    return out2

def asof_attach_vars(out_df, long_df, vars_full,
                     out_time_col="window_start",
                     long_time_col="utc_timestamp",
                     var_col="var_full",
                     value_col="value",
                     tolerance="24h",
                     direction="backward"):
    out2 = out_df.copy()
    if isinstance(out2.index, pd.MultiIndex) and out_time_col not in out2.columns:
        out2 = out2.reset_index()

    out2[out_time_col] = pd.to_datetime(out2[out_time_col], utc=True)
    out2 = out2.sort_values(out_time_col)

    long2 = long_df.sort_values(long_time_col)

    for v in vars_full:
        tmp = (long2.loc[long2[var_col].eq(v), [long_time_col, value_col]]
                    .dropna()
                    .groupby(long_time_col, as_index=False)[value_col].mean()
                    .rename(columns={value_col: v}))

        out2 = pd.merge_asof(
            out2,
            tmp,
            left_on=out_time_col,
            right_on=long_time_col,
            direction=direction,
            tolerance=pd.Timedelta(tolerance),
            suffixes=("", "__new"),
        )  # merge_asof supports direction/tolerance for time alignment [web:82][web:85]

        # fill only NaNs in the existing column
        if v in out2.columns and f"{v}__new" in out2.columns:
            out2[v] = out2[v].combine_first(out2[f"{v}__new"])
            out2 = out2.drop(columns=[f"{v}__new"])
        else:
            # if column didn't exist before, just rename "__new" into place
            out2 = out2.rename(columns={f"{v}__new": v})

        out2 = out2.drop(columns=[long_time_col])

    return out2

def coalesce_xy_columns(df):
    df = df.copy()
    x_cols = [c for c in df.columns if c.endswith("_x")]

    for cx in x_cols:
        base = cx[:-2]
        cy = base + "_y"

        if cy in df.columns:
            # prefer _y; only fill gaps in _y from _x
            df[base] = df[cy].combine_first(df[cx])  # caller takes priority
            df = df.drop(columns=[cx, cy])

    return df

def prep_long_noon_table(long_df,
                    time_col="utc_timestamp",
                    quantity_col="quantity_name",
                    source_col="source_name",
                    value_col="value"):
    long2 = long_df.copy()
    long2[time_col] = pd.to_datetime(long2[time_col], utc=True)

    # full name matches your out naming convention
    long2["var_full"] = (
        long2[quantity_col].astype(str).str.strip()
        + " (" + long2[source_col].astype(str).str.strip() + ")"
    )

    # parse numeric values (handles strings like "%:  -3.85")
    long2[value_col] = long2[value_col].map(parse_numeric_value)
    return long2


# --- Aggregate ---

# get the aggregation methods
agg_map = SENSOR_DATA_AGGREGATION_METHODS
angle_cols = ANGLE_COLUMNS

# Change angles to circular mean instead of arithmetic mean
for c in angle_cols:
    agg_map[c] = circular_mean_deg
    logger.info(f'set column "{c}" to use circular mean aggregation')

# Change cumulative counters to use counter_increase (or counter_delta if you want net change including resets)
for col in CUMULATIVE_COLS:
    if col in agg_map:
        agg_map[col] = counter_increase   
        logger.info(f'set column "{col}" to use counter_increase aggregation, since it is cumulative')

# --- Grouping: seg_id + 15-min bins ---
g = df.groupby(
    [
        "seg_id",
        pd.Grouper(key="utc_timestamp", freq=WINDOW_LENGTH, label=WINDOW_LABEL, closed=WINDOW_SIDE),
    ]
) 
logger.info(f'Grouped dataframe into {g.ngroups} observations')

# aggregate into the multi-index output object using the specified aggregation methods
out = g.agg(agg_map)

# Define the sampling frequency and expected observations per window
dt_seconds = 15.0
expected_n = int(pd.Timedelta(WINDOW_LENGTH).total_seconds() / dt_seconds)
logger.info(f'Expected number of observations per window based on sampling frequency of {dt_seconds}s: {expected_n}')

# calculate coverage for each window
support = g.size().rename("n_obs")
out = out.join(support)
out["coverage"] = out["n_obs"] / expected_n
logger.info(f'Number of windows with sufficient coverage (>= {MIN_WINDOW_COVERAGE*100:.0f}%): {(out["coverage"] >= MIN_WINDOW_COVERAGE).sum()} ({(out["coverage"] >= MIN_WINDOW_COVERAGE).mean() * 100:.2f}%)')

# Filter weak/partial windows (e.g., last partial chunk of a segment)
out = out[out["coverage"] >= MIN_WINDOW_COVERAGE]

# --- Add weather data ----

# Define weather cols
weather_cols = [c for c in out.columns if c.startswith("Vessel External Conditions")]
logger.info(f'Merging in {len(weather_cols)} weather columns')

# forward fill weather values 
max_forward_fill = int(pd.Timedelta("1h").total_seconds() / pd.Timedelta(WINDOW_LENGTH).total_seconds()) 
logger.info(f'Max observations for forward fill of weather values: {max_forward_fill}')

out[weather_cols] = (
    out.groupby("seg_id")[weather_cols]
       .ffill(
           limit=max_forward_fill
           )
)

# Bring seg_id + window_start back as columns
out = out.reset_index().rename(columns={"utc_timestamp": "window_start"})

weather_long = mixed_long[mixed_long["qid_mapping"].str.startswith("4")]
logger.info(f"weather_long shape: {weather_long.shape}")

# add full variable names to align with synchronized table
weather_long = add_var_full(weather_long) 
logger.info(f'added column with full variable names to weather_long')

# Choose variables to join
weather_cols_in_out = [c for c in out.columns if c.startswith("Vessel External Conditions")]
logger.info(f'weather columns in out: {weather_cols_in_out} (including 2 on board sensors)')

# Subtract weather columns measured on board
weather_cols_in_out = [c for c in weather_cols_in_out if c not in ["Vessel External Conditions Wind Relative Speed (knots)", "Vessel External Conditions Wind Relative Angle (degrees)"]]
logger.info(f'weather columns to join from long: {weather_cols_in_out} (excluding 2 on board sensors)')

# Execute the join
weather_vars = sorted(weather_long["quantity_name"].dropna().unique())
logger.info(f"weather variables to join: {weather_vars}")

out_with_weather = join_long_vars_asof(
    out_df=out,
    long_df=weather_long,
    vars_full=weather_cols_in_out,
    out_time_col="window_start",     # adjust if yours is named differently
    tolerance="1h",                  
)

out_with_weather = coalesce_xy_columns(out_with_weather)

logger.info(f'Joined weather variables in and coalesced double weather columns. Total NaNs in weather columns after join: {out_with_weather[weather_cols_in_out].isna().sum().sum()}')

# --- Add noon report data ---

# get only the noon report values from the appended table
noon_long = mixed_long[mixed_long["qid_mapping"].str.startswith("0")]
noon_long = prep_long_noon_table(noon_long)
noon_long = noon_long[noon_long["source_name"].eq("Noon Report")].copy()
logger.info(f"noon_long shape: {noon_long.shape}")

# define the noon variables of interest
noon_vars = ["Fwd Draft (Noon Report)", "Mid Draft (Noon Report)", "Aft Draft (Noon Report)"]
logger.info(f"noon report variables to join: {noon_vars}")

# attach noon report values to the table
out_with_weather_and_noon = asof_attach_vars(out_with_weather, noon_long, noon_vars, tolerance="24h")
logger.info(f'Joined noon report variables in. Total NaNs in noon report columns after join: {out_with_weather_and_noon[noon_vars].isna().sum().sum()}')

logger.info(f'Final shape after joining weather and noon report data: {out_with_weather_and_noon.shape}')

# --- Last cleaning ---

# drop rows with NaN values if less than 1% of observations
total_nans = out_with_weather_and_noon.isna().sum().sum()
total_cells = out_with_weather_and_noon.size
nan_pct = total_nans / total_cells * 100
logger.info(f"Total NaNs in out_with_weather_and_noon: {total_nans} ({nan_pct:.2f}%)")
if nan_pct < 1.0:
    out_with_weather_and_noon = out_with_weather_and_noon.dropna()
    logger.info(f"Dropped NaN rows, new shape: {out_with_weather_and_noon.shape}") 
else:
    out_with_weather_and_noon = out_with_weather_and_noon.dropna()
    logger.warning(f'expected less than 1% NaNs at this point, but got {nan_pct:.2f}%. Consider reviewing the join steps and NaN handling.')

# ---- Saving ----

# Save the aggregated table to a csv file with the window length in the name
output_path = os.path.join(aggregated_dir, f'aggregated_{WINDOW_LENGTH}.csv')
out_with_weather_and_noon.to_csv(output_path, index=False)
logger.info(f'Saved aggregated table to {output_path}')




