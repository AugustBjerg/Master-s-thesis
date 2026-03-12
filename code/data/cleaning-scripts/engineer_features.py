import pandas as pd
import numpy as np
import os
import re
from config import WINDOW_LENGTH, JULY_CLEANING_DATE, JANUARY_CLEANING_DATE, FOULING_PROXY_VAR_NAME_WITH_UNIT
from typing import List
from datetime import datetime
from loguru import logger

# define paths
script_dir = os.path.dirname(os.path.abspath(__file__))
aggregated_dir = os.path.join(script_dir, '..', 'aggregated')
aggregated_data_path = os.path.join(aggregated_dir, f'aggregated_{WINDOW_LENGTH}.csv')
engineered_dir = os.path.join(script_dir, '..', 'engineered')
feature_engineering_output_dir = os.path.join(script_dir, '..', '..', 'outputs', 'feature-engineering')

# Create the engineered directory if it doesn't exist
if not os.path.exists(engineered_dir):
    os.makedirs(engineered_dir)
    logger.info(f'Created engineered directory: {engineered_dir}')
else:
    logger.info(f'Engineered directory already exists: {engineered_dir}')

# start logger

# Create the feature engineering output directory for filtering results if it doesn't exist
if not os.path.exists(feature_engineering_output_dir):
    os.makedirs(feature_engineering_output_dir)
    logger.info(f'Created feature engineering output directory: {feature_engineering_output_dir}')
else:
    logger.info(f'Feature engineering output directory already exists: {feature_engineering_output_dir}')

log_path = os.path.join(feature_engineering_output_dir, f'pre_agg_cleaning_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logger.add(
    log_path,
    level='INFO',
    format='{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}'
)

# load data
df = pd.read_csv(aggregated_data_path)

# Ensure datetime datatypes
df["window_start"] = pd.to_datetime(df["window_start"], format="ISO8601", utc=True)

logger.info(f'Loaded data from {aggregated_data_path} with shape {df.shape}')

# --- Functions ---

# --- Key Features ---
def add_days_since_cleaning(df, new_column_name: str, cleaning_dates: List):
    if "window_start" not in df.columns:
        raise KeyError("Column 'window_start' is required to calculate days since cleaning.")

    cleaning_ts = pd.to_datetime(cleaning_dates, utc=True, errors="coerce")
    cleaning_ts = pd.Series(cleaning_ts).dropna().drop_duplicates().sort_values()

    if cleaning_ts.empty:
        raise ValueError("No valid cleaning dates were provided.")

    timestamps = pd.to_datetime(df["window_start"], utc=True, errors="coerce")

    if timestamps.isna().any():
        raise ValueError("Column 'window_start' contains invalid datetime values.")

    timestamps_df = pd.DataFrame(
        {
            "window_start": timestamps,
            "_row_order": np.arange(len(df)),
        }
    ).sort_values("window_start")

    cleaning_df = pd.DataFrame({"cleaning_date": cleaning_ts}).sort_values("cleaning_date")

    merged = pd.merge_asof(
        timestamps_df,
        cleaning_df,
        left_on="window_start",
        right_on="cleaning_date",
        direction="backward",
    )

    merged[new_column_name] = (
        (merged["window_start"] - merged["cleaning_date"]).dt.total_seconds() / 86400
    )

    df[new_column_name] = (
        merged.sort_values("_row_order")[new_column_name].to_numpy()
    )

    return df

def add_mid_draft(df, new_column_name: str, fore_draft_col_name: str, aft_draft_col_name: str):

    df[new_column_name] = (df[fore_draft_col_name] + df[aft_draft_col_name]) / 2

    return df

def add_trim(df, new_column_name: str, fore_draft_col_name: str, aft_draft_col_name: str):

    df[new_column_name] = df[aft_draft_col_name] - df[fore_draft_col_name]

    return df

def add_speed_cubed(df, new_column_name: str, speed_col_name: str):

    df[new_column_name] = df[speed_col_name] ** 3

    return df

def add_speed_dsc_interaction(df, new_column_name: str, speed_col_name: str, dsc_col_name: str):

    df[new_column_name] = df[speed_col_name] * df[dsc_col_name]

    return df

def add_cubic_speed_dsc_interaction(df, new_column_name: str, speed_col_name: str, dsc_col_name: str):

    df[new_column_name] = (df[speed_col_name] ** 3) * df[dsc_col_name]

    return df

def add_SOG_STW_difference(df, new_column_name: str, sog_col_name: str, stw_col_name: str):

    df[new_column_name] = df[sog_col_name] - df[stw_col_name]

    return df

def _true_to_relative_angle(true_angle_deg: pd.Series, heading_deg: pd.Series) -> pd.Series:
    """
    Convert a true (meteorological) angle to a ship-relative angle.
    
    Meteorological convention: angle FROM which the wind/wave comes, in degrees true north.
    Ship-relative: 0° = from the bow, 90° = from starboard, 180° = from stern, 270° = from port.
    
    Parameters
    ----------
    true_angle_deg : pd.Series
        Direction the wind/wave comes FROM, in degrees true north (meteorological convention).
    heading_deg : pd.Series
        Ship's true heading in degrees.
    
    Returns
    -------
    pd.Series
        Relative angle in [0, 360).
    """
    relative = (true_angle_deg - heading_deg) % 360
    return relative

def _longitudinal_component(speed: pd.Series, relative_angle_deg: pd.Series) -> pd.Series:
    """
    Extract the longitudinal (bow-stern axis) component of a vector.
    
    Positive = opposing forward motion (headwind/head-sea resistance).
    Negative = assisting forward motion (tailwind/following sea).
    
    Parameters
    ----------
    speed : pd.Series
        Magnitude of the vector (wind speed, wave height, etc.).
    relative_angle_deg : pd.Series
        Ship-relative angle in degrees (0° = from bow).
    
    Returns
    -------
    pd.Series
        Longitudinal component.
    """
    return speed * np.cos(np.radians(relative_angle_deg))

def add_longitudinal_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add longitudinal wind and wave force components to the dataframe,
    computing relative angles from true (provider) directions and ship heading.

    Parameters
    ----------
    df : pd.DataFrame
        Raw feature dataframe containing heading, wind and wave columns.

    Returns
    -------
    pd.DataFrame
        Copy of df with new longitudinal feature columns appended.
    """

    heading = df["Vessel Hull Heading True Angle (degrees)"]

    # ── Wind (Provider S) ───────────────────────────────────────────────────
    # Reconstruct true wind direction from eastward + northward components
    # atan2 gives angle FROM which wind comes (meteorological convention)
    wind_east  = df["Vessel External Conditions Eastward Wind Velocity (Provider S)"]
    wind_north = df["Vessel External Conditions Northward Wind Velocity (Provider S)"]
    wind_true_speed = np.sqrt(wind_east**2 + wind_north**2)
    wind_true_angle = (np.degrees(np.arctan2(-wind_east, -wind_north)) % 360)

    wind_relative_angle = _true_to_relative_angle(wind_true_angle, heading)
    df["longitudinal_wind_force (calculated)"] = _longitudinal_component(
        wind_true_speed, wind_relative_angle
    )

    # ── Waves (Provider MB) ─────────────────────────────────────────────────
    wave_true_angle = df["Vessel External Conditions Wind True Angle (Provider MB)"]
    wave_height     = df["Vessel External Conditions Wave Significant Height (Provider MB)"]

    wave_relative_angle = _true_to_relative_angle(wave_true_angle, heading)
    df["longitudinal_wave_force (calculated)"] = _longitudinal_component(
        wave_height, wave_relative_angle
    )

    # ── Swell (Provider MB) ─────────────────────────────────────────────────
    # No separate swell direction available — use same wave true angle as proxy
    swell_height = df["Vessel External Conditions Swell Significant Height (Provider MB)"]
    df["longitudinal_swell_force (calculated)"] = _longitudinal_component(
        swell_height, wave_relative_angle
    )

    return df

def add_day_of_year(df, new_column_name: str):
    df[new_column_name] = df["window_start"].dt.dayofyear
    return df

# --- Executions ---

columns_before = set(df.columns)

df = add_days_since_cleaning(df, "Days Since Last Cleaning", [JANUARY_CLEANING_DATE, JULY_CLEANING_DATE])
df = add_mid_draft(df, "Avg Draft (Calculated)", "Fwd Draft (Noon Report)", "Aft Draft (Noon Report)")
df = add_trim(df, "Draft Trim (Calculated)", "Fwd Draft (Noon Report)", "Aft Draft (Noon Report)")
df = add_speed_cubed(df, "Speed Through Water^3 (m/s)", "Vessel Hull Through Water Longitudinal Speed (knots)")
df = add_speed_dsc_interaction(df, "Speed x DSC (calculated)", "Vessel Hull Through Water Longitudinal Speed (knots)", "Days Since Last Cleaning")
df = add_cubic_speed_dsc_interaction(df, "Speed^3 x DSC (calculated)", "Vessel Hull Through Water Longitudinal Speed (knots)", "Days Since Last Cleaning")
df = add_SOG_STW_difference(df, "SOG - STW (calculated)", "Vessel Hull Over Ground Speed (knots)", "Vessel Hull Through Water Longitudinal Speed (knots)")
df = add_longitudinal_features(df)
df = add_day_of_year(df, "Day of Year")

# One-hot encode actual_voyage_id and drop raw voyage columns
if "actual_voyage_id (calculated)" in df.columns:
    voyage_dummies = pd.get_dummies(
        df["actual_voyage_id (calculated)"],
        prefix="voyage",
        drop_first=True,
    )
    voyage_dummies.columns = voyage_dummies.columns.astype(str)
    df = pd.concat([df, voyage_dummies], axis=1)
    logger.info(f"Created {voyage_dummies.shape[1]} voyage dummy columns: {sorted(voyage_dummies.columns.tolist())}")
else:
    logger.warning("'actual_voyage_id (calculated)' not found — skipping voyage dummy creation.")

# Drop raw voyage columns that are not needed as model features
for col_to_drop in ["actual_voyage_id (calculated)", "temporary_voyage_id (calculated)", "voyage_duration_hours (calculated)"]:
    if col_to_drop in df.columns:
        df = df.drop(columns=[col_to_drop])
        logger.info(f"Dropped column '{col_to_drop}'")

# get the first value of every day in january to check if the feature is correct
first_values_january = df[df["window_start"].dt.month == 1].groupby(df["window_start"].dt.date).first()[["window_start", "Days Since Last Cleaning"]]

logger.info("Added 'Days Since Last Cleaning' feature. First values in January:")
logger.info(first_values_january.head(31))

# Check if the fouling proxy variable is present
if FOULING_PROXY_VAR_NAME_WITH_UNIT in df.columns:
    logger.info(f"Fouling proxy variable '{FOULING_PROXY_VAR_NAME_WITH_UNIT}'")
else:
    logger.warning(f"Fouling proxy variable '{FOULING_PROXY_VAR_NAME_WITH_UNIT}' not found in the data after feature engineering. Please check why.")

# check if the fouling proxy feature is present
if FOULING_PROXY_VAR_NAME_WITH_UNIT not in df.columns:
    logger.warning(f"Fouling proxy variable '{FOULING_PROXY_VAR_NAME_WITH_UNIT}' not found in the data. Please check why.")

# save the dateframe with the new features
output_path = os.path.join(engineered_dir, f"engineered_features_{WINDOW_LENGTH}.csv")
df.to_csv(output_path, index=False)
logger.info(f"Saved data with engineered features to {output_path}")

columns_after = set(df.columns)
new_columns = columns_after - columns_before
logger.info(f"Added {len(new_columns)} new columns: {sorted(list(new_columns))}")