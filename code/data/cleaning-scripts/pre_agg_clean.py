# --- BEFORE WRITING THIS: WHAT SHOULD BE DONE NOW AND WHAT SHOULD BE DONE AFTER AGGREGATION? ---   

# TODO: (optional - if noon report data is included)clean value column on noon report data from scale or unit-related contamination
# TODO: Make sure this plan is represented in onenote
# TODO: make the below as a function and apply it to each synchronized time segment separately (to avoid interpolating across intervals)
# TODO: every time something is removed, make sure to log the amount of N and % of both the original dataframe length and the current dataframe length that is removed


# --- Sentinel / dropout /invalid values (no valid measurement. SHOULD THESE BE DROPPED OR REPLACED WITH NaN?)) ---
# TODO: remove (or replace with NaN - TBD) impossible / inconsistent data points (based on single values)
    # Drop draft columns with all 0 measurements
     # 1. rows where propeller shaft rotational speed is 0 AND main engine rotational speed is above 0
     # 2. rows where propeller RPM and Shaft power have different sign (must have the same sign)
    # 3. rows where wave height, wave period, wind speed are negative 
    # 4. Headings / angles outside their defined ranges
    # 5. replace negative hull over ground speed values with Nan
    # 6. replace negative values of main engine rotation with NaN (it cannot be negative)
     # 1. replace sea temp = exactly 6 with NaN (obvious sensor dropout)
    # Insert NaN values for cumulative revs that decrease (by making a new column with delta, and replacing both cumulative and delta with NaN where delta is negative)


# Optional
    # 7. rows where Vessel External Conditions Eastward AND northward (2 variables) Sea Water Velocity (provider MB) is 0. Consider whether this is likely to just be a "near 0" value
    # 8. rows where Vessel External Conditions wave signfiicant height (provider S) is 0. Consider whether this is likely to just be a "near 0" value
    # 9. rows where Vessel External Conditions Eastward AND northward (2 variables) Sea Water Velocity (provider S) is -0.4. Consider whether this is likely to just be a "near 0" value
    # 10. Swell significant height of 0

# --- NaN removal where appropriate ---

# --- CLEANING of "undesirable" data ---
# TODO: remove any signs of a ship "in reverse" or maneuvering
    # 1. Start with DNVs method for rolling-window variance for stw and hull heading (include boundaries as config variable for tweaking). This should remove most "unsteady" operations
    # 2. continue with removing too low speed (below 4 knots according to Dalheim & Stein). A ship could be steady in this region without it being of interest
    # 3. negative propeller shaft rotational speed (remove rows)
    # 4. rows where propeller shaft power are negative (is either reversing/maneuvering or a bad measurement - remove). This is done because i want to document a more steady state

# TODO: optional
    # 1. (optional) remove rows where the ship is "cruising" (propeller turned off / 0 but still moving)
    # 2. heavy/violent weather (i think i will keep this for now in case there is some variance to feed the model with)
    # 3. Anything where the ship is accerelating very fast (not yet sure if this might actually be useful information to the model, so keep for now)

# TODO: deal with observations with incongruent main engine fuel load and shaft power

# --- Outlier removal ---
# TODO: for every column, have chat pick obvious (for physical/practical reasons) thresholds that a no-brainer outliers
# TODO: after that, consider adding additional outlier removal based on statistical methods (e.g. IQR method, z-score method, etc.)

    # 1. Propeller shaft power and propeller shaft rotational speed has some pretty clear outliers with negative values (remove)

# --- NaN imputation ---
# TODO: when imputing, keep a dummy column that flags "imputed" so i retain the information that this was a bad measurement
# TODO: TBD - maybe impute weather data now - maybe wait until aggregation step

# TODO: for sea temp: 
    # if less than 4 in a row and not in the end of a time segment, use linear interpolation
    # if 4 or more, impute with the median for that time segment (Note in report that it is model convenience / judgment call, not strict practice)

# --- Formatting --- 
    # 1. Change the sign on thrust force (currently negative)
    # 2. Rename columns to their real names instead of qids