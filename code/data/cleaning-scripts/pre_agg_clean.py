# --- BEFORE WRITING THIS: WHAT SHOULD BE DONE NOW AND WHAT SHOULD BE DONE AFTER AGGREGATION? ---   

# TODO: (optional - if noon report data is included)clean value column on noon report data from scale or unit-related contamination
# TODO: Make sure this plan is represented in onenote
# TODO: make the below as a function and apply it to each synchronized time segment separately (to avoid interpolating across intervals)

# --- Sentinel / dropout /invalid values (no valid measurement) ---
# TODO: remove (or replace with NaN - TBD) impossible / inconsistent data points (based on single values)
     # 1. rows where propeller shaft rotational speed is 0 AND main engine rotational speed is above 0
     # 2. Tiws where propeller RPM and Shaft power have different sign (must have the same sign)
    # 3. rows where wave height, wave period, wind speed are negative 
    # 4. Headings / angles outside their defined ranges
    # 5. replace negative hull over ground speed values with Nan
    # 6. replace negative values of main engine rotation with NaN (it cannot be negative)

# TODO: Treat sensor dropouts 
    # 1. replace sea temp = exactly 6 with NaN (obvious sensor dropout)

# --- NaN removal where appropriate ---

# --- CLEANING of "wrong" data ---


# TODO: remove any signs of a ship "in reverse" or maneuvering
    # 1. Start with a no-brainer like too low stw (below 4 knots according to Dalheim & Stein). This should remove most - the rest is just included as an extra assurance
    # 2. Negative speed through water values (remove rows)
    # 3. negative propeller shaft rotational speed (remove rows)
   
    # 5. rows where propeller shaft power are negative (is either reversing/maneuvering or a bad measurement - remove). This is done because i want to document a more steady state

# TODO: optional
    # 1. (optional) remove rows where the ship is "cruising" (propeller turned off / 0 but still moving)


# TODO: deal with observations with incongruent main engine fuel load and shaft power

# --- Outlier removal ---
# TODO: for every column, have chat pick obvious (for physical/practical reasons) thresholds that a no-brainer outliers
# TODO: after that, consider adding additional outlier removal based on statistical methods (e.g. IQR method, z-score method, etc.)

    # 1. Propeller shaft power and propeller shaft rotational speed has some pretty clear outliers with negative values (remove)

# --- NaN imputation ---
# TODO: when imputing, keep a dummy column that flags "imputed" so i retain the information that this was a bad measurement
# TODO: keeo in mind that weather data is supposed to have some NaN data for now, so only impute on the hourly mark

# TODO: for sea temp: 
    # if less than 4 in a row and not in the end of a time segment, use linear interpolation
    # if 4 or more, impute with the median for that time segment (Note in report that it is model convenience / judgment call, not strict practice)