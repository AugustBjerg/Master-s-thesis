# --- BEFORE WRITING THIS: WHAT SHOULD BE DONE NOW AND WHAT SHOULD BE DONE AFTER AGGREGATION? ---   

# TODO: (optional - if noon report data is included)clean value column on noon report data from scale or unit-related contamination

# --- Sentinel / dropout values (no valid measurement) ---
# TODO: Replace sentinel values with NaN, 
# TODO: replace negative values of main engine rotation with NaN (it cannot be negative)

# --- NaN removal where appropriate ---

# --- CLEANING of "wrong" data ---
# TODO: remove any signs of a ship "in reverse" or maneuvering
    # 1. Negative speed values (remove rows)
    # 2. negative propeller shaft rotational speed (remove rows)
    # 3. rows where propeller shaft rotational speed is 0 AND main engine rotational speed is above 0 with NaN for both (impossible - remove)

    # 4. (optional) remove rows where the ship is "cruising" (propeller turned off but still moving)


# TODO: deal with observations with incongruent main engine fuel load and shaft power

# --- Outlier removal ---
    # 1. Propeller shaft power and propeller shaft rotational speed has some pretty clear outliers with negative values (remove)

# --- NaN imputation ---
# TODO: when imputing, keep a dummy column that flags "imputed" so i retain the information that this was a bad measurement