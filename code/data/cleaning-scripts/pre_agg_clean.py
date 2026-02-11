# TODO: (optional - if noon report data is included)clean value column on noon report data from scale or unit-related contamination
# TODO: Replace negative speed values with NaN

# --- Sentinel / dropout values (no valid measurement) ---
# TODO: Replace sentinel values with NaN, 
# TODO: replace negative values of main engine rotation with NaN (it cannot be negative)


# --- NaN removal where appropriate ---

# --- CLEANING of "wrong" data ---
# TODO: remove any signs of a ship "in reverse"
    # 1. Negative speed values (remove rows)
    # 2. negative propeller shaft rotational speed (remove rows)

# TODO: deal with observations with incongruent main engine fuel load and shaft power

# --- Outlier removal ---

# --- NaN imputation ---
# TODO: when imputing, keep a dummy column that flags "imputed" so i retain the information that this was a bad measurement