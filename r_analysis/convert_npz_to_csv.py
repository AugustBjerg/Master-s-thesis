"""
Convert the pre-built train/test split from NPZ to CSV for use in R.

Source : code/data/train-and-test/train_test_splits_15min.npz
Output : r_analysis/train.csv
         r_analysis/test.csv

Columns are renamed to snake_case per the mapping in ANALYSIS_BRIEF.md.
The target variable (shaft mechanical power in kW) is appended as shaft_power_kw.
No scaling is applied — values are raw, matching what the Python sklearn Pipeline
receives before its internal StandardScaler step.
"""

import pathlib
import numpy as np
import pandas as pd

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
NPZ_PATH  = REPO_ROOT / "code" / "data" / "train-and-test" / "train_test_splits_15min.npz"
OUT_DIR   = REPO_ROOT / "r_analysis"

COLUMN_MAP = {
    "Vessel External Conditions Sea Water Temperature (Provider S)": "sea_water_temp",
    "Vessel External Conditions Wave Period (Provider S)":           "wave_period",
    "longitudinal_wave_force (calculated)":                         "long_wave_force",
    "longitudinal_wind_force (calculated)":                         "long_wind_force",
    "longitudinal_swell_force (calculated)":                        "long_swell_force",
    "transversal_swell_force (calculated)":                         "trans_swell_force",
    "transversal_wind_force (calculated)":                          "trans_wind_force",
    "transversal_wave_force (calculated)":                          "trans_wave_force",
    "Avg Draft (Calculated)":                                        "avg_draft",
    "SOG - STW (calculated)":                                        "long_current",
    "Draft Trim (Calculated)":                                       "draft_trim",
    "Days Since Last Cleaning":                                      "dsc",
    "Vessel Hull Through Water Longitudinal Speed (knots)":          "stw",
}

def main():
    data = np.load(NPZ_PATH, allow_pickle=True)

    raw_cols = data["X_columns"].tolist()
    print(f"Columns in NPZ ({len(raw_cols)}): {raw_cols}")

    X_train = pd.DataFrame(data["X_train"], columns=raw_cols)
    X_test  = pd.DataFrame(data["X_test"],  columns=raw_cols)
    y_train = data["y_train"].ravel()
    y_test  = data["y_test"].ravel()

    missing = [c for c in raw_cols if c not in COLUMN_MAP]
    if missing:
        raise ValueError(f"Unmapped columns: {missing}")

    X_train = X_train.rename(columns=COLUMN_MAP)
    X_test  = X_test.rename(columns=COLUMN_MAP)

    X_train["shaft_power_kw"] = y_train
    X_test["shaft_power_kw"]  = y_test

    train_path = OUT_DIR / "train.csv"
    test_path  = OUT_DIR / "test.csv"
    X_train.to_csv(train_path, index=False)
    X_test.to_csv(test_path,  index=False)

    print(f"\nTrain: {X_train.shape}  ->  {train_path}")
    print(f"Test:  {X_test.shape}   ->  {test_path}")
    print(f"\nTrain columns: {X_train.columns.tolist()}")
    print(f"\nTrain head:\n{X_train.head(3).to_string()}")
    print(f"\nshaft_power_kw stats (train): min={y_train.min():.1f}  max={y_train.max():.1f}  mean={y_train.mean():.1f}")

if __name__ == "__main__":
    main()
