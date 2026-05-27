# Supplementary Analysis: GAM with Tensor Product Interaction

## Goal
Refit my thesis GAM (PyGAM version available in code/modelling/gam.py) in R using mgcv, adding a tensor product smooth te(DSC, STW) 
to capture the fouling × speed interaction that the original additive GAM couldn't represent. Compare predictive accuracy to the Python baseline and produce an interpretable interaction surface between DSC and Speed, being aware that one for DSC and Draft might be needed later.

## Context
This is a supplementary analysis to my submitted master's thesis on marine biofouling estimation. The thesis found that the additive GAM was too rigid: it imposes constant absolute added power from fouling across all speeds, which contradicts ship propulsion theory (added power as % of total power should decrease with speed). The thesis discussion explicitly flags tensor-product GAMs as the natural next step.

## Baseline (Python GAM, pyGAM)
- python file available at code/modelling/gam.py
- Test RMSE: 286.5 kW
- Test MAPE: 7.34%
- Test MAE: 213.6 kW
- Structure: purely additive, 13 univariate smooths, 10 cubic B-splines per term
- Global smoothing parameter: λ ≈ 5.18e-8 (selected via grid search, GCV)
- Monotonicity constraints applied to: Avg_Draft, DSC, STW (increasing); 
  Sea_Water_Temp (decreasing); Long_Wave_Force, Long_Wind_Force, Long_Swell_Force (increasing)
- 80/20 random train/test split (already done; reuse the same files located in the code/data/train-and-test)

## This Analysis (R, mgcv)
- Replicate the additive GAM in mgcv first (sanity check that metrics roughly match Python)
- Then refit with te(DSC, STW) as well as the two separate s(DSC) and s(STW) terms.
- Keep the monotonicity constraints on the isolated marginal terms and leave the 2 interaction unconstrained
- Compare test metrics against the Python baseline AND the R additive replication
- Produce a 2D interaction surface (3D plot) that allows me to visually inspect the interaction

## Data
- File: code/data/train_test_splits_15min.npz (single file containing all four arrays)
- Load in R with: `np <- reticulate::np$load("train_test_splits_15min.npz")` or convert to CSV first with a small Python script — whatever you deem the easiest
- Arrays: X_train (12048×13), X_test (3013×13), y_train (12048,), y_test (3013,)
- Target (y): Stored as a separate array (y_train, y_test) — not a column in X. Represents shaft mechanical power in kW, range ~99–5598 kW, mean ~3368 kW.
- Rename columns to clean snake_case immediately on load (e.g. janitor::clean_names()) to avoid backtick hell throughout the script

## Features (exact column names in order)
| Index | Raw name | Suggested R name |
|-------|----------|-----------------|
| 0  | Vessel External Conditions Sea Water Temperature (Provider S) | sea_water_temp |
| 1  | Vessel External Conditions Wave Period (Provider S) | wave_period |
| 2  | longitudinal_wave_force (calculated) | long_wave_force |
| 3  | longitudinal_wind_force (calculated) | long_wind_force |
| 4  | longitudinal_swell_force (calculated) | long_swell_force |
| 5  | transversal_swell_force (calculated) | trans_swell_force |
| 6  | transversal_wind_force (calculated) | trans_wind_force |
| 7  | transversal_wave_force (calculated) | trans_wave_force |
| 8  | Avg Draft (Calculated) | avg_draft |
| 9  | SOG - STW (calculated) | long_current |
| 10 | Draft Trim (Calculated) | draft_trim |
| 11 | Days Since Last Cleaning | dsc |
| 12 | Vessel Hull Through Water Longitudinal Speed (knots) | stw |

## Success criteria
- mgcv additive GAM produces test metrics within a reasonable range of the Python baseline - or there is a natural explanation as to why it doesn't.
- Interaction surface for te(DSC, STW) is interpretable: ideally shows that the 
  fouling penalty grows with DSC but at a rate that varies with speed (however, it might not show such a clean relationship due to concurvity or other data artifacts)

## Things NOT to do
- Don't refit anything from raw data — the preprocessing is finalized
- Don't change the train/test split — use the existing files exactly
- Don't add or remove features beyond what's needed for the tensor product swap¨
- Don't change files in the original code/ folder - you should ONLY read these