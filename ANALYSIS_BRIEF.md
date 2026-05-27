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
- Train file: code/data/train_split.csv (or equivalent — check the folder)
- Test file: code/data/test_split.csv (or equivalent — check the folder)
- Target: Vessel_Propeller_Shaft_Mechanical_Power_KW (or similar — check the column names)
- Features (13): STW, DSC, Avg_Draft, Draft_Trim, Sea_Water_Temp, Wave_Period, 
  Long_Wave_Force, Trans_Wave_Force, Long_Wind_Force, Trans_Wind_Force, 
  Long_Swell_Force, Trans_Swell_Force, Long_Current

## Success criteria
- mgcv additive GAM produces test metrics within a reasonable range of the Python baseline - or there is a natural explanation as to why it doesn't.
- Interaction surface for te(DSC, STW) is interpretable: ideally shows that the 
  fouling penalty grows with DSC but at a rate that varies with speed (however, it might not show such a clean relationship due to concurvity or other data artifacts)

## Things NOT to do
- Don't refit anything from raw data — the preprocessing is finalized
- Don't change the train/test split — use the existing files exactly
- Don't add or remove features beyond what's needed for the tensor product swap¨
- Don't change files in the original code/ folder - you should ONLY read these