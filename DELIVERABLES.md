# Expected Outputs

## Files to generate (in r_analysis/ or repo root)
- gam_additive_r.rds — fitted additive GAM (replication of Python baseline)
- gam_tensor_r.rds — fitted GAM with te(DSC, STW)
- model_comparison.csv — Python baseline vs R additive vs R tensor (RMSE, MAPE, MAE for both train and test sets)
- interaction_surface.png — 3D plot of te(DSC, STW) effect on shaft power
- dsc_partial_effect_curve.png - partial effect of dsc on power according to the additive term
- stw_partial_effect_curve.png - partial effect of speed on power according to the additive term
- draft_partial_effect_curve.png - partial effect of draft on power according to the additive term
- gam_analysis.R — full reproducible script, well-commented

## Required plots
1. Interaction surface — contour plot with DSC on x-axis, STW on y-axis, 
   predicted shaft power as color/contours. Use gratia::draw() or vis.gam().
2. Marginal smooths for the remaining 11 features (sanity check that they look 
   similar to the thesis Fig 6.6 partial effects)
3. Residual diagnostics: QQ plot, residuals vs fitted