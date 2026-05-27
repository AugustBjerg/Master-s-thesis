# R Environment & Style

## Packages
- mgcv (GAM fitting — gold standard for tensor products)
- gratia (modern visualization for mgcv objects, much better than base plot.gam)
- dplyr, readr, ggplot2 (data wrangling + plotting)

## Style
- Native pipe |> over magrittr %>% 
- Snake_case for variables, matching the Python convention if possible
- Comment the GAM formula clearly — every smooth term should have a one-line 
  comment explaining why it's there and what shape is expected
- No tidyverse-heavy data wrangling — the data should already be clean

## Data quirks/artifcats worth knowing
- Draft is effectively discrete (24 unique (avg_draft, trim) combinations)
- DSC has noticeable gaps in distribution due to filtering of port stays
- DSC and Avg_Draft are correlated (Pearson -0.616), which is expected since 
  the vessel sails heavier shortly after cleaning. This affects interpretation.
- All features were standardized before fitting in Python — check whether the 
  saved CSVs are pre- or post-standardization, and standardize in R if needed 
  to keep the comparison fair