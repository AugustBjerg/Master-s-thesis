# =============================================================================
# gam_analysis.R  —  Supplementary Analysis: GAM with Tensor Product Interaction
# =============================================================================
# Goal:
#   1. Replicate the Python pyGAM additive model in mgcv (sanity check)
#   2. Extend with ti(dsc, stw) to capture fouling × speed interaction
#   3. Compare test metrics against Python baseline; produce interpretable plots
#
# Working directory: set to repo root (Master-s-thesis/) before running.
#
# Required packages: mgcv, gratia, dplyr, readr, ggplot2
# Install if needed:
#   install.packages(c("mgcv", "gratia", "dplyr", "readr", "ggplot2"))
# =============================================================================

library(mgcv)
library(scam)      # registers mpi/mpd smooth constructors so gam() can use them
library(gratia)
library(patchwork) # required for plot_annotation(); gratia depends on it but doesn't re-export it
library(dplyr)
library(readr)
library(ggplot2)

# ---- Paths ------------------------------------------------------------------
OUT_DIR    <- "r_analysis"
TRAIN_PATH <- file.path(OUT_DIR, "train.csv")
TEST_PATH  <- file.path(OUT_DIR, "test.csv")

# ---- Load data --------------------------------------------------------------
train_raw <- read_csv(TRAIN_PATH, show_col_types = FALSE)
test_raw  <- read_csv(TEST_PATH,  show_col_types = FALSE)

stopifnot(nrow(train_raw) == 12048, nrow(test_raw) == 3013, ncol(train_raw) == 14)
cat(sprintf("Loaded: train=%d rows | test=%d rows | %d columns\n",
            nrow(train_raw), nrow(test_raw), ncol(train_raw)))

# ---- Standardise features ---------------------------------------------------
# The Python sklearn Pipeline applies StandardScaler (mean=0, sd=1) to all 13
# continuous features inside the model. Parameters are fitted on X_train only.
# We replicate this here to keep the comparison fair.
TARGET       <- "shaft_power_kw"
FEATURE_COLS <- setdiff(names(train_raw), TARGET)

train_means <- colMeans(train_raw[FEATURE_COLS])
train_sds   <- sapply(train_raw[FEATURE_COLS], sd)

scale_df <- function(df, means, sds) {
  df[FEATURE_COLS] <- mapply(
    function(col, m, s) (col - m) / s,
    df[FEATURE_COLS], means, sds,
    SIMPLIFY = FALSE
  )
  df
}

train <- scale_df(train_raw, train_means, train_sds)
test  <- scale_df(test_raw,  train_means, train_sds)  # use train params — no leakage

# ---- Metric helpers ---------------------------------------------------------
rmse <- function(y, yhat) sqrt(mean((y - yhat)^2))
mape <- function(y, yhat) mean(abs((y - yhat) / y)) * 100  # as percentage
mae  <- function(y, yhat) mean(abs(y - yhat))

eval_metrics <- function(model, tr, te) {
  yhat_tr <- predict(model, newdata = tr)
  yhat_te <- predict(model, newdata = te)
  list(
    train_rmse = rmse(tr[[TARGET]], yhat_tr),
    train_mape = mape(tr[[TARGET]], yhat_tr),
    train_mae  = mae( tr[[TARGET]], yhat_tr),
    test_rmse  = rmse(te[[TARGET]], yhat_te),
    test_mape  = mape(te[[TARGET]], yhat_te),
    test_mae   = mae( te[[TARGET]], yhat_te)
  )
}

# ---- Plot helper ------------------------------------------------------------
save_plot <- function(p, filename, width = 7, height = 5, dpi = 150) {
  ggsave(file.path(OUT_DIR, filename), plot = p,
         width = width, height = height, dpi = dpi)
  cat("Saved:", filename, "\n")
}

# =============================================================================
# MODEL 1: Additive GAM  (replication of Python pyGAM baseline)
# =============================================================================
# 13 univariate smooths, k=10 each (matches Python's n_splines=10).
# Monotonicity constraints replicate pyGAM's constraints= argument:
#   bs="mpi"  monotone-increasing P-spline (Pya & Wood 2015; mgcv >= 1.8-36)
#   bs="mpd"  monotone-decreasing P-spline
#   bs="cr"   cubic regression spline (unconstrained)
#
# method="REML" is preferred over GCV for stable smoothing parameter selection.
# Python used GCV — minor differences in effective df per term are expected.
# This model is a sanity check: metrics should be broadly comparable to the
# Python baseline (test RMSE ~286 kW, MAPE ~7.3%, MAE ~214 kW).

formula_additive <- shaft_power_kw ~
  s(sea_water_temp,   bs = "mpd", k = 10) +  # colder water -> higher viscosity -> more resistance
  s(wave_period,      bs = "cr",  k = 10) +  # resonance effects are non-monotone
  s(long_wave_force,  bs = "mpi", k = 10) +  # head-on wave resistance: additive/increasing
  s(long_wind_force,  bs = "mpi", k = 10) +  # head-on wind resistance: additive/increasing
  s(long_swell_force, bs = "mpi", k = 10) +  # head-on swell resistance: additive/increasing
  s(trans_swell_force,bs = "cr",  k = 10) +  # transversal forces can aid or oppose
  s(trans_wind_force, bs = "cr",  k = 10) +
  s(trans_wave_force, bs = "cr",  k = 10) +
  s(avg_draft,        bs = "mpi", k = 10) +  # deeper draft -> more wetted area -> more resistance
  s(long_current,     bs = "cr",  k = 10) +  # current component: can help or hinder
  s(draft_trim,       bs = "cr",  k = 10) +  # optimal trim exists; effect is non-monotone
  s(dsc,              bs = "mpi", k = 10) +  # more days since cleaning -> more fouling -> more power
  s(stw,              bs = "mpi", k = 10)    # more speed through water -> more power (cube law)

cat("\n=== Fitting Model 1: Additive GAM ===\n")
gam_additive <- gam(
  formula_additive, data = train, method = "REML",
  control = gam.control(maxit = 200)
)
print(summary(gam_additive))
cat("\nk-index check (edf/k close to 1 warns that k may be too small):\n")
print(k.check(gam_additive))

saveRDS(gam_additive, file.path(OUT_DIR, "gam_additive_r.rds"))
cat("Saved: gam_additive_r.rds\n")

m1 <- eval_metrics(gam_additive, train, test)
cat(sprintf(
  "\nAdditive GAM  | Train RMSE: %.1f kW | Test RMSE: %.1f kW | Test MAPE: %.2f%% | Test MAE: %.1f kW\n",
  m1$train_rmse, m1$test_rmse, m1$test_mape, m1$test_mae
))

# =============================================================================
# MODEL 2: Tensor Product GAM  — ti(dsc, stw) pure interaction
# =============================================================================
# ANOVA decomposition (statistically correct approach):
#   s(dsc, bs="mpi")  constrained monotone marginal for fouling main effect
#   s(stw, bs="mpi")  constrained monotone marginal for speed main effect
#   ti(dsc, stw)      pure 2D interaction (marginals projected out; unconstrained)
#
# WHY NOT te(dsc,stw) + s(dsc) + s(stw)?
#   te() already subsumes the marginal main effects. Adding separate s() terms
#   creates an unidentifiable over-parameterised model. ti() is the correct
#   construct: it gives exactly the part of f(dsc,stw) not explained by the
#   marginals, while the constrained s() terms capture the main effects.
#
# Motivation: propulsion theory predicts the fouling power penalty is a smaller
# proportion of total power at high speeds (resistance grows as STW^2-3, but
# fouling adds roughly constant drag). An additive GAM cannot represent this
# interaction — ti() can, without giving up the monotone marginals.

formula_tensor <- shaft_power_kw ~
  s(sea_water_temp,   bs = "mpd", k = 10) +
  s(wave_period,      bs = "cr",  k = 10) +
  s(long_wave_force,  bs = "mpi", k = 10) +
  s(long_wind_force,  bs = "mpi", k = 10) +
  s(long_swell_force, bs = "mpi", k = 10) +
  s(trans_swell_force,bs = "cr",  k = 10) +
  s(trans_wind_force, bs = "cr",  k = 10) +
  s(trans_wave_force, bs = "cr",  k = 10) +
  s(avg_draft,        bs = "mpi", k = 10) +
  s(long_current,     bs = "cr",  k = 10) +
  s(draft_trim,       bs = "cr",  k = 10) +
  s(dsc,              bs = "mpi", k = 10) +  # constrained marginal: fouling main effect
  s(stw,              bs = "mpi", k = 10) +  # constrained marginal: speed main effect
  ti(dsc, stw,        k  = c(10, 10))         # pure fouling x speed interaction (unconstrained)

cat("\n=== Fitting Model 2: Tensor Product GAM ===\n")
gam_tensor <- gam(
  formula_tensor, data = train, method = "REML",
  control = gam.control(maxit = 200)
)
print(summary(gam_tensor))
cat("\nk-index check:\n")
print(k.check(gam_tensor))

saveRDS(gam_tensor, file.path(OUT_DIR, "gam_tensor_r.rds"))
cat("Saved: gam_tensor_r.rds\n")

m2 <- eval_metrics(gam_tensor, train, test)
cat(sprintf(
  "\nTensor GAM    | Train RMSE: %.1f kW | Test RMSE: %.1f kW | Test MAPE: %.2f%% | Test MAE: %.1f kW\n",
  m2$train_rmse, m2$test_rmse, m2$test_mape, m2$test_mae
))

# =============================================================================
# COMPARISON TABLE
# =============================================================================
# Python baseline metrics from ANALYSIS_BRIEF.md (test set only; train not reported)
python_baseline <- list(
  train_rmse = NA_real_, train_mape = NA_real_, train_mae = NA_real_,
  test_rmse  = 286.5,    test_mape  = 7.34,     test_mae  = 213.6
)

comparison <- bind_rows(
  as_tibble(c(model = "python_gam_baseline",     python_baseline)),
  as_tibble(c(model = "r_additive_gam",          m1)),
  as_tibble(c(model = "r_tensor_gam_ti_dsc_stw", m2))
)

write_csv(comparison, file.path(OUT_DIR, "model_comparison.csv"))
cat("\nModel comparison (test metrics):\n")
print(comparison |> select(model, test_rmse, test_mape, test_mae))
cat("Saved: model_comparison.csv\n")

# =============================================================================
# PLOTS
# =============================================================================

# ---- 1. Interaction surface: ti(dsc, stw) -----------------------------------
# Build a regular grid over (dsc, stw), set all other features to their
# standardised values of 0 (= training-set median in original scale), and
# extract the ti() contribution via predict(..., type="terms").

n_grid    <- 60
dsc_grid  <- seq(min(train$dsc), max(train$dsc), length.out = n_grid)
stw_grid  <- seq(min(train$stw), max(train$stw), length.out = n_grid)
pred_grid <- expand.grid(dsc = dsc_grid, stw = stw_grid)

# Fill remaining features with 0 (standardised median)
for (col in setdiff(FEATURE_COLS, c("dsc", "stw"))) pred_grid[[col]] <- 0L

terms_mat  <- predict(gam_tensor, newdata = pred_grid, type = "terms")
ti_col_idx <- grep("ti\\(", colnames(terms_mat))

if (length(ti_col_idx) == 0) {
  stop("Could not locate ti() column in predict output. Columns: ",
       paste(colnames(terms_mat), collapse = ", "))
}

pred_grid$ti_effect <- as.numeric(terms_mat[, ti_col_idx[1]])

# Back-transform axes to original (unstandardised) scale for readable labels
pred_grid$dsc_orig <- pred_grid$dsc * train_sds["dsc"] + train_means["dsc"]
pred_grid$stw_orig <- pred_grid$stw * train_sds["stw"] + train_means["stw"]

p_interaction <- ggplot(pred_grid, aes(x = dsc_orig, y = stw_orig, fill = ti_effect)) +
  geom_tile() +
  geom_contour(aes(x = dsc_orig, y = stw_orig, z = ti_effect),
               inherit.aes = FALSE, colour = "white", alpha = 0.55, linewidth = 0.3) +
  scale_fill_distiller(palette = "RdYlBu", direction = -1,
                       name = "Interaction\neffect (kW)") +
  labs(
    title    = "Fouling × Speed interaction surface  [ti(dsc, stw)]",
    subtitle = "Pure interaction term — deviation from additive (marginal) prediction",
    x        = "Days since last cleaning",
    y        = "Speed through water (knots)"
  ) +
  theme_bw(base_size = 12)

save_plot(p_interaction, "interaction_surface.png", width = 8, height = 6)

# ---- 2, 3, 4. Partial effect curves (tensor model marginal smooths) ----------
# gratia::smooth_estimates() evaluates a single smooth over a sequence of n
# values. add_confint() appends approximate 95% CIs.

plot_marginal_smooth <- function(model, term, x_col, x_label, filename) {
  sm <- smooth_estimates(model, select = term, n = 300) |>
        add_confint()

  # Back-transform x to original scale
  sm[[x_col]] <- sm[[x_col]] * train_sds[x_col] + train_means[x_col]

  p <- ggplot(sm, aes(x = .data[[x_col]])) +
    geom_ribbon(aes(ymin = .lower_ci, ymax = .upper_ci),
                fill = "steelblue3", alpha = 0.25) +
    geom_line(aes(y = .estimate), colour = "steelblue4", linewidth = 1) +
    geom_hline(yintercept = 0, linetype = "dashed",
               colour = "grey50", linewidth = 0.5) +
    labs(
      title    = paste0("Partial effect: ", x_label),
      subtitle = "Tensor product GAM — marginal smooth term with 95% CI",
      x        = x_label,
      y        = "Partial effect on shaft power (kW)"
    ) +
    theme_bw(base_size = 12)

  save_plot(p, filename)
}

plot_marginal_smooth(gam_tensor, "s(dsc)",      "dsc",      "Days since last cleaning",
                     "dsc_partial_effect_curve.png")
plot_marginal_smooth(gam_tensor, "s(stw)",      "stw",      "Speed through water (knots)",
                     "stw_partial_effect_curve.png")
plot_marginal_smooth(gam_tensor, "s(avg_draft)","avg_draft","Average draft (m)",
                     "draft_partial_effect_curve.png")

# ---- 5. All marginal smooths — additive model (sanity check) ----------------
# Compare visually to thesis Fig 6.6. Uses gratia::draw() which returns a
# patchwork of individual smooth panels.
p_all_smooths <- draw(gam_additive, residuals = FALSE) +
  plot_annotation(
    title    = "R additive GAM — all 13 marginal smooths",
    subtitle = "Sanity check: compare shape/direction to Python baseline (thesis Fig 6.6)"
  )
save_plot(p_all_smooths, "additive_all_smooths.png", width = 15, height = 10)

# ---- 6. Residual diagnostics — tensor model ---------------------------------
# gratia::appraise() produces QQ plot, residuals vs fitted, histogram, and
# response vs fitted. point_alpha helps with overplotting at n=12k.
p_diag <- appraise(gam_tensor, point_alpha = 0.08)
save_plot(p_diag, "residual_diagnostics.png", width = 10, height = 8)

cat("\n=== All done. Output files written to:", OUT_DIR, "===\n")
