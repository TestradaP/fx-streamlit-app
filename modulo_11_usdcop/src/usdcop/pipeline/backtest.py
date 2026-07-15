from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from usdcop.config import load_settings
from usdcop.data.repository import SeriesRepository
from usdcop.features.build import build_daily_panel, engineer_market_features
from usdcop.models.candidates import CANDIDATE_NAMES, DirectCandidateForecaster
from usdcop.models.trainer import make_direct_targets
from usdcop.pipeline.train import load_named_series, series_frequencies


BENCHMARK_NAMES = ("random_walk", "carry")
ENSEMBLE_NAME = "ensemble_equal"
WEIGHTED_ENSEMBLE_NAME = "ensemble_weighted"
ENSEMBLE_NAMES = (ENSEMBLE_NAME, WEIGHTED_ENSEMBLE_NAME)


def _rate_decimal(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce") / 100


def _price_errors(
    actual_return: pd.Series, predicted_return: pd.Series, spot: pd.Series
) -> pd.Series:
    valid = actual_return.notna() & predicted_return.notna() & spot.notna()
    actual_price = spot.loc[valid] * np.exp(actual_return.loc[valid])
    predicted_price = spot.loc[valid] * np.exp(predicted_return.loc[valid])
    return predicted_price - actual_price


def _metric_row(
    horizon: int,
    model_name: str,
    actual_return: pd.Series,
    predicted_return: pd.Series,
    spot: pd.Series,
    random_walk_mae: float,
    period: str = "overall",
) -> dict:
    valid = actual_return.notna() & predicted_return.notna() & spot.notna()
    actual = actual_return.loc[valid]
    predicted = predicted_return.loc[valid]
    errors = _price_errors(actual, predicted, spot.loc[valid])
    mae = float(errors.abs().mean())
    return {
        "period": period,
        "horizon_days": int(horizon),
        "model": model_name,
        "observations": int(valid.sum()),
        "test_start": str(actual.index.min().date()),
        "test_end": str(actual.index.max().date()),
        "mae_cop": mae,
        "rmse_cop": float(np.sqrt(np.mean(np.square(errors)))),
        "mean_error_cop": float(errors.mean()),
        "directional_accuracy": float((np.sign(predicted) == np.sign(actual)).mean()),
        "mase_vs_random_walk": float(mae / random_walk_mae) if random_walk_mae else np.nan,
        "skill_vs_random_walk_pct": (
            float((1 - mae / random_walk_mae) * 100) if random_walk_mae else np.nan
        ),
    }


def _block_bootstrap_loss_difference(
    candidate_errors: pd.Series,
    random_errors: pd.Series,
    *,
    seed: int,
    samples: int = 1000,
    block_size: int = 20,
) -> tuple[float, float]:
    aligned = pd.concat(
        [candidate_errors.abs().rename("candidate"), random_errors.abs().rename("random")],
        axis=1,
    ).dropna()
    differences = (aligned["candidate"] - aligned["random"]).to_numpy()
    if len(differences) < block_size * 2:
        return np.nan, np.nan
    rng = np.random.default_rng(seed)
    starts = np.arange(0, len(differences) - block_size + 1)
    means = np.empty(samples)
    blocks_needed = int(np.ceil(len(differences) / block_size))
    for index in range(samples):
        chosen = rng.choice(starts, size=blocks_needed, replace=True)
        sample = np.concatenate(
            [differences[start : start + block_size] for start in chosen]
        )[: len(differences)]
        means[index] = sample.mean()
    return tuple(np.quantile(means, [0.025, 0.975]))


def _conformal_radius(residuals: pd.Series, coverage: float = 0.80) -> float:
    values = np.abs(pd.to_numeric(residuals, errors="coerce").dropna().to_numpy())
    if len(values) < 30:
        raise ValueError("At least 30 calibration residuals are required")
    quantile = min(1.0, np.ceil((len(values) + 1) * coverage) / len(values))
    return float(np.quantile(values, quantile, method="higher"))


def _historical_ensemble_weights(
    predictions: pd.DataFrame | None,
    horizon: int,
) -> dict[str, float]:
    """Inverse-MAE weights using only already completed OOS blocks."""
    if predictions is None or predictions.empty:
        return {name: 1 / len(CANDIDATE_NAMES) for name in CANDIDATE_NAMES}
    actual = predictions[f"actual_{horizon}d"]
    spot = predictions["spot"]
    inverse_errors: dict[str, float] = {}
    for name in CANDIDATE_NAMES:
        errors = _price_errors(actual, predictions[f"{name}_{horizon}d"], spot).abs()
        mae = float(errors.mean())
        inverse_errors[name] = 1 / max(mae, 1e-6)
    total = sum(inverse_errors.values())
    return {name: value / total for name, value in inverse_errors.items()}


def _pinball_loss(actual: pd.Series, predicted: pd.Series, quantile: float) -> float:
    valid = actual.notna() & predicted.notna()
    error = actual.loc[valid] - predicted.loc[valid]
    return float(np.maximum(quantile * error, (quantile - 1) * error).mean())


def run_backtest(project_root: str | Path | None = None) -> pd.DataFrame:
    """Evaluate challengers with purged expanding windows and publish a registry."""
    paths, settings, catalog = load_settings(project_root)
    repository = SeriesRepository(paths.storage_root)
    named = load_named_series(repository, catalog)
    panel = build_daily_panel(named).ffill(
        limit=int(settings["model"].get("max_feature_staleness_days", 120))
    )
    features = engineer_market_features(panel, series_frequencies(catalog))
    horizons = list(settings["horizons_calendar_days"])
    targets = make_direct_targets(panel["trm"], horizons)
    feature_columns = [
        column
        for column in features.columns
        if column != "trm_level" and features[column].notna().sum() >= 200
    ]
    target_columns = [f"target_log_return_{horizon}d" for horizon in horizons]
    last_complete_date = panel.index.max() - pd.Timedelta(days=max(horizons))
    eligible_dates = targets.loc[:last_complete_date, target_columns].dropna().index
    test_rows = int(settings["validation"].get("backtest_rows", 500))
    if len(eligible_dates) < test_rows:
        raise RuntimeError(
            f"Only {len(eligible_dates)} fully observed dates are available; {test_rows} required"
        )
    test_dates = eligible_dates[-test_rows:]
    step = int(settings["validation"].get("refit_block_rows", 100))
    prediction_blocks: list[pd.DataFrame] = []
    ensemble_weight_history: list[dict] = []

    for block_start in range(0, len(test_dates), step):
        block_dates = test_dates[block_start : block_start + step]
        first_test_date = block_dates[0]
        purge_cutoff = first_test_date - pd.Timedelta(days=max(horizons))
        train_dates = features.index[features.index <= purge_cutoff]
        if len(train_dates) < int(settings.get("minimum_training_rows", 750)):
            raise RuntimeError("Insufficient purged training history for walk-forward evaluation")
        model = DirectCandidateForecaster(
            tuple(horizons), random_state=int(settings["model"].get("random_seed", 20260715))
        ).fit(
            features.loc[train_dates, feature_columns],
            targets.loc[train_dates, target_columns],
        )
        predicted = model.predict_all(features.loc[block_dates, feature_columns])
        predicted_quantiles = model.predict_quantiles(
            features.loc[block_dates, feature_columns]
        )
        block = pd.DataFrame(index=block_dates)
        block["spot"] = panel.loc[block_dates, "trm"]
        block["regime"] = pd.cut(
            features.loc[block_dates, "vix_level"],
            bins=[-np.inf, 15, 25, np.inf],
            labels=["calm", "normal", "stress"],
        ).astype("string")
        ibr = _rate_decimal(panel.loc[block_dates, "ibr_on"])
        sofr = _rate_decimal(panel.loc[block_dates, "sofr"])
        for horizon in horizons:
            tau = horizon / int(settings.get("day_count_basis", 360))
            block[f"actual_{horizon}d"] = targets.loc[
                block_dates, f"target_log_return_{horizon}d"
            ]
            for name in CANDIDATE_NAMES:
                block[f"{name}_{horizon}d"] = predicted[
                    f"{name}_pred_log_return_{horizon}d"
                ]
            block[f"{ENSEMBLE_NAME}_{horizon}d"] = block[
                [f"{name}_{horizon}d" for name in CANDIDATE_NAMES]
            ].mean(axis=1)
            previous = pd.concat(prediction_blocks) if prediction_blocks else None
            weights = _historical_ensemble_weights(previous, horizon)
            block[f"{WEIGHTED_ENSEMBLE_NAME}_{horizon}d"] = sum(
                block[f"{name}_{horizon}d"] * weight
                for name, weight in weights.items()
            )
            block[f"quantile_p10_{horizon}d"] = predicted_quantiles[
                f"pred_log_return_p10_{horizon}d"
            ]
            block[f"quantile_p90_{horizon}d"] = predicted_quantiles[
                f"pred_log_return_p90_{horizon}d"
            ]
            ensemble_weight_history.append(
                {
                    "block_start": str(block_dates[0].date()),
                    "horizon_days": horizon,
                    "weights": weights,
                }
            )
            block[f"random_walk_{horizon}d"] = 0.0
            block[f"carry_{horizon}d"] = np.log((1 + ibr * tau) / (1 + sofr * tau))
        prediction_blocks.append(block)

    predictions = pd.concat(prediction_blocks).sort_index()
    predictions.index.name = "as_of_date"
    predictions.to_csv(paths.output_root / "backtest_predictions.csv")

    model_names = (*CANDIDATE_NAMES, *ENSEMBLE_NAMES, *BENCHMARK_NAMES)
    overall_metrics: list[dict] = []
    window_metrics: list[dict] = []
    regime_metrics: list[dict] = []
    registry: dict[str, dict] = {}
    minimum_skill = float(settings["validation"].get("minimum_skill_pct", 2.0))
    minimum_positive_windows = float(
        settings["validation"].get("minimum_positive_window_share", 0.60)
    )
    random_seed = int(settings["model"].get("random_seed", 20260715))
    stability_window_rows = int(
        settings["validation"].get("stability_window_rows", 125)
    )
    confirmation_fraction = float(
        settings["validation"].get("confirmation_fraction", 0.20)
    )

    for horizon in horizons:
        actual = predictions[f"actual_{horizon}d"]
        spot = predictions["spot"]
        random_errors = _price_errors(actual, predictions[f"random_walk_{horizon}d"], spot)
        random_walk_mae = float(random_errors.abs().mean())
        horizon_metrics: dict[str, dict] = {}
        for model_index, model_name in enumerate(model_names):
            predicted_return = predictions[f"{model_name}_{horizon}d"]
            row = _metric_row(
                horizon,
                model_name,
                actual,
                predicted_return,
                spot,
                random_walk_mae,
            )
            candidate_errors = _price_errors(actual, predicted_return, spot)
            ci_low, ci_high = _block_bootstrap_loss_difference(
                candidate_errors,
                random_errors,
                seed=random_seed + horizon * 100 + model_index,
                block_size=max(20, int(np.ceil(horizon * 5 / 7))),
            )
            row["loss_difference_ci_low_cop"] = ci_low
            row["loss_difference_ci_high_cop"] = ci_high
            overall_metrics.append(row)
            horizon_metrics[model_name] = row

            if model_name == "quantile_boosting":
                low = predictions[f"quantile_p10_{horizon}d"]
                high = predictions[f"quantile_p90_{horizon}d"]
                valid_interval = actual.notna() & low.notna() & high.notna()
                row["quantile_interval_coverage"] = float(
                    ((actual.loc[valid_interval] >= low.loc[valid_interval])
                    & (actual.loc[valid_interval] <= high.loc[valid_interval])).mean()
                )
                row["quantile_interval_mean_width"] = float(
                    (high.loc[valid_interval] - low.loc[valid_interval]).mean()
                )
                row["pinball_loss_p10"] = _pinball_loss(actual, low, 0.10)
                row["pinball_loss_p90"] = _pinball_loss(actual, high, 0.90)

            for window_number, window_start in enumerate(
                range(0, len(predictions), stability_window_rows), start=1
            ):
                window_dates = predictions.index[
                    window_start : window_start + stability_window_rows
                ]
                if len(window_dates) < 30:
                    continue
                window_random_mae = float(
                    _price_errors(
                        actual.loc[window_dates],
                        predictions.loc[window_dates, f"random_walk_{horizon}d"],
                        spot.loc[window_dates],
                    ).abs().mean()
                )
                window_metrics.append(
                    _metric_row(
                        horizon,
                        model_name,
                        actual.loc[window_dates],
                        predicted_return.loc[window_dates],
                        spot.loc[window_dates],
                        window_random_mae,
                        period=f"window_{window_number:03d}",
                    )
                )

            for regime, dates in predictions.groupby("regime").groups.items():
                regime_dates = pd.DatetimeIndex(dates)
                if len(regime_dates) < 30:
                    continue
                regime_random_mae = float(
                    _price_errors(
                        actual.loc[regime_dates],
                        predictions.loc[regime_dates, f"random_walk_{horizon}d"],
                        spot.loc[regime_dates],
                    ).abs().mean()
                )
                regime_metrics.append(
                    _metric_row(
                        horizon,
                        model_name,
                        actual.loc[regime_dates],
                        predicted_return.loc[regime_dates],
                        spot.loc[regime_dates],
                        regime_random_mae,
                        period=str(regime),
                    )
                )

        window_frame = pd.DataFrame(window_metrics)
        eligible: list[tuple[str, float]] = []
        confirmation_rows = max(60, int(len(predictions) * confirmation_fraction))
        selection_dates = predictions.index[:-confirmation_rows]
        confirmation_dates = predictions.index[-confirmation_rows:]
        for model_index, model_name in enumerate((*CANDIDATE_NAMES, *ENSEMBLE_NAMES)):
            row = horizon_metrics[model_name]
            windows = window_frame.loc[
                window_frame["horizon_days"].eq(horizon)
                & window_frame["model"].eq(model_name)
            ]
            positive_window_share = float(windows["skill_vs_random_walk_pct"].gt(0).mean())
            selection_random_errors = _price_errors(
                actual.loc[selection_dates],
                predictions.loc[selection_dates, f"random_walk_{horizon}d"],
                spot.loc[selection_dates],
            )
            selection_random_mae = float(selection_random_errors.abs().mean())
            selection_row = _metric_row(
                horizon,
                model_name,
                actual.loc[selection_dates],
                predictions.loc[selection_dates, f"{model_name}_{horizon}d"],
                spot.loc[selection_dates],
                selection_random_mae,
                period="selection",
            )
            selection_errors = _price_errors(
                actual.loc[selection_dates],
                predictions.loc[selection_dates, f"{model_name}_{horizon}d"],
                spot.loc[selection_dates],
            )
            _, selection_ci_high = _block_bootstrap_loss_difference(
                selection_errors,
                selection_random_errors,
                seed=random_seed + horizon * 1000 + model_index,
                block_size=max(20, int(np.ceil(horizon * 5 / 7))),
            )
            confirmation_random_mae = float(
                _price_errors(
                    actual.loc[confirmation_dates],
                    predictions.loc[confirmation_dates, f"random_walk_{horizon}d"],
                    spot.loc[confirmation_dates],
                ).abs().mean()
            )
            confirmation_row = _metric_row(
                horizon,
                model_name,
                actual.loc[confirmation_dates],
                predictions.loc[confirmation_dates, f"{model_name}_{horizon}d"],
                spot.loc[confirmation_dates],
                confirmation_random_mae,
                period="sealed_confirmation",
            )
            statistically_better = bool(selection_ci_high < 0)
            probabilistic_calibration_passed = bool(
                model_name != "quantile_boosting"
                or 0.72 <= float(row.get("quantile_interval_coverage", np.nan)) <= 0.90
            )
            qualifies = bool(
                selection_row["skill_vs_random_walk_pct"] >= minimum_skill
                and positive_window_share >= minimum_positive_windows
                and selection_row["directional_accuracy"] >= 0.45
                and statistically_better
                and confirmation_row["skill_vs_random_walk_pct"] > 0
                and confirmation_row["directional_accuracy"] >= 0.45
                and probabilistic_calibration_passed
            )
            row["positive_window_share"] = positive_window_share
            row["statistically_better"] = statistically_better
            row["selection_skill_pct"] = selection_row["skill_vs_random_walk_pct"]
            row["confirmation_skill_pct"] = confirmation_row["skill_vs_random_walk_pct"]
            row["confirmation_directional_accuracy"] = confirmation_row[
                "directional_accuracy"
            ]
            row["probabilistic_calibration_passed"] = probabilistic_calibration_passed
            row["qualifies"] = qualifies
            if qualifies:
                eligible.append((model_name, selection_row["skill_vs_random_walk_pct"]))

        selected_model = (
            max(eligible, key=lambda item: item[1])[0]
            if eligible
            else "random_walk"
        )
        selected_prediction = predictions[f"{selected_model}_{horizon}d"]
        residuals = (actual - selected_prediction).dropna()
        calibration_end = max(30, int(len(residuals) * 0.70))
        calibration_residuals = residuals.iloc[:calibration_end]
        coverage_residuals = residuals.iloc[calibration_end:]
        radius = _conformal_radius(calibration_residuals, 0.80)
        empirical_coverage = float(
            coverage_residuals.abs().le(radius).mean()
        ) if not coverage_residuals.empty else np.nan
        registry[str(horizon)] = {
            "selected_model": selected_model,
            "fallback_used": selected_model == "random_walk",
            "metrics": horizon_metrics[selected_model],
            "ensemble_weights": _historical_ensemble_weights(predictions, horizon),
            "sealed_confirmation_rows": int(confirmation_rows),
            "calibration": {
                "observations": int(len(calibration_residuals)),
                "coverage_test_observations": int(len(coverage_residuals)),
                "coverage": 0.80,
                "empirical_coverage": empirical_coverage,
                "conformal_radius_log_return": radius,
                "residual_q10": float(calibration_residuals.quantile(0.10)),
                "residual_q90": float(calibration_residuals.quantile(0.90)),
                "residuals": [float(value) for value in calibration_residuals],
            },
        }

    metrics_frame = pd.DataFrame(overall_metrics)
    metrics_frame.to_csv(paths.output_root / "backtest_metrics.csv", index=False)
    pd.DataFrame(window_metrics).to_csv(
        paths.output_root / "backtest_window_metrics.csv", index=False
    )
    pd.DataFrame(regime_metrics).to_csv(
        paths.output_root / "backtest_regime_metrics.csv", index=False
    )
    registry_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "purged expanding-window challenger selection",
        "horizons": registry,
        "ensemble_weight_history": ensemble_weight_history,
    }
    (paths.output_root / "champion_registry.json").write_text(
        json.dumps(registry_payload, indent=2), encoding="utf-8"
    )

    candidate_selected_all = all(
        not item["fallback_used"] for item in registry.values()
    )
    governance = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "design": f"purged expanding-window; {step}-observation refit blocks",
        "test_rows": test_rows,
        "purge_calendar_days": max(horizons),
        "stability_window_rows": stability_window_rows,
        "sealed_confirmation_fraction": confirmation_fraction,
        "point_in_time_capture_active": True,
        "historical_vintage_complete": False,
        "point_forecast_validation_passed": candidate_selected_all,
        "operational_forecast_valid": True,
        "academic_ready": False,
        "selected_models": {
            horizon: item["selected_model"] for horizon, item in registry.items()
        },
        "academic_blockers": [
            "La captura point-in-time ya está activa, pero su historia comienza "
            "con el primer snapshot guardado y aún no cubre toda la muestra retrospectiva.",
            "Se requiere validación externa en una muestra completamente sellada.",
            "No se dispone aún de NDF, opciones, CDS ni flujos de mercado.",
        ],
    }
    (paths.output_root / "model_validation.json").write_text(
        json.dumps(governance, indent=2), encoding="utf-8"
    )
    return metrics_frame
