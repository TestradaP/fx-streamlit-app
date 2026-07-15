from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn

from usdcop.config import load_settings
from usdcop.data.repository import SeriesRepository
from usdcop.features.build import build_daily_panel, engineer_market_features
from usdcop.models.baselines import baseline_table
from usdcop.pipeline.train import load_named_series, series_frequencies

LOGGER = logging.getLogger(__name__)

DRIVER_COLUMNS = [
    "horizon_days",
    "feature",
    "driver_group",
    "feature_value",
    "standardized_value",
    "coefficient",
    "contribution_log_return",
    "contribution_cop_approx",
    "direction",
]


def _validate_artifact_runtime(artifact: dict) -> None:
    trained_version = artifact.get("sklearn_version")
    if trained_version and trained_version != sklearn.__version__:
        raise RuntimeError(
            f"Model requires scikit-learn {trained_version}; runtime has {sklearn.__version__}"
        )


def _latest_value(repository: SeriesRepository, source: str, name: str) -> float:
    frame = repository.load_series(source, name)
    return float(frame.sort_values("observation_date").iloc[-1]["value"])


def _driver_group(feature: str) -> str:
    name = feature.lower()
    if name == "intercept":
        return "base_model"
    if name.startswith("trm_") or name in {"trm", "spot"}:
        return "technical_fx"
    if any(token in name for token in ("vix", "broad_usd", "brent", "financial_conditions")):
        return "global_risk"
    if any(token in name for token in ("current_account", "reserves", "trade_balance")):
        return "external_flows"
    if any(
        token in name
        for token in ("ibr", "sofr", "treasury", "tes_", "policy_rate", "carry")
    ):
        return "rates_and_carry"
    if "inflation" in name:
        return "domestic_macro"
    return "other"


def _elastic_net_driver_table(model, latest: pd.DataFrame, spot: float) -> pd.DataFrame:
    """Return the exact linear contribution of every input for each horizon."""
    rows: list[dict] = []
    model_input = latest.reindex(columns=model.feature_names)

    for horizon, pipeline in sorted(model.models.items()):
        imputed = pipeline.named_steps["imputer"].transform(model_input)
        standardized = pipeline.named_steps["scale"].transform(imputed)
        estimator = pipeline.named_steps["model"]
        coefficients = np.asarray(estimator.coef_, dtype=float).reshape(-1)
        values = np.asarray(standardized, dtype=float)[0]

        for index, feature in enumerate(model.feature_names):
            contribution = float(values[index] * coefficients[index])
            raw_value = model_input.iloc[0, index]
            rows.append(
                {
                    "horizon_days": int(horizon),
                    "feature": feature,
                    "driver_group": _driver_group(feature),
                    "feature_value": float(raw_value) if pd.notna(raw_value) else np.nan,
                    "standardized_value": float(values[index]),
                    "coefficient": float(coefficients[index]),
                    "contribution_log_return": contribution,
                    "contribution_cop_approx": float(spot * contribution),
                    "direction": (
                        "up" if contribution > 0 else "down" if contribution < 0 else "neutral"
                    ),
                }
            )

        intercept = float(np.asarray(estimator.intercept_).reshape(-1)[0])
        rows.append(
            {
                "horizon_days": int(horizon),
                "feature": "intercept",
                "driver_group": _driver_group("intercept"),
                "feature_value": np.nan,
                "standardized_value": 1.0,
                "coefficient": intercept,
                "contribution_log_return": intercept,
                "contribution_cop_approx": float(spot * intercept),
                "direction": (
                    "up" if intercept > 0 else "down" if intercept < 0 else "neutral"
                ),
            }
        )

    return pd.DataFrame(rows, columns=DRIVER_COLUMNS)


def _candidate_prediction(model, values: pd.DataFrame, name: str, horizon: int) -> float:
    if name == "ensemble_equal":
        return float(
            np.mean(
                [
                    pipelines[horizon].predict(values[model.feature_names])[0]
                    for pipelines in model.models.values()
                ]
            )
        )
    return float(model.models[name][horizon].predict(values[model.feature_names])[0])


def _candidate_driver_table(
    model,
    latest: pd.DataFrame,
    spot: float,
    registry: dict,
    feature_summary: dict,
) -> pd.DataFrame:
    """Calculate model-agnostic local effects by replacing one input with its median."""
    rows: list[dict] = []
    for horizon_text, selection in registry.get("horizons", {}).items():
        horizon = int(horizon_text)
        model_name = selection.get("selected_model", "random_walk")
        if model_name in {"random_walk", "carry"}:
            continue
        prediction = _candidate_prediction(model, latest, model_name, horizon)
        for feature in model.feature_names:
            summary = feature_summary.get(feature, {})
            median_proxy = summary.get("median", summary.get("mean"))
            if median_proxy is None or not np.isfinite(median_proxy):
                continue
            counterfactual = latest.copy()
            counterfactual.loc[:, feature] = float(median_proxy)
            counterfactual_prediction = _candidate_prediction(
                model, counterfactual, model_name, horizon
            )
            contribution = prediction - counterfactual_prediction
            raw_value = latest.iloc[0][feature]
            standard_deviation = float(summary.get("std") or np.nan)
            standardized = (
                (float(raw_value) - float(summary.get("mean", 0))) / standard_deviation
                if pd.notna(raw_value) and np.isfinite(standard_deviation) and standard_deviation > 0
                else np.nan
            )
            rows.append(
                {
                    "horizon_days": horizon,
                    "feature": feature,
                    "driver_group": _driver_group(feature),
                    "feature_value": float(raw_value) if pd.notna(raw_value) else np.nan,
                    "standardized_value": standardized,
                    "coefficient": np.nan,
                    "contribution_log_return": contribution,
                    "contribution_cop_approx": float(spot * contribution),
                    "direction": (
                        "up" if contribution > 0 else "down" if contribution < 0 else "neutral"
                    ),
                }
            )
    return pd.DataFrame(rows, columns=DRIVER_COLUMNS)


def _feature_drift(latest: pd.DataFrame, feature_summary: dict) -> dict:
    evaluated = 0
    outside = []
    missing = []
    for feature in latest.columns:
        value = latest.iloc[0][feature]
        summary = feature_summary.get(feature, {})
        if pd.isna(value):
            missing.append(feature)
            continue
        if not summary:
            continue
        evaluated += 1
        if value < summary.get("q01", -np.inf) or value > summary.get("q99", np.inf):
            outside.append(feature)
    drift_ratio = len(outside) / evaluated if evaluated else 1.0
    return {
        "evaluated_features": evaluated,
        "outside_training_range": outside,
        "missing_features": missing,
        "outside_ratio": drift_ratio,
        "severe": drift_ratio > 0.20 or len(missing) > max(5, len(latest.columns) * 0.20),
    }


def run_forecast(project_root: str | Path | None = None) -> pd.DataFrame:
    paths, settings, catalog = load_settings(project_root)
    repository = SeriesRepository(paths.storage_root)
    spot = _latest_value(repository, "banrep", "trm")
    ibr = _latest_value(repository, "banrep", "ibr_on")
    sofr = _latest_value(repository, "fred", "sofr")
    # Both official series are configured and stored in percentage points.
    ibr_decimal = ibr / 100
    sofr_decimal = sofr / 100
    as_of = date.today()
    output = baseline_table(as_of, spot, ibr_decimal, sofr_decimal, list(settings["horizons_calendar_days"]))
    output["median"] = np.nan
    output["p10"] = np.nan
    output["p90"] = np.nan
    output["status"] = "BENCHMARK_ONLY_NOT_TRAINED"
    output["model_version"] = None
    output["model_error"] = None
    output["selected_model"] = None
    output["probability_up"] = np.nan
    drivers = pd.DataFrame(columns=DRIVER_COLUMNS)

    champion_file = paths.output_root / "champion_model.txt"
    if champion_file.exists():
        try:
            artifact_path = paths.output_root / champion_file.read_text(encoding="utf-8").strip()
            artifact = joblib.load(artifact_path)
            _validate_artifact_runtime(artifact)
            model = artifact["model"]
            features_needed = artifact["feature_columns"]
            named = load_named_series(repository, catalog)
            panel = build_daily_panel(named).ffill(
                limit=int(settings["model"].get("max_feature_staleness_days", 120))
            )
            features = engineer_market_features(panel, series_frequencies(catalog))
            latest = features.reindex(columns=features_needed).iloc[[-1]]
            registry_path = paths.output_root / "champion_registry.json"
            registry = (
                json.loads(registry_path.read_text(encoding="utf-8"))
                if registry_path.exists()
                else {}
            )
            feature_summary = artifact.get("feature_summary", {})
            monitor = _feature_drift(latest, feature_summary) if feature_summary else {
                "evaluated_features": 0,
                "outside_training_range": [],
                "missing_features": [],
                "outside_ratio": 0.0,
                "severe": False,
            }
            monitor["generated_at"] = datetime.now(timezone.utc).isoformat()
            (paths.output_root / "model_monitor.json").write_text(
                json.dumps(monitor, indent=2), encoding="utf-8"
            )
            candidate_predictions = (
                model.predict_all(latest).iloc[0] if hasattr(model, "predict_all") else None
            )
            legacy_predictions = (
                model.predict_log_returns(latest).iloc[0]
                if candidate_predictions is None
                else None
            )
            drivers = (
                _candidate_driver_table(model, latest, spot, registry, feature_summary)
                if candidate_predictions is not None
                else _elastic_net_driver_table(model, latest, spot)
            )
            selected_models: list[str] = []
            for index, row in output.iterrows():
                horizon = int(row["horizon_days"])
                selection = registry.get("horizons", {}).get(str(horizon), {})
                selected_model = selection.get("selected_model", "elastic_net")
                if monitor["severe"]:
                    selected_model = "random_walk"
                if candidate_predictions is None:
                    log_return = float(legacy_predictions[f"pred_log_return_{horizon}d"])
                    selected_model = "legacy_elastic_net"
                elif selected_model == "random_walk":
                    log_return = 0.0
                elif selected_model == "carry":
                    log_return = float(np.log(output.loc[index, "forward_anchor"] / spot))
                elif selected_model == "ensemble_equal":
                    log_return = float(
                        np.mean(
                            [
                                candidate_predictions[f"{name}_pred_log_return_{horizon}d"]
                                for name in model.models
                            ]
                        )
                    )
                else:
                    log_return = float(
                        candidate_predictions[
                            f"{selected_model}_pred_log_return_{horizon}d"
                        ]
                    )
                output.loc[index, "median"] = spot * np.exp(log_return)
                output.loc[index, "selected_model"] = selected_model
                selected_models.append(selected_model)
                calibration = selection.get("calibration", {})
                radius = calibration.get("conformal_radius_log_return")
                if radius is not None:
                    output.loc[index, "p10"] = spot * np.exp(log_return - float(radius))
                    output.loc[index, "p90"] = spot * np.exp(log_return + float(radius))
                residuals = np.asarray(calibration.get("residuals", []), dtype=float)
                if residuals.size:
                    output.loc[index, "probability_up"] = float(
                        np.mean(log_return + residuals > 0)
                    )
            if monitor["severe"]:
                output["status"] = "SUSPENDED_DATA_OR_DRIFT"
            elif candidate_predictions is None:
                output["status"] = "MODEL_RESEARCH_VALIDATION_FAILED"
            elif all(name != "random_walk" for name in selected_models):
                output["status"] = "MODEL_ACTIVE_BENCHMARK_GATED"
            else:
                output["status"] = "VALIDATED_BENCHMARK_FALLBACK"
            output["model_version"] = artifact["version"]
        except Exception as exc:  # noqa: BLE001 - retain an explicit benchmark fallback
            LOGGER.exception("Champion model unavailable; emitting benchmark-only forecast")
            output["status"] = "BENCHMARK_ONLY_MODEL_ERROR"
            output["model_error"] = f"{type(exc).__name__}: {exc}"

    output["generated_at"] = datetime.now(timezone.utc).isoformat()
    output.to_csv(paths.output_root / "latest_forecasts.csv", index=False)
    drivers.to_csv(paths.output_root / "forecast_drivers.csv", index=False)
    (paths.output_root / "forecast_status.json").write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": str(output["status"].iloc[0]),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return output
