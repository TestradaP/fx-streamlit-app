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
    if any(token in name for token in ("vix", "broad_usd", "brent")):
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


def run_forecast(project_root: str | Path | None = None) -> pd.DataFrame:
    paths, settings, catalog = load_settings(project_root)
    repository = SeriesRepository(paths.storage_root)
    spot = _latest_value(repository, "banrep", "trm")
    ibr = _latest_value(repository, "banrep", "ibr_on")
    sofr = _latest_value(repository, "fred", "sofr")
    # Official series may be stored as percentages. Normalize if needed.
    ibr_decimal = ibr / 100 if abs(ibr) > 1 else ibr
    sofr_decimal = sofr / 100 if abs(sofr) > 1 else sofr
    as_of = date.today()
    output = baseline_table(as_of, spot, ibr_decimal, sofr_decimal, list(settings["horizons_calendar_days"]))
    output["median"] = np.nan
    output["p10"] = np.nan
    output["p90"] = np.nan
    output["status"] = "BENCHMARK_ONLY_NOT_TRAINED"
    output["model_version"] = None
    output["model_error"] = None
    drivers = pd.DataFrame(columns=DRIVER_COLUMNS)

    champion_file = paths.output_root / "champion_model.txt"
    if champion_file.exists():
        try:
            artifact_path = paths.output_root / champion_file.read_text(encoding="utf-8").strip()
            artifact = joblib.load(artifact_path)
            _validate_artifact_runtime(artifact)
            model = artifact["model"]
            features_needed = artifact["feature_columns"]
            named: dict[str, pd.DataFrame] = {}
            for source in ("banrep", "fred"):
                for item in catalog.get(source, []):
                    if item.get("enabled"):
                        try:
                            named[item["name"]] = repository.load_series(source, item["name"])
                        except FileNotFoundError:
                            pass
            panel = build_daily_panel(named).ffill(
                limit=int(settings["model"].get("max_feature_staleness_days", 120))
            )
            features = engineer_market_features(panel)
            latest = features.reindex(columns=features_needed).iloc[[-1]]
            predicted = model.predict_log_returns(latest).iloc[0]
            drivers = _elastic_net_driver_table(model, latest, spot)
            for index, row in output.iterrows():
                horizon = int(row["horizon_days"])
                log_return = float(predicted[f"pred_log_return_{horizon}d"])
                output.loc[index, "median"] = spot * np.exp(log_return)
            output["status"] = "MODEL_ACTIVE_AUTOMATED_DAILY"
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
