from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from usdcop.config import load_settings
from usdcop.data.repository import SeriesRepository
from usdcop.features.build import build_daily_panel, engineer_market_features
from usdcop.models.baselines import baseline_table

LOGGER = logging.getLogger(__name__)


def _latest_value(repository: SeriesRepository, source: str, name: str) -> float:
    frame = repository.load_series(source, name)
    return float(frame.sort_values("observation_date").iloc[-1]["value"])


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

    champion_file = paths.output_root / "champion_model.txt"
    if champion_file.exists():
        artifact_path = paths.output_root / champion_file.read_text(encoding="utf-8").strip()
        artifact = joblib.load(artifact_path)
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
        panel = build_daily_panel(named).ffill(limit=int(settings["model"].get("max_feature_staleness_days", 120)))
        features = engineer_market_features(panel)
        latest = features.reindex(columns=features_needed).iloc[[-1]]
        predicted = model.predict_log_returns(latest).iloc[0]
        for index, row in output.iterrows():
            horizon = int(row["horizon_days"])
            log_return = float(predicted[f"pred_log_return_{horizon}d"])
            output.loc[index, "median"] = spot * np.exp(log_return)
        output["status"] = "MODEL_TRAINED_PENDING_FORMAL_APPROVAL"
        output["model_version"] = artifact["version"]

    output["generated_at"] = datetime.now(timezone.utc).isoformat()
    output.to_csv(paths.output_root / "latest_forecasts.csv", index=False)
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
