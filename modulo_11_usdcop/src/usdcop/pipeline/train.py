from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn

from usdcop.config import load_settings
from usdcop.data.repository import SeriesRepository
from usdcop.features.build import (
    apply_availability_lag,
    build_daily_panel,
    engineer_market_features,
)
from usdcop.models.candidates import CANDIDATE_NAMES, DirectCandidateForecaster
from usdcop.models.trainer import make_direct_targets

LOGGER = logging.getLogger(__name__)


def load_named_series(repository: SeriesRepository, series_catalog: dict) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for source in ("banrep", "fred"):
        for item in series_catalog.get(source, []):
            if not item.get("enabled"):
                continue
            try:
                frame = repository.load_series(source, item["name"])
                frames[item["name"]] = apply_availability_lag(
                    frame, int(item.get("availability_lag_days", 0))
                )
            except FileNotFoundError:
                LOGGER.warning("Missing stored series %s:%s", source, item["name"])
    return frames


def series_frequencies(series_catalog: dict) -> dict[str, str]:
    return {
        item["name"]: str(item.get("frequency", "daily"))
        for source in ("banrep", "fred")
        for item in series_catalog.get(source, [])
        if item.get("enabled")
    }


def train_models(project_root: str | Path | None = None) -> dict:
    paths, settings, catalog = load_settings(project_root)
    repository = SeriesRepository(paths.storage_root)
    named = load_named_series(repository, catalog)
    if "trm" not in named:
        raise RuntimeError("TRM is required before training")
    panel = build_daily_panel(named)
    # Levels are forward-filled only after their official availability-adjusted timestamp.
    panel = panel.ffill(limit=int(settings["model"].get("max_feature_staleness_days", 120)))
    features = engineer_market_features(panel, series_frequencies(catalog))
    targets = make_direct_targets(panel["trm"], list(settings["horizons_calendar_days"]))

    excluded = {"trm_level"}
    feature_columns = [
        column for column in features.columns
        if column not in excluded and features[column].notna().sum() >= 200
    ]
    dataset = features[feature_columns].join(targets)
    minimum_rows = int(settings.get("minimum_training_rows", 750))
    eligible = dataset.dropna(how="all", subset=[f"target_log_return_{h}d" for h in settings["horizons_calendar_days"]])
    if len(eligible) < minimum_rows:
        raise RuntimeError(f"Only {len(eligible)} eligible rows; minimum is {minimum_rows}")

    model = DirectCandidateForecaster(
        tuple(settings["horizons_calendar_days"]),
        random_state=int(settings["model"].get("random_seed", 20260715)),
    )
    model.fit(dataset[feature_columns], targets)
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    feature_summary = {
        column: {
            "mean": float(pd.to_numeric(dataset[column], errors="coerce").mean()),
            "median": float(pd.to_numeric(dataset[column], errors="coerce").median()),
            "std": float(pd.to_numeric(dataset[column], errors="coerce").std()),
            "q01": float(pd.to_numeric(dataset[column], errors="coerce").quantile(0.01)),
            "q99": float(pd.to_numeric(dataset[column], errors="coerce").quantile(0.99)),
            "missing_rate": float(dataset[column].isna().mean()),
        }
        for column in feature_columns
    }
    model_path = paths.output_root / "candidate_models_latest.joblib"
    joblib.dump(
        {
            "model": model,
            "feature_columns": feature_columns,
            "version": version,
            "sklearn_version": sklearn.__version__,
            "candidate_names": list(CANDIDATE_NAMES),
            "feature_summary": feature_summary,
        },
        model_path,
    )
    (paths.output_root / "champion_model.txt").write_text(model_path.name, encoding="utf-8")
    metadata = {
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "rows": len(dataset),
        "features": feature_columns,
        "horizons": settings["horizons_calendar_days"],
        "candidate_models": list(CANDIDATE_NAMES),
        "sklearn_version": sklearn.__version__,
        "validation_cv": "TimeSeriesSplit(n_splits=5)",
        "availability_lags_applied": True,
        "status": "TRAINED_NOT_YET_GOVERNANCE_APPROVED",
    }
    (paths.output_root / "model_metadata_latest.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return metadata
