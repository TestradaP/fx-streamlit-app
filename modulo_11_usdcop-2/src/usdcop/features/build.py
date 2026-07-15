from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd


def build_daily_panel(
    named_series: Mapping[str, pd.DataFrame],
    *,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Create a business-day panel from already availability-adjusted series."""
    frames: list[pd.DataFrame] = []
    for name, frame in named_series.items():
        item = frame[["observation_date", "value"]].copy()
        item["observation_date"] = pd.to_datetime(item["observation_date"])
        item = item.rename(columns={"value": name}).drop_duplicates("observation_date", keep="last")
        frames.append(item.set_index("observation_date"))
    if not frames:
        raise ValueError("No series supplied")
    panel = pd.concat(frames, axis=1).sort_index()
    lower = pd.Timestamp(start) if start is not None else panel.index.min()
    upper = pd.Timestamp(end) if end is not None else panel.index.max()
    index = pd.date_range(lower, upper, freq="B")
    return panel.reindex(index).rename_axis("date")


def engineer_market_features(panel: pd.DataFrame) -> pd.DataFrame:
    features = panel.copy()
    for column in list(panel.columns):
        numeric = pd.to_numeric(panel[column], errors="coerce")
        features[f"{column}_level"] = numeric
        features[f"{column}_chg_1"] = numeric.diff(1)
        features[f"{column}_pct_1"] = numeric.pct_change(1)
        features[f"{column}_pct_5"] = numeric.pct_change(5)
        features[f"{column}_z_60"] = (
            (numeric - numeric.rolling(60, min_periods=20).mean())
            / numeric.rolling(60, min_periods=20).std()
        )
    if "trm" in panel:
        log_return = np.log(pd.to_numeric(panel["trm"], errors="coerce")).diff()
        features["trm_realized_vol_20"] = log_return.rolling(20, min_periods=10).std() * np.sqrt(252)
        features["trm_momentum_5"] = np.log(panel["trm"]).diff(5)
        features["trm_momentum_20"] = np.log(panel["trm"]).diff(20)
    if {"ibr_on", "sofr"}.issubset(panel.columns):
        features["carry_spread_pp"] = panel["ibr_on"] - panel["sofr"]
        if "trm_realized_vol_20" in features:
            features["carry_to_risk"] = features["carry_spread_pp"] / features["trm_realized_vol_20"].replace(0, np.nan)
    return features.replace([np.inf, -np.inf], np.nan)
