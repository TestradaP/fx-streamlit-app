from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd


def apply_availability_lag(frame: pd.DataFrame, lag_days: int = 0) -> pd.DataFrame:
    """Move observations to their first knowable business date.

    Authoritative release timestamps (for example ALFRED initial releases) take
    precedence.  Configured lags remain the conservative fallback for sources
    that expose only an observation date.
    """
    adjusted = frame.copy()
    observation_dates = pd.to_datetime(adjusted["observation_date"])
    fallback_dates = observation_dates + pd.to_timedelta(int(lag_days), unit="D")
    release_values = adjusted.get(
        "release_timestamp", pd.Series(pd.NaT, index=adjusted.index)
    )
    release_dates = pd.to_datetime(
        release_values, errors="coerce", utc=True
    ).dt.tz_localize(None)
    authoritative = adjusted.get(
        "release_timestamp_is_authoritative",
        pd.Series(False, index=adjusted.index),
    ).fillna(False).astype(bool)
    dates = fallback_dates.where(~authoritative | release_dates.isna(), release_dates)
    dates = pd.concat([dates.rename("available"), observation_dates.rename("observed")], axis=1).max(
        axis=1
    )
    adjusted["observation_date"] = pd.to_datetime(dates).dt.normalize() + pd.offsets.BDay(0)
    adjusted["availability_timestamp_source"] = np.where(
        authoritative & release_dates.notna(),
        "provider_release_timestamp",
        "configured_lag",
    )
    return adjusted


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


def engineer_market_features(
    panel: pd.DataFrame,
    frequencies: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Build transformations appropriate to each series' publication frequency."""
    frequencies = frequencies or {}
    features = pd.DataFrame(index=panel.index)
    for column in list(panel.columns):
        numeric = pd.to_numeric(panel[column], errors="coerce")
        features[f"{column}_level"] = numeric
        frequency = str(frequencies.get(column, "daily")).lower()
        if frequency == "daily":
            features[f"{column}_chg_1"] = numeric.diff(1)
            features[f"{column}_pct_1"] = numeric.pct_change(1, fill_method=None)
            features[f"{column}_pct_5"] = numeric.pct_change(5, fill_method=None)
            rolling = numeric.rolling(60, min_periods=20)
            features[f"{column}_z_60"] = (numeric - rolling.mean()) / rolling.std()
        else:
            changed = numeric.diff().where(numeric.diff().ne(0))
            features[f"{column}_release_change"] = changed.ffill(limit=260)
            update_marker = numeric.ne(numeric.shift()) & numeric.notna()
            groups = update_marker.cumsum()
            features[f"{column}_days_since_update"] = (
                numeric.groupby(groups).cumcount().where(numeric.notna())
            )
            window = 756 if "quarter" in frequency else 252
            minimum = 252 if "quarter" in frequency else 60
            rolling = numeric.rolling(window, min_periods=minimum)
            features[f"{column}_slow_z"] = (numeric - rolling.mean()) / rolling.std()
    if "trm" in panel:
        log_return = np.log(pd.to_numeric(panel["trm"], errors="coerce")).diff()
        features["trm_return_1"] = log_return
        features["trm_realized_vol_5"] = (
            log_return.rolling(5, min_periods=3).std() * np.sqrt(252)
        )
        features["trm_realized_vol_20"] = (
            log_return.rolling(20, min_periods=10).std() * np.sqrt(252)
        )
        features["trm_realized_vol_60"] = (
            log_return.rolling(60, min_periods=30).std() * np.sqrt(252)
        )
        features["trm_momentum_5"] = np.log(panel["trm"]).diff(5)
        features["trm_momentum_20"] = np.log(panel["trm"]).diff(20)
        features["trm_momentum_60"] = np.log(panel["trm"]).diff(60)
    if {"ibr_on", "sofr"}.issubset(panel.columns):
        features["carry_spread_pp"] = panel["ibr_on"] - panel["sofr"]
        if "trm_realized_vol_20" in features:
            features["carry_to_risk"] = features["carry_spread_pp"] / features[
                "trm_realized_vol_20"
            ].replace(0, np.nan)
    if {"tes_cop_10y", "tes_cop_1y"}.issubset(panel.columns):
        features["tes_slope_10y_1y_pp"] = panel["tes_cop_10y"] - panel["tes_cop_1y"]
    if {"tes_cop_10y", "tes_cop_5y", "tes_cop_1y"}.issubset(panel.columns):
        features["tes_curvature_pp"] = (
            2 * panel["tes_cop_5y"] - panel["tes_cop_1y"] - panel["tes_cop_10y"]
        )
    if {"treasury_10y", "treasury_2y"}.issubset(panel.columns):
        features["treasury_slope_10y_2y_pp"] = (
            panel["treasury_10y"] - panel["treasury_2y"]
        )
    if {"tes_cop_10y", "treasury_10y"}.issubset(panel.columns):
        features["sovereign_rate_spread_10y_pp"] = (
            panel["tes_cop_10y"] - panel["treasury_10y"]
        )
    if {"tes_cop_1y", "treasury_2y"}.issubset(panel.columns):
        features["sovereign_rate_spread_short_pp"] = (
            panel["tes_cop_1y"] - panel["treasury_2y"]
        )
    if {"policy_rate", "sofr"}.issubset(panel.columns):
        features["policy_rate_spread_pp"] = panel["policy_rate"] - panel["sofr"]
    if {"vix", "broad_usd"}.issubset(panel.columns):
        vix_z = features.get("vix_z_60")
        usd_z = features.get("broad_usd_z_60")
        if vix_z is not None and usd_z is not None:
            features["global_risk_usd_interaction"] = vix_z * usd_z
    if {"brent", "broad_usd"}.issubset(panel.columns):
        features["brent_usd_20d_interaction"] = (
            np.log(pd.to_numeric(panel["brent"], errors="coerce")).diff(20)
            * np.log(pd.to_numeric(panel["broad_usd"], errors="coerce")).diff(20)
        )
    return features.replace([np.inf, -np.inf], np.nan)
