from __future__ import annotations

from pathlib import Path

import pandas as pd


class MarketDataAdapter:
    """Adapter for an optional licensed or internally supplied market feed."""

    REQUIRED_COLUMNS = {"observation_date", "series_id", "value"}

    @classmethod
    def from_csv(cls, path: str | Path) -> pd.DataFrame:
        frame = pd.read_csv(path)
        missing = cls.REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"Market file is missing columns: {sorted(missing)}")
        frame["observation_date"] = pd.to_datetime(frame["observation_date"])
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(subset=["observation_date", "value"])
        frame["source"] = frame.get("source", "licensed_or_internal_market")
        return frame
