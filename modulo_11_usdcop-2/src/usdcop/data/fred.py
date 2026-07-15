from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

from .http import build_session


class FredClient:
    API_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, timeout_seconds: int = 30, api_key: str | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        self.session = build_session()

    def fetch_series(self, series_id: str) -> pd.DataFrame:
        if not self.api_key:
            raise RuntimeError("FRED_API_KEY is not configured")
        response = self.session.get(
            self.API_URL,
            params={
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "observation_start": "2021-01-01",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        observations = payload.get("observations") if isinstance(payload, dict) else None
        if not isinstance(observations, list):
            raise ValueError(f"Unexpected FRED JSON for {series_id}")
        frame = pd.DataFrame(observations)
        if not {"date", "value"}.issubset(frame.columns):
            raise ValueError(f"FRED response has no observations for {series_id}")
        frame = frame.rename(columns={"date": "observation_date"})
        frame["observation_date"] = pd.to_datetime(frame["observation_date"], errors="coerce")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(subset=["observation_date", "value"]).copy()
        if frame.empty:
            raise ValueError(f"FRED response has no numeric observations for {series_id}")
        frame["series_id"] = series_id
        frame["source"] = "fred"
        frame["retrieved_at"] = datetime.now(timezone.utc)
        frame["release_timestamp"] = frame["observation_date"]
        return frame[
            [
                "series_id",
                "observation_date",
                "value",
                "release_timestamp",
                "retrieved_at",
                "source",
            ]
        ]
