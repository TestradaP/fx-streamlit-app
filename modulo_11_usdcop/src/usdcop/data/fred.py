from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from .http import build_session

LOGGER = logging.getLogger(__name__)


class FredClient:
    API_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, timeout_seconds: int = 30, api_key: str | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        self.session = build_session()

    def fetch_series(self, series_id: str) -> pd.DataFrame:
        if not self.api_key:
            raise RuntimeError("FRED_API_KEY is not configured")
        current = self._fetch_observations(
            series_id,
            {
                "realtime_start": datetime.now(timezone.utc).date().isoformat(),
                "realtime_end": datetime.now(timezone.utc).date().isoformat(),
                "output_type": 1,
            },
        )
        try:
            initial = self._fetch_observations(
                series_id,
                {
                    "realtime_start": "1776-07-04",
                    "realtime_end": "9999-12-31",
                    "output_type": 4,
                },
            )
        except Exception as exc:  # noqa: BLE001 - current values remain usable
            LOGGER.warning(
                "ALFRED initial-release history unavailable for %s: %s",
                series_id,
                exc,
            )
            initial = pd.DataFrame()
        initial_release = pd.DataFrame()
        if {"date", "value", "realtime_start"}.issubset(initial.columns):
            initial_release = (
                initial.dropna(subset=["realtime_start"])
                .sort_values(["date", "realtime_start"])
                .drop_duplicates("date", keep="first")
                .set_index("date")[["realtime_start", "value"]]
            )
        frame = current.copy()
        frame["initial_release_date"] = frame["date"].map(
            initial_release.get("realtime_start", pd.Series(dtype="object"))
        )
        frame["initial_release_value"] = frame["date"].map(
            initial_release.get("value", pd.Series(dtype="object"))
        )
        return self._normalize(series_id, frame)

    def _fetch_observations(
        self, series_id: str, extra_params: dict[str, object]
    ) -> pd.DataFrame:
        response = self.session.get(
            self.API_URL,
            params={
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "limit": 100000,
                **extra_params,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        observations = payload.get("observations") if isinstance(payload, dict) else None
        if not isinstance(observations, list):
            raise ValueError(f"Unexpected FRED JSON for {series_id}")
        frame = pd.DataFrame(observations)
        if frame.empty:
            raise ValueError(f"FRED response has no observations for {series_id}")
        return frame

    @staticmethod
    def _normalize(series_id: str, frame: pd.DataFrame) -> pd.DataFrame:
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
        initial_release = pd.to_datetime(
            frame.get("initial_release_date"), errors="coerce", utc=True
        )
        frame["initial_release_value"] = pd.to_numeric(
            frame.get("initial_release_value"), errors="coerce"
        )
        frame["release_timestamp"] = initial_release.fillna(
            pd.to_datetime(frame["observation_date"], utc=True)
        )
        frame["release_timestamp_is_authoritative"] = initial_release.notna()
        frame["release_timestamp_source"] = np.where(
            initial_release.notna(), "alfred_initial_release", "observation_date_fallback"
        )
        return frame[
            [
                "series_id",
                "observation_date",
                "value",
                "release_timestamp",
                "retrieved_at",
                "source",
                "release_timestamp_is_authoritative",
                "release_timestamp_source",
                "initial_release_value",
            ]
        ]
