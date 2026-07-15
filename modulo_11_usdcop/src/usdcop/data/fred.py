from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO

import pandas as pd

from .http import build_session


class FredClient:
    CSV_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = build_session()
        # Disfraz para evitar el bloqueo anti-bots de FRED
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

    def fetch_series(self, series_id: str) -> pd.DataFrame:
        response = self.session.get(
        self.CSV_URL,
        params={
            "series_id": series_id,
            "api_key": "3bf7d33d48baea0233b409388e170433", # Pega tu código de FRED
            "file_type": "json"
        },
        timeout=self.timeout_seconds,
    )
        response.raise_for_status()
        frame = pd.read_csv(StringIO(response.text))
        if frame.shape[1] < 2:
            raise ValueError(f"Unexpected FRED CSV for {series_id}")
        frame.columns = ["observation_date", "value", *frame.columns[2:]]
        frame["observation_date"] = pd.to_datetime(frame["observation_date"], errors="coerce")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(subset=["observation_date", "value"]).copy()
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
