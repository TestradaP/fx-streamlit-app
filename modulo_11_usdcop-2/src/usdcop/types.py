from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class SeriesObservation:
    series_id: str
    observation_date: date
    value: float
    source: str
    release_timestamp: datetime | None = None
    retrieved_at: datetime | None = None
    vintage_id: str | None = None


@dataclass(frozen=True)
class ForecastPoint:
    as_of_date: date
    horizon_days: int
    target_date: date
    median: float | None
    p10: float | None
    p90: float | None
    spot: float
    forward_anchor: float
    status: str
    model_version: str | None = None
