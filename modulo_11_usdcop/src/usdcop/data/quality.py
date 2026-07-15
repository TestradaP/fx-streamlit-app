from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass(frozen=True)
class QualityResult:
    passed: bool
    rows: int
    duplicate_dates: int
    missing_values: int
    age_days: int | None
    messages: tuple[str, ...]


def assess_series(frame: pd.DataFrame, as_of: date, max_staleness_days: int) -> QualityResult:
    messages: list[str] = []
    if frame.empty:
        return QualityResult(False, 0, 0, 0, None, ("empty series",))
    dates = pd.to_datetime(frame["observation_date"])
    duplicates = int(dates.duplicated().sum())
    missing = int(pd.to_numeric(frame["value"], errors="coerce").isna().sum())
    age = (pd.Timestamp(as_of) - dates.max().normalize()).days
    if duplicates:
        messages.append(f"{duplicates} duplicate observation dates")
    if missing:
        messages.append(f"{missing} missing or non-numeric values")
    if age > max_staleness_days:
        messages.append(f"series is stale by {age} days")
    passed = duplicates == 0 and missing == 0 and age <= max_staleness_days
    return QualityResult(passed, len(frame), duplicates, missing, age, tuple(messages))
