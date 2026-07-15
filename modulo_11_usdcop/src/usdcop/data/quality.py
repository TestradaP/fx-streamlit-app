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
    latest_value: float | None
    messages: tuple[str, ...]


def assess_series(
    frame: pd.DataFrame,
    as_of: date,
    max_staleness_days: int,
    expected_min: float | None = None,
    expected_max: float | None = None,
) -> QualityResult:
    messages: list[str] = []
    if frame.empty:
        return QualityResult(False, 0, 0, 0, None, None, ("empty series",))
    dates = pd.to_datetime(frame["observation_date"])
    duplicates = int(dates.duplicated().sum())
    missing = int(pd.to_numeric(frame["value"], errors="coerce").isna().sum())
    latest_index = dates.idxmax()
    latest_numeric = pd.to_numeric(frame.loc[latest_index, "value"], errors="coerce")
    latest_value = float(latest_numeric) if pd.notna(latest_numeric) else None
    age = (pd.Timestamp(as_of) - dates.max().normalize()).days
    if duplicates:
        messages.append(f"{duplicates} duplicate observation dates")
    if missing:
        messages.append(f"{missing} missing or non-numeric values")
    if age < 0:
        messages.append(f"series contains observations {-age} days in the future")
    if age > max_staleness_days:
        messages.append(f"series is stale by {age} days")
    if latest_value is not None and expected_min is not None and latest_value < expected_min:
        messages.append(f"latest value {latest_value} is below expected minimum {expected_min}")
    if latest_value is not None and expected_max is not None and latest_value > expected_max:
        messages.append(f"latest value {latest_value} is above expected maximum {expected_max}")
    bounds_passed = not any("expected" in message for message in messages)
    passed = (
        duplicates == 0
        and missing == 0
        and 0 <= age <= max_staleness_days
        and bounds_passed
    )
    return QualityResult(
        passed, len(frame), duplicates, missing, age, latest_value, tuple(messages)
    )
