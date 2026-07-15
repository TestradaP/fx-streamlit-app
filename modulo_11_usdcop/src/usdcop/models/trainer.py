from __future__ import annotations

import numpy as np
import pandas as pd


def make_direct_targets(prices: pd.Series, horizons_calendar_days: list[int]) -> pd.DataFrame:
    """Create direct log-return targets using first observation on/after calendar horizon."""
    values = pd.to_numeric(prices, errors="coerce")
    dates = pd.DatetimeIndex(values.index)
    if not dates.is_monotonic_increasing:
        values = values.sort_index()
        dates = pd.DatetimeIndex(values.index)
    output = pd.DataFrame(index=dates)
    date_values = dates.values
    price_values = values.to_numpy(dtype=float)
    for horizon in horizons_calendar_days:
        future_values = np.full(len(values), np.nan)
        for index, current in enumerate(dates):
            target = current + pd.Timedelta(days=horizon)
            position = int(np.searchsorted(date_values, target.to_datetime64(), side="left"))
            if position < len(price_values):
                future_values[index] = price_values[position]
        output[f"target_log_return_{horizon}d"] = np.log(future_values / price_values)
    return output


def expanding_windows(n_rows: int, initial_train: int, step: int = 20):
    if initial_train >= n_rows:
        return
    end = initial_train
    while end < n_rows:
        test_end = min(end + step, n_rows)
        yield slice(0, end), slice(end, test_end)
        end = test_end
