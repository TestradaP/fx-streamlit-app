from __future__ import annotations

import numpy as np
import pandas as pd


def inverse_error_weights(errors: pd.Series | dict[str, float], floor: float = 1e-6) -> pd.Series:
    series = pd.Series(errors, dtype=float)
    series = series.replace([np.inf, -np.inf], np.nan).dropna()
    if series.empty:
        raise ValueError("No finite errors supplied")
    adjusted = 1.0 / series.clip(lower=floor)
    return adjusted / adjusted.sum()


def combine_forecasts(forecasts: pd.DataFrame, weights: pd.Series) -> pd.Series:
    common = [column for column in forecasts.columns if column in weights.index]
    if not common:
        raise ValueError("No common models between forecasts and weights")
    normalized = weights.loc[common] / weights.loc[common].sum()
    return forecasts[common].mul(normalized, axis=1).sum(axis=1)
