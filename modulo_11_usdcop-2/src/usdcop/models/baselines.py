from __future__ import annotations

from datetime import date, timedelta

import pandas as pd


def theoretical_carry_anchor(
    spot: float,
    cop_rate_decimal: float,
    usd_rate_decimal: float,
    horizon_days: int,
    day_count_basis: int = 360,
    basis_decimal: float = 0.0,
) -> float:
    if spot <= 0:
        raise ValueError("spot must be positive")
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    tau = horizon_days / day_count_basis
    return spot * (1 + cop_rate_decimal * tau) / (1 + usd_rate_decimal * tau) * (1 + basis_decimal * tau)


def next_business_day_on_or_after(value: date) -> date:
    current = value
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def target_date(as_of: date, horizon_calendar_days: int) -> date:
    return next_business_day_on_or_after(as_of + timedelta(days=horizon_calendar_days))


def baseline_table(
    as_of: date,
    spot: float,
    cop_rate_decimal: float,
    usd_rate_decimal: float,
    horizons: list[int],
) -> pd.DataFrame:
    rows = []
    for horizon in horizons:
        anchor = theoretical_carry_anchor(spot, cop_rate_decimal, usd_rate_decimal, horizon)
        rows.append(
            {
                "as_of_date": as_of,
                "horizon_days": horizon,
                "target_date": target_date(as_of, horizon),
                "spot_random_walk": spot,
                "forward_anchor": anchor,
                "forward_points_cop": anchor - spot,
                "premium_pct": (anchor / spot - 1) * 100,
            }
        )
    return pd.DataFrame(rows)
