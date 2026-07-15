from __future__ import annotations

import pandas as pd


def asof_join(
    base: pd.DataFrame,
    released: pd.DataFrame,
    *,
    base_time: str = "as_of_timestamp",
    release_time: str = "release_timestamp",
    suffix: str = "_released",
) -> pd.DataFrame:
    """Backward as-of join that prevents use of information released later."""
    left = base.copy()
    right = released.copy()
    left[base_time] = pd.to_datetime(left[base_time], utc=True)
    right[release_time] = pd.to_datetime(right[release_time], utc=True)
    left = left.sort_values(base_time)
    right = right.sort_values(release_time)
    joined = pd.merge_asof(
        left,
        right,
        left_on=base_time,
        right_on=release_time,
        direction="backward",
        suffixes=("", suffix),
    )
    if (joined[release_time].notna() & (joined[release_time] > joined[base_time])).any():
        raise AssertionError("look-ahead detected in as-of join")
    return joined
