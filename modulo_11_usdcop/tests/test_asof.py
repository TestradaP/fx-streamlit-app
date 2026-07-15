import unittest
from tempfile import TemporaryDirectory

import pandas as pd

from usdcop.features.asof import asof_join
from usdcop.features.build import apply_availability_lag
from usdcop.data.repository import SeriesRepository


class AsOfJoinTests(unittest.TestCase):
    def test_no_future_release_is_used(self):
        base = pd.DataFrame(
            {"as_of_timestamp": pd.to_datetime(["2026-01-10", "2026-01-20"], utc=True)}
        )
        released = pd.DataFrame(
            {
                "release_timestamp": pd.to_datetime(["2026-01-05", "2026-01-15"], utc=True),
                "remittances": [100.0, 110.0],
            }
        )
        joined = asof_join(base, released)
        self.assertEqual(joined.loc[0, "remittances"], 100.0)
        self.assertEqual(joined.loc[1, "remittances"], 110.0)
        self.assertTrue((joined.release_timestamp <= joined.as_of_timestamp).all())

    def test_availability_lag_rolls_weekend_to_next_business_day(self):
        frame = pd.DataFrame({"observation_date": ["2026-07-10"], "value": [1.0]})

        adjusted = apply_availability_lag(frame, lag_days=1)

        self.assertEqual(adjusted.loc[0, "observation_date"], pd.Timestamp("2026-07-13"))

    def test_repository_retains_revisions_and_loads_as_of_snapshot(self):
        with TemporaryDirectory() as temporary:
            repository = SeriesRepository(temporary)
            first = pd.DataFrame(
                {
                    "observation_date": ["2026-01-01"],
                    "value": [100.0],
                    "retrieved_at": ["2026-01-10T12:00:00Z"],
                }
            )
            revised = pd.DataFrame(
                {
                    "observation_date": ["2026-01-01"],
                    "value": [110.0],
                    "retrieved_at": ["2026-02-10T12:00:00Z"],
                }
            )
            repository.save_series(first, "test", "macro")
            repository.save_series(first, "test", "macro")
            repository.save_series(revised, "test", "macro")

            old_view = repository.load_series_as_of(
                "test", "macro", "2026-01-31T23:59:59Z"
            )
            new_view = repository.load_series_as_of(
                "test", "macro", "2026-02-28T23:59:59Z"
            )

            self.assertEqual(old_view["value"].iloc[0], 100.0)
            self.assertEqual(new_view["value"].iloc[0], 110.0)
            self.assertEqual(len(repository.load_vintages("test", "macro")), 2)


if __name__ == "__main__":
    unittest.main()
