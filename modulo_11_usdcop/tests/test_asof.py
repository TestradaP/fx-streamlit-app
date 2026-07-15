import unittest

import pandas as pd

from usdcop.features.asof import asof_join


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


if __name__ == "__main__":
    unittest.main()
