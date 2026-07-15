import unittest

import numpy as np
import pandas as pd

from usdcop.models.trainer import make_direct_targets


class TargetTests(unittest.TestCase):
    def test_calendar_horizon_uses_first_observation_on_or_after(self):
        index = pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-09", "2026-01-12"])
        prices = pd.Series([100.0, 101.0, 103.0, 106.0], index=index)
        targets = make_direct_targets(prices, [7])
        # Jan 2 + 7 days is Jan 9.
        self.assertAlmostEqual(targets.iloc[0, 0], np.log(103.0 / 100.0))
        # Jan 5 + 7 days is Jan 12.
        self.assertAlmostEqual(targets.iloc[1, 0], np.log(106.0 / 101.0))


if __name__ == "__main__":
    unittest.main()
