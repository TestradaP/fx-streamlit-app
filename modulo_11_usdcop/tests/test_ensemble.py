import unittest

import pandas as pd

from usdcop.models.ensemble import combine_forecasts, inverse_error_weights


class EnsembleTests(unittest.TestCase):
    def test_lower_error_gets_higher_weight(self):
        weights = inverse_error_weights({"spot": 100.0, "forward": 80.0, "model": 50.0})
        self.assertGreater(weights["model"], weights["forward"])
        self.assertAlmostEqual(weights.sum(), 1.0)

    def test_combination(self):
        forecasts = pd.DataFrame({"a": [10.0], "b": [20.0]})
        weights = pd.Series({"a": 0.25, "b": 0.75})
        self.assertAlmostEqual(combine_forecasts(forecasts, weights).iloc[0], 17.5)


if __name__ == "__main__":
    unittest.main()
