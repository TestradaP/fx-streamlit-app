import unittest

import numpy as np
import pandas as pd

from usdcop.features.build import engineer_market_features
from usdcop.models.candidates import CANDIDATE_NAMES, DirectCandidateForecaster
from usdcop.pipeline.backtest import (
    _block_bootstrap_loss_difference,
    _conformal_radius,
)
from usdcop.pipeline.forecast import _feature_drift


class CandidateModelTests(unittest.TestCase):
    def test_frequency_aware_features_do_not_create_slow_daily_percentages(self):
        index = pd.date_range("2025-01-01", periods=100, freq="B")
        panel = pd.DataFrame(
            {
                "trm": np.linspace(4000, 4100, len(index)),
                "inflation": np.where(np.arange(len(index)) < 50, 5.0, 5.2),
            },
            index=index,
        )

        features = engineer_market_features(
            panel, {"trm": "daily", "inflation": "monthly"}
        )

        self.assertIn("trm_pct_1", features)
        self.assertIn("inflation_release_change", features)
        self.assertIn("inflation_days_since_update", features)
        self.assertNotIn("inflation_pct_1", features)

    def test_all_challengers_fit_and_predict(self):
        index = pd.date_range("2020-01-01", periods=140, freq="B")
        X = pd.DataFrame(
            {
                "signal": np.sin(np.linspace(0, 12, len(index))),
                "trend": np.linspace(-1, 1, len(index)),
            },
            index=index,
        )
        targets = pd.DataFrame(
            {"target_log_return_15d": 0.01 * X["signal"]}, index=index
        )

        model = DirectCandidateForecaster((15,), random_state=7).fit(X, targets)
        predicted = model.predict_all(X.iloc[[-1]])

        self.assertEqual(set(model.models), set(CANDIDATE_NAMES))
        self.assertEqual(predicted.shape[1], len(CANDIDATE_NAMES))
        self.assertTrue(np.isfinite(predicted.to_numpy()).all())

    def test_conformal_radius_and_block_bootstrap_are_finite(self):
        residuals = pd.Series(np.linspace(-0.05, 0.05, 100))
        candidate = pd.Series(np.linspace(-5, 5, 100))
        benchmark = pd.Series(np.linspace(-8, 8, 100))

        radius = _conformal_radius(residuals, 0.80)
        low, high = _block_bootstrap_loss_difference(
            candidate, benchmark, seed=7, samples=100
        )

        self.assertGreater(radius, 0)
        self.assertTrue(np.isfinite([low, high]).all())

    def test_feature_drift_flags_extreme_values(self):
        latest = pd.DataFrame([{"feature": 10.0}])
        summary = {"feature": {"q01": -2.0, "q99": 2.0}}

        result = _feature_drift(latest, summary)

        self.assertEqual(result["outside_training_range"], ["feature"])
        self.assertTrue(result["severe"])


if __name__ == "__main__":
    unittest.main()
