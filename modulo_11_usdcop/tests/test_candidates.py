import unittest

import numpy as np
import pandas as pd

from usdcop.features.build import engineer_market_features
from usdcop.models.candidates import CANDIDATE_NAMES, DirectCandidateForecaster
from usdcop.pipeline.backtest import (
    _block_bootstrap_loss_difference,
    _conformal_radius,
    _historical_ensemble_weights,
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
        quantiles = model.predict_quantiles(X.iloc[[-1]])
        self.assertLessEqual(
            quantiles["pred_log_return_p10_15d"].iloc[0],
            quantiles["pred_log_return_p90_15d"].iloc[0],
        )

    def test_engineered_spreads_are_available(self):
        index = pd.date_range("2024-01-01", periods=100, freq="B")
        panel = pd.DataFrame(
            {
                "trm": np.linspace(3900, 4100, len(index)),
                "tes_cop_1y": 8.0,
                "tes_cop_5y": 9.0,
                "tes_cop_10y": 10.0,
                "treasury_2y": 4.0,
                "treasury_10y": 4.5,
                "policy_rate": 9.5,
                "sofr": 4.25,
            },
            index=index,
        )

        features = engineer_market_features(panel)

        self.assertAlmostEqual(features["tes_slope_10y_1y_pp"].iloc[-1], 2.0)
        self.assertAlmostEqual(features["sovereign_rate_spread_10y_pp"].iloc[-1], 5.5)
        self.assertAlmostEqual(features["policy_rate_spread_pp"].iloc[-1], 5.25)

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

    def test_weighted_ensemble_rewards_lower_past_oos_error(self):
        index = pd.date_range("2025-01-01", periods=50, freq="B")
        predictions = pd.DataFrame(
            {
                "spot": 4000.0,
                "actual_15d": 0.01,
            },
            index=index,
        )
        for position, name in enumerate(CANDIDATE_NAMES, start=1):
            predictions[f"{name}_15d"] = 0.01 + position * 0.001

        weights = _historical_ensemble_weights(predictions, 15)

        self.assertAlmostEqual(sum(weights.values()), 1.0)
        self.assertEqual(max(weights, key=weights.get), CANDIDATE_NAMES[0])

    def test_feature_drift_flags_extreme_values(self):
        latest = pd.DataFrame([{"feature": 10.0}])
        summary = {"feature": {"q01": -2.0, "q99": 2.0}}

        result = _feature_drift(latest, summary)

        self.assertEqual(result["outside_training_range"], ["feature"])
        self.assertTrue(result["severe"])


if __name__ == "__main__":
    unittest.main()
