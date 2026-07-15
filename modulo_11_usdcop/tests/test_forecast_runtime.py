import unittest
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from usdcop.pipeline.forecast import (
    _driver_group,
    _elastic_net_driver_table,
    _validate_artifact_runtime,
)


class ForecastRuntimeTests(unittest.TestCase):
    def test_matching_sklearn_version_is_accepted(self):
        with patch("usdcop.pipeline.forecast.sklearn.__version__", "1.6.1"):
            _validate_artifact_runtime({"sklearn_version": "1.6.1"})

    def test_mismatched_sklearn_version_is_rejected(self):
        with patch("usdcop.pipeline.forecast.sklearn.__version__", "1.7.0"):
            with self.assertRaisesRegex(RuntimeError, "requires scikit-learn 1.6.1"):
                _validate_artifact_runtime({"sklearn_version": "1.6.1"})

    def test_legacy_artifact_without_version_is_allowed(self):
        _validate_artifact_runtime({})

    def test_driver_contributions_reconstruct_prediction(self):
        X = pd.DataFrame(
            {
                "vix_level": np.linspace(10, 30, 120),
                "carry_spread_pp": np.sin(np.linspace(0, 8, 120)),
            }
        )
        y = 0.01 + 0.002 * X["vix_level"] - 0.03 * X["carry_spread_pp"]
        pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("model", LinearRegression()),
            ]
        ).fit(X, y)
        forecaster = SimpleNamespace(
            feature_names=list(X.columns),
            models={30: pipeline},
        )
        latest = X.iloc[[-1]]

        drivers = _elastic_net_driver_table(forecaster, latest, spot=4000.0)
        reconstructed = drivers["contribution_log_return"].sum()
        predicted = float(pipeline.predict(latest)[0])

        self.assertAlmostEqual(reconstructed, predicted)
        self.assertEqual(set(drivers["direction"]), {"up", "down"})

    def test_driver_groups_reflect_available_features(self):
        self.assertEqual(_driver_group("vix_level"), "global_risk")
        self.assertEqual(_driver_group("carry_spread_pp"), "rates_and_carry")
        self.assertEqual(_driver_group("current_account_balance"), "external_flows")
        self.assertEqual(_driver_group("trm_return_5d"), "technical_fx")


if __name__ == "__main__":
    unittest.main()
