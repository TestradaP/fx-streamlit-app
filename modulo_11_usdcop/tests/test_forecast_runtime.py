import unittest
from unittest.mock import patch

from usdcop.pipeline.forecast import _validate_artifact_runtime


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


if __name__ == "__main__":
    unittest.main()
