import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from usdcop.ui.module import (
    _forecast_review_id,
    _load_drivers,
    _load_monitor,
    _load_point_in_time_coverage,
    _load_quality_snapshot,
    _load_registry,
    _load_validation,
)


class DailyReviewTests(unittest.TestCase):
    def test_review_id_changes_with_forecast_generation(self):
        first = pd.DataFrame(
            [{"generated_at": "2026-07-15T10:00:00Z", "model_version": "v1", "as_of_date": "2026-07-15"}]
        )
        second = first.copy()
        second.loc[0, "generated_at"] = "2026-07-16T10:00:00Z"

        self.assertNotEqual(_forecast_review_id(first), _forecast_review_id(second))

    def test_loads_published_quality_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            expected = {"status": "success", "quality": [{"series": "trm", "passed": True}]}
            (output_root / "data_quality_latest.json").write_text(json.dumps(expected), encoding="utf-8")

            actual = _load_quality_snapshot(SimpleNamespace(output_root=output_root))

        self.assertEqual(actual, expected)

    def test_loads_published_drivers(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            expected = pd.DataFrame(
                [
                    {
                        "horizon_days": 30,
                        "feature": "vix_level",
                        "contribution_cop_approx": 12.5,
                    }
                ]
            )
            expected.to_csv(output_root / "forecast_drivers.csv", index=False)

            actual = _load_drivers(SimpleNamespace(output_root=output_root))

        pd.testing.assert_frame_equal(actual, expected)

    def test_loads_model_validation(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            expected = {"point_forecast_validation_passed": False}
            (output_root / "model_validation.json").write_text(
                json.dumps(expected), encoding="utf-8"
            )

            actual = _load_validation(SimpleNamespace(output_root=output_root))

            self.assertEqual(actual, expected)

    def test_loads_point_in_time_coverage(self):
        with tempfile.TemporaryDirectory() as temporary:
            output_root = Path(temporary)
            expected = {"historical_vintage_complete": False, "series": []}
            (output_root / "point_in_time_coverage.json").write_text(
                json.dumps(expected), encoding="utf-8"
            )

            actual = _load_point_in_time_coverage(
                SimpleNamespace(output_root=output_root)
            )

            self.assertEqual(actual, expected)

    def test_loads_registry_and_monitor(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_root = Path(temporary_directory)
            registry = {"horizons": {"15": {"selected_model": "random_walk"}}}
            monitor = {"severe": False, "outside_ratio": 0.02}
            (output_root / "champion_registry.json").write_text(
                json.dumps(registry), encoding="utf-8"
            )
            (output_root / "model_monitor.json").write_text(
                json.dumps(monitor), encoding="utf-8"
            )

            paths = SimpleNamespace(output_root=output_root)
            actual_registry = _load_registry(paths)
            actual_monitor = _load_monitor(paths)

        self.assertEqual(actual_registry, registry)
        self.assertEqual(actual_monitor, monitor)


if __name__ == "__main__":
    unittest.main()
