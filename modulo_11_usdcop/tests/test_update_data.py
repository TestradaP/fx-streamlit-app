import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from usdcop.pipeline.update_data import update_all
from usdcop.data.quality import assess_series


class UpdateDataQualityTests(unittest.TestCase):
    def test_future_observation_fails_quality(self):
        frame = pd.DataFrame(
            {"observation_date": [pd.Timestamp("2099-01-01")], "value": [100.0]}
        )
        result = assess_series(frame, pd.Timestamp("2026-07-15").date(), 5)
        self.assertFalse(result.passed)
        self.assertIn("future", result.messages[0])

    def test_failed_quality_does_not_persist_series(self):
        stale_frame = pd.DataFrame(
            {
                "observation_date": [pd.Timestamp("2020-01-01")],
                "value": [100.0],
            }
        )
        repository = Mock()
        repository_type = Mock(return_value=repository)
        banrep = Mock()
        banrep.fetch_series.return_value = stale_frame
        catalog = {
            "banrep": [
                {
                    "name": "stale_test",
                    "series_id": 1,
                    "enabled": True,
                    "max_staleness_days": 1,
                }
            ],
            "fred": [],
            "dane": {},
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            paths = SimpleNamespace(storage_root=Path(temporary_directory))
            dane_summary = SimpleNamespace(
                period_label="test",
                deficit_usd_millions=0.0,
                publication_date_label=None,
                source_url="test",
                retrieved_at=datetime.now(timezone.utc),
            )
            with (
                patch("usdcop.pipeline.update_data.load_settings", return_value=(paths, {}, catalog)),
                patch("usdcop.pipeline.update_data.SeriesRepository", repository_type),
                patch("usdcop.pipeline.update_data.BanRepClient", return_value=banrep),
                patch("usdcop.pipeline.update_data.DaneTradeClient") as dane_type,
            ):
                dane_type.return_value.fetch_latest_summary.return_value = dane_summary
                result = update_all()

        repository.save_series.assert_not_called()
        self.assertEqual(result["status"], "partial_success")
        self.assertFalse(result["quality"][0]["passed"])
        self.assertEqual(result["failed"][0]["series"], "banrep:stale_test")


if __name__ == "__main__":
    unittest.main()
