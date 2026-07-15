import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from usdcop.ui.module import _forecast_review_id, _load_quality_snapshot


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


if __name__ == "__main__":
    unittest.main()
