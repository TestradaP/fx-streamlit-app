import unittest
from datetime import date

from usdcop.models.baselines import target_date, theoretical_carry_anchor


class ForwardTests(unittest.TestCase):
    def test_current_snapshot_30d(self):
        value = theoretical_carry_anchor(3252.11, 0.11181, 0.0363, 30)
        self.assertAlmostEqual(value, 3272.5122, places=3)

    def test_target_moves_weekend_forward(self):
        # 2026-07-17 + 1 day is Saturday; target becomes Monday.
        self.assertEqual(target_date(date(2026, 7, 17), 1), date(2026, 7, 20))

    def test_rejects_invalid_horizon(self):
        with self.assertRaises(ValueError):
            theoretical_carry_anchor(3252.11, 0.1, 0.03, 0)


if __name__ == "__main__":
    unittest.main()
