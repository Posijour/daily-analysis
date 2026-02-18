from datetime import datetime, timezone
import unittest

from window import analysis_window_utc


class AnalysisWindowUTCTests(unittest.TestCase):
    def test_after_11_utc_uses_same_day_boundary(self):
        now = datetime(2026, 1, 10, 15, 30, tzinfo=timezone.utc)
        start, end = analysis_window_utc(now)

        self.assertEqual(start, datetime(2026, 1, 9, 11, 0, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 1, 10, 11, 0, tzinfo=timezone.utc))

    def test_before_11_utc_rolls_back_one_more_day(self):
        now = datetime(2026, 1, 10, 9, 45, tzinfo=timezone.utc)
        start, end = analysis_window_utc(now)

        self.assertEqual(start, datetime(2026, 1, 8, 11, 0, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 1, 9, 11, 0, tzinfo=timezone.utc))

    def test_exactly_11_utc_ends_at_same_timestamp(self):
        now = datetime(2026, 1, 10, 11, 0, tzinfo=timezone.utc)
        start, end = analysis_window_utc(now)

        self.assertEqual(start, datetime(2026, 1, 9, 11, 0, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 1, 10, 11, 0, tzinfo=timezone.utc))

    def test_naive_datetime_is_treated_as_utc(self):
        now = datetime(2026, 1, 10, 9, 45)
        start, end = analysis_window_utc(now)

        self.assertEqual(start, datetime(2026, 1, 8, 11, 0, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 1, 9, 11, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
