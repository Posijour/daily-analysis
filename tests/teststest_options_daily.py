import os
import unittest
import warnings
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from options_daily import _series_has_true, run_options_daily


class OptionsDailyTests(unittest.TestCase):
    def test_series_has_true_handles_object_values(self):
        series = pd.Series([None, "false", "TRUE", 0])
        self.assertTrue(_series_has_true(series))

    @patch("options_daily.supabase_post")
    @patch("options_daily.load_event")
    def test_run_options_daily_no_fillna_futurewarning(self, mock_load_event, mock_supabase_post):
        ts = pd.Timestamp(datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc))
        cycle = pd.DataFrame(
            {
                "ts": [ts, ts],
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "regime": ["CALM", "CALM"],
                "mci": [0.1, 0.2],
                "mci_slope": [0.01, 0.02],
                "mci_phase": ["CALM", "CALM"],
                "mci_phase_confidence": [0.8, 0.9],
                "phase_divergence": [None, True],
                "phase_divergence_type": [None, "LEAD"],
            }
        )
        market = pd.DataFrame(
            {
                "symbol": ["BTCUSDT"],
                "miti_regime": ["CALM"],
            }
        )
        mock_load_event.side_effect = [cycle, market]

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            run_options_daily(datetime.now(timezone.utc), datetime.now(timezone.utc))

        future_warnings = [w for w in caught if issubclass(w.category, FutureWarning)]
        self.assertEqual(future_warnings, [])
        mock_supabase_post.assert_called_once()


if __name__ == "__main__":
    unittest.main()
