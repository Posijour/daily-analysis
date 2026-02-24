import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from deribit_daily import run_deribit_daily


class DeribitDailyTests(unittest.TestCase):
    @patch("deribit_daily.supabase_post")
    @patch("deribit_daily.load_event")
    def test_run_deribit_daily_works_without_vbi_pattern(self, mock_load_event, mock_supabase_post):
        ts = pd.Timestamp(datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc))
        mock_load_event.return_value = pd.DataFrame(
            {
                "ts": [ts, ts],
                "symbol": ["BTC", "BTC"],
                "vbi_state": ["WARM", "HOT"],
                "near_iv": [0.55, 0.61],
                "far_iv": [0.57, 0.62],
                "iv_slope": [0.02, 0.01],
                "curvature": [0.0, 0.01],
                "skew": [1.03, 0.99],
            }
        )

        run_deribit_daily(datetime.now(timezone.utc), datetime.now(timezone.utc))

        mock_supabase_post.assert_called_once()
        payload = mock_supabase_post.call_args.kwargs.get("payload")
        if payload is None:
            payload = mock_supabase_post.call_args.args[1]

        self.assertEqual(payload["vbi_pattern_dominant"], "NONE")
        self.assertEqual(payload["vbi_pattern_share_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
