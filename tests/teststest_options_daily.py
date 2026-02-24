import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from options_daily import DAILY_OPTIONS_TABLE, _clean_signal_series, run_options_daily


class OptionsDailyTests(unittest.TestCase):
    def test_clean_signal_series_filters_empty_and_none(self):
        series = pd.Series([None, "", "NONE", "null", "PRE_BREAK_TENSION"])
        cleaned = _clean_signal_series(series)
        self.assertEqual(cleaned.tolist(), ["PRE_BREAK_TENSION"])

    @patch("options_daily.supabase_post")
    @patch("options_daily.load_event")
    def test_run_options_daily_builds_v2_payload(self, mock_load_event, mock_supabase_post):
        ts_asia = pd.Timestamp(datetime(2026, 2, 20, 3, 0, tzinfo=timezone.utc))
        ts_us = pd.Timestamp(datetime(2026, 2, 20, 18, 0, tzinfo=timezone.utc))

        cycle = pd.DataFrame(
            {
                "ts": [ts_asia, ts_us],
                "symbol": ["BTC", "ETH"],
                "regime": ["CALM", "UNCERTAIN"],
                "mci": [0.8, 0.6],
                "mci_slope": [0.01, -0.02],
                "mci_phase": ["OVERCOMPRESSED", "RELEASING"],
                "mci_phase_confidence": [0.85, 0.6],
                "okx_olsi": [0.42, 0.36],
                "okx_olsi_slope": [0.07, -0.03],
                "divergence": ["STRONG", "NONE"],
                "divergence_diff": [0.8, 0.1],
                "liquidity_phase": ["LIQUIDITY_EXPANDING", "LIQUIDITY_CRUSH"],
                "phase_divergence": ["PRE_BREAK_TENSION", None],
                "market_calm_ratio": [0.5, 0.6],
            }
        )

        market = pd.DataFrame(
            {
                "symbol": ["MARKET"],
                "liquidity_regime": ["LIQUIDITY_EXPANSION"],
                "market_calm_ratio": [0.55],
            }
        )
        mock_load_event.side_effect = [cycle, market]

        run_options_daily(datetime.now(timezone.utc), datetime.now(timezone.utc))

        mock_supabase_post.assert_called_once()
        table, payload = mock_supabase_post.call_args.args
        self.assertEqual(table, DAILY_OPTIONS_TABLE)

        self.assertEqual(payload["dominant_liquidity_regime"], "LIQUIDITY_EXPANSION")
        self.assertEqual(payload["dominant_divergence_level"], "STRONG")
        self.assertEqual(payload["dominant_phase_divergence"], "PRE_BREAK_TENSION")
        self.assertEqual(payload["phase_divergence_share_pct"], 50.0)
        self.assertEqual(payload["divergence_diff_abs_avg"], 0.45)
        self.assertIn("ASIA", payload["session_breakdown"])
        self.assertIn("US", payload["session_breakdown"])


    @patch("options_daily.supabase_post")
    @patch("options_daily.load_event")
    def test_run_options_daily_handles_none_divergence_diff(self, mock_load_event, mock_supabase_post):
        ts = pd.Timestamp(datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc))
        cycle = pd.DataFrame(
            {
                "ts": [ts, ts],
                "symbol": ["BTC", "ETH"],
                "regime": ["CALM", "CALM"],
                "mci": [0.7, 0.8],
                "mci_slope": [0.01, 0.01],
                "mci_phase": ["OVERCOMPRESSED", "OVERCOMPRESSED"],
                "mci_phase_confidence": [0.7, 0.8],
                "divergence": ["STRONG", "NONE"],
                "divergence_diff": [0.7, None],
                "phase_divergence": [None, None],
            }
        )
        market = pd.DataFrame({"symbol": ["MARKET"]})
        mock_load_event.side_effect = [cycle, market]

        run_options_daily(datetime.now(timezone.utc), datetime.now(timezone.utc))

        payload = mock_supabase_post.call_args.args[1]
        self.assertEqual(payload["divergence_diff_abs_avg"], 0.7)

    @patch("options_daily.supabase_post")
    @patch("options_daily.load_event")
    def test_run_options_daily_no_cycle_data(self, mock_load_event, mock_supabase_post):
        mock_load_event.side_effect = [pd.DataFrame(), pd.DataFrame()]

        run_options_daily(datetime.now(timezone.utc), datetime.now(timezone.utc))

        mock_supabase_post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
