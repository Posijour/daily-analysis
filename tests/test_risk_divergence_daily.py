import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from risk_divergence_daily import run_risk_divergence_daily


class RiskDivergenceDailyTests(unittest.TestCase):
    @patch("risk_divergence_daily.supabase_post")
    @patch("risk_divergence_daily.load_event")
    def test_run_risk_divergence_daily_adds_1h_market_context(self, mock_load_event, mock_supabase_post):
        event_ts = pd.Timestamp(datetime(2026, 2, 20, 12, 0, tzinfo=timezone.utc))

        divergence = pd.DataFrame(
            {
                "ts": [event_ts],
                "symbol": ["BTCUSDT"],
                "divergence_type": ["BULLISH"],
                "risk": [3],
                "price": [100000],
            }
        )

        risk_eval = pd.DataFrame(
            {
                "ts": [
                    event_ts - pd.Timedelta(minutes=55),
                    event_ts - pd.Timedelta(minutes=25),
                    event_ts,
                ],
                "risk": [1, 2, 3],
            }
        )

        market_regime = pd.DataFrame(
            {
                "ts": [event_ts - pd.Timedelta(minutes=40), event_ts - pd.Timedelta(minutes=5)],
                "liquidity_regime": ["LIQUIDITY_FLAT", "LIQUIDITY_FLAT"],
            }
        )

        deribit = pd.DataFrame(
            {
                "ts": [event_ts - pd.Timedelta(minutes=30), event_ts - pd.Timedelta(minutes=10)],
                "vbi": [0.12, 0.18],
            }
        )

        mock_load_event.side_effect = [divergence, risk_eval, market_regime, deribit]

        run_risk_divergence_daily(
            datetime(2026, 2, 20, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 2, 21, 0, 0, tzinfo=timezone.utc),
        )

        mock_supabase_post.assert_called_once()
        table = mock_supabase_post.call_args.args[0]
        rows = mock_supabase_post.call_args.args[1]
        self.assertEqual(table, "daily_risk_divergences")
        self.assertEqual(len(rows), 1)

        row = rows[0]
        self.assertEqual(row["market_risk_avg_1h_pre_event"], 2.0)
        self.assertEqual(row["market_buildups_share_pct_1h_pre_event"], 66.67)
        self.assertEqual(row["market_liquidity_regime_1h_pre_event"], "LIQUIDITY_FLAT")
        self.assertEqual(row["market_vbi_avg_1h_pre_event"], 0.15)


if __name__ == "__main__":
    unittest.main()
