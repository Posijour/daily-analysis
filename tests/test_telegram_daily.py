import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from telegram_daily import generate_daily_log, run_telegram_daily


class TelegramDailyTests(unittest.TestCase):
    @patch("telegram_daily.next_counter", return_value=20)
    @patch("telegram_daily.load_event")
    def test_generate_daily_log_format(self, mock_load_event, _mock_counter):
        mock_load_event.side_effect = [
            pd.DataFrame({"risk": [2, 1, 0, 3, 2, 1, 0, 1, 2, 1, 0, 1, 3]}),
            pd.DataFrame({"type": ["BUILDUP"] * 17}),
            pd.DataFrame({"regime": ["CALM", "CALM", "NEUTRAL"]}),
            pd.DataFrame({"regime": ["CALM", "CALM", "BUILDING"]}),
            pd.DataFrame({"vbi_state": ["CALM", "CALM"], "vbi_pattern": ["NONE", "NONE"]}),
        ]

        text = generate_daily_log(datetime.now(timezone.utc), datetime.now(timezone.utc))

        self.assertIn("Risk Log #20", text)
        self.assertIn("Futures (Binance)", text)
        self.assertIn("• Elevated risk: 38.5%", text)
        self.assertIn("• Buildups: 17", text)
        self.assertIn("• Regime: CALM", text)
        self.assertIn("Options (Bybit / OKX)", text)
        self.assertIn("• Short-dated IV: stable", text)
        self.assertIn("• Skew: neutral", text)
        self.assertIn("Deribit (meta)", text)
        self.assertIn("• Vol term structure flat", text)
        self.assertIn("• No PRE-BREAK patterns", text)
        self.assertIn("Market log.", text)

    @patch("telegram_daily.supabase_post")
    @patch("telegram_daily.generate_daily_log", return_value="daily telegram text")
    def test_run_telegram_daily_posts_log(self, _mock_generate, mock_supabase_post):
        run_telegram_daily(datetime.now(timezone.utc), datetime.now(timezone.utc))

        mock_supabase_post.assert_called_once_with("telegram_logs", {"text": "daily telegram text"})


if __name__ == "__main__":
    unittest.main()
