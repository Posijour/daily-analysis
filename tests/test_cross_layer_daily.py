import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from cross_layer import SOURCE_MODE_DAILY_24H, process_cross_layer_daily_window


class CrossLayerDailyTests(unittest.TestCase):
    @patch("cross_layer._persist_cross_layer_event")
    @patch("cross_layer.get_cross_context_for_window")
    @patch("cross_layer._load_rows")
    def test_process_cross_layer_daily_window_uses_daily_mode_and_event_key(
        self,
        mock_load_rows,
        mock_get_context,
        mock_persist,
    ):
        ts_from = 1000
        ts_to = 2000
        mock_load_rows.return_value = [
            {"data": {"symbol": "BTC", "risk": 4, "price": 100, "direction": "up"}, "ts": 1500},
            {"data": {"symbol": "BTC", "risk": 3, "price": 101, "direction": "up"}, "ts": 1800},
        ]
        mock_get_context.return_value = {
            "bybit": None,
            "deribit_btc": None,
            "deribit_eth": None,
            "missing_parts": ["bybit", "deribit_btc", "deribit_eth"],
            "is_complete": False,
        }

        # proper CrossContext instance
        from cross_layer import CrossContext

        mock_get_context.return_value = CrossContext(None, None, None, ["bybit", "deribit_btc", "deribit_eth"])

        counters = process_cross_layer_daily_window(ts_from, ts_to)

        self.assertEqual(counters["window_start_ts_ms"], ts_from)
        self.assertEqual(counters["window_end_ts_ms"], ts_to)
        self.assertEqual(counters["qualified_symbols"], 1)
        self.assertEqual(counters["processed"], 1)

        payload = mock_persist.call_args.args[0]
        self.assertEqual(payload["source_mode"], SOURCE_MODE_DAILY_24H)
        self.assertEqual(payload["event_key"], f"BTC:{ts_to}:{SOURCE_MODE_DAILY_24H}")


class CrossLayerJsonSanitizationTests(unittest.TestCase):
    @patch("cross_layer.supabase_post")
    def test_persist_cross_layer_event_converts_nan_to_none(self, mock_supabase_post):
        from cross_layer import _persist_cross_layer_event

        _persist_cross_layer_event({"event_key": "BTC:1:DAILY_24H", "direction": float("nan")})

        payload = mock_supabase_post.call_args.args[1]
        self.assertIsNone(payload["direction"])


if __name__ == "__main__":
    unittest.main()
