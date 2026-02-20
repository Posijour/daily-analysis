import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
from requests import HTTPError

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from meta_daily import deribit_context_payload, post_with_optional_columns, run_meta_daily


class MetaDailyTests(unittest.TestCase):
    def test_deribit_context_payload_maps_summary(self):
        deribit = pd.DataFrame(
            {
                "vbi_state": ["CALM", "CALM", "STRESS", "CALM"],
                "vbi_pattern": ["NONE", "PRE-BREAK", "NONE", "NONE"],
            }
        )

        payload = deribit_context_payload(deribit)

        self.assertEqual(payload["deribit_state"], "CALM")
        self.assertEqual(payload["deribit_state_share"], 75.0)
        self.assertEqual(payload["deribit_pattern"], "NONE")
        self.assertEqual(payload["deribit_confidence"], 75.0)

    @patch("meta_daily.supabase_post")
    def test_post_with_optional_columns_retries_without_unknown_column(self, mock_post):
        err = HTTPError("bad request")
        err.response = type(
            "Resp",
            (),
            {
                "status_code": 400,
                "text": "Could not find the 'deribit_pattern' column of 'daily_meta_v2' in the schema cache",
            },
        )()
        mock_post.side_effect = [err, object()]

        base_payload = {"date": "2026-02-20", "meta_score": 55.0}
        optional_payload = {"deribit_state": "CALM", "deribit_pattern": "NONE"}

        post_with_optional_columns("daily_meta_v2", base_payload, optional_payload)

        self.assertEqual(mock_post.call_count, 2)
        second_payload = mock_post.call_args_list[1][0][1]
        self.assertNotIn("deribit_pattern", second_payload)
        self.assertIn("deribit_state", second_payload)

    @patch("meta_daily.supabase_post")
    @patch("meta_daily.load_event")
    def test_run_meta_daily_includes_deribit_fields(self, mock_load_event, mock_post):
        ts = pd.Timestamp(datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc))
        mock_load_event.side_effect = [
            pd.DataFrame({"ts": [ts], "symbol": ["BTCUSDT"], "risk": [1]}),
            pd.DataFrame({"ts": [ts], "symbol": ["BTCUSDT"], "regime": ["CALM"], "mci": [0.2]}),
            pd.DataFrame(),
            pd.DataFrame({"ts": [ts], "vbi_state": ["CALM"], "vbi_pattern": ["NONE"]}),
        ]

        run_meta_daily(datetime.now(timezone.utc), datetime.now(timezone.utc))

        first_call_payload = mock_post.call_args_list[0][0][1]
        self.assertIn("deribit_state", first_call_payload)
        self.assertIn("deribit_state_share", first_call_payload)
        self.assertIn("deribit_pattern", first_call_payload)


if __name__ == "__main__":
    unittest.main()
