from datetime import datetime, timedelta, timezone
import unittest

import pandas as pd
from requests import HTTPError

from internal_aggregates_daily import (
    _build_layer_metrics,
    _window_alignment_metrics,
    run_internal_aggregates_daily,
)


class InternalAggregatesTests(unittest.TestCase):
    def test_build_layer_metrics_contains_shares_transitions_and_failed_follow(self):
        start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(hours=4)
        df = pd.DataFrame(
            {
                "ts": [
                    start,
                    start + timedelta(hours=1),
                    start + timedelta(hours=2),
                    start + timedelta(hours=3),
                ],
                "regime": ["CALM", "RISK", "CALM", "CALM"],
            }
        )

        metrics = _build_layer_metrics(df, "regime", start, end)

        self.assertEqual(metrics.longest_state, "CALM")
        self.assertEqual(metrics.longest_state_seconds, 3600)
        self.assertEqual(metrics.transition_map_counts["CALM->RISK"], 1)
        self.assertEqual(metrics.transition_map_counts["RISK->CALM"], 1)
        self.assertEqual(metrics.failed_follow_through, 1)
        self.assertAlmostEqual(metrics.transitions_per_24h, 12.0)

    def test_window_alignment_metrics(self):
        df = pd.DataFrame(
            {
                "window_alignment": ["aligned", "conflicted", "aligned", None],
                "is_aligned": [None, None, None, False],
            }
        )
        metrics = _window_alignment_metrics(df)
        self.assertEqual(metrics["aligned"], 2)
        self.assertEqual(metrics["conflicted"], 2)

    def test_run_internal_aggregates_daily_writes_structured_rows(self):
        start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(days=1)

        cycle_df = pd.DataFrame(
            {
                "ts": [start, start + timedelta(hours=12)],
                "regime": ["CALM", "RISK"],
                "window_alignment": ["aligned", "conflicted"],
            }
        )
        market_df = pd.DataFrame(
            {
                "ts": [start, start + timedelta(hours=6), start + timedelta(hours=18)],
                "volatility_state": ["LOW", "HIGH", "HIGH"],
            }
        )
        divergence_df = pd.DataFrame({"ts": [start + timedelta(hours=8)]})

        def fake_load_event(event, _start, _end):
            return {
                "options_ticker_cycle": cycle_df,
                "options_market_state": market_df,
                "risk_divergence": divergence_df,
            }[event]

        recorded = {}

        def fake_supabase_post(table, payload, upsert=False):
            recorded["table"] = table
            recorded["payload"] = payload
            recorded["upsert"] = upsert

        run_internal_aggregates_daily(
            start,
            end,
            load_event_fn=fake_load_event,
            supabase_post_fn=fake_supabase_post,
        )

        self.assertEqual(recorded["table"], "daily_aggregates")
        self.assertFalse(recorded["upsert"])
        rows = recorded["payload"]
        self.assertTrue(all(set(row.keys()) == {"date", "layer", "metric", "value"} for row in rows))


    def test_run_internal_aggregates_daily_skips_expected_http_errors(self):
        start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(days=1)

        cycle_df = pd.DataFrame({"ts": [start], "regime": ["CALM"]})
        market_df = pd.DataFrame({"ts": [start], "volatility_state": ["LOW"]})
        divergence_df = pd.DataFrame({"ts": [start]})

        def fake_load_event(event, _start, _end):
            return {
                "options_ticker_cycle": cycle_df,
                "options_market_state": market_df,
                "risk_divergence": divergence_df,
            }[event]

        err = HTTPError("bad request")
        err.response = type("Resp", (), {"status_code": 400, "text": "invalid input syntax"})()

        def failing_supabase_post(_table, _payload, upsert=False):
            raise err

        # Should not raise for expected schema/permission errors.
        run_internal_aggregates_daily(
            start,
            end,
            load_event_fn=fake_load_event,
            supabase_post_fn=failing_supabase_post,
        )


if __name__ == "__main__":
    unittest.main()
