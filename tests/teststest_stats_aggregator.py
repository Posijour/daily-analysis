from datetime import datetime, timedelta, timezone
import unittest

from stats_aggregator import (
    EventRow,
    compute_conditional_probability,
    compute_event_rate,
    compute_event_share,
    compute_mode_share,
    compute_top_values,
)


class StatsAggregatorTests(unittest.TestCase):
    def test_compute_mode_share_uses_state_transitions(self):
        start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(hours=10)

        events = [
            EventRow(ts=start + timedelta(hours=2), data={"mode": "calm"}),
            EventRow(ts=start + timedelta(hours=7), data={"mode": "risk"}),
        ]

        share = compute_mode_share(events, start, end, field="mode", value="calm", initial_state="risk")
        self.assertAlmostEqual(share, 50.0)

    def test_compute_conditional_probability_optionally_uses_symbol(self):
        t0 = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
        x_events = [
            EventRow(ts=t0, data={}, symbol="BTC"),
            EventRow(ts=t0 + timedelta(hours=2), data={}, symbol="ETH"),
        ]
        y_events = [
            EventRow(ts=t0 + timedelta(minutes=30), data={}, symbol="BTC"),
            EventRow(ts=t0 + timedelta(hours=2, minutes=30), data={}, symbol="BTC"),
        ]

        by_symbol = compute_conditional_probability(
            x_events,
            y_events,
            max_lag=timedelta(hours=1),
            require_same_symbol=True,
        )
        without_symbol = compute_conditional_probability(
            x_events,
            y_events,
            max_lag=timedelta(hours=3),
            require_same_symbol=False,
        )

        self.assertAlmostEqual(by_symbol, 50.0)
        self.assertAlmostEqual(without_symbol, 100.0)

    def test_compute_event_share(self):
        events = [
            EventRow(ts=datetime.now(timezone.utc), data={"result": "Y"}),
            EventRow(ts=datetime.now(timezone.utc), data={"result": "N"}),
            EventRow(ts=datetime.now(timezone.utc), data={"result": "Y"}),
        ]
        self.assertAlmostEqual(compute_event_share(events, "result", "Y"), 66.6666666, places=4)

    def test_compute_event_rate(self):
        start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(days=2)
        events = [EventRow(ts=start, data={}) for _ in range(12)]
        rate = compute_event_rate(events, start, end)
        self.assertEqual(rate["events_count"], 12.0)
        self.assertAlmostEqual(rate["events_per_day"], 6.0)

    def test_compute_top_values(self):
        events = [
            EventRow(ts=datetime.now(timezone.utc), data={"mode": "calm"}),
            EventRow(ts=datetime.now(timezone.utc), data={"mode": "calm"}),
            EventRow(ts=datetime.now(timezone.utc), data={"mode": "risk"}),
        ]
        top = compute_top_values(events, "mode", top_n=2)
        self.assertEqual(top[0]["value"], "calm")
        self.assertEqual(top[0]["count"], 2.0)


if __name__ == "__main__":
    unittest.main()
