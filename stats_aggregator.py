from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence


@dataclass(frozen=True)
class EventRow:
    ts: datetime
    data: dict
    symbol: str | None = None


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_mode_share(
    events: Sequence[EventRow],
    window_start: datetime,
    window_end: datetime,
    field: str,
    value: str,
    initial_state: str | None = None,
) -> float:
    """Return percentage of window duration where `data[field] == value`.

    Events are interpreted as state changes. If there is no event exactly at
    window_start, pass initial_state (state before window).
    """
    start = _to_utc(window_start)
    end = _to_utc(window_end)
    if end <= start:
        raise ValueError("window_end must be greater than window_start")

    ordered = sorted((e for e in events if start <= _to_utc(e.ts) <= end), key=lambda e: e.ts)
    current_state = initial_state
    cursor = start
    matched_seconds = 0.0

    for event in ordered:
        event_ts = _to_utc(event.ts)
        if event_ts > cursor and current_state == value:
            matched_seconds += (event_ts - cursor).total_seconds()
        current_state = event.data.get(field)
        cursor = max(cursor, event_ts)

    if cursor < end and current_state == value:
        matched_seconds += (end - cursor).total_seconds()

    return 100.0 * matched_seconds / (end - start).total_seconds()


def compute_conditional_probability(
    x_events: Sequence[EventRow],
    y_events: Sequence[EventRow],
    *,
    max_lag: timedelta,
    require_same_symbol: bool,
) -> float:
    """P(Y | X): share of X events followed by Y within max_lag.

    If require_same_symbol=True, only Y with the same symbol as X are accepted.
    """
    if not x_events:
        return 0.0

    sorted_y = sorted((_to_utc(e.ts), e.symbol) for e in y_events)
    success = 0

    for x in sorted(x_events, key=lambda e: e.ts):
        x_ts = _to_utc(x.ts)
        deadline = x_ts + max_lag
        matched = False
        for y_ts, y_symbol in sorted_y:
            if y_ts <= x_ts:
                continue
            if y_ts > deadline:
                break
            if require_same_symbol and y_symbol != x.symbol:
                continue
            matched = True
            break
        if matched:
            success += 1

    return 100.0 * success / len(x_events)


def compute_event_share(events: Sequence[EventRow], field: str, value: str) -> float:
    """Share of rows where data[field] == value."""
    if not events:
        return 0.0
    matched = sum(1 for event in events if event.data.get(field) == value)
    return 100.0 * matched / len(events)


def compute_event_rate(events: Sequence[EventRow], window_start: datetime, window_end: datetime) -> dict[str, float]:
    """Return events/hour and events/day for a fixed window."""
    start = _to_utc(window_start)
    end = _to_utc(window_end)
    hours = (end - start).total_seconds() / 3600
    if hours <= 0:
        raise ValueError("window_end must be greater than window_start")
    count = len(events)
    return {
        "events_count": float(count),
        "events_per_hour": count / hours,
        "events_per_day": (count / hours) * 24,
    }


def compute_top_values(events: Sequence[EventRow], field: str, top_n: int) -> list[dict[str, float | str]]:
    """Most frequent values for data[field]."""
    values = [str(event.data[field]) for event in events if field in event.data]
    total = len(values)
    if total == 0:
        return []
    counter = Counter(values)
    top_items = counter.most_common(top_n)
    return [
        {
            "value": value,
            "count": float(count),
            "percentage": 100.0 * count / total,
        }
        for value, count in top_items
    ]


def _fetch_logs(event: str, start: datetime, end: datetime, limit: int = 1000) -> list[dict]:
    from config import HEADERS, SUPABASE_URL
    from http_client import request_with_retry

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    cursor_ms = start_ms
    rows: list[dict] = []

    while cursor_ms <= end_ms:
        params = [
            ("select", "*"),
            ("event", f"eq.{event}"),
            ("ts", f"gte.{cursor_ms}"),
            ("ts", f"lte.{end_ms}"),
            ("order", "ts.asc"),
            ("limit", str(limit)),
        ]

        response = request_with_retry(
            "GET",
            f"{SUPABASE_URL}/rest/v1/logs",
            headers=HEADERS,
            params=params,
        )
        batch = response.json()
        if not batch:
            break

        rows.extend(batch)
        last_ts = int(batch[-1]["ts"])
        cursor_ms = max(cursor_ms + 1, last_ts + 1)
        if len(batch) < limit:
            break

    return rows


def _fetch_last_state_before(event: str, before: datetime, field: str) -> str | None:
    from config import HEADERS, SUPABASE_URL
    from http_client import request_with_retry

    params = [
        ("select", "*"),
        ("event", f"eq.{event}"),
        ("ts", f"lt.{int(before.timestamp() * 1000)}"),
        ("order", "ts.desc"),
        ("limit", "1"),
    ]
    response = request_with_retry(
        "GET",
        f"{SUPABASE_URL}/rest/v1/logs",
        headers=HEADERS,
        params=params,
    )
    rows = response.json()
    if not rows:
        return None
    return (rows[0].get("data") or {}).get(field)


def _rows_to_events(rows: Iterable[dict]) -> list[EventRow]:
    result = []
    for row in rows:
        result.append(
            EventRow(
                ts=datetime.fromtimestamp(row["ts"] / 1000, tz=timezone.utc),
                data=row.get("data") or {},
                symbol=row.get("symbol"),
            )
        )
    return result


def _window(days: int) -> tuple[datetime, datetime]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start, end


def _cmd_mode_share(args: argparse.Namespace) -> int:
    start, end = _window(args.days)
    events = _rows_to_events(_fetch_logs(args.event, start, end))
    initial_state = _fetch_last_state_before(args.event, start, args.field)
    percentage = compute_mode_share(events, start, end, args.field, args.value, initial_state)
    print(json.dumps({
        "metric": "mode_share",
        "window_days": args.days,
        "event": args.event,
        "field": args.field,
        "value": args.value,
        "percentage": round(percentage, 2),
    }, ensure_ascii=False))
    return 0


def _cmd_conditional(args: argparse.Namespace) -> int:
    start, end = _window(args.days)
    x_events = _rows_to_events(_fetch_logs(args.event_x, start, end))
    y_events = _rows_to_events(_fetch_logs(args.event_y, start, end))
    percentage = compute_conditional_probability(
        x_events,
        y_events,
        max_lag=timedelta(hours=args.max_lag_hours),
        require_same_symbol=args.same_symbol,
    )
    print(json.dumps({
        "metric": "conditional_probability",
        "window_days": args.days,
        "event_x": args.event_x,
        "event_y": args.event_y,
        "max_lag_hours": args.max_lag_hours,
        "same_symbol": args.same_symbol,
        "percentage": round(percentage, 2),
    }, ensure_ascii=False))
    return 0


def _cmd_event_share(args: argparse.Namespace) -> int:
    start, end = _window(args.days)
    events = _rows_to_events(_fetch_logs(args.event, start, end))
    percentage = compute_event_share(events, args.field, args.value)
    print(json.dumps({
        "metric": "event_share",
        "window_days": args.days,
        "event": args.event,
        "field": args.field,
        "value": args.value,
        "percentage": round(percentage, 2),
        "sample_size": len(events),
    }, ensure_ascii=False))
    return 0


def _cmd_event_rate(args: argparse.Namespace) -> int:
    start, end = _window(args.days)
    events = _rows_to_events(_fetch_logs(args.event, start, end))
    rates = compute_event_rate(events, start, end)
    print(json.dumps({
        "metric": "event_rate",
        "window_days": args.days,
        "event": args.event,
        "events_count": int(rates["events_count"]),
        "events_per_hour": round(rates["events_per_hour"], 4),
        "events_per_day": round(rates["events_per_day"], 2),
    }, ensure_ascii=False))
    return 0


def _cmd_top_values(args: argparse.Namespace) -> int:
    start, end = _window(args.days)
    events = _rows_to_events(_fetch_logs(args.event, start, end))
    top = compute_top_values(events, args.field, args.top_n)
    print(json.dumps({
        "metric": "top_values",
        "window_days": args.days,
        "event": args.event,
        "field": args.field,
        "top": [
            {
                "value": item["value"],
                "count": int(item["count"]),
                "percentage": round(float(item["percentage"]), 2),
            }
            for item in top
        ],
    }, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggregate behaviour stats from logs (mode shares, probabilities, rates, distributions)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    mode_share = subparsers.add_parser("mode-share", help="Share of time spent in a given mode.")
    mode_share.add_argument("--event", required=True, help="Event name storing mode transitions")
    mode_share.add_argument("--field", default="mode", help="Field inside data payload")
    mode_share.add_argument("--value", required=True, help="Target mode value")
    mode_share.add_argument("--days", type=int, default=7, help="Window size in days")
    mode_share.set_defaults(func=_cmd_mode_share)

    conditional = subparsers.add_parser("conditional", help="P(Y|X) over a recent period.")
    conditional.add_argument("--event-x", required=True, help="Condition event X")
    conditional.add_argument("--event-y", required=True, help="Outcome event Y")
    conditional.add_argument("--days", type=int, default=30, help="Window size in days")
    conditional.add_argument("--max-lag-hours", type=int, default=24, help="Maximum allowed delay between X and Y")
    conditional.add_argument("--same-symbol", action="store_true", help="Require same symbol for X and Y")
    conditional.set_defaults(func=_cmd_conditional)

    event_share = subparsers.add_parser("event-share", help="Share of events where field equals a value.")
    event_share.add_argument("--event", required=True)
    event_share.add_argument("--field", required=True)
    event_share.add_argument("--value", required=True)
    event_share.add_argument("--days", type=int, default=30)
    event_share.set_defaults(func=_cmd_event_share)

    event_rate = subparsers.add_parser("event-rate", help="Event frequency metrics (per hour/day).")
    event_rate.add_argument("--event", required=True)
    event_rate.add_argument("--days", type=int, default=30)
    event_rate.set_defaults(func=_cmd_event_rate)

    top_values = subparsers.add_parser("top-values", help="Top values distribution for a payload field.")
    top_values.add_argument("--event", required=True)
    top_values.add_argument("--field", required=True)
    top_values.add_argument("--top-n", type=int, default=5)
    top_values.add_argument("--days", type=int, default=30)
    top_values.set_defaults(func=_cmd_top_values)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
