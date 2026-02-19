# loaders.py
import pandas as pd

from config import SUPABASE_URL, HEADERS
from http_client import request_with_retry
from runtime_metrics import METRICS


_RUN_CACHE: dict[tuple[str, int, int], pd.DataFrame] = {}


def _coerce_rows(batch):
    rows = []
    for row in batch:
        d = row.get("data") or {}
        d["symbol"] = row.get("symbol")
        d["ts"] = pd.to_datetime(row["ts"], unit="ms", utc=True)
        d["id"] = row.get("id")
        rows.append(d)
    return rows


def load_event(event: str, start, end) -> pd.DataFrame:
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)
    cache_key = (event, start_ts, end_ts)
    if cache_key in _RUN_CACHE:
        return _RUN_CACHE[cache_key].copy()

    rows = []
    limit = 1000
    cursor_ts = start_ts

    while True:
        params = [
            ("select", "*"),
            ("event", f"eq.{event}"),
            ("ts", f"gte.{cursor_ts}"),
            ("ts", f"lte.{end_ts}"),
            ("order", "ts.asc"),
            ("limit", limit),
        ]

        METRICS.request_count += 1
        r = request_with_retry(
            "GET",
            f"{SUPABASE_URL}/rest/v1/logs",
            headers=HEADERS,
            params=params,
        )

        batch = r.json()
        if not batch:
            break

        chunk_rows = _coerce_rows(batch)
        rows.extend(chunk_rows)
        METRICS.payload_rows_in += len(chunk_rows)

        last_ts = int(pd.Timestamp(chunk_rows[-1]["ts"]).timestamp() * 1000)
        if last_ts == cursor_ts and len(batch) >= limit:
            cursor_ts += 1
        else:
            cursor_ts = last_ts + 1

        if len(batch) < limit or cursor_ts > end_ts:
            break

    df = pd.DataFrame(rows)
    _RUN_CACHE[cache_key] = df
    return df.copy()
