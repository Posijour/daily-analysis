# loaders.py
import requests
import pandas as pd
from config import SUPABASE_URL, HEADERS


def load_event(event: str, start, end) -> pd.DataFrame:
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)

    rows = []
    offset = 0
    limit = 1000

    while True:
        params = [
            ("select", "*"),
            ("event", f"eq.{event}"),
            ("ts", f"gte.{start_ts}"),
            ("ts", f"lte.{end_ts}"),
            ("order", "ts.asc"),
            ("limit", limit),
            ("offset", offset),
        ]

        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/logs",
            headers=HEADERS,
            params=params,
            timeout=30,
        )
        r.raise_for_status()

        batch = r.json()
        if not batch:
            break

        for row in batch:
            d = row.get("data") or {}
            d["symbol"] = row.get("symbol")
            d["ts"] = pd.to_datetime(row["ts"], unit="ms", utc=True)
            rows.append(d)

        if len(batch) < limit:
            break

        offset += limit

    return pd.DataFrame(rows)
