# loaders.py
import pandas as pd
from supabase import supabase_get

def load_event(event, start, end):
    params = {
        "select": "*",
        "event": f"eq.{event}",
        "ts": f"gte.{int(start.timestamp()*1000)}",
    }
    rows = supabase_get("logs", params)
    out = []
    for r in rows:
        d = r.get("data") or {}
        d["symbol"] = r.get("symbol")
        d["ts"] = pd.to_datetime(r["ts"], unit="ms", utc=True)
        out.append(d)
    return pd.DataFrame(out)
