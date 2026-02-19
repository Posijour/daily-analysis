# risk_divergence_daily.py
import pandas as pd
from loaders import load_event
from supabase import supabase_post


def run_risk_divergence_daily(start, end):
    df = load_event("risk_divergence", start, end)
    if df.empty:
        return

    rows = []

    for _, r in df.iterrows():
        rows.append({
            "ts": r["ts"].isoformat(),
            "date": r["ts"].date().isoformat(),

            "symbol": r.get("symbol"),

            "divergence_type": r.get("divergence_type") or r.get("type"),
            "risk": int(r.get("risk", 0)),
            "price": r.get("price"),
        })

    for payload in rows:
        supabase_post(
            "daily_risk_divergences",
            payload,
        )
