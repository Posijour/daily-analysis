# risk_divergence_daily.py
from loaders import load_event
from supabase import supabase_post


def run_risk_divergence_daily(start, end):
    df = load_event("risk_divergence", start, end)
    if df.empty:
        return

    rows = []
    for r in df.itertuples(index=False):
        ts = getattr(r, "ts")
        rows.append({
            "ts": ts.isoformat(),
            "date": ts.date().isoformat(),
            "symbol": getattr(r, "symbol", None),
            "divergence_type": getattr(r, "divergence_type", None) or getattr(r, "type", None),
            "risk": int(getattr(r, "risk", 0) or 0),
            "price": getattr(r, "price", None),
        })

    if rows:
        supabase_post("daily_risk_divergences", rows, upsert=False)
