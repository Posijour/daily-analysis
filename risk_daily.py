import pandas as pd
from loaders import load_event
from supabase import supabase_post

def trading_session(ts):
    h = ts.hour
    if h < 8:
        return "ASIA"
    if h < 16:
        return "EU"
    return "US"

def run_risk_daily(start, end):
    df = load_event("risk_eval", start, end)
    if df.empty:
        return

    r = df.copy()
    r["risk"] = pd.to_numeric(r.get("risk", 0), errors="coerce").fillna(0)
    r["session"] = r["ts"].apply(trading_session)

    total = len(r)
    dist_counts = r["risk"].value_counts().sort_index()
    dist_pct = (dist_counts / total * 100).round(2)

    sessions_counts = {}
    sessions_pct = {}

    for s in ["ASIA", "EU", "US"]:
        sub = r[r["session"] == s]
        if sub.empty:
            continue
        sessions_counts[s] = {
            "total": int(len(sub)),
            "buildups": int((sub["risk"] >= 2).sum()),
        }
        sessions_pct[s] = {
            "of_day": round(len(sub) / total * 100, 2),
            "buildups_in_session": round(
                (sub["risk"] >= 2).sum() / len(sub) * 100, 2
            ),
        }

    payload = {
        "date": end.date().isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),

        "total_risk_evals": total,
        "risk_distribution_counts": dist_counts.to_dict(),
        "risk_distribution_pct": dist_pct.to_dict(),
        "sessions_counts": sessions_counts,
        "sessions_pct": sessions_pct,
    }

    supabase_post("daily_risk_snapshot", payload)
