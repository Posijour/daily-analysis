import pandas as pd
from requests import HTTPError
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

    session_stats = (
        r.assign(buildup=(r["risk"] >= 2).astype(int))
        .groupby("session")
        .agg(total=("risk", "size"), buildups=("buildup", "sum"))
    )

    sessions_counts = {}
    sessions_pct = {}
    for s in ["ASIA", "EU", "US"]:
        if s not in session_stats.index:
            continue
        row = session_stats.loc[s]
        sessions_counts[s] = {
            "total": int(row["total"]),
            "buildups": int(row["buildups"]),
        }
        sessions_pct[s] = {
            "of_day": round(row["total"] / total * 100, 2),
            "buildups_in_session": round(row["buildups"] / row["total"] * 100, 2),
        }

    risk_0_pct = round((r["risk"] == 0).sum() / total * 100, 2)
    risk_2plus_pct = round((r["risk"] >= 2).sum() / total * 100, 2)

    payload = {
        "date": end.date().isoformat(),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "total_risk_evals": total,
        "risk_distribution_counts": dist_counts.to_dict(),
        "risk_distribution_pct": dist_pct.to_dict(),
        "risk_0_pct": risk_0_pct,
        "risk_2plus_pct": risk_2plus_pct,
        "sessions_counts": sessions_counts,
        "sessions_pct": sessions_pct,
    }

    try:
        supabase_post("daily_risk_snapshot", payload, upsert=False)
    except HTTPError as err:
        status = err.response.status_code if err.response is not None else None
        if status not in (400, 401, 403, 404, 409):
            raise
        details = ""
        if err.response is not None:
            details = err.response.text[:500]
        print(f"Risk daily snapshot skipped (HTTP {status}): {details}")
