import pandas as pd
from loaders import load_event
from supabase import supabase_post

def dominant(series):
    vc = series.value_counts(normalize=True)
    return vc.index[0], round(vc.iloc[0] * 100, 1)

def run_deribit_daily(start, end):
    df = load_event("deribit_vbi_snapshot", start, end)
    if df.empty:
        return

    date_utc = start.date().isoformat()
    ts_from = int(start.timestamp() * 1000)
    ts_to = int(end.timestamp() * 1000)

    for symbol in df["symbol"].unique():
        sub = df[df["symbol"] == symbol]

        state, state_pct = dominant(sub["vbi_state"])
        pattern, pattern_pct = dominant(sub["vbi_pattern"])

        payload = {
            "date_utc": date_utc,
            "symbol": symbol,

            "vbi_state_dominant": state,
            "vbi_state_share_pct": state_pct,
            "vbi_pattern_dominant": pattern,
            "vbi_pattern_share_pct": pattern_pct,

            "near_iv_avg": round(sub["near_iv"].mean(), 2),
            "far_iv_avg": round(sub["far_iv"].mean(), 2),
            "iv_slope_avg": round(sub["iv_slope"].mean(), 2),
            "curvature_avg": round(sub["curvature"].mean(), 2),
            "skew_avg": round(sub["skew"].mean(), 3),

            "ts_from": ts_from,
            "ts_to": ts_to,
        }

        supabase_post("daily_deribit_vbi", payload)
