import pandas as pd
from requests import HTTPError
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

        try:
            supabase_post("daily_deribit_vbi", payload, on_conflict="date_utc,symbol")
        except HTTPError as err:
            status = err.response.status_code if err.response is not None else None

            # Некоторые ключи Supabase (publishable/anon) не имеют прав на upsert через on_conflict.
            # В таком случае делаем обычный insert и молча пропускаем дубликат (409).
            if status in (401, 403):
                try:
                    supabase_post("daily_deribit_vbi", payload, upsert=False)
                except HTTPError as insert_err:
                    insert_status = (
                        insert_err.response.status_code
                        if insert_err.response is not None
                        else None
                    )
                    if insert_status != 409:
                        raise
            elif status != 409:
                raise
