import pandas as pd
from requests import HTTPError

from loaders import load_event
from supabase import supabase_post


DEFAULT_PATTERN = "NONE"


def dominant(series, default_value="UNKNOWN", default_pct=0.0):
    clean = series.dropna() if hasattr(series, "dropna") else series
    if clean is None or len(clean) == 0:
        return default_value, default_pct
    vc = clean.value_counts(normalize=True)
    return vc.index[0], round(float(vc.iloc[0]) * 100, 1)


def numeric_mean(series, digits):
    if series is None or len(series) == 0:
        return None
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return round(float(values.mean()), digits)


def run_deribit_daily(start, end):
    df = load_event("deribit_vbi_snapshot", start, end)
    if df.empty:
        return

    date_utc = start.date().isoformat()
    ts_from = int(start.timestamp() * 1000)
    ts_to = int(end.timestamp() * 1000)

    for symbol in df["symbol"].dropna().unique():
        sub = df[df["symbol"] == symbol]

        state, state_pct = dominant(sub.get("vbi_state"))

        if "vbi_pattern" in sub.columns:
            pattern, pattern_pct = dominant(sub.get("vbi_pattern"), default_value=DEFAULT_PATTERN, default_pct=0.0)
        else:
            pattern, pattern_pct = DEFAULT_PATTERN, 0.0

        payload = {
            "date_utc": date_utc,
            "symbol": symbol,
            "vbi_state_dominant": state,
            "vbi_state_share_pct": state_pct,
            "vbi_pattern_dominant": pattern,
            "vbi_pattern_share_pct": pattern_pct,
            "near_iv_avg": numeric_mean(sub.get("near_iv"), 2),
            "far_iv_avg": numeric_mean(sub.get("far_iv"), 2),
            "iv_slope_avg": numeric_mean(sub.get("iv_slope"), 2),
            "curvature_avg": numeric_mean(sub.get("curvature"), 2),
            "skew_avg": numeric_mean(sub.get("skew"), 3),
            "ts_from": ts_from,
            "ts_to": ts_to,
        }

        try:
            supabase_post("daily_deribit_vbi", payload, on_conflict="date_utc,symbol")
        except HTTPError as err:
            status = err.response.status_code if err.response is not None else None
            if status in (401, 403):
                try:
                    supabase_post("daily_deribit_vbi", payload, upsert=False)
                except HTTPError as insert_err:
                    insert_status = insert_err.response.status_code if insert_err.response is not None else None
                    if insert_status != 409:
                        raise
            elif status != 409:
                raise
