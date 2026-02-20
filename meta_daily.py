import re

import pandas as pd
from requests import HTTPError

from loaders import load_event
from supabase import supabase_post

ALIGN = 30 * 60 * 1000

META_SCORE_MAP = {
    "TRUE_CALM": 20,
    "CROWD_NO_CONFIRM": 45,
    "OPTION_LED_MOVE": 55,
    "MIXED": 60,
    "HIDDEN_PRESSURE": 80,
    "CONFIRMED_STRESS": 90,
}


def base_meta(row):
    risk = row.get("risk", 0)
    regime = row.get("regime")
    mci = row.get("mci")

    if risk >= 3 and regime == "CALM" and mci and mci >= 0.6:
        return "HIDDEN_PRESSURE"
    if risk >= 3:
        return "CONFIRMED_STRESS"
    if risk <= 1 and regime != "CALM":
        return "OPTION_LED_MOVE"
    if risk == 2 and regime == "CALM":
        return "CROWD_NO_CONFIRM"
    if risk <= 1 and regime == "CALM":
        return "TRUE_CALM"
    return "MIXED"


def trading_session(ts):
    h = ts.hour
    if h < 8:
        return "ASIA"
    if h < 16:
        return "EU"
    return "US"


def dominant_with_pct(series, default_value=None, default_pct=None):
    clean = series.dropna() if hasattr(series, "dropna") else series
    if clean is None or len(clean) == 0:
        return default_value, default_pct
    vc = clean.value_counts(normalize=True)
    return vc.index[0], round(float(vc.iloc[0]) * 100, 1)


def deribit_context_payload(deribit: pd.DataFrame) -> dict:
    if deribit.empty:
        return {
            "deribit_state": None,
            "deribit_state_share": None,
            "deribit_pattern": None,
            "deribit_confidence": None,
        }

    state, state_share = dominant_with_pct(deribit.get("vbi_state"))
    pattern, pattern_share = dominant_with_pct(deribit.get("vbi_pattern"))
    confidence = None
    if state_share is not None and pattern_share is not None:
        confidence = round((state_share + pattern_share) / 2, 1)

    return {
        "deribit_state": state,
        "deribit_state_share": state_share,
        "deribit_pattern": pattern,
        "deribit_confidence": confidence,
    }


def _extract_unknown_column(response_text: str) -> str | None:
    patterns = [
        r"Could not find the '([^']+)' column",
        r'column\s+"([^"]+)"\s+does not exist',
    ]
    for pattern in patterns:
        match = re.search(pattern, response_text)
        if match:
            return match.group(1)
    return None


def post_with_optional_columns(table: str, base_payload: dict, optional_payload: dict):
    payload = {**base_payload, **optional_payload}

    while True:
        try:
            return supabase_post(table, payload)
        except HTTPError as err:
            status = err.response.status_code if err.response is not None else None
            response_text = getattr(err.response, "text", "") if err.response is not None else ""

            if status != 400:
                raise

            unknown_column = _extract_unknown_column(response_text)
            if not unknown_column or unknown_column not in payload:
                raise

            # Retry without columns that are absent in the live table schema.
            payload.pop(unknown_column)


def run_meta_daily(start, end):
    # ---------- LOAD CORE DATA ----------
    risk = load_event("risk_eval", start, end)
    cycle = load_event("options_ticker_cycle", start, end)

    if risk.empty or cycle.empty:
        return

    # ---------- LOAD DIVERGENCES (NEW, SEPARATE) ----------
    divergence = load_event("risk_divergence", start, end)
    deribit = load_event("deribit_vbi_snapshot", start, end)

    # ---------- NORMALIZE TIME ----------
    risk["ts_unix_ms"] = risk["ts"].astype("int64") // 10**6
    cycle["ts_unix_ms"] = cycle["ts"].astype("int64") // 10**6

    if not divergence.empty:
        divergence["ts_unix_ms"] = divergence["ts"].astype("int64") // 10**6

    # ---------- MERGE RISK + OPTIONS ----------
    df = pd.merge_asof(
        risk.sort_values("ts_unix_ms"),
        cycle.sort_values("ts_unix_ms"),
        on="ts_unix_ms",
        by="symbol",
        tolerance=ALIGN,
        direction="backward",
    )

    # ---------- META CORE ----------
    df["meta"] = df.apply(base_meta, axis=1)
    score = round(df["meta"].map(META_SCORE_MAP).mean(), 1)
    dist = df["meta"].value_counts(normalize=True) * 100

    # ---------- DIVERGENCE CONTEXT (NO MERGE) ----------
    if divergence.empty:
        divergence_share = 0.0
        dominant_divergence = None
        divergence_conf_avg = None
    else:
        divergence_share = round(len(divergence) / len(risk) * 100, 1) if not risk.empty else None
        dominant_divergence = divergence["divergence_type"].value_counts().idxmax()
        divergence_conf_avg = round(divergence["confidence"].mean(), 2)

    # ---------- DAILY META PAYLOAD ----------
    payload = {
        "date": end.date().isoformat(),
        "meta_score": score,
        "dominant_meta": dist.idxmax(),
        "share_hidden_pressure": round(dist.get("HIDDEN_PRESSURE", 0), 1),
        "share_confirmed_stress": round(dist.get("CONFIRMED_STRESS", 0), 1),
        "share_true_calm": round(dist.get("TRUE_CALM", 0), 1),
        # --- divergence context (NEW) ---
        "divergence_share": divergence_share,
        "dominant_divergence": dominant_divergence,
        "divergence_confidence_avg": divergence_conf_avg,
    }

    post_with_optional_columns("daily_meta_v2", payload, deribit_context_payload(deribit))

    # ---------- SESSION BREAKDOWN ----------
    ts_col = "ts_x" if "ts_x" in df.columns else "ts"
    if ts_col in df.columns:
        df["session"] = df[ts_col].apply(trading_session)
        for s in ["ASIA", "EU", "US"]:
            sub = df[df["session"] == s]
            if sub.empty:
                continue
            session_dist = sub["meta"].value_counts(normalize=True) * 100
            session_payload = {
                "date": end.date().isoformat(),
                "session": s,
                "meta_score": round(sub["meta"].map(META_SCORE_MAP).mean(), 1),
                "dominant_meta": session_dist.idxmax(),
                "share_hidden_pressure": round(session_dist.get("HIDDEN_PRESSURE", 0), 1),
                "share_confirmed_stress": round(session_dist.get("CONFIRMED_STRESS", 0), 1),
                "share_true_calm": round(session_dist.get("TRUE_CALM", 0), 1),
            }
            supabase_post("daily_meta_sessions", session_payload)
