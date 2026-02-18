import pandas as pd
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


def run_meta_daily(start, end):
    # ---------- LOAD CORE DATA ----------
    risk = load_event("risk_eval", start, end)
    cycle = load_event("options_ticker_cycle", start, end)

    if risk.empty or cycle.empty:
        return

    # ---------- LOAD DIVERGENCES (NEW, SEPARATE) ----------
    divergence = load_event("risk_divergence", start, end)

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
        divergence_share = (
            round(len(divergence) / len(risk) * 100, 1)
            if not risk.empty else None
        )
        dominant_divergence = (
            divergence["divergence_type"].value_counts().idxmax()
        )
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

    supabase_post("daily_meta_v2", payload)

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
