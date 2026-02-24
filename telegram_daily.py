from requests import HTTPError

from counters import next_counter
from loaders import load_event
from supabase import supabase_post


OPTIONS_IV_TEXT = {
    "CALM": "stable",
    "NEUTRAL": "stable",
    "BUILDING": "firming",
    "STRESS": "elevated",
    "UNKNOWN": "mixed",
}

OPTIONS_SKEW_TEXT = {
    "CALM": "neutral",
    "NEUTRAL": "neutral",
    "BUILDING": "bid",
    "STRESS": "defensive",
    "UNKNOWN": "mixed",
}

DERIBIT_VOL_TEXT = {
    "CALM": "flat",
    "NEUTRAL": "flat",
    "BUILDING": "building",
    "STRESS": "elevated",
    "UNKNOWN": "mixed",
}


def dominant(series, default_value="UNKNOWN"):
    clean = series.dropna() if hasattr(series, "dropna") else series
    if clean is None or len(clean) == 0:
        return default_value
    return clean.value_counts().idxmax()


def dominant_with_pct(series, default_value="UNKNOWN", default_pct=0.0):
    clean = series.dropna() if hasattr(series, "dropna") else series
    if clean is None or len(clean) == 0:
        return default_value, default_pct
    vc = clean.value_counts(normalize=True)
    return vc.index[0], round(vc.iloc[0] * 100, 1)


def map_deribit_summary(deribit):
    if deribit.empty:
        return "mixed", False

    state, _ = dominant_with_pct(deribit.get("vbi_state"), default_value="UNKNOWN", default_pct=0.0)
    pattern_source = deribit.get("vbi_pattern")
    pattern, pattern_pct = dominant_with_pct(
        pattern_source, default_value="NONE", default_pct=0.0
    )

    has_pre_break = str(pattern).upper() == "PRE-BREAK" and pattern_pct >= 40
    return DERIBIT_VOL_TEXT.get(state, DERIBIT_VOL_TEXT["UNKNOWN"]), has_pre_break


def map_notes(futures_regime, options_regime, has_pre_break):
    aligned = futures_regime == options_regime and futures_regime != "UNKNOWN"

    if aligned and not has_pre_break:
        return (
            "This regime remains shallow.\n"
            "Layer alignment persists,\n"
            "but maturity is low."
        )

    if has_pre_break:
        return (
            "This regime is transitioning.\n"
            "Layer alignment is fragile,\n"
            "vol breakout risk is active."
        )

    return (
        "This regime is mixed.\n"
        "Layer alignment is incomplete,\n"
        "conviction remains low."
    )


def generate_daily_log(start, end):
    risk = load_event("risk_eval", start, end)
    alerts_df = load_event("alert_sent", start, end)
    market = load_event("market_regime", start, end)
    options_market = load_event("options_market_state", start, end)
    deribit = load_event("deribit_vbi_snapshot", start, end)

    total = len(risk)
    elevated = 0

    if not risk.empty and "risk" in risk.columns:
        risk["risk"] = risk["risk"].fillna(0)
        elevated = int((risk["risk"] >= 2).sum())

    elevated_share = round(elevated / total * 100, 1) if total else 0
    buildups = len(alerts_df)

    futures_regime = (
        dominant(market["regime"])
        if not market.empty and "regime" in market.columns
        else "UNKNOWN"
    )
    options_regime = (
        dominant(options_market["regime"])
        if not options_market.empty and "regime" in options_market.columns
        else "UNKNOWN"
    )

    options_iv = OPTIONS_IV_TEXT.get(options_regime, OPTIONS_IV_TEXT["UNKNOWN"])
    options_skew = OPTIONS_SKEW_TEXT.get(options_regime, OPTIONS_SKEW_TEXT["UNKNOWN"])

    deribit_vol, has_pre_break = map_deribit_summary(deribit)
    deribit_pre_break_line = "PRE-BREAK patterns active" if has_pre_break else "No PRE-BREAK patterns"

    notes = map_notes(futures_regime, options_regime, has_pre_break)
    log_number = next_counter("daily_telegram_log")

    return f"""Risk Log #{log_number}
24h snapshot

Futures (Binance)
• Elevated risk: {elevated_share}%
• Buildups: {buildups}
• Regime: {futures_regime}

Options (Bybit / OKX)
• Short-dated IV: {options_iv}
• Skew: {options_skew}
• No directional pricing

Deribit (meta)
• Vol term structure {deribit_vol}
• {deribit_pre_break_line}

Notes:
{notes}

Market log.""".strip()


def run_telegram_daily(start, end):
    daily_text = generate_daily_log(start, end)

    try:
        supabase_post("telegram_logs", {"text": daily_text})
    except HTTPError as err:
        status = err.response.status_code if err.response is not None else None
        if status in (401, 403, 404):
            response_text = getattr(err.response, "text", "") if err.response is not None else ""
            raise RuntimeError(
                "Failed to write daily Telegram log to Supabase table 'telegram_logs' "
                f"(HTTP {status}). Check SUPABASE_KEY permissions and RLS policies. "
                f"Supabase response: {response_text}"
            ) from err
        raise
