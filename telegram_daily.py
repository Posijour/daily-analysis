from requests import HTTPError

from counters import next_counter
from loaders import load_event
from supabase import supabase_post
from config import AUTO_POST_TELEGRAM, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from telegram_api import post_telegram_message
from twitter_daily import build_options_summary, build_deribit_summary


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


def _first_value(df, column):
    if df.empty or column not in df.columns:
        return None
    values = df[column].dropna()
    if len(values) == 0:
        return None
    return values.iloc[0]


def _map_options_text(options_summary):
    summary_text = (options_summary.get("summary_text") or "").strip()
    if summary_text:
        return f"IV/skew: {summary_text}"

    summary_class = options_summary.get("summary_class")
    bias_direction = options_summary.get("bias_direction")

    options_map = {
        ("no_signal", "neutral"): "IV/skew: no clear signal",
        ("weak_bias", "bearish"): "IV/skew: mild bearish bias",
        ("weak_bias", "calm"): "IV/skew: mild calm/compression bias",
        ("weak_bias", "compression"): "IV/skew: mild calm/compression bias",
        ("conflict", "unknown"): "IV/skew: conflicting signal",
        ("strong_bias", "calm"): "IV/skew: strong calm state",
        ("strong_bias", "bearish"): "IV/skew: strong bearish bias",
    }
    return options_map.get((summary_class, bias_direction), "IV/skew: no clear signal")


def _map_deribit_text(deribit_summary):
    summary_text = (deribit_summary.get("summary_text") or "").strip()
    if summary_text:
        return f"Vol state: {summary_text}"

    summary_class = deribit_summary.get("summary_class")
    bias_direction = deribit_summary.get("bias_direction")

    deribit_map = {
        ("no_signal", "neutral"): "Vol state: neutral",
        ("weak_bias", "expansion"): "Vol state: mild expansion bias",
        ("strong_bias", "expansion"): "Vol state: strong expansion",
        ("conflict", "unknown"): "Vol state: conflict",
        ("pre_break", "breakout_risk"): "Vol state: PRE-BREAK conditions present",
        ("pre_break", "breakdown_risk"): "Vol state: PRE-BREAK conditions present",
    }
    return deribit_map.get((summary_class, bias_direction), "Vol state: neutral")


def _map_notes_text(futures_regime, elevated_share, options_summary, deribit_summary, final_summary_text):
    if final_summary_text:
        return final_summary_text.strip()

    low_stress = futures_regime in ("CALM", "NEUTRAL") and elevated_share < 25
    layers = {futures_regime, options_summary.get("summary_class"), deribit_summary.get("summary_class")}
    layers.discard("UNKNOWN")
    balanced_layers = len(layers) <= 2 and deribit_summary.get("summary_class") != "pre_break"

    if low_stress and balanced_layers:
        return "Low systemic stress.\nBalanced across layers."
    if low_stress and not balanced_layers:
        return "Low systemic stress.\nLayer imbalance detected."
    return "Systemic stress is building.\nLayer alignment remains partial."


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
    options_summary = build_options_summary(options_market)
    deribit_summary = build_deribit_summary(deribit)

    options_line = _map_options_text(options_summary)
    deribit_vol_line = _map_deribit_text(deribit_summary)

    pattern_source = deribit.get("vbi_pattern")
    pattern, pattern_pct = dominant_with_pct(pattern_source, default_value="NONE", default_pct=0.0)
    has_pre_break = str(pattern).upper() == "PRE-BREAK" and pattern_pct >= 40
    if deribit_summary.get("summary_class") == "pre_break":
        has_pre_break = True
    deribit_pre_break_line = "PRE-BREAK: present" if has_pre_break else "PRE-BREAK: not detected"

    final_summary_text = _first_value(options_market, "final_summary_text")
    notes = _map_notes_text(
        futures_regime=futures_regime,
        elevated_share=elevated_share,
        options_summary=options_summary,
        deribit_summary=deribit_summary,
        final_summary_text=final_summary_text,
    )

    print(
        "Telegram daily summary debug:",
        {
            "options_summary_class": options_summary.get("summary_class"),
            "options_bias_direction": options_summary.get("bias_direction"),
            "options_summary_text": options_summary.get("summary_text"),
            "deribit_summary_class": deribit_summary.get("summary_class"),
            "deribit_bias_direction": deribit_summary.get("bias_direction"),
            "deribit_summary_text": deribit_summary.get("summary_text"),
            "notes_text": notes,
        },
    )
    log_number = next_counter("tg_daily_log")

    return f"""Risk Log #{log_number}
24h snapshot

Futures (Binance)
• Elevated risk: {elevated_share}%
• Buildups: {buildups}
• Regime: {futures_regime}

Options (Bybit / OKX)
• {options_line}

Deribit (meta)
• {deribit_vol_line}
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

    if AUTO_POST_TELEGRAM:
        missing = [
            name
            for name, value in [
                ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
                ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
            ]
            if not value
        ]
        if missing:
            raise RuntimeError(
                "AUTO_POST_TELEGRAM is enabled but missing Telegram credentials: " + ", ".join(missing)
            )

        try:
            post_telegram_message(
                daily_text,
                bot_token=TELEGRAM_BOT_TOKEN,
                chat_id=TELEGRAM_CHAT_ID,
            )
        except HTTPError as err:
            status = err.response.status_code if err.response is not None else None
            response_text = getattr(err.response, "text", "") if err.response is not None else ""
            raise RuntimeError(
                "Failed to auto-post daily Telegram log "
                f"(HTTP {status}). Check Telegram bot token/chat id and bot permissions. "
                f"Telegram response: {response_text}"
            ) from err
