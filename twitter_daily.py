from requests import HTTPError

from counters import next_counter
from loaders import load_event
from supabase import supabase_post
from config import AUTO_POST_TWITTER


REGIME_INTERPRETATION = {
    "CALM": "Low systemic stress. Crowd positioning balanced.",
    "CROWD_IMBALANCE": (
        "Crowded positioning dominates.\n"
        "Asymmetric risk is building."
    ),
    "STRESS": (
        "Market under systemic stress.\n"
        "Volatility expansion possible."
    ),
}

OPTIONS_INTERPRETATION = {
    "CALM": "IV stable, skew neutral",
    "NEUTRAL": "IV stable, skew neutral",
    "BUILDING": "IV firming, skew bid",
    "STRESS": "IV elevated, skew defensive",
    "UNKNOWN": "IV/skew signal mixed",
}

DERIBIT_VOL_TEXT = {
    "CALM": "flat vol",
    "NEUTRAL": "flat vol",
    "BUILDING": "vol buildup",
    "STRESS": "elevated vol",
    "UNKNOWN": "vol mixed",
}


def detect_activity_regime(alerts_count):
    if alerts_count <= 2:
        return "CALM"
    if alerts_count <= 5:
        return "FRAGILE_CALM"
    return "STRESS"


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


def map_deribit_line(deribit):
    if deribit.empty:
        return "vol mixed, no clear pattern", False

    state, _ = dominant_with_pct(deribit.get("vbi_state"), default_value="UNKNOWN", default_pct=0.0)
    pattern_source = deribit.get("vbi_pattern")
    pattern, pattern_pct = dominant_with_pct(
                pattern_source, default_value="NONE", default_pct=0.0
    )

    state_text = DERIBIT_VOL_TEXT.get(state, DERIBIT_VOL_TEXT["UNKNOWN"])
    has_pre_break = str(pattern).upper() == "PRE-BREAK" and pattern_pct >= 40
    pattern_text = "PRE-BREAK active" if has_pre_break else "no PRE-BREAK"

    return f"{state_text}, {pattern_text}", has_pre_break


# ---------------- ANOMALIES ----------------

def detect_anomaly(start, end):
    alerts = load_event("alert_sent", start, end)

    if alerts.empty or "type" not in alerts.columns:
        return None

    buildup_alerts = alerts[
        (alerts["type"] == "BUILDUP")
    ]

    if buildup_alerts.empty:
        return None

    grouped = (
        buildup_alerts
        .groupby("symbol")
        .size()
        .sort_values(ascending=False)
    )

    if grouped.iloc[0] < 3:
        return None

    symbol = grouped.index[0].replace("USDT", "")

    return f"""Observed anomaly (futures positioning):

{symbol}
– repeated risk buildups
– unstable positioning response

Behavior logged."""


# ---------------- DAILY LOG ----------------

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
    alerts = len(alerts_df)

    dominant_regime = (
        dominant(market["regime"])
        if not market.empty and "regime" in market.columns
        else "UNKNOWN"
    )

    options_regime = (
        dominant(options_market["regime"])
        if not options_market.empty and "regime" in options_market.columns
        else "UNKNOWN"
    )

    options_text = OPTIONS_INTERPRETATION.get(options_regime, "")
    deribit_text, has_pre_break = map_deribit_line(deribit)

    low_stress = dominant_regime in ("CALM", "NEUTRAL") and elevated_share < 25
    summary_line_1 = "Low systemic stress." if low_stress else "Systemic stress is present."

    layers = {dominant_regime, options_regime}
    layers.discard("UNKNOWN")
    balanced_layers = len(layers) <= 1 and not has_pre_break
    summary_line_2 = "Balanced across layers." if balanced_layers else "Layer imbalance detected."

    log_number = next_counter("daily_risk_log")

    text = f"""Risk Log #{log_number} · 24h

Futures: {dominant_regime} ({elevated_share}% | {alerts} buildups)
Options: {options_text}
Deribit: {deribit_text}

{summary_line_1}
{summary_line_2}

Market log, not a forecast.
""".strip()

    return text


# ---------------- MAIN RUN ----------------

def run_twitter_daily(start, end):
    daily_text = generate_daily_log(start, end)

    try:
        supabase_post("twitter_logs", {"text": daily_text})
    except HTTPError as err:
        status = err.response.status_code if err.response is not None else None
        if status in (401, 403, 404):
            response_text = getattr(err.response, "text", "") if err.response is not None else ""
            raise RuntimeError(
                "Failed to write daily Twitter log to Supabase table 'twitter_logs' "
                f"(HTTP {status}). Check SUPABASE_KEY permissions and RLS policies. "
                f"Supabase response: {response_text}"
            ) from err
        raise

    anomaly_text = detect_anomaly(start, end)
    if anomaly_text:
        try:
            supabase_post("twitter_logs", {"text": anomaly_text})
        except HTTPError as err:
            status = err.response.status_code if err.response is not None else None
            if status in (401, 403, 404):
                response_text = getattr(err.response, "text", "") if err.response is not None else ""
                raise RuntimeError(
                    "Failed to write anomaly Twitter log to Supabase table 'twitter_logs' "
                    f"(HTTP {status}). Check SUPABASE_KEY permissions and RLS policies. "
                    f"Supabase response: {response_text}"
                ) from err
            raise

    if AUTO_POST_TWITTER:
        pass  # Twitter API v2
