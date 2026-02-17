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

REGIME_IMPLICATION = {
    "CALM": "Implication: stable conditions with low tolerance for complacency.",
    "CROWD_IMBALANCE": "Implication: reduced tolerance for aggressive exposure.",
    "STRESS": "Implication: errors are likely to compound.",
    "UNKNOWN": "Implication: market conditions unclear. Risk framing required.",
}

OPTIONS_INTERPRETATION = {
    "CALM": "Volatility priced neutrally. No directional stress.",
    "NEUTRAL": "Volatility priced neutrally. No directional stress.",
    "BUILDING": "Volatility starting to reprice. Early risk premium forming.",
    "STRESS": "Options pricing reflects elevated tail risk.",
    "UNKNOWN": "Options market provides no clear signal.",
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


def generate_daily_log(start, end):
    risk = load_event("risk_eval", start, end)
    alerts_df = load_event("alert_sent", start, end)
    market = load_event("market_regime", start, end)

    options_cycle = load_event("options_ticker_cycle", start, end)
    options_market = load_event("options_market_state", start, end)

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

    activity_regime = detect_activity_regime(alerts)

    interpretation = REGIME_INTERPRETATION.get(dominant_regime, "")
    implication = REGIME_IMPLICATION.get(
        dominant_regime,
        REGIME_IMPLICATION["UNKNOWN"],
    )

    options_regime = (
        dominant(options_market["regime"])
        if not options_market.empty and "regime" in options_market.columns
        else "UNKNOWN"
    )

    options_text = OPTIONS_INTERPRETATION.get(options_regime, "")

    log_number = next_counter("daily_risk_log")

    text = f"""Risk Log #{log_number}

24h snapshot

• Elevated risk: {elevated_share}%
• Buildups: {alerts}
• Market regime: {dominant_regime}
• Activity regime: {activity_regime}

{interpretation}
{options_text}

{implication}

Market log, not a forecast.
""".strip()

    return text


def run_twitter_daily(start, end):
    text = generate_daily_log(start, end)

    try:
        supabase_post("twitter_logs", {"text": text})
    except HTTPError as err:
        status = err.response.status_code if err.response is not None else None

        # Twitter-лог не должен ронять весь daily job
        if status not in (401, 403, 404):
            raise

    if AUTO_POST_TWITTER:
        pass  # Twitter API v2
