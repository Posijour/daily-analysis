from requests import HTTPError

from counters import next_counter
from loaders import load_event
from supabase import supabase_post
from config import (
    AUTO_POST_TWITTER,
    TWITTER_API_KEY,
    TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
)
from twitter_api import post_tweet


OPTIONS_SUMMARY_TEMPLATES = {
    "no_signal": {
        "neutral": ["no clear IV/skew signal", "options neutral", "IV/skew mostly neutral"],
        "unknown": ["no clear IV/skew signal"],
    },
    "conflict": {
        "unknown": ["IV/skew conflict across expiries", "options signal split", "mixed options signal"],
    },
    "weak_bias": {
        "bearish": ["slight bearish IV/skew bias", "mild bearish options tilt"],
        "bullish": ["slight bullish IV/skew bias", "mild bullish options tilt"],
        "calm": ["mild calm bias", "options leaning calm"],
        "compression": ["mild calm/compression bias", "weak compression bias"],
        "expansion": ["weak expansion bias", "mild expansion pressure in options"],
        "unknown": ["weak options bias"],
    },
    "strong_bias": {
        "bearish": ["clear bearish IV/skew pressure", "strong bearish options signal"],
        "bullish": ["clear bullish IV/skew pressure", "strong bullish options signal"],
        "calm": ["strong calm signal", "clear calm options backdrop"],
        "compression": ["strong calm/compression signal", "clear compression signal in options"],
        "expansion": ["clear expansion bias", "strong expansion options signal"],
        "unknown": ["strong options bias"],
    },
}

DERIBIT_SUMMARY_TEMPLATES = {
    "no_signal": {
        "neutral": ["vol neutral", "vol mostly balanced", "no clear vol signal"],
        "unknown": ["no clear vol signal"],
    },
    "conflict": {
        "unknown": ["vol mixed", "vol signal conflict", "cross-metric vol conflict"],
    },
    "weak_bias": {
        "expansion": ["mild vol expansion bias", "slight expansion tilt in vol"],
        "compression": ["mild compression bias", "soft compression backdrop"],
        "warm": ["slight warm-up in vol backdrop", "mild warm vol state"],
        "unknown": ["mild vol bias"],
    },
    "strong_bias": {
        "expansion": ["clear vol expansion signal", "strong expansion vol state"],
        "compression": ["clear compression backdrop", "strong compression vol signal"],
        "warm": ["strong warm vol state", "clear warm vol regime"],
        "unknown": ["strong vol bias"],
    },
    "pre_break": {
        "breakout_risk": ["PRE-BREAK conditions present", "vol setup approaching break", "pre-break volatility setup"],
        "breakdown_risk": ["PRE-BREAK downside conditions present", "downside pre-break volatility setup"],
        "unknown": ["PRE-BREAK conditions present"],
    },
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


def _safe_upper(value, default="UNKNOWN"):
    if value is None:
        return default
    text = str(value).strip()
    return text.upper() if text else default


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value):
    return max(0.0, min(1.0, float(value)))


def _mean_abs(frame, column, scale):
    if frame.empty or column not in frame.columns:
        return 0.0
    values = frame[column].dropna()
    if len(values) == 0:
        return 0.0
    return _clamp(float(values.abs().mean()) / scale)


def _pick_template(templates, summary_class, bias_direction, strength_score):
    class_templates = templates.get(summary_class, {})
    variants = class_templates.get(bias_direction) or class_templates.get("unknown") or ["signal unavailable"]
    idx = min(int(_clamp(strength_score) * len(variants)), len(variants) - 1)
    return variants[idx]


def _normalize_options_direction(value):
    regime = _safe_upper(value)
    if regime in {"STRESS", "DIRECTIONAL_DOWN"}:
        return "bearish"
    if regime in {"BUILDING", "DIRECTIONAL_UP"}:
        return "bullish"
    if regime in {"CALM", "NEUTRAL"}:
        return "calm"
    if regime in {"OVERCOMPRESSED", "COMPRESSION"}:
        return "compression"
    if regime in {"RELEASING", "EXPANSION"}:
        return "expansion"
    return "unknown"


def _normalize_deribit_direction(value):
    state = _safe_upper(value)
    if state in {"STRESS", "HOT", "EXPANSION"}:
        return "expansion"
    if state in {"CALM", "NEUTRAL"}:
        return "compression"
    if state in {"BUILDING", "WARM"}:
        return "warm"
    return "unknown"


def build_options_summary(options_market):
    if options_market.empty:
        return {
            "summary_class": "no_signal",
            "bias_direction": "neutral",
            "strength_score": 0.0,
            "reason_flags": ["no_options_rows"],
            "summary_text": _pick_template(OPTIONS_SUMMARY_TEMPLATES, "no_signal", "neutral", 0.0),
            "old_state": "UNKNOWN",
        }

    old_state = _safe_upper(dominant(options_market.get("regime"), default_value="UNKNOWN"))
    near_state = _safe_upper(dominant(options_market.get("near_expiry_state"), default_value=old_state))
    mid_state = _safe_upper(dominant(options_market.get("mid_expiry_state"), default_value=old_state))

    near_dir = _normalize_options_direction(near_state)
    mid_dir = _normalize_options_direction(mid_state)

    reason_flags = []
    if near_dir == mid_dir and near_dir != "unknown":
        reason_flags.append("near_mid_agree")
    elif near_dir != "unknown" and mid_dir != "unknown" and near_dir != mid_dir:
        reason_flags.append("near_mid_conflict")

    mci_mean = _safe_float(options_market["mci"].mean() if "mci" in options_market.columns else None)
    slope_mean = _safe_float(options_market["mci_slope"].mean() if "mci_slope" in options_market.columns else None)
    confidence_mean = _safe_float(options_market["confidence"].mean() if "confidence" in options_market.columns else None)

    if mci_mean > 0.1:
        reason_flags.append("mci_positive")
    elif mci_mean < -0.1:
        reason_flags.append("mci_negative")

    if confidence_mean < 0.45:
        reason_flags.append("low_confidence")

    if slope_mean > 0.01:
        reason_flags.append("slope_up")
    elif slope_mean < -0.01:
        reason_flags.append("slope_down")

    if "divergence" in options_market.columns:
        div = options_market["divergence"].dropna().astype(str).str.upper()
        if len(div) and div.isin({"STRONG", "CONFLICT", "SPLIT"}).mean() >= 0.4:
            reason_flags.append("bybit_okx_conflict")

    candidates = [d for d in (near_dir, mid_dir) if d != "unknown"]
    if mci_mean > 0.15:
        candidates.append("bullish")
    elif mci_mean < -0.15:
        candidates.append("bearish")
    if slope_mean > 0.015:
        candidates.append("expansion")
    elif slope_mean < -0.015:
        candidates.append("compression")

    if candidates:
        counts = {}
        for item in candidates:
            counts[item] = counts.get(item, 0) + 1
        bias_direction = max(counts, key=counts.get)
        dominance = counts[bias_direction] / len(candidates)
    else:
        bias_direction = "neutral"
        dominance = 0.0

    skew_strength = _mean_abs(options_market, "skew", 0.12)
    credit_strength = _mean_abs(options_market, "credit", 0.12)
    mci_strength = _clamp(abs(mci_mean) / 1.0)
    slope_strength = _clamp(abs(slope_mean) / 0.05)
    confidence_component = _clamp(confidence_mean)

    conflict = "near_mid_conflict" in reason_flags or "bybit_okx_conflict" in reason_flags
    strength_score = _clamp(
        0.24 * skew_strength
        + 0.22 * credit_strength
        + 0.22 * mci_strength
        + 0.16 * slope_strength
        + 0.16 * confidence_component
        + (0.12 if "near_mid_agree" in reason_flags else 0.0)
        - (0.18 if conflict else 0.0)
    )

    if bias_direction == "neutral" and strength_score < 0.35:
        summary_class = "no_signal"
    elif conflict and strength_score >= 0.2:
        summary_class = "conflict"
    elif strength_score >= 0.7 and dominance >= 0.6 and confidence_mean >= 0.5 and not conflict:
        summary_class = "strong_bias"
    elif strength_score >= 0.3 and bias_direction != "neutral":
        summary_class = "weak_bias"
    elif conflict:
        summary_class = "conflict"
    else:
        summary_class = "no_signal"

    if summary_class == "no_signal":
        bias_direction = "neutral"
    if summary_class == "conflict":
        bias_direction = "unknown"

    return {
        "summary_class": summary_class,
        "bias_direction": bias_direction,
        "strength_score": round(strength_score, 3),
        "reason_flags": sorted(set(reason_flags)),
        "summary_text": _pick_template(OPTIONS_SUMMARY_TEMPLATES, summary_class, bias_direction, strength_score),
        "old_state": old_state,
    }


def build_deribit_summary(deribit):
    if deribit.empty:
        return {
            "summary_class": "no_signal",
            "bias_direction": "neutral",
            "strength_score": 0.0,
            "reason_flags": ["no_deribit_rows"],
            "summary_text": _pick_template(DERIBIT_SUMMARY_TEMPLATES, "no_signal", "neutral", 0.0),
            "old_state": "UNKNOWN",
        }

    old_state = _safe_upper(dominant(deribit.get("vbi_state"), default_value="UNKNOWN"))
    bias_direction = _normalize_deribit_direction(old_state)

    pattern, pattern_pct = dominant_with_pct(deribit.get("vbi_pattern"), default_value="NONE", default_pct=0.0)
    has_pre_break = _safe_upper(pattern) == "PRE-BREAK" and pattern_pct >= 40

    slope_mean = _safe_float(deribit["iv_slope"].mean() if "iv_slope" in deribit.columns else None)
    skew_mean = _safe_float(deribit["skew"].mean() if "skew" in deribit.columns else None)
    curvature_mean = _safe_float(deribit["curvature"].mean() if "curvature" in deribit.columns else None)
    vbi_score_mean = _safe_float(deribit["vbi_score"].mean() if "vbi_score" in deribit.columns else None)

    reason_flags = []
    if slope_mean > 0.008:
        reason_flags.append("iv_slope_up")
    elif slope_mean < -0.008:
        reason_flags.append("iv_slope_down")

    if abs(curvature_mean) < 0.008:
        reason_flags.append("curvature_flat")
    elif curvature_mean > 0:
        reason_flags.append("curvature_expanding")

    votes = []
    if slope_mean > 0.010:
        votes.append("expansion")
    elif slope_mean < -0.010:
        votes.append("compression")
    if curvature_mean > 0.012:
        votes.append("expansion")
    elif curvature_mean < -0.012:
        votes.append("compression")
    if old_state in {"WARM", "BUILDING"}:
        votes.append("warm")
    if old_state in {"STRESS", "HOT"}:
        votes.append("expansion")
    if old_state in {"CALM", "NEUTRAL"}:
        votes.append("compression")

    if votes:
        counts = {}
        for item in votes:
            counts[item] = counts.get(item, 0) + 1
        top = max(counts, key=counts.get)
        agreement = counts[top] / len(votes)
        if agreement < 0.6 and len(counts) > 1:
            reason_flags.append("cross_metric_conflict")
            bias_direction = "unknown"
        else:
            bias_direction = top
    else:
        agreement = 0.0
        bias_direction = "neutral"

    if has_pre_break:
        reason_flags.append("pre_break_candidate")
        bias_direction = "breakout_risk" if slope_mean >= 0 else "breakdown_risk"

    if len(deribit) >= 4 and old_state != "UNKNOWN":
        reason_flags.append("persistence_confirmed")

    strength_score = _clamp(
        0.28 * _clamp(abs(vbi_score_mean) / 1.0)
        + 0.28 * _clamp(abs(slope_mean) / 0.05)
        + 0.22 * _clamp(abs(skew_mean) / 0.12)
        + 0.22 * _clamp(abs(curvature_mean) / 0.08)
        + 0.1 * agreement
        + (0.1 if "persistence_confirmed" in reason_flags else 0.0)
        - (0.22 if "cross_metric_conflict" in reason_flags else 0.0)
    )

    if has_pre_break:
        summary_class = "pre_break"
    elif bias_direction == "neutral" and strength_score < 0.3:
        summary_class = "no_signal"
    elif "cross_metric_conflict" in reason_flags and strength_score >= 0.2:
        summary_class = "conflict"
    elif strength_score >= 0.62 and bias_direction not in {"neutral", "unknown"}:
        summary_class = "strong_bias"
    elif strength_score >= 0.3 and bias_direction not in {"neutral", "unknown"}:
        summary_class = "weak_bias"
    elif "cross_metric_conflict" in reason_flags:
        summary_class = "conflict"
    else:
        summary_class = "no_signal"

    if summary_class == "no_signal":
        bias_direction = "neutral"
    if summary_class == "conflict":
        bias_direction = "unknown"

    return {
        "summary_class": summary_class,
        "bias_direction": bias_direction,
        "strength_score": round(strength_score, 3),
        "reason_flags": sorted(set(reason_flags)),
        "summary_text": _pick_template(DERIBIT_SUMMARY_TEMPLATES, summary_class, bias_direction, strength_score),
        "old_state": old_state,
    }


def map_deribit_line(deribit):
    summary = build_deribit_summary(deribit)
    if summary["summary_class"] == "pre_break":
        return summary["summary_text"], True
    return f"{summary['summary_text']}, no PRE-BREAK", False


# ---------------- ANOMALIES ----------------

def detect_anomaly(start, end):
    alerts = load_event("alert_sent", start, end)

    if alerts.empty or "type" not in alerts.columns:
        return None

    buildup_alerts = alerts[(alerts["type"] == "BUILDUP")]

    if buildup_alerts.empty:
        return None

    grouped = buildup_alerts.groupby("symbol").size().sort_values(ascending=False)

    if grouped.iloc[0] >= 3:
        symbol = grouped.index[0].replace("USDT", "")
        return f"""Observed anomaly (futures positioning):

{symbol}
– repeated risk buildups
– unstable positioning response

Behavior logged."""

    if "timestamp" in buildup_alerts.columns:
        ts = buildup_alerts["timestamp"].sort_values()

        if len(ts) >= 5:
            window = ts.diff().dt.total_seconds().rolling(4).sum()
            if (window <= 180).any():
                return f"""Observed anomaly (activity burst):

Multiple buildups within 3 minutes
– ticker-agnostic
– direction-agnostic

Short-term activity spike logged."""

    return None


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

    dominant_regime = dominant(market["regime"]) if not market.empty and "regime" in market.columns else "UNKNOWN"
    options_summary = build_options_summary(options_market)
    deribit_summary = build_deribit_summary(deribit)

    print(
        "Daily summary debug:",
        {
            "options": options_summary,
            "deribit": deribit_summary,
        },
    )

    low_stress = dominant_regime in ("CALM", "NEUTRAL") and elevated_share < 25
    summary_line_1 = "Low systemic stress." if low_stress else "Systemic stress is present."

    layers = {dominant_regime, options_summary["summary_class"], deribit_summary["summary_class"]}
    layers.discard("UNKNOWN")
    balanced_layers = len(layers) <= 2 and deribit_summary["summary_class"] != "pre_break"
    summary_line_2 = "Balanced across layers." if balanced_layers else "Layer imbalance detected."

    log_number = next_counter("daily_risk_log")

    return f"""Risk Log #{log_number} · 24h

Futures: {dominant_regime} ({elevated_share}% | {alerts} buildups)
Options: {options_summary['summary_text']}
Deribit: {deribit_summary['summary_text']}

{summary_line_1}
{summary_line_2}

Market log, not a forecast.
""".strip()


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
        missing = [
            name
            for name, value in [
                ("TWITTER_API_KEY", TWITTER_API_KEY),
                ("TWITTER_API_SECRET", TWITTER_API_SECRET),
                ("TWITTER_ACCESS_TOKEN", TWITTER_ACCESS_TOKEN),
                ("TWITTER_ACCESS_TOKEN_SECRET", TWITTER_ACCESS_TOKEN_SECRET),
            ]
            if not value
        ]
        if missing:
            raise RuntimeError(
                "AUTO_POST_TWITTER is enabled but missing Twitter credentials: " + ", ".join(missing)
            )

        try:
            post_tweet(
                daily_text,
                api_key=TWITTER_API_KEY,
                api_secret=TWITTER_API_SECRET,
                access_token=TWITTER_ACCESS_TOKEN,
                access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
            )
        except HTTPError as err:
            status = err.response.status_code if err.response is not None else None
            response_text = getattr(err.response, "text", "") if err.response is not None else ""
            raise RuntimeError(
                "Failed to auto-post daily Twitter log "
                f"(HTTP {status}). Check Twitter app credentials and token permissions. "
                f"Twitter response: {response_text}"
            ) from err
