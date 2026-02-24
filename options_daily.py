import pandas as pd

from loaders import load_event
from supabase import supabase_post


DAILY_OPTIONS_TABLE = "daily_options_analysis_v2"


def dominant(series, default_value="UNKNOWN", default_pct=0.0):
    clean = series.dropna() if hasattr(series, "dropna") else series
    if clean is None or len(clean) == 0:
        return default_value, default_pct
    vc = clean.value_counts(normalize=True)
    return vc.index[0], round(float(vc.iloc[0]) * 100, 1)


def session(ts):
    h = ts.hour
    if h < 8:
        return "ASIA"
    if h < 16:
        return "EU"
    return "US"


def _clean_signal_series(series):
    if series is None or len(series) == 0:
        return pd.Series(dtype="object")

    cleaned = (
        series.dropna()
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "NONE": pd.NA, "none": pd.NA, "null": pd.NA, "NULL": pd.NA})
        .dropna()
    )
    return cleaned


def _to_numeric_mean(series, digits):
    if series is None or len(series) == 0:
        return None
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return round(float(values.mean()), digits)


def run_options_daily(start, end):
    cycle = load_event("options_ticker_cycle", start, end)
    market = load_event("options_market_state", start, end)

    if cycle.empty:
        return

    payload = {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
    }

    payload["dominant_regime"], payload["dominant_regime_pct"] = dominant(cycle.get("regime"))
    payload["dominant_mci_phase"], payload["dominant_mci_phase_pct"] = dominant(cycle.get("mci_phase"))

    payload["mci_avg"] = _to_numeric_mean(cycle.get("mci"), 2)
    payload["mci_slope_avg"] = _to_numeric_mean(cycle.get("mci_slope"), 3)
    payload["mci_phase_confidence_avg"] = _to_numeric_mean(cycle.get("mci_phase_confidence"), 2)

    payload["okx_olsi_avg"] = _to_numeric_mean(cycle.get("okx_olsi"), 4)
    payload["okx_olsi_slope_avg"] = _to_numeric_mean(cycle.get("okx_olsi_slope"), 4)
        
    payload["dominant_liquidity_phase"], payload["dominant_liquidity_phase_pct"] = dominant(
        cycle.get("liquidity_phase"), default_value="UNKNOWN", default_pct=0.0
    )

    liquidity_regime_source = market.get("liquidity_regime") if not market.empty else cycle.get("liquidity_regime")
    payload["dominant_liquidity_regime"], payload["dominant_liquidity_regime_pct"] = dominant(
        liquidity_regime_source, default_value="UNKNOWN", default_pct=0.0
    )

    divergence_series = _clean_signal_series(cycle.get("divergence"))
    payload["dominant_divergence_level"], payload["dominant_divergence_level_pct"] = dominant(
        divergence_series, default_value="NONE", default_pct=0.0
    )
    payload["divergence_diff_abs_avg"] = _to_numeric_mean(cycle.get("divergence_diff").abs(), 4) if "divergence_diff" in cycle else None

    phase_divergence_series = _clean_signal_series(cycle.get("phase_divergence"))
    payload["phase_divergence_share_pct"] = round(len(phase_divergence_series) / len(cycle) * 100, 1)
    payload["dominant_phase_divergence"], payload["dominant_phase_divergence_pct"] = dominant(
        phase_divergence_series, default_value="NONE", default_pct=0.0
    )

    calm_ratio_source = cycle.get("market_calm_ratio")
    if calm_ratio_source is None and not market.empty:
        calm_ratio_source = market.get("market_calm_ratio")
    payload["market_calm_ratio_avg"] = _to_numeric_mean(calm_ratio_source, 3)

    cycle = cycle.copy()
    cycle["session"] = cycle["ts"].apply(session)
    session_breakdown = {}

    for s in ["ASIA", "EU", "US"]:
        sub = cycle[cycle["session"] == s]
        if sub.empty:
            continue
        session_phase, session_phase_pct = dominant(sub.get("mci_phase"))
        session_liquidity, session_liquidity_pct = dominant(
            sub.get("liquidity_phase"), default_value="UNKNOWN", default_pct=0.0
        )
        session_breakdown[s] = {
            "phase": session_phase,
            "phase_pct": session_phase_pct,
            "liquidity_phase": session_liquidity,
            "liquidity_phase_pct": session_liquidity_pct,
        }

    payload["session_breakdown"] = session_breakdown

    supabase_post(DAILY_OPTIONS_TABLE, payload)
