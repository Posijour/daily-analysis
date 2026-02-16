import pandas as pd
from loaders import load_event
from supabase import supabase_post

def dominant(series, default_value="UNKNOWN", default_pct=0.0):
    clean = series.dropna() if hasattr(series, "dropna") else series
    if clean is None or len(clean) == 0:
        return default_value, default_pct
    vc = clean.value_counts(normalize=True)
    return vc.index[0], round(vc.iloc[0] * 100, 1)


def session(ts):
    h = ts.hour
    if h < 8:
        return "ASIA"
    if h < 16:
        return "EU"
    return "US"


def run_options_daily(start, end):
    cycle = load_event("options_ticker_cycle", start, end)
    market = load_event("options_market_state", start, end)

    if cycle.empty or market.empty:
        return

    dom_regime, dom_regime_pct = dominant(cycle["regime"])
    mci_avg = round(cycle["mci"].mean(), 2)
    mci_slope_avg = round(cycle["mci_slope"].mean(), 3)

    phase, phase_pct = dominant(cycle["mci_phase"])
    confidence_avg = round(cycle["mci_phase_confidence"].mean(), 2)

    if "miti_regime" in market.columns:
        miti_state, miti_pct = dominant(market["miti_regime"])
    else:
        miti_state, miti_pct = "UNKNOWN", 0.0

    phase_divergence = False
    if "phase_divergence" in market.columns:
        phase_divergence = bool(market["phase_divergence"].fillna(False).any())
    elif "phase_divergence" in cycle.columns:
        phase_divergence = bool(cycle["phase_divergence"].fillna(False).any())

    if "phase_divergence_type" in market.columns:
        phase_divergence_type, _ = dominant(
            market["phase_divergence_type"], default_value="NONE", default_pct=0.0
        )
    elif "phase_divergence_type" in cycle.columns:
        phase_divergence_type, _ = dominant(
            cycle["phase_divergence_type"], default_value="NONE", default_pct=0.0
        )
    else:
        phase_divergence_type = "NONE"
        
    cycle["session"] = cycle["ts"].apply(session)
    session_breakdown = {}

    for s in ["ASIA", "EU", "US"]:
        sub = cycle[cycle["session"] == s]
        if not sub.empty:
            p, pct = dominant(sub["mci_phase"])
            session_breakdown[s] = {"phase": p, "pct": pct}

    payload = {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "dominant_regime": dom_regime,
        "dominant_regime_pct": dom_regime_pct,
        "mci_avg": mci_avg,
        "mci_slope_avg": mci_slope_avg,
        "dominant_mci_phase": phase,
        "dominant_mci_phase_pct": phase_pct,
        "confidence_avg": confidence_avg,
        "miti_state": miti_state,
        "miti_state_pct": miti_pct,
        "phase_divergence": phase_divergence,
        "phase_divergence_type": phase_divergence_type,
        "session_breakdown": session_breakdown,
    }

    supabase_post("daily_options_analysis", payload)
