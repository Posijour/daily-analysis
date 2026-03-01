# risk_divergence_daily.py
import pandas as pd

from loaders import load_event
from supabase import supabase_post


ONE_HOUR = pd.Timedelta(hours=1)


def _window(df: pd.DataFrame, end_ts):
    if df.empty or "ts" not in df.columns:
        return df
    start_ts = end_ts - ONE_HOUR
    return df[(df["ts"] >= start_ts) & (df["ts"] <= end_ts)]


def _dominant_text(series, default="UNKNOWN"):
    if series is None:
        return default
    clean = series.dropna() if hasattr(series, "dropna") else series
    if clean is None or len(clean) == 0:
        return default
    return clean.astype(str).value_counts().idxmax()


def _numeric_mean(df: pd.DataFrame, candidates: list[str], digits: int = 2):
    for column in candidates:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if values.empty:
            continue
        return round(float(values.mean()), digits)
    return None


def _one_hour_market_context(ts, risk_df: pd.DataFrame, market_df: pd.DataFrame, deribit_df: pd.DataFrame):
    risk_window = _window(risk_df, ts)
    total_risk_logs = len(risk_window)

    avg_market_risk = None
    buildups_share_pct = None
    if total_risk_logs:
        risk_values = pd.to_numeric(risk_window.get("risk"), errors="coerce").fillna(0)
        avg_market_risk = round(float(risk_values.mean()), 2)
        buildups_share_pct = round(float((risk_values >= 2).sum() / total_risk_logs * 100), 2)

    market_window = _window(market_df, ts)
    liquidity_regime = "UNKNOWN"
    for candidate in ("liquidity_regime", "market_volatility", "regime"):
        if candidate in market_window.columns:
            liquidity_regime = _dominant_text(market_window[candidate], default="UNKNOWN")
            break

    deribit_window = _window(deribit_df, ts)
    vbi_avg = _numeric_mean(deribit_window, ["vbi", "vbi_value", "vbi_index", "vbi_score"], digits=3)

    return {
        "market_risk_avg_1h_pre_event": avg_market_risk,
        "market_buildups_share_pct_1h_pre_event": buildups_share_pct,
        "market_liquidity_regime_1h_pre_event": liquidity_regime,
        "market_vbi_avg_1h_pre_event": vbi_avg,
    }


def run_risk_divergence_daily(start, end):
    df = load_event("risk_divergence", start, end)
    if df.empty:
        return

    risk = load_event("risk_eval", start, end)
    market = load_event("market_regime", start, end)
    deribit = load_event("deribit_vbi_snapshot", start, end)

    rows = []
    for r in df.itertuples(index=False):
        ts = getattr(r, "ts")
        rows.append({
            "ts": ts.isoformat(),
            "date": ts.date().isoformat(),
            "symbol": getattr(r, "symbol", None),
            "divergence_type": getattr(r, "divergence_type", None) or getattr(r, "type", None),
            "risk": int(getattr(r, "risk", 0) or 0),
            "price": getattr(r, "price", None),
            **_one_hour_market_context(ts, risk, market, deribit),
        })

    if rows:
        supabase_post("daily_risk_divergences", rows, upsert=False)
