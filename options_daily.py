import pandas as pd
from requests import HTTPError

from loaders import load_event
from supabase import supabase_post


DAILY_OPTIONS_TABLE = "daily_options_analysis"
DAILY_META_SESSIONS_TABLE = "daily_meta_sessions"


# ======================
# helpers
# ======================

def dominant(series, default_value="UNKNOWN", default_pct=0.0):
    if series is None or len(series) == 0:
        return default_value, default_pct
    clean = series.dropna()
    if clean.empty:
        return default_value, default_pct
    vc = clean.value_counts(normalize=True)
    return vc.index[0], round(float(vc.iloc[0]) * 100, 1)


def _clean_signal_series(series):
    if series is None or len(series) == 0:
        return pd.Series(dtype="object")
    return (
        series.dropna()
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "NONE": pd.NA, "none": pd.NA, "null": pd.NA, "NULL": pd.NA})
        .dropna()
    )


def _to_numeric_mean(series, digits):
    if series is None or len(series) == 0:
        return None
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return round(float(values.mean()), digits)


def session(ts):
    h = ts.hour
    if h < 8:
        return "ASIA"
    if h < 16:
        return "EU"
    return "US"


# ======================
# main
# ======================

def run_options_daily(start, end):
    bybit = load_event("bybit_market_state", start, end)
    okx = load_event("okx_market_state", start, end)

    if bybit.empty and okx.empty:
        return

    # --------------------------------------------------
    # DAILY OPTIONS ANALYSIS (НЕ ТРОГАЕМ ЛОГИКУ)
    # --------------------------------------------------

    payload = {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
    }

    # ----- BYBIT -----
    if not bybit.empty:
        payload["bybit_mci_avg"] = _to_numeric_mean(bybit.get("mci"), 2)
        payload["bybit_mci_slope_avg"] = _to_numeric_mean(bybit.get("mci_slope"), 3)
        payload["bybit_confidence_avg"] = _to_numeric_mean(bybit.get("confidence"), 2)

        payload["dominant_bybit_regime"], payload["dominant_bybit_regime_pct"] = dominant(
            bybit.get("regime"), "UNKNOWN", 0.0
        )

        payload["dominant_bybit_phase"], payload["dominant_bybit_phase_pct"] = dominant(
            bybit.get("mci_phase"), "UNKNOWN", 0.0
        )

    # ----- OKX -----
    if not okx.empty:
        payload["okx_olsi_avg"] = _to_numeric_mean(okx.get("okx_olsi_avg"), 4)
        payload["okx_olsi_slope_avg"] = _to_numeric_mean(okx.get("okx_olsi_slope"), 4)

        payload["dominant_okx_liquidity_regime"], payload["dominant_okx_liquidity_regime_pct"] = dominant(
            okx.get("okx_liquidity_regime"), "UNKNOWN", 0.0
        )

        divergence_series = _clean_signal_series(okx.get("divergence"))
        payload["dominant_divergence"], payload["dominant_divergence_pct"] = dominant(
            divergence_series, "NONE", 0.0
        )

        payload["divergence_strength_avg"] = _to_numeric_mean(
            okx.get("divergence_strength"), 3
        )

        payload["divergence_diff_avg"] = _to_numeric_mean(
            okx.get("divergence_diff"), 4
        )

    supabase_post(DAILY_OPTIONS_TABLE, payload)

    # --------------------------------------------------
    # SESSION META (НОВОЕ, В ОТДЕЛЬНУЮ ТАБЛИЦУ)
    # --------------------------------------------------

    # готовим данные
    if not bybit.empty:
        bybit = bybit.copy()
        bybit["session"] = bybit["ts"].apply(session)

    if not okx.empty:
        okx = okx.copy()
        okx["session"] = okx["ts"].apply(session)

    day = start.date().isoformat()

    for s in ["ASIA", "EU", "US"]:
        rows = []

        # ---------- BYBIT SESSION META ----------
        if not bybit.empty:
            sub = bybit[bybit["session"] == s]
            if not sub.empty:
                dominant_phase, _ = dominant(sub.get("mci_phase"))
                dominant_regime, _ = dominant(sub.get("regime"))

                rows.append({
                    "date": day,
                    "session": s,
                    "meta_score": round(_to_numeric_mean(sub.get("confidence"), 2) or 0, 1),
                    "dominant_meta": dominant_phase,
                    "share_hidden_pressure": 0,
                    "share_confirmed_stress": round(
                        len(sub[sub["regime"] == "DIRECTIONAL_DOWN"]) / len(sub) * 100, 1
                    ),
                    "share_true_calm": round(
                        len(sub[sub["regime"] == "CALM"]) / len(sub) * 100, 1
                    ),
                })

        # ---------- OKX SESSION META ----------
        if not okx.empty:
            sub = okx[okx["session"] == s]
            if not sub.empty:
                dominant_liquidity, _ = dominant(sub.get("okx_liquidity_regime"))
                dominant_div, _ = dominant(_clean_signal_series(sub.get("divergence")))

                rows.append({
                    "date": day,
                    "session": s,
                    "meta_score": round(_to_numeric_mean(sub.get("divergence_strength"), 2) or 0, 1),
                    "dominant_meta": dominant_div,
                    "share_hidden_pressure": 0,
                    "share_confirmed_stress": 0,
                    "share_true_calm": 0,
                })

        # запись (одна строка на сессию)
        for r in rows:
            supabase_post(DAILY_META_SESSIONS_TABLE, r)
