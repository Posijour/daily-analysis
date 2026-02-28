import pandas as pd
from requests import HTTPError

from loaders import load_event
from supabase import supabase_post


DAILY_OPTIONS_TABLE = "daily_options_analysis"


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


def run_options_daily(start, end):
    bybit = load_event("bybit_market_state", start, end)
    okx = load_event("okx_market_state", start, end)

    if bybit.empty and okx.empty:
        return

    payload = {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
    }

    # ======================
    # BYBIT MARKET STATE
    # ======================
    if not bybit.empty:
        payload["bybit_mci_avg"] = _to_numeric_mean(bybit.get("mci"), 2)
        payload["bybit_mci_slope_avg"] = _to_numeric_mean(bybit.get("mci_slope"), 3)
        payload["bybit_confidence_avg"] = _to_numeric_mean(bybit.get("confidence"), 2)

        payload["dominant_bybit_regime"], payload["dominant_bybit_regime_pct"] = dominant(
            bybit.get("regime"), default_value="UNKNOWN", default_pct=0.0
        )

        payload["dominant_bybit_phase"], payload["dominant_bybit_phase_pct"] = dominant(
            bybit.get("mci_phase"), default_value="UNKNOWN", default_pct=0.0
        )

    # ======================
    # OKX MARKET STATE
    # ======================
    if not okx.empty:
        payload["okx_olsi_avg"] = _to_numeric_mean(okx.get("okx_olsi_avg"), 4)
        payload["okx_olsi_slope_avg"] = _to_numeric_mean(okx.get("okx_olsi_slope"), 4)

        payload["dominant_okx_liquidity_regime"], payload["dominant_okx_liquidity_regime_pct"] = dominant(
            okx.get("okx_liquidity_regime"), default_value="UNKNOWN", default_pct=0.0
        )

        divergence_series = _clean_signal_series(okx.get("divergence"))
        payload["dominant_divergence"], payload["dominant_divergence_pct"] = dominant(
            divergence_series, default_value="NONE", default_pct=0.0
        )

        payload["divergence_strength_avg"] = _to_numeric_mean(
            okx.get("divergence_strength"), 3
        )

        payload["divergence_diff_avg"] = _to_numeric_mean(
            okx.get("divergence_diff"), 4
        )

    try:
        supabase_post(DAILY_OPTIONS_TABLE, payload)
    except HTTPError as err:
        status = err.response.status_code if err.response is not None else None
        if status in (401, 403, 404):
            response_text = getattr(err.response, "text", "") if err.response is not None else ""
            raise RuntimeError(
                "Failed to write daily options analysis to Supabase table "
                f"'{DAILY_OPTIONS_TABLE}' (HTTP {status}). "
                f"Supabase response: {response_text}"
            ) from err
        raise
