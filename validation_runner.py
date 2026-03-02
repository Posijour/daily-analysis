import os
import math
import time
import requests
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter, defaultdict

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

LOGS_TABLE = os.getenv("LOGS_TABLE", "logs")

# ---- Events (как у тебя) ----
EV_RISK_EVAL = "risk_eval"
EV_RISK_DIVERGENCE = "risk_divergence"
EV_BYBIT_STATE = "bybit_market_state"
EV_OKX_STATE = "okx_market_state"
EV_DERIBIT = os.getenv("EV_DERIBIT", "deribit_vbi_snapshot")  # если нет — будет N/A

# ---- Horizons ----
HORIZONS_H = [1, 6, 12]

# ---- Dispersion thresholds (из dispersion.txt) ----
RISK_THRESHOLDS = {"LOW": 0.05, "MEDIUM": 0.15}

# ---- Sampling ----
# Разрежение точек t (в минутах). Например 15 = брать точки не чаще чем раз в 15 минут.
# Если 0 или <=0 — без разрежения.
VAL_STEP_MINUTES = int(os.getenv("VAL_STEP_MINUTES", "0"))


# ----------------- Supabase helpers -----------------
def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def sb_get(path: str, params: Dict[str, str]) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = requests.get(url, headers=sb_headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def sb_post(path: str, rows: List[Dict[str, Any]]) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = requests.post(url, headers=sb_headers(), json=rows, timeout=60)
    r.raise_for_status()


# ----------------- Utilities -----------------
def ms_now() -> int:
    return int(time.time() * 1000)


def coerce_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def categorical_dispersion(values: List[str]) -> str:
    u = set(values)
    if len(u) == 1:
        return "LOW"
    if len(u) == 2:
        return "MEDIUM"
    return "HIGH"


def risk_dispersion(values: List[float]) -> str:
    diff = max(values) - min(values)
    if diff < RISK_THRESHOLDS["LOW"]:
        return "LOW"
    if diff < RISK_THRESHOLDS["MEDIUM"]:
        return "MEDIUM"
    return "HIGH"


def pair_dispersion(vals: Dict[str, Any], a: str, b: str, mode: str) -> str:
    if a not in vals or b not in vals:
        return "N/A"
    if mode == "numeric":
        return risk_dispersion([float(vals[a]), float(vals[b])])
    return categorical_dispersion([str(vals[a]), str(vals[b])])


def downsample_times(times_ms: List[int], step_minutes: int) -> List[int]:
    if step_minutes <= 0:
        return times_ms
    step_ms = step_minutes * 60 * 1000
    out: List[int] = []
    last: Optional[int] = None
    for t in times_ms:
        if last is None or (t - last) >= step_ms:
            out.append(t)
            last = t
    return out


# ----------------- Load logs -----------------
def load_logs(event: str, ts_from: int, ts_to: int) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{LOGS_TABLE}"
    r = requests.get(
        url,
        headers=sb_headers(),
        params=[
            ("select", "ts,event,symbol,data"),
            ("event", f"eq.{event}"),
            ("ts", f"gte.{ts_from}"),
            ("ts", f"lt.{ts_to}"),
            ("order", "ts.asc"),
        ],
        timeout=120,
    )
    r.raise_for_status()
    return r.json()


# ----------------- Core: price series -----------------
def build_price_series(risk_rows: List[Dict[str, Any]]) -> Dict[str, List[Tuple[int, float]]]:
    out: Dict[str, List[Tuple[int, float]]] = defaultdict(list)
    for row in risk_rows:
        ts = int(row["ts"])
        data = row.get("data") or {}
        sym = data.get("symbol") or row.get("symbol")
        px = coerce_float(data.get("price"))
        if sym and px is not None:
            out[sym].append((ts, px))
    return out


def find_price_at_or_after(series: List[Tuple[int, float]], t: int) -> Optional[float]:
    # series sorted by ts asc
    lo, hi = 0, len(series) - 1
    if hi < 0:
        return None
    if t <= series[0][0]:
        return series[0][1]
    if t > series[hi][0]:
        return None
    # binary search first ts >= t
    while lo < hi:
        mid = (lo + hi) // 2
        if series[mid][0] < t:
            lo = mid + 1
        else:
            hi = mid
    return series[lo][1]


def abs_ret(p0: float, p1: float) -> float:
    return abs((p1 / p0) - 1.0) if p0 > 0 else 0.0


def sign(x: float) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)


# ----------------- Market-state forward fill -----------------
def last_state_at_or_before(rows: List[Dict[str, Any]], t: int, key: str) -> Optional[Any]:
    # rows sorted asc by ts
    lo, hi = 0, len(rows) - 1
    if hi < 0:
        return None
    if t < int(rows[0]["ts"]):
        return None
    # binary search last ts <= t
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if int(rows[mid]["ts"]) <= t:
            lo = mid
        else:
            hi = mid - 1
    data = rows[lo].get("data") or {}
    return data.get(key)


# ----------------- Dispersion at time t (по твоей логике) -----------------
def window_avg_risk(risk_rows: List[Dict[str, Any]], ts_from: int, ts_to: int) -> Optional[float]:
    vals = []
    for r in risk_rows:
        ts = int(r["ts"])
        if ts < ts_from:
            continue
        if ts >= ts_to:
            break
        data = r.get("data") or {}
        v = coerce_float(data.get("risk"))
        if v is not None:
            vals.append(v)
    if not vals:
        return None
    return sum(vals) / len(vals)


def window_mode_from_event(rows: List[Dict[str, Any]], ts_from: int, ts_to: int, key: str) -> Optional[str]:
    vals = []
    for r in rows:
        ts = int(r["ts"])
        if ts < ts_from:
            continue
        if ts >= ts_to:
            break
        data = r.get("data") or {}
        v = data.get(key)
        if v is not None:
            vals.append(str(v))
    if not vals:
        return None
    return Counter(vals).most_common(1)[0][0]


def compute_dispersion_at(
    t: int,
    risk_eval_rows_sorted: List[Dict[str, Any]],
    okx_rows_sorted: List[Dict[str, Any]],
    deribit_rows_sorted: List[Dict[str, Any]],
) -> Dict[str, Dict[str, str]]:
    # как в dispersion.txt: windows 12h,6h,1h; риск numeric; структура categorical; вола categorical
    windows = {"12h": 12 * 3600 * 1000, "6h": 6 * 3600 * 1000, "1h": 1 * 3600 * 1000}

    risk_vals = {}
    struct_vals = {}
    vol_vals = {}

    for w, ms in windows.items():
        ts_from, ts_to = t - ms, t

        ar = window_avg_risk(risk_eval_rows_sorted, ts_from, ts_to)
        if ar is not None:
            risk_vals[w] = ar

        # структура: используем okx_liquidity_regime как категорию
        reg = window_mode_from_event(okx_rows_sorted, ts_from, ts_to, "okx_liquidity_regime")
        if reg is not None:
            struct_vals[w] = reg

        # вола: используем vbi_state как категорию (если есть)
        vbi = window_mode_from_event(deribit_rows_sorted, ts_from, ts_to, "vbi_state")
        if vbi is not None:
            vol_vals[w] = vbi

    return {
        "risk": {
            "12h_1h": pair_dispersion(risk_vals, "12h", "1h", "numeric"),
            "6h_1h": pair_dispersion(risk_vals, "6h", "1h", "numeric"),
        },
        "structure": {
            "12h_1h": pair_dispersion(struct_vals, "12h", "1h", "categorical"),
            "6h_1h": pair_dispersion(struct_vals, "6h", "1h", "categorical"),
        },
        "volatility": {
            "12h_1h": pair_dispersion(vol_vals, "12h", "1h", "categorical"),
            "6h_1h": pair_dispersion(vol_vals, "6h", "1h", "categorical"),
        },
    }


# ----------------- Signal builders -----------------
def build_signal_times_by_symbol_risk_divergence(rows: List[Dict[str, Any]]) -> Dict[str, List[int]]:
    out = defaultdict(list)
    for r in rows:
        ts = int(r["ts"])
        data = r.get("data") or {}
        sym = data.get("symbol") or r.get("symbol")
        if sym:
            out[sym].append(ts)
    return out


def is_bybit_calm_at(t: int, bybit_rows_sorted: List[Dict[str, Any]]) -> bool:
    regime = last_state_at_or_before(bybit_rows_sorted, t, "regime")
    return str(regime) == "CALM"


def okx_divergence_strength_at(t: int, okx_rows_sorted: List[Dict[str, Any]]) -> Optional[float]:
    strength = last_state_at_or_before(okx_rows_sorted, t, "divergence_strength")
    dtype = last_state_at_or_before(okx_rows_sorted, t, "divergence_type")
    if dtype is None or str(dtype).upper() in ("NONE", "NULL", ""):
        return None
    return coerce_float(strength)


# ----------------- Validation core -----------------
def mean(xs: List[float]) -> Optional[float]:
    return (sum(xs) / len(xs)) if xs else None


def run_one_signal(
    signal_key: str,
    symbols: List[str],
    t_points: Dict[str, List[int]],
    price_series: Dict[str, List[Tuple[int, float]]],
    bybit_rows: List[Dict[str, Any]],
    okx_rows: List[Dict[str, Any]],
    deribit_rows: List[Dict[str, Any]],
    risk_eval_rows: List[Dict[str, Any]],
    t_start: int,
    t_end: int,
) -> None:
    for H in HORIZONS_H:
        h_ms = H * 3600 * 1000

        # --- разный time_end по горизонту ---
        # чтобы для каждой точки t гарантированно был future t+H внутри общего окна
        effective_end = t_end - h_ms
        if effective_end <= t_start:
            print(f"[validation] SKIP {signal_key} H{H}: effective_end <= t_start", flush=True)
            continue

        buckets = {"ALL": {"with_abs": [], "without_abs": [], "with_cont": [], "without_cont": [], "n_with": 0, "n_without": 0}}
        for s in symbols:
            buckets[s] = {"with_abs": [], "without_abs": [], "with_cont": [], "without_cont": [], "n_with": 0, "n_without": 0}

        for sym in symbols:
            series = price_series.get(sym, [])
            if not series:
                continue

            # t-точки в диапазоне [t_start, effective_end)
            times = [ts for ts, _ in series if t_start <= ts < effective_end]
            if not times:
                continue

            # --- разрежение точек t ---
            times = downsample_times(times, VAL_STEP_MINUTES)

            for t in times:
                p0 = find_price_at_or_after(series, t)
                p1 = find_price_at_or_after(series, t + h_ms)
                p_prev = find_price_at_or_after(series, t - 3600 * 1000)

                if p0 is None or p1 is None or p_prev is None:
                    continue

                flag = False

                if signal_key == "S1_futures_divergence":
                    # событие дивера близко к t (в пределах 10 минут)
                    for td in t_points.get(sym, []):
                        if abs(td - t) <= 10 * 60 * 1000:
                            flag = True
                            break

                elif signal_key == "S3_bybit_calm":
                    flag = is_bybit_calm_at(t, bybit_rows)

                elif signal_key == "S4_okx_bybit_divergence":
                    st = okx_divergence_strength_at(t, okx_rows)
                    flag = (st is not None and st > 0)

                elif signal_key == "S2_dispersion_low":
                    d = compute_dispersion_at(t, risk_eval_rows, okx_rows, deribit_rows)
                    flag = (d.get("risk", {}).get("12h_1h") == "LOW")

                else:
                    continue

                ar = abs_ret(p0, p1)
                prev_ret = (p0 / p_prev) - 1.0
                next_ret = (p1 / p0) - 1.0
                cont = 1.0 if sign(prev_ret) != 0 and sign(prev_ret) == sign(next_ret) else 0.0

                segs = ["ALL", sym]
                for seg in segs:
                    if flag:
                        buckets[seg]["with_abs"].append(ar)
                        buckets[seg]["with_cont"].append(cont)
                        buckets[seg]["n_with"] += 1
                    else:
                        buckets[seg]["without_abs"].append(ar)
                        buckets[seg]["without_cont"].append(cont)
                        buckets[seg]["n_without"] += 1

        run_row = {
            "signal_key": signal_key,
            "horizon_hours": H,
            "time_start": t_start,
            "time_end": effective_end,  # <-- сохраняем реальный end, используемый для горизонта
            "symbols": symbols,
            "params": {"val_step_minutes": VAL_STEP_MINUTES},
            "status": "OK",
        }

        try:
            url = f"{SUPABASE_URL}/rest/v1/validation_runs"
            r = requests.post(
                url,
                headers={**sb_headers(), "Prefer": "return=representation"},
                json=[run_row],
                timeout=60,
            )
            r.raise_for_status()
            run_id = r.json()[0]["id"]

            results_rows = []
            for seg, b in buckets.items():
                with_abs = mean(b["with_abs"])
                without_abs = mean(b["without_abs"])
                with_cont = mean(b["with_cont"])
                without_cont = mean(b["without_cont"])

                if with_abs is not None and without_abs is not None:
                    results_rows.append({
                        "run_id": run_id,
                        "metric_key": "abs_ret",
                        "segment": seg,
                        "n": b["n_with"] + b["n_without"],
                        "with_signal": with_abs,
                        "without_signal": without_abs,
                        "delta": with_abs - without_abs,
                    })

                if with_cont is not None and without_cont is not None:
                    results_rows.append({
                        "run_id": run_id,
                        "metric_key": "continue_prob",
                        "segment": seg,
                        "n": b["n_with"] + b["n_without"],
                        "with_signal": with_cont,
                        "without_signal": without_cont,
                        "delta": with_cont - without_cont,
                    })

            if results_rows:
                sb_post("validation_results", results_rows)

            for rr in results_rows:
                if rr["segment"] == "ALL":
                    print(
                        f"{signal_key} H{H} {rr['metric_key']} "
                        f"with={rr['with_signal']:.6f} without={rr['without_signal']:.6f} "
                        f"delta={rr['delta']:.6f} n={rr['n']} "
                        f"end_ms={effective_end}",
                        flush=True
                    )

        except Exception as e:
            print(f"[validation] FAILED {signal_key} H{H}: {e}", flush=True)


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY env missing")

    now = ms_now()
    t_end = int(os.getenv("VAL_END_MS", str(now)))
    t_start = int(os.getenv("VAL_START_MS", str(t_end - 24 * 3600 * 1000)))

    symbols_env = os.getenv("VAL_SYMBOLS", "").strip()

    risk_eval_rows = load_logs(EV_RISK_EVAL, t_start, t_end)
    price_series = build_price_series(risk_eval_rows)

    if symbols_env:
        symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
    else:
        symbols = sorted(list(price_series.keys()))

    risk_div_rows = load_logs(EV_RISK_DIVERGENCE, t_start, t_end)
    div_times = build_signal_times_by_symbol_risk_divergence(risk_div_rows)

    bybit_rows = load_logs(EV_BYBIT_STATE, t_start - 48 * 3600 * 1000, t_end)
    okx_rows = load_logs(EV_OKX_STATE, t_start - 48 * 3600 * 1000, t_end)

    try:
        deribit_rows = load_logs(EV_DERIBIT, t_start - 48 * 3600 * 1000, t_end)
    except Exception:
        deribit_rows = []

    bybit_rows.sort(key=lambda r: int(r["ts"]))
    okx_rows.sort(key=lambda r: int(r["ts"]))
    deribit_rows.sort(key=lambda r: int(r["ts"]))
    risk_eval_rows.sort(key=lambda r: int(r["ts"]))

    signals = [
        ("S1_futures_divergence", div_times),
        ("S2_dispersion_low", {}),
        ("S3_bybit_calm", {}),
        ("S4_okx_bybit_divergence", {}),
    ]

    for sk, tp in signals:
        run_one_signal(
            signal_key=sk,
            symbols=symbols,
            t_points=tp,
            price_series=price_series,
            bybit_rows=bybit_rows,
            okx_rows=okx_rows,
            deribit_rows=deribit_rows,
            risk_eval_rows=risk_eval_rows,
            t_start=t_start,
            t_end=t_end,
        )


if __name__ == "__main__":
    main()
