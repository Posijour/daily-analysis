from __future__ import annotations

"""Internal-only daily aggregator.

Aggregator output is internal-only.
It is intended for internal analysis and manual weekly summaries.
No direct publication is allowed.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
from requests import HTTPError


STRUCTURE_LAYER = "market_structure"
VOLATILITY_LAYER = "market_volatility"
CONTEXT_LAYER = "market_context"


@dataclass(frozen=True)
class StateMetrics:
    shares_pct: dict[str, float]
    longest_state: str | None
    longest_state_seconds: int
    transitions_per_24h: float
    transition_map_counts: dict[str, int]
    failed_follow_through: int


def _normalize_state_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _duration_shares_and_longest(df: pd.DataFrame, state_col: str, end: datetime) -> tuple[dict[str, float], str | None, int]:
    if df.empty or state_col not in df.columns:
        return {}, None, 0

    ordered = df.sort_values("ts").copy()
    ordered["_state"] = ordered[state_col].map(_normalize_state_value)
    ordered = ordered[ordered["_state"].notna()]
    if ordered.empty:
        return {}, None, 0

    durations: Counter[str] = Counter()
    longest_state = None
    longest_seconds = -1

    for idx, (_, row) in enumerate(ordered.iterrows()):
        state = row["_state"]
        ts = row["ts"]
        next_ts = end if idx == len(ordered) - 1 else ordered.iloc[idx + 1]["ts"]
        seconds = int(max((next_ts - ts).total_seconds(), 0))
        durations[state] += seconds
        if seconds > longest_seconds:
            longest_seconds = seconds
            longest_state = state

    total_seconds = sum(durations.values())
    if total_seconds <= 0:
        return {}, longest_state, max(longest_seconds, 0)

    shares = {state: round(seconds * 100 / total_seconds, 2) for state, seconds in durations.items()}
    return shares, longest_state, max(longest_seconds, 0)


def _transition_map(df: pd.DataFrame, state_col: str) -> tuple[dict[str, int], int]:
    if df.empty or state_col not in df.columns:
        return {}, 0

    ordered = df.sort_values("ts").copy()
    ordered["_state"] = ordered[state_col].map(_normalize_state_value)
    states = [s for s in ordered["_state"].tolist() if s is not None]
    if len(states) < 2:
        return {}, 0

    transition_counts: Counter[str] = Counter()
    for prev, cur in zip(states, states[1:]):
        if prev == cur:
            continue
        transition_counts[f"{prev}->{cur}"] += 1
    return dict(transition_counts), sum(transition_counts.values())


def _failed_follow_through_from_states(df: pd.DataFrame, state_col: str) -> int:
    """Count transitions that revert at the next step (A->B->A)."""
    if df.empty or state_col not in df.columns:
        return 0

    ordered = df.sort_values("ts").copy()
    ordered["_state"] = ordered[state_col].map(_normalize_state_value)
    states = [s for s in ordered["_state"].tolist() if s is not None]
    if len(states) < 3:
        return 0

    failed = 0
    for a, b, c in zip(states, states[1:], states[2:]):
        if a != b and c == a:
            failed += 1
    return failed


def _event_rate_per_24h(count: int, start: datetime, end: datetime) -> float:
    hours = (end - start).total_seconds() / 3600
    if hours <= 0:
        return 0.0
    return round(count / hours * 24, 2)


def _dominant_state(df: pd.DataFrame, state_col: str) -> str | None:
    if df.empty or state_col not in df.columns:
        return None
    clean = df[state_col].map(_normalize_state_value).dropna()
    if clean.empty:
        return None
    return clean.value_counts().idxmax()


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _window_alignment_metrics(cycle_df: pd.DataFrame) -> dict[str, int]:
    if cycle_df.empty:
        return {"aligned": 0, "conflicted": 0}

    aligned = 0
    conflicted = 0

    for _, row in cycle_df.iterrows():
        value = None
        for key in ("window_alignment", "alignment", "windows_alignment"):
            if key in cycle_df.columns:
                value = row.get(key)
                break

        text = str(value).strip().lower() if value is not None else ""
        if text in {"aligned", "align", "true", "1"}:
            aligned += 1
            continue
        if text in {"conflicted", "conflict", "mixed", "false", "0"}:
            conflicted += 1
            continue

        # Fallback to boolean-style fields if explicit status doesn't exist.
        flag = row.get("is_aligned") if "is_aligned" in cycle_df.columns else None
        if flag is True:
            aligned += 1
        elif flag is False:
            conflicted += 1

    return {"aligned": aligned, "conflicted": conflicted}


def _build_layer_metrics(df: pd.DataFrame, state_col: str, start: datetime, end: datetime) -> StateMetrics:
    shares, longest_state, longest_seconds = _duration_shares_and_longest(df, state_col, end)
    transition_map, transitions_count = _transition_map(df, state_col)
    transitions_per_24h = _event_rate_per_24h(transitions_count, start, end)
    failed_follow = _failed_follow_through_from_states(df, state_col)

    return StateMetrics(
        shares_pct=shares,
        longest_state=longest_state,
        longest_state_seconds=longest_seconds,
        transitions_per_24h=transitions_per_24h,
        transition_map_counts=transition_map,
        failed_follow_through=failed_follow,
    )


def _metric_row(date_iso: str, layer: str, metric: str, value: Any) -> dict[str, Any]:
    return {
        "date": date_iso,
        "layer": layer,
        "metric": metric,
        "value": value,
    }


def run_internal_aggregates_daily(
    start: datetime,
    end: datetime,
    load_event_fn=None,
    supabase_post_fn=None,
):
    if load_event_fn is None:
        from loaders import load_event as load_event_fn
    if supabase_post_fn is None:
        from supabase import supabase_post as supabase_post_fn

    cycle = load_event_fn("options_ticker_cycle", start, end)
    market = load_event_fn("options_market_state", start, end)
    divergence = load_event_fn("risk_divergence", start, end)

    structure_col = _pick_col(cycle, ["market_structure", "structure_state", "regime"])
    volatility_col = _pick_col(market, ["market_volatility", "volatility_state", "liquidity_regime", "regime"])

    structure_metrics = _build_layer_metrics(cycle, structure_col, start, end) if structure_col else StateMetrics({}, None, 0, 0.0, {}, 0)
    volatility_metrics = _build_layer_metrics(market, volatility_col, start, end) if volatility_col else StateMetrics({}, None, 0, 0.0, {}, 0)

    divergence_rate = _event_rate_per_24h(len(divergence), start, end)
    alignment = _window_alignment_metrics(cycle)

    rows = [
        _metric_row(end.date().isoformat(), STRUCTURE_LAYER, "state_share_pct", structure_metrics.shares_pct),
        _metric_row(end.date().isoformat(), STRUCTURE_LAYER, "longest_continuous_state", {
            "state": structure_metrics.longest_state,
            "duration_seconds": structure_metrics.longest_state_seconds,
        }),
        _metric_row(end.date().isoformat(), STRUCTURE_LAYER, "state_transitions_per_24h", structure_metrics.transitions_per_24h),
        _metric_row(end.date().isoformat(), STRUCTURE_LAYER, "dominant_state", _dominant_state(cycle, structure_col) if structure_col else None),
        _metric_row(end.date().isoformat(), VOLATILITY_LAYER, "state_share_pct", volatility_metrics.shares_pct),
        _metric_row(end.date().isoformat(), VOLATILITY_LAYER, "longest_continuous_state", {
            "state": volatility_metrics.longest_state,
            "duration_seconds": volatility_metrics.longest_state_seconds,
        }),
        _metric_row(end.date().isoformat(), VOLATILITY_LAYER, "state_transitions_per_24h", volatility_metrics.transitions_per_24h),
        _metric_row(end.date().isoformat(), VOLATILITY_LAYER, "dominant_state", _dominant_state(market, volatility_col) if volatility_col else None),
        _metric_row(end.date().isoformat(), CONTEXT_LAYER, "divergence_events_per_24h", divergence_rate),
        _metric_row(end.date().isoformat(), CONTEXT_LAYER, "transition_map_counts", {
            STRUCTURE_LAYER: structure_metrics.transition_map_counts,
            VOLATILITY_LAYER: volatility_metrics.transition_map_counts,
        }),
        _metric_row(end.date().isoformat(), CONTEXT_LAYER, "failed_follow_through_count", {
            STRUCTURE_LAYER: structure_metrics.failed_follow_through,
            VOLATILITY_LAYER: volatility_metrics.failed_follow_through,
        }),
        _metric_row(end.date().isoformat(), CONTEXT_LAYER, "window_alignment_frequency", alignment),
    ]

    try:
        supabase_post_fn("daily_aggregates", rows, upsert=False)
    except HTTPError as err:
        status = err.response.status_code if err.response is not None else None
        if status not in (400, 401, 403, 404, 409):
            raise
        details = err.response.text[:500] if err.response is not None else ""
        print(f"Internal aggregates daily write skipped (HTTP {status}): {details}")
