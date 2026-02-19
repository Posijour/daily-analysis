# supabase.py
from config import SUPABASE_URL, HEADERS
from http_client import request_with_retry
from runtime_metrics import METRICS


def _url(table: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table}"


def supabase_get(table, params):
    METRICS.request_count += 1
    r = request_with_retry(
        "GET",
        _url(table),
        headers=HEADERS,
        params=params,
    )
    return r.json()


def supabase_post(table, payload, upsert: bool = True, on_conflict: str | None = None):
    headers = HEADERS.copy()
    params = None

    if upsert:
        headers["Prefer"] = "resolution=merge-duplicates"
        if on_conflict:
            params = {"on_conflict": on_conflict}

    if isinstance(payload, list):
        METRICS.payload_rows_out += len(payload)
    else:
        METRICS.payload_rows_out += 1

    METRICS.request_count += 1
    r = request_with_retry(
        "POST",
        _url(table),
        headers=headers,
        params=params,
        json=payload,
    )
    return r


def supabase_patch(table, params, payload):
    METRICS.request_count += 1
    r = request_with_retry(
        "PATCH",
        _url(table),
        headers=HEADERS,
        params=params,
        json=payload,
    )
    return r
