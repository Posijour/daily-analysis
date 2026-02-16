# supabase.py
import requests
from config import SUPABASE_URL, HEADERS


def supabase_get(table, params):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        params=params,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def supabase_post(table, payload, upsert: bool = True, on_conflict: str | None = None):
    headers = HEADERS.copy()
    params = None
    
    if upsert:
        headers["Prefer"] = "resolution=merge-duplicates"
        if on_conflict:
            params = {"on_conflict": on_conflict}

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=headers,
        params=params,
        json=payload,
        timeout=30,
    )
    r.raise_for_status()


def supabase_patch(table, params, payload):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
        params=params,
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
