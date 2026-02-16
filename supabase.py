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

def supabase_post(table, payload):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=HEADERS,
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
