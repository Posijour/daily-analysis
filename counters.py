# counters.py
import requests
from config import SUPABASE_URL, HEADERS

def next_counter(name: str) -> int:
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/increment_counter",
        headers=HEADERS,
        json={"counter_name": name},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()
