# job_log.py
import json
from datetime import date, datetime, timedelta
from pathlib import Path

from requests import HTTPError
from requests.exceptions import ConnectionError

from config import LOCK_STALE_MINUTES
from supabase import supabase_get, supabase_post, supabase_patch


LOCAL_LOCK_FILE = Path(".daily_job_lock.json")


def _write_local_lock_state(state: dict) -> None:
    LOCAL_LOCK_FILE.write_text(json.dumps(state), encoding="utf-8")


def _read_local_lock_state() -> dict:
    if not LOCAL_LOCK_FILE.exists():
        return {}
    try:
        return json.loads(LOCAL_LOCK_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def acquire_daily_lock() -> bool:
    today = date.today().isoformat()
    now = datetime.utcnow()
    
    try:
        supabase_post(
            "daily_job_runs",
            {"date": today, "started_at": now.isoformat(), "status": "running"},
            upsert=False,
        )
        print(f"Daily lock acquired for {today}: inserted running state.")
        return True
    except HTTPError as err:
        if err.response is None or err.response.status_code != 409:
            raise
    except ConnectionError:
        _write_local_lock_state(
            {
                "date": today,
                "started_at": now.isoformat(),
                "status": "running",
                "backend": "local_fallback",
            }
        )
        print(f"Daily lock acquired for {today}: local fallback state created.")
        return True

    existing = supabase_get("daily_job_runs", {"select": "status,started_at", "date": f"eq.{today}"})
    if not existing:
        return False

    status = existing[0].get("status")
    started_at = _parse_dt(existing[0].get("started_at"))
    stale_threshold = now - timedelta(minutes=LOCK_STALE_MINUTES)


    if status in ("ok", "running") and started_at and started_at > stale_threshold:
        print(f"Daily lock denied for {today}: current status is '{status}'.")
        return False

    supabase_patch(
        "daily_job_runs",
        {"date": f"eq.{today}"},
        {"status": "running", "started_at": now.isoformat(), "finished_at": None},
    )
    print(f"Daily lock reacquired for {today}: stale/failed run replaced with running.")
    
    return True


def finish_daily_job(status: str = "ok"):
    today = date.today().isoformat()
    finished_at = datetime.utcnow().isoformat()

    try:
        supabase_patch(
            "daily_job_runs",
            {"date": f"eq.{today}"},
            {"finished_at": finished_at, "status": status},
        )
        print(f"Daily job status synced to Supabase: {status} ({today}).")
    except HTTPError as err:
        code = err.response.status_code if err.response is not None else None
        print(f"Warning: failed to patch daily job status in Supabase (HTTP {code}). Trying fallback insert.")

        fallback_payload = {
            "date": today,
            "started_at": datetime.utcnow().isoformat(),
            "finished_at": finished_at,
            "status": status,
        }
        try:
            supabase_post("daily_job_runs", fallback_payload, upsert=False)
            print(f"Daily job status inserted to Supabase via fallback: {status} ({today}).")
        except HTTPError as fallback_err:
            fallback_code = (
                fallback_err.response.status_code
                if fallback_err.response is not None
                else None
            )
            print(f"Warning: failed to insert fallback daily job status in Supabase (HTTP {fallback_code}).")
    except ConnectionError:
        state = _read_local_lock_state()
        state.update(
            {
                "date": today,
                "status": status,
                "finished_at": finished_at,
                "backend": "local_fallback",
            }
        )
        state.setdefault("started_at", datetime.utcnow().isoformat())
        _write_local_lock_state(state)
        print(f"Daily job status saved locally: {status} ({today}).")
