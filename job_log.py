# job_log.py
from datetime import date, datetime, timedelta
from requests import HTTPError

from config import LOCK_STALE_MINUTES
from supabase import supabase_get, supabase_post, supabase_patch


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

    payload = {
        "finished_at": datetime.utcnow().isoformat(),
        "status": status,
    }

    try:
        supabase_patch("daily_job_runs", {"date": f"eq.{today}"}, payload)
        print(f"Daily job status synced to Supabase: {status} ({today}).")
    except HTTPError as err:
        code = err.response.status_code if err.response is not None else None
        print(f"Warning: failed to patch daily job status in Supabase (HTTP {code}). Trying fallback insert.")

        fallback_payload = {
            "date": today,
            "started_at": datetime.utcnow().isoformat(),
            "finished_at": payload["finished_at"],
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
