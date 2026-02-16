# job_log.py
from datetime import date, datetime
from requests import HTTPError
from supabase import supabase_get, supabase_post, supabase_patch


def acquire_daily_lock() -> bool:
    """
    Возвращает True, если lock успешно получен.
    Возвращает False, если job уже запускался сегодня.
    """
    today = date.today().isoformat()

    existing = supabase_get(
        "daily_job_runs",
        {
            "select": "status",
            "date": f"eq.{today}",
        },
    )

    # Если запись уже есть — второй запуск запрещаем
    if existing:
        status = existing[0]["status"]
        if status in ("running", "ok"):
            print(f"Daily lock denied for {today}: current status is '{status}'.")
            return False

    # Пытаемся создать lock
    supabase_post(
        "daily_job_runs",
        {
            "date": today,
            "started_at": datetime.utcnow().isoformat(),
            "status": "running",
        },
    )
    print(f"Daily lock acquired for {today}: status set to 'running' in Supabase.")
    
    return True


def finish_daily_job(status: str = "ok"):
    """
    Обновляет статус job в конце выполнения
    """
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
