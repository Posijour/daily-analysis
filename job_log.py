# job_log.py
from datetime import date, datetime
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

    return True


def finish_daily_job(status: str = "ok"):
    """
    Обновляет статус job в конце выполнения
    """
    today = date.today().isoformat()

    supabase_patch(
        "daily_job_runs",
        {"date": f"eq.{today}"},
        {
            "finished_at": datetime.utcnow().isoformat(),
            "status": status,
        },
    )
