# window.py
from datetime import datetime, timedelta, timezone

def analysis_window_utc(now: datetime | None = None):
    """
    Возвращает UTC-окно анализа строго 11:00 -> 11:00.

    Если запуск раньше 11:00 UTC, окно будет за предыдущие сутки
    (позавчера 11:00 -> вчера 11:00).
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    end = now.replace(hour=11, minute=0, second=0, microsecond=0)
    if now < end:
        end -= timedelta(days=1)
    start = end - timedelta(days=1)
    return start, end
