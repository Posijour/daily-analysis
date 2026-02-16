# window.py
from datetime import datetime, timedelta, timezone

def analysis_window_utc():
    now = datetime.now(timezone.utc)
    end = now.replace(hour=11, minute=0, second=0, microsecond=0)
    if now < end:
        end -= timedelta(days=1)
    start = end - timedelta(days=1)
    return start, end
