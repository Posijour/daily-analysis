from requests import HTTPError
from counters import next_counter
from supabase import supabase_post
from config import AUTO_POST_TWITTER

def run_twitter_daily(start, end):
    n = next_counter("daily_risk_log")

    text = f"""Risk Log #{n}

24h snapshot complete.
Market context recorded.

Market log, not a forecast.
"""

    try:
        supabase_post("twitter_logs", {"text": text})
    except HTTPError as err:
        status = err.response.status_code if err.response is not None else None

        # Twitter-лог не должен ронять весь daily job, если таблица не создана
        # или у ключа нет прав на запись.
        if status not in (401, 403, 404):
            raise

    if AUTO_POST_TWITTER:
        pass  # Twitter API v2 здесь
