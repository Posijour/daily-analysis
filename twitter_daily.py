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

    supabase_post("twitter_logs", {"text": text})

    if AUTO_POST_TWITTER:
        pass  # Twitter API v2 здесь
