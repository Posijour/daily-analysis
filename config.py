# config.py
import os


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


SUPABASE_URL = _require_env("SUPABASE_URL")
SUPABASE_KEY = _require_env("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

AUTO_POST_TWITTER = os.getenv("AUTO_POST_TWITTER", "true").lower() in {"1", "true", "yes"}

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET = os.getenv("TWITTER_API_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
LOCK_STALE_MINUTES = int(os.getenv("LOCK_STALE_MINUTES", "180"))
