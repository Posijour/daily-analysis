# config.py
SUPABASE_URL = "https://qcusrlmueapuqbjwuwvh.supabase.co"
SUPABASE_KEY = "sb_publishable_XXXXXXXX"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

AUTO_POST_TWITTER = True   # ← одним флагом выключаешь автопостинг
