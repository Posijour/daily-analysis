# job_log.py
from datetime import datetime, date
from supabase import supabase_post, supabase_patch

def log_job_start():
    supabase_post(
        "daily_job_runs",
        {
            "date": date.today().isoformat(),
            "started_at": datetime.utcnow().isoformat(),
            "status": "running",
        },
    )

def log_job_finish(status="ok"):
    supabase_patch(
        "daily_job_runs",
        {"date": f"eq.{date.today().isoformat()}"},
        {
            "finished_at": datetime.utcnow().isoformat(),
            "status": status,
        },
    )
