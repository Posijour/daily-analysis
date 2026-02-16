# main.py
from window import analysis_window_utc
from job_log import acquire_daily_lock, finish_daily_job

from deribit_daily import run_deribit_daily
from options_daily import run_options_daily
from risk_daily import run_risk_daily
from meta_daily import run_meta_daily
from twitter_daily import run_twitter_daily


def main():
    # 🔒 Защита от двойного запуска
    if not acquire_daily_lock():
        print("Daily analysis skipped: job already running or completed for today.")
        return

    status = "ok"

    try:
        start, end = analysis_window_utc()
        print(f"Daily analysis started. Window UTC: {start.isoformat()} -> {end.isoformat()}")
        
        run_deribit_daily(start, end)
        print("Deribit daily completed.")
        run_options_daily(start, end)
        print("Options daily completed.")
        run_risk_daily(start, end)
        print("Risk daily completed.")
        run_meta_daily(start, end)
        print("Meta daily completed.")
        run_twitter_daily(start, end)
        print("Twitter daily completed.")

    except Exception:
        status = "failed"
        raise

    finally:
        finish_daily_job(status)
        print(f"Daily analysis finished with status: {status}")

if __name__ == "__main__":
    main()
