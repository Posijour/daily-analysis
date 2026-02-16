# main.py
from window import analysis_window_utc
from job_log import log_job_start, log_job_finish

from deribit_daily import run_deribit_daily
from options_daily import run_options_daily
from risk_daily import run_risk_daily
from meta_daily import run_meta_daily
from twitter_daily import run_twitter_daily

def main():
    log_job_start()
    status = "ok"

    try:
        start, end = analysis_window_utc()

        run_deribit_daily(start, end)
        run_options_daily(start, end)
        run_risk_daily(start, end)
        run_meta_daily(start, end)
        run_twitter_daily(start, end)

    except Exception:
        status = "failed"
        raise
    finally:
        log_job_finish(status)

if __name__ == "__main__":
    main()
