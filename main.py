# main.py
import uuid
from time import perf_counter

from window import analysis_window_utc
from job_log import acquire_daily_lock, finish_daily_job
from observability import log_event
from runtime_metrics import METRICS

from deribit_daily import run_deribit_daily
from options_daily import run_options_daily
from risk_daily import run_risk_daily
from risk_divergence_daily import run_risk_divergence_daily
from meta_daily import run_meta_daily
from twitter_daily import run_twitter_daily

MODULES = [
    ("deribit", run_deribit_daily),
    ("options", run_options_daily),
    ("risk", run_risk_daily),
    ("risk_divergence", run_risk_divergence_daily),
    ("meta", run_meta_daily),
    ("twitter", run_twitter_daily),
]


def main():
    run_id = str(uuid.uuid4())
    t0 = perf_counter()

    if not acquire_daily_lock():
        log_event("daily.skipped", run_id=run_id, reason="lock_denied")
        return

    status = "ok"
    module_status = {}

    try:
        start, end = analysis_window_utc()
        log_event("daily.started", run_id=run_id, window_start=start.isoformat(), window_end=end.isoformat())

        for module_name, runner in MODULES:
            METRICS.start(module_name)
            try:
                runner(start, end)
                module_status[module_name] = "ok"
                log_event("daily.module.ok", run_id=run_id, module=module_name)
            except Exception as err:
                status = "failed"
                module_status[module_name] = f"failed:{type(err).__name__}"
                log_event("daily.module.failed", run_id=run_id, module=module_name, error=repr(err))
            finally:
                METRICS.stop(module_name)

    finally:
        try:
            finish_daily_job(status)
        except Exception as log_err:
            log_event("daily.status_sync.failed", run_id=run_id, error=repr(log_err))

        elapsed = round(perf_counter() - t0, 3)
        log_event(
            "daily.finished",
            run_id=run_id,
            status=status,
            elapsed_sec=elapsed,
            module_status=module_status,
            request_count=METRICS.request_count,
            payload_rows_in=METRICS.payload_rows_in,
            payload_rows_out=METRICS.payload_rows_out,
            module_durations=METRICS.module_durations,
        )


if __name__ == "__main__":
    main()
