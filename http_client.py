import random
import time
import requests

from config import HTTP_RETRIES, HTTP_TIMEOUT

RETRYABLE = {429, 500, 502, 503, 504}


def request_with_retry(method: str, url: str, **kwargs):
    timeout = kwargs.pop("timeout", HTTP_TIMEOUT)
    retries = kwargs.pop("retries", HTTP_RETRIES)

    last_exc = None
    for attempt in range(retries + 1):
        try:
            r = requests.request(method, url, timeout=timeout, **kwargs)
            if r.status_code in RETRYABLE and attempt < retries:
                backoff = (2 ** attempt) * 0.25 + random.uniform(0.05, 0.2)
                time.sleep(backoff)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as err:
            last_exc = err
            if attempt >= retries:
                raise
            backoff = (2 ** attempt) * 0.25 + random.uniform(0.05, 0.2)
            time.sleep(backoff)

    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_retry failed unexpectedly")
