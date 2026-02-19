import random
import time

import requests

from config import HTTP_RETRIES, HTTP_TIMEOUT

RETRYABLE = {429, 500, 502, 503, 504}


def _backoff_sleep(attempt: int):
    backoff = (2 ** attempt) * 0.25 + random.uniform(0.05, 0.2)
    time.sleep(backoff)


def _request_without_proxy(method: str, url: str, timeout: float, **kwargs):
    # Explicitly bypass env/system proxy settings (trust_env=False)
    # for environments where corporate proxy blocks HTTPS tunnel for Supabase.
    with requests.Session() as session:
        session.trust_env = False
        return session.request(
            method,
            url,
            timeout=timeout,
            proxies={"http": None, "https": None},
            **kwargs,
        )


def request_with_retry(method: str, url: str, **kwargs):
    timeout = kwargs.pop("timeout", HTTP_TIMEOUT)
    retries = kwargs.pop("retries", HTTP_RETRIES)

    last_exc = None
    for attempt in range(retries + 1):
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
        except requests.exceptions.ProxyError:
            # One immediate direct attempt per retry iteration without proxy.
            try:
                response = _request_without_proxy(method, url, timeout=timeout, **kwargs)
            except requests.RequestException as err:
                last_exc = err
                if attempt >= retries:
                    raise
                _backoff_sleep(attempt)
                continue
        except requests.RequestException as err:
            last_exc = err
            if attempt >= retries:
                raise
            _backoff_sleep(attempt)
            continue

        if response.status_code in RETRYABLE and attempt < retries:
            _backoff_sleep(attempt)
            continue

        response.raise_for_status()
        return response

    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_retry failed unexpectedly")

