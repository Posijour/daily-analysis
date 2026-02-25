import base64
import hashlib
import hmac
import secrets
import time
from urllib.parse import quote

from http_client import request_with_retry


TWITTER_POST_URL = "https://api.twitter.com/2/tweets"


def _percent_encode(value):
    return quote(str(value), safe="~")


def _build_oauth_header(method, url, api_key, api_secret, access_token, access_token_secret):
    oauth_params = {
        "oauth_consumer_key": api_key,
        "oauth_nonce": secrets.token_hex(16),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": access_token,
        "oauth_version": "1.0",
    }

    param_string = "&".join(
        f"{_percent_encode(key)}={_percent_encode(value)}"
        for key, value in sorted(oauth_params.items())
    )

    signature_base = "&".join([
        method.upper(),
        _percent_encode(url),
        _percent_encode(param_string),
    ])

    signing_key = f"{_percent_encode(api_secret)}&{_percent_encode(access_token_secret)}"
    digest = hmac.new(signing_key.encode(), signature_base.encode(), hashlib.sha1).digest()
    oauth_params["oauth_signature"] = base64.b64encode(digest).decode()

    header = ", ".join(
        f'{_percent_encode(key)}="{_percent_encode(value)}"'
        for key, value in sorted(oauth_params.items())
    )
    return f"OAuth {header}"


def post_tweet(text, api_key, api_secret, access_token, access_token_secret):
    auth_header = _build_oauth_header(
        "POST",
        TWITTER_POST_URL,
        api_key,
        api_secret,
        access_token,
        access_token_secret,
    )

    return request_with_retry(
        "POST",
        TWITTER_POST_URL,
        headers={
            "Authorization": auth_header,
            "Content-Type": "application/json",
        },
        json={"text": text},
    )
