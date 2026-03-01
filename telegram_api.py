from http_client import request_with_retry


TELEGRAM_API_BASE = "https://api.telegram.org"


def post_telegram_message(text: str, bot_token: str, chat_id: str):
    return request_with_retry(
        "POST",
        f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
    )
