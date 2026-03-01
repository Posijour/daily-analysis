import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")
import telegram_daily


class TelegramAutopostTests(unittest.TestCase):
    @patch("telegram_daily.post_telegram_message")
    @patch("telegram_daily.supabase_post")
    @patch("telegram_daily.generate_daily_log", return_value="daily")
    def test_autoposts_daily_log(self, *_mocks):
        with patch.object(telegram_daily, "AUTO_POST_TELEGRAM", True), patch.object(telegram_daily, "TELEGRAM_BOT_TOKEN", "bot-token"), patch.object(telegram_daily, "TELEGRAM_CHAT_ID", "123"):
            telegram_daily.run_telegram_daily(None, None)

        telegram_daily.supabase_post.assert_called_once_with("telegram_logs", {"text": "daily"})
        telegram_daily.post_telegram_message.assert_called_once_with(
            "daily",
            bot_token="bot-token",
            chat_id="123",
        )

    @patch("telegram_daily.post_telegram_message")
    @patch("telegram_daily.supabase_post")
    @patch("telegram_daily.generate_daily_log", return_value="daily")
    def test_raises_when_credentials_missing(self, *_mocks):
        with patch.object(telegram_daily, "AUTO_POST_TELEGRAM", True), patch.object(telegram_daily, "TELEGRAM_BOT_TOKEN", None), patch.object(telegram_daily, "TELEGRAM_CHAT_ID", "123"):
            with self.assertRaises(RuntimeError) as ctx:
                telegram_daily.run_telegram_daily(None, None)

        self.assertIn("missing Telegram credentials", str(ctx.exception))
        telegram_daily.post_telegram_message.assert_not_called()


if __name__ == "__main__":
    unittest.main()
