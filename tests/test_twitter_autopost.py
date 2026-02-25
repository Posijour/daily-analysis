import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")
os.environ.setdefault("AUTO_POST_TWITTER", "true")

import twitter_daily


class TwitterAutopostTests(unittest.TestCase):
    @patch("twitter_daily.post_tweet")
    @patch("twitter_daily.supabase_post")
    @patch("twitter_daily.detect_anomaly", return_value="anomaly")
    @patch("twitter_daily.generate_daily_log", return_value="daily")
    def test_autoposts_only_daily_log(self, *_mocks):
        with patch.object(twitter_daily, "TWITTER_API_KEY", "k"), patch.object(twitter_daily, "TWITTER_API_SECRET", "s"), patch.object(twitter_daily, "TWITTER_ACCESS_TOKEN", "t"), patch.object(twitter_daily, "TWITTER_ACCESS_TOKEN_SECRET", "ts"):
            twitter_daily.run_twitter_daily(None, None)

        self.assertEqual(twitter_daily.supabase_post.call_count, 2)
        twitter_daily.supabase_post.assert_any_call("twitter_logs", {"text": "daily"})
        twitter_daily.supabase_post.assert_any_call("twitter_logs", {"text": "anomaly"})
        twitter_daily.post_tweet.assert_called_once_with(
            "daily",
            api_key="k",
            api_secret="s",
            access_token="t",
            access_token_secret="ts",
        )

    @patch("twitter_daily.post_tweet")
    @patch("twitter_daily.supabase_post")
    @patch("twitter_daily.detect_anomaly", return_value=None)
    @patch("twitter_daily.generate_daily_log", return_value="daily")
    def test_raises_when_credentials_missing(self, *_mocks):
        with patch.object(twitter_daily, "TWITTER_API_KEY", None), patch.object(twitter_daily, "TWITTER_API_SECRET", "s"), patch.object(twitter_daily, "TWITTER_ACCESS_TOKEN", "t"), patch.object(twitter_daily, "TWITTER_ACCESS_TOKEN_SECRET", "ts"):
            with self.assertRaises(RuntimeError) as ctx:
                twitter_daily.run_twitter_daily(None, None)

        self.assertIn("missing Twitter credentials", str(ctx.exception))
        twitter_daily.post_tweet.assert_not_called()


if __name__ == "__main__":
    unittest.main()
