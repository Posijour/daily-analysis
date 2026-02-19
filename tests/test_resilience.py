import os
import unittest

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from options_daily import dominant as options_dominant
from twitter_daily import dominant as twitter_dominant, detect_activity_regime
from meta_daily import base_meta


class ResilienceLogicTests(unittest.TestCase):
    def test_options_dominant_empty(self):
        value, pct = options_dominant([])
        self.assertEqual(value, "UNKNOWN")
        self.assertEqual(pct, 0.0)

    def test_twitter_dominant_empty(self):
        self.assertEqual(twitter_dominant([]), "UNKNOWN")

    def test_activity_regime_thresholds(self):
        self.assertEqual(detect_activity_regime(0), "CALM")
        self.assertEqual(detect_activity_regime(4), "FRAGILE_CALM")
        self.assertEqual(detect_activity_regime(8), "STRESS")

    def test_base_meta_handles_missing_fields(self):
        self.assertEqual(base_meta({}), "OPTION_LED_MOVE")


if __name__ == "__main__":
    unittest.main()
