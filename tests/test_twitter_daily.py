import os
import unittest

import pandas as pd

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from twitter_daily import build_deribit_summary, build_options_summary, map_deribit_line


class TwitterDailyTests(unittest.TestCase):
    def test_map_deribit_line_without_pattern_column(self):
        deribit = pd.DataFrame({"vbi_state": ["CALM", "CALM"]})
        line, has_pre_break = map_deribit_line(deribit)
        self.assertIn("no PRE-BREAK", line)
        self.assertFalse(has_pre_break)

    def test_build_options_summary_conflict(self):
        options = pd.DataFrame(
            {
                "regime": ["BUILDING", "STRESS"],
                "near_expiry_state": ["BUILDING", "BUILDING"],
                "mid_expiry_state": ["STRESS", "STRESS"],
                "mci": [0.2, -0.2],
                "mci_slope": [0.02, -0.02],
                "confidence": [0.6, 0.6],
            }
        )

        summary = build_options_summary(options)
        self.assertEqual(summary["summary_class"], "conflict")
        self.assertIn("near_mid_conflict", summary["reason_flags"])

    def test_build_deribit_summary_pre_break(self):
        deribit = pd.DataFrame(
            {
                "vbi_state": ["WARM", "WARM", "WARM", "WARM"],
                "vbi_pattern": ["PRE-BREAK", "PRE-BREAK", "PRE-BREAK", "NONE"],
                "iv_slope": [0.03, 0.03, 0.02, 0.03],
                "skew": [0.08, 0.07, 0.08, 0.09],
                "curvature": [0.03, 0.02, 0.04, 0.03],
            }
        )

        summary = build_deribit_summary(deribit)
        self.assertEqual(summary["summary_class"], "pre_break")
        self.assertIn("pre_break_candidate", summary["reason_flags"])


if __name__ == "__main__":
    unittest.main()
