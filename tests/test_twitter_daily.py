import os
import unittest

import pandas as pd

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from twitter_daily import map_deribit_line


class TwitterDailyTests(unittest.TestCase):
    def test_map_deribit_line_without_pattern_column(self):
        deribit = pd.DataFrame({"vbi_state": ["CALM", "CALM"]})
        line, has_pre_break = map_deribit_line(deribit)
        self.assertIn("flat vol", line)
        self.assertFalse(has_pre_break)


if __name__ == "__main__":
    unittest.main()
