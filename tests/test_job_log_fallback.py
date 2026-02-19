import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import requests

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

import job_log


class JobLogFallbackTests(unittest.TestCase):
    def test_acquire_daily_lock_uses_local_fallback_on_connection_error(self):
        with tempfile.TemporaryDirectory() as td:
            lock_file = Path(td) / ".daily_job_lock.json"
            with patch("job_log.LOCAL_LOCK_FILE", lock_file), patch(
                "job_log.supabase_post", side_effect=requests.exceptions.ConnectionError("offline")
            ):
                acquired = job_log.acquire_daily_lock()

            self.assertTrue(acquired)
            state = json.loads(lock_file.read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "running")
            self.assertEqual(state["backend"], "local_fallback")

    def test_finish_daily_job_writes_local_status_on_connection_error(self):
        with tempfile.TemporaryDirectory() as td:
            lock_file = Path(td) / ".daily_job_lock.json"
            seed = {
                "date": datetime.utcnow().date().isoformat(),
                "started_at": datetime.utcnow().isoformat(),
                "status": "running",
                "backend": "local_fallback",
            }
            lock_file.write_text(json.dumps(seed), encoding="utf-8")

            with patch("job_log.LOCAL_LOCK_FILE", lock_file), patch(
                "job_log.supabase_patch", side_effect=requests.exceptions.ConnectionError("offline")
            ):
                job_log.finish_daily_job("ok")

            state = json.loads(lock_file.read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "ok")
            self.assertIn("finished_at", state)


if __name__ == "__main__":
    unittest.main()
