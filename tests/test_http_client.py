import os
import unittest
from unittest.mock import patch, MagicMock

import requests

os.environ.setdefault("SUPABASE_URL", "http://localhost:54321")
os.environ.setdefault("SUPABASE_KEY", "test-key")

from http_client import request_with_retry


class HttpClientTests(unittest.TestCase):
    @patch("http_client.requests.Session")
    @patch("http_client.requests.request")
    def test_proxy_error_falls_back_to_direct_request(self, request_mock, session_cls_mock):
        request_mock.side_effect = requests.exceptions.ProxyError("proxy blocked")

        session_mock = MagicMock()
        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.raise_for_status.return_value = None
        session_mock.request.return_value = response_mock

        session_cm = MagicMock()
        session_cm.__enter__.return_value = session_mock
        session_cm.__exit__.return_value = False
        session_cls_mock.return_value = session_cm

        resp = request_with_retry("GET", "https://example.com", retries=0)

        self.assertIs(resp, response_mock)
        request_mock.assert_called_once()
        session_mock.request.assert_called_once()


if __name__ == "__main__":
    unittest.main()
