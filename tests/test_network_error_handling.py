import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from telegram.error import NetworkError

import main


class NetworkErrorWindowTests(unittest.TestCase):
    def setUp(self):
        self.original_reset_seconds = main.NETWORK_ERROR_RESET_SECONDS
        main.NETWORK_ERROR_RESET_SECONDS = 300
        main._network_error_count = 0
        main._last_network_error_at = None

    def tearDown(self):
        main.NETWORK_ERROR_RESET_SECONDS = self.original_reset_seconds
        main._network_error_count = 0
        main._last_network_error_at = None

    def test_errors_inside_window_are_accumulated(self):
        self.assertEqual(main._record_network_error(now=100.0), 1)
        self.assertEqual(main._record_network_error(now=399.9), 2)

    def test_error_at_window_boundary_starts_new_incident(self):
        self.assertEqual(main._record_network_error(now=100.0), 1)
        self.assertEqual(main._record_network_error(now=400.0), 1)

    def test_httpx_error_names_are_recognized(self):
        self.assertTrue(main._is_network_error(Exception("httpx.ConnectError")))
        self.assertTrue(main._is_network_error(Exception("httpx.ConnectTimeout")))
        self.assertFalse(main._is_network_error(RuntimeError("业务处理失败")))


class NetworkErrorHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        main._network_error_count = 0
        main._last_network_error_at = None

    async def asyncTearDown(self):
        main._network_error_count = 0
        main._last_network_error_at = None

    async def test_network_error_does_not_stop_application(self):
        application = MagicMock()
        context = SimpleNamespace(
            error=NetworkError("代理连接失败"),
            application=application,
        )

        await main.error_handler(None, context)

        application.stop_running.assert_not_called()
        self.assertEqual(main._network_error_count, 1)

    async def test_non_network_error_is_still_logged(self):
        application = MagicMock()
        context = SimpleNamespace(
            error=RuntimeError("业务处理失败"),
            application=application,
        )

        with self.assertLogs(main.logger, level="ERROR") as captured_logs:
            await main.error_handler(None, context)

        application.stop_running.assert_not_called()
        self.assertIn("非网络异常", "\n".join(captured_logs.output))


if __name__ == "__main__":
    unittest.main()
