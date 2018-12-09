"""Test the retry decorator"""

from datetime import datetime, timedelta
from unittest import TestCase

from mock import call, patch, sentinel

from edx.analytics.tasks.util.retry import RetryTimeoutError, retry


class RetryTestCase(TestCase):
    """Test the retry decorator"""

    def setUp(self):
        sleep_patcher = patch('edx.analytics.tasks.util.retry.time.sleep', return_value=None)
        self.mock_sleep = sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

        self.func_call_counter = 0
        self.current_time = datetime(2016, 1, 1, 0, 0, 0, 0)
        self.time_offsets = [0]

        class MockDateTime(datetime):
            """A fake datetime module"""

            @classmethod
            def utcnow(cls):
                """Return a different time depending on the call counter."""
                return self.current_time + timedelta(seconds=self.time_offsets[self.func_call_counter])

        datetime_patcher = patch('edx.analytics.tasks.util.retry.datetime', MockDateTime)
        datetime_patcher.start()
        self.addCleanup(datetime_patcher.stop)

    def test_single_retry(self):

        @retry()
        def some_func():
            """A sample function"""
            self.func_call_counter += 1
            if self.func_call_counter == 1:
                raise Exception('error')

        some_func()
        self.assertEqual(self.func_call_counter, 2)
        self.mock_sleep.assert_called_once_with(0.5)

    def test_decorated_function_with_args(self):

        @retry()
        def some_func(a, b=0, c=1):  # pylint: disable=invalid-name
            """A sample function"""
            self.func_call_counter += 1
            if self.func_call_counter == 1:
                raise Exception('error')
            else:
                return a, b, c

        res_a, res_b, res_c = some_func(sentinel.a, b=sentinel.b)
        self.assertEqual(res_a, sentinel.a)
        self.assertEqual(res_b, sentinel.b)
        self.assertEqual(res_c, 1)
        self.assertEqual(self.func_call_counter, 2)

    def test_multiple_retry(self):

        @retry()
        def some_func():
            """A sample function"""
            self.func_call_counter += 1
            if self.func_call_counter <= 3:
                raise Exception('error')

        some_func()
        self.assertEqual(self.func_call_counter, 4)
        self.assertItemsEqual(self.mock_sleep.mock_calls, [call(0.5), call(1), call(2)])

    def test_different_base_delay(self):

        @retry(base_delay=1)
        def some_func():
            """A sample function"""
            self.func_call_counter += 1
            if self.func_call_counter <= 4:
                raise Exception('error')

        some_func()
        self.assertItemsEqual(self.mock_sleep.mock_calls, [call(1), call(2), call(4), call(8)])

    def test_fatal_exception(self):

        @retry(should_retry=retry_on_runtime_errors)
        def some_func():
            """A sample function"""
            raise UserError()

        with self.assertRaises(UserError):
            some_func()

    def test_retry_on_configured_exception(self):

        @retry(should_retry=retry_on_runtime_errors)
        def some_func():
            """A sample function"""
            self.func_call_counter += 1
            if self.func_call_counter == 1:
                raise RuntimeError()
            else:
                return "success"

        self.assertEqual(some_func(), "success")
        self.assertEqual(self.func_call_counter, 2)

    def test_fatal_error_after_retry(self):

        @retry(should_retry=retry_on_runtime_errors)
        def some_func():
            """A sample function"""
            self.func_call_counter += 1
            if self.func_call_counter == 1:
                raise RuntimeError()
            else:
                raise UserError()

        with self.assertRaises(UserError):
            some_func()

        self.assertEqual(self.func_call_counter, 2)

    def test_timeout(self):
        self.time_offsets = [0, 1.5]

        @retry(timeout=1)
        def some_func():
            """A sample function"""
            self.func_call_counter += 1
            raise RuntimeError()

        with self.assertRaises(RetryTimeoutError):
            some_func()

        self.assertEqual(self.func_call_counter, 1)

    def test_timeout_after_retry(self):
        self.time_offsets = [0, 1.5, 3.5]

        @retry(timeout=2)
        def some_func():
            """A sample function"""
            self.func_call_counter += 1
            raise RuntimeError()

        with self.assertRaises(RetryTimeoutError):
            some_func()

        self.assertEqual(self.func_call_counter, 2)


def retry_on_runtime_errors(exc):
    """Retry on any runtime error."""
    return isinstance(exc, RuntimeError)


class UserError(Exception):
    """An example error."""
    pass
