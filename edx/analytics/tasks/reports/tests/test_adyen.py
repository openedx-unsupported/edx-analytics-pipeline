"""
Tests for Adyen financial reporting.
"""
import os

from ddt import ddt, data
import httpretty
import luigi
from mock import MagicMock, patch
import requests

from edx.analytics.tasks.reports.adyen import (
    DailyPullFromAdyenTask, DailyProcessFromAdyenTask, DEFAULT_RETRY_STATUS_CODES
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget
from edx.analytics.tasks.util.retry import RetryTimeoutError


TEST_URL = 'https://ca-test.adyen.com/reports/download'
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data/')


@ddt
@httpretty.activate
class TestAdyenDailyPullFromAdyenTask(unittest.TestCase):
    """Tests for fetching Adyen reports."""

    DEFAULT_DATE = '2016-08-17'

    def setUp(self):
        self.task = DailyPullFromAdyenTask(
            run_date=luigi.DateParameter().parse(self.DEFAULT_DATE),
            output_root='/fake/output',
            company_account='TestCompany',
            merchant_account='TestMerchant',
        )
        self.output_target = FakeTarget()
        self.task.output = MagicMock(return_value=self.output_target)
        dummy_adyen_report_path = os.path.join(TEST_DATA_DIR, 'adyen/2016-08/adyen_TestMerchant_20160817.csv')
        self.dummy_adyen_report = open(dummy_adyen_report_path, 'r').read()

    def test_normal_run(self):
        httpretty.register_uri(
            method=httpretty.GET,
            uri=self.task.report_url,
            status=200,
            body=self.dummy_adyen_report,
        )
        self.task.run()

        # verify that the fetch report request is made only once if there is
        # no error in downloading adyen report
        latest_requests = httpretty.HTTPretty.latest_requests
        self.assertEqual(len(latest_requests), 1)

        # verify that the task's output value is same as our dummy adyen
        # report file content
        output_target_value = self.output_target.value.strip()
        self.assertEqual(output_target_value, self.dummy_adyen_report)

    @data(
        *DEFAULT_RETRY_STATUS_CODES
    )
    @patch('edx.analytics.tasks.reports.adyen.DailyPullFromAdyenTask.REPORT_FETCH_TIMEOUT_SECONDS', 1)
    def test_fetch_report_with_retry_on_error(self, error_response_code):
        # create a list of http response statuses with the value provided as
        # 'error_response_code' and a success status '200'.
        statuses = [error_response_code, requests.codes.all_okay]

        responses = []
        for status in statuses:
            responses.append(httpretty.Response(body=self.dummy_adyen_report, status=status))

        httpretty.register_uri(
            method=httpretty.GET,
            uri=self.task.report_url,
            responses=responses
        )
        self.task.run()

        # verify that the fetch report request is made multiple times if the
        # responses status codes are retry-able
        latest_requests = httpretty.HTTPretty.latest_requests
        self.assertEqual(len(latest_requests), 2)

        # verify that the task's output value is same as our dummy adyen
        # report file content
        output_target_value = self.output_target.value.strip()
        self.assertEqual(output_target_value, self.dummy_adyen_report)

    @patch('edx.analytics.tasks.reports.adyen.DailyPullFromAdyenTask.REPORT_FETCH_TIMEOUT_SECONDS', 0.001)
    def test_fetch_report_with_error(self):
        """Verify that task 'TestAdyenDailyPullFromAdyenTask' raises exception
        'RetryTimeoutError' when the timeout expires even with exponential
        back-off and retry mechanism for recoverable, failed requests.
        """
        httpretty.register_uri(
            method=httpretty.GET,
            uri=self.task.report_url,
            status=503,
            body=self.dummy_adyen_report,
        )

        with self.assertRaises(RetryTimeoutError):
            self.task.run()


@ddt
class TestAdyenDailyProcessFromAdyenTask(unittest.TestCase):
    """Tests for processing the Adyen reports."""

    DEFAULT_DATE = '2016-08-17'

    def setUp(self):
        # set 'TEST_DATA_DIR' to local dir 'test_data' which contains fake adyen
        # csv report and that file will serve as input for this task
        self.task = DailyProcessFromAdyenTask(
            run_date=luigi.DateParameter().parse(self.DEFAULT_DATE),
            output_root=TEST_DATA_DIR,
            company_account='TestCompany',
            merchant_account='TestMerchant',
        )

        self.output_target = FakeTarget()
        self.task.output = MagicMock(return_value=self.output_target)

    def test_normal_run(self):
        self.task.run()

        expected_record = [
            [
                '2016-08-17', 'adyen', 'EdXORG', 'EDX-100431', 'USD', '273.85', '\\N', 'refund', 'credit_card',
                'visa', '8514713641604195'
            ],
            [
                '2016-08-17', 'adyen', 'EdXORG', 'EDX-100430', 'USD', '250.00', '\\N', 'sale', 'credit_card',
                'visa', '7914712985122903'
            ],
        ]
        expected_record_in_tsv_format = '\n'.join(['\t'.join(line) for line in expected_record])
        output_target_value = self.output_target.value.strip().split('\n at ')[0]

        # now verify that this task generates TSV file with selected columns
        # and record lines with status 'Settled' or 'Refunded' are used
        self.assertEqual(output_target_value, expected_record_in_tsv_format)
