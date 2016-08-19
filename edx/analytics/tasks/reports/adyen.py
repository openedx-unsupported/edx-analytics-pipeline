"""
Collect transactions information from Adyen for financial reporting.
"""
import csv
from datetime import datetime
import logging

import luigi
from luigi import date_interval
from luigi.configuration import get_config
from pytz import timezone, utc
import requests

from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.retry import retry


log = logging.getLogger(__name__)
ADYEN_CONFIG_SECTION = 'adyen'
DEFAULT_RETRY_STATUS_CODES = (
    requests.codes.request_timeout,         # HTTP Status Code 408
    requests.codes.too_many_requests,       # HTTP Status Code 429
    requests.codes.service_unavailable,     # HTTP Status Code 503
    requests.codes.gateway_timeout          # HTTP Status Code 504
)
DEFAULT_TIMEOUT_SECONDS = 7200
# These are the transaction states (accounting record types) that we consider
# relevant from a list of possible accounting record types, e.g. Received,
# Authorised, Refused, SentForSettle, Settled, SentForRefund, Refunded.
TRANSACTION_TYPE_MAP = {
    'Settled': 'sale',
    'Refunded': 'refund'
}


class PullFromAdyenTaskMixin(OverwriteOutputMixin):
    """Define common parameters for Adyen pull and downstream tasks."""

    company_account = luigi.Parameter(
        description='Adyen company acount identifier.',
    )
    merchant_account = luigi.Parameter(
        description='Adyen merchant identifier.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )


class DailyPullFromAdyenTask(PullFromAdyenTaskMixin, luigi.Task):
    """A task that reads out Adyen reports "Payment Accounting Report" from a
    remote Adyen account with a report user and generates a CSV file.

    Subscribe Adyen daily reports "Payment Accounting Report" with CSV format.
    https://docs.adyen.com/developers/reporting-manual#reportdownload

    Pulls are made for only a single day. It allows runs to be performed
    incrementally on a daily tempo.

    """
    # Date to fetch Adyen report.
    run_date = luigi.DateParameter(
        default=datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    username = luigi.Parameter(
        default_from_config={'section': ADYEN_CONFIG_SECTION, 'name': 'username'},
        description='Username for the Adyen account, for which data is being gathered.',
    )
    url = luigi.Parameter(
        default_from_config={'section': ADYEN_CONFIG_SECTION, 'name': 'url'},
        description='Base URL for the Adyen reports.',
    )

    REPORT_FETCH_TIMEOUT_SECONDS = DEFAULT_TIMEOUT_SECONDS
    REPORT_NAME = 'payments_accounting_report'
    REPORT_FORMAT = 'csv'

    def __init__(self, *args, **kwargs):
        super(DailyPullFromAdyenTask, self).__init__(*args, **kwargs)

        config = get_config()
        self.password = config.get(ADYEN_CONFIG_SECTION, 'password')

    def run(self):
        self.remove_output_on_overwrite()

        # Fetch the report
        response = self.fetch_report(timeout_seconds=self.REPORT_FETCH_TIMEOUT_SECONDS)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to Adyen for {}".format(response.status_code, self.run_date)
            raise Exception(msg)

        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """File based output which contains actual Adyen csv report.

        Output form: {output_root}/adyen/{CCYY-mm}/adyen_{merchant_account}_{CCYYmmdd}.csv
        """
        month_year_string = self.run_date.strftime('%Y-%m')  # pylint: disable=no-member
        date_string = self.run_date.strftime('%Y%m%d')  # pylint: disable=no-member
        filename = 'adyen_{merchant_account}_{date_string}.{report_format}'.format(
            merchant_account=self.merchant_account,
            date_string=date_string,
            report_format=self.REPORT_FORMAT,
        )
        url_with_filename = url_path_join(self.output_root, 'adyen', month_year_string, filename)
        return get_target_from_url(url_with_filename)

    def fetch_report(self, timeout_seconds, retry_on=DEFAULT_RETRY_STATUS_CODES):
        """Fetch a report from Adyen account with retry mechanism.

        Arguments:
            timeout_seconds (float): When requesting a report, keep retrying
                unless this much time has elapsed.
            retry_on (iterable): This is a set of HTTP status codes that should
                trigger a retry of the request if they are received from the
                server in the response. If one is received the system
                implements an exponential back-off and repeatedly requests the
                page until either the timeout expires, a fatal exception
                occurs, or an OK response is received.

        """
        auth = (
            '{report_username}%Company.{company_account}'.format(
                report_username=self.username,
                company_account=self.company_account
            ), self.password
        )

        def should_retry_fetch_report(error):
            """Retry the report download if the response status code is in the
            set of status codes that are retry-able.
            """
            error_response = getattr(error, 'response', None)
            if error_response is None:
                return False

            return error_response.status_code in retry_on

        @retry(should_retry=should_retry_fetch_report, timeout=timeout_seconds)
        def fetch_report_with_retry():
            """Fetches Adyen report, using an exponential back-off to retry
            recoverable, failed requests.
            """
            report_response = requests.get(self.report_url, auth=auth)

            # raise an error if the request failed
            report_response.raise_for_status()
            return report_response

        response = fetch_report_with_retry()
        return response

    @property
    def report_url(self):
        """Generate the url to download a report from an Adyen account.

        Example URL:
            https://ca-test.adyen.com/reports/download/MerchantAccount/EdXORG/payments_accounting_report_2016_08_17.csv
        """
        url = '{base_url}/MerchantAccount/{merchant_account}/{report_name}_{report_date}.{report_format}'.format(
            base_url=self.url,
            merchant_account=self.merchant_account,
            report_name=self.REPORT_NAME,
            report_date=self.run_date.strftime('%Y_%m_%d'),
            report_format=self.REPORT_FORMAT
        )
        return url


class DailyProcessFromAdyenTask(PullFromAdyenTaskMixin, luigi.Task):
    """This task reads a local file generated from a daily Adyen pull, and
    writes to a TSV file in custom format with selected fields.

    The output file should be readable by Hive, and be in a common format
    across other payment accounts.

    """
    run_date = luigi.DateParameter(
        default=datetime.utcnow().date(),
        description='Date to fetch Adyen report. Default is today, UTC.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )

    def requires(self):
        args = {
            'run_date': self.run_date,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
            'company_account': self.company_account,
            'merchant_account': self.merchant_account,
        }
        return DailyPullFromAdyenTask(**args)

    def run(self):
        # Read from local input and reformat for output.
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            # DictReader class skips the header row and uses it for named indexing.
            csv_reader = csv.DictReader(input_file, delimiter=',')

            with self.output().open('w') as output_file:
                for row in csv_reader:
                    # Output most of the fields from the original source file.
                    # https://docs.adyen.com/developers/reporting-manual#paymentaccountingreportstructure

                    # Read only settled (sale) or refunded (refund) transaction records.
                    transaction_type = row['Record Type']
                    if transaction_type not in TRANSACTION_TYPE_MAP:
                        continue

                    booking_date = datetime.strptime(
                        row['Booking Date'], '%Y-%m-%d %H:%M:%S'
                    ).replace(tzinfo=timezone(row['TimeZone'])).astimezone(utc)

                    result = [
                        # date (ISO8601 formatted transaction creation date)
                        booking_date.date().isoformat(),
                        # payment_gateway_id (Name of payment system)
                        'adyen',
                        # payment_gateway_account_id (Adyen merchant account used for the transaction)
                        row['Merchant Account'],
                        # payment_ref_id (Merchant order reference or tracking number, e.g. EDX-1111)
                        row['Merchant Reference'],
                        # iso_currency_code (ISO currency code used for the transaction)
                        row['Main Currency'],
                        # amount (Total order amount)
                        row['Main Amount'],
                        # transaction_fee (Charges of Payment gateway to process the transaction)
                        '\\N',
                        # transaction_type (This should either be 'sale' or 'refund')
                        TRANSACTION_TYPE_MAP[transaction_type],
                        # payment_method (We currently only process credit card transactions)
                        'credit_card',
                        # payment_method_type (The subbrand of the payment method used, e.g. visa, mc, amex etc)
                        row['Payment Method'],
                        # transaction_id (Psp Reference: Adyen identifier for the transaction)
                        row['Psp Reference'],
                    ]
                    output_file.write('\t'.join(result))
                    output_file.write('\n')

    def output(self):
        """Output is set up so it can be read in as a Hive table with partitions.

        Output form: {output_root}/payments/dt={CCYY-mm-dd}/adyen_{merchant}.tsv
        """
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "adyen_{}.tsv".format(self.merchant_account)
        url_with_filename = url_path_join(self.output_root, "payments", partition_path_spec, filename)
        return get_target_from_url(url_with_filename)


class AdyenTransactionsIntervalTask(PullFromAdyenTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """Generate Adyen transaction reports for each day in an interval."""

    interval = luigi.DateIntervalParameter(
        default=None,
    )
    interval_start = luigi.DateParameter(
        default_from_config={'section': ADYEN_CONFIG_SECTION, 'name': 'interval_start'},
        significant=False,
    )
    interval_end = luigi.DateParameter(
        default=datetime.utcnow().date(),
        significant=False,
        description='Default is today, UTC.',
    )

    # Overwrite parameter definition to make it optional.
    output_root = luigi.Parameter(
        default=None,
        description='URL of location to write output.',
    )

    def __init__(self, *args, **kwargs):
        super(AdyenTransactionsIntervalTask, self).__init__(*args, **kwargs)
        # Provide default for output_root at this level.
        if self.output_root is None:
            self.output_root = self.warehouse_path
        if self.interval is None:
            self.interval = date_interval.Custom(self.interval_start, self.interval_end)

    def requires(self):
        """Internal method to actually calculate required tasks once."""
        args = {
            'company_account': self.company_account,
            'merchant_account': self.merchant_account,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
        }

        for run_date in self.interval:
            args['run_date'] = run_date
            yield DailyProcessFromAdyenTask(**args)

    def output(self):
        return [task.output() for task in self.requires()]
