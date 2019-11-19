"""Collect information about payments from third-party sources for financial reporting."""

from __future__ import absolute_import

import csv
import datetime
import logging
import os

import luigi
import requests
from luigi import date_interval
from luigi.configuration import get_config

from edx.analytics.tasks.common.pathutil import PathSelectionByDateIntervalTask, PathSetTask
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class PullFromCybersourceTaskMixin(OverwriteOutputMixin):
    """Define common parameters for Cybersource pull and downstream tasks."""

    merchant_id = luigi.Parameter(
        description='Cybersource merchant identifier.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )
    is_empty_transaction_allowed = luigi.BoolParameter(
        default=False,
        description='Allow empty transactions from payment processors to be processsed, default is False.'
    )

    def __init__(self, *args, **kwargs):
        super(PullFromCybersourceTaskMixin, self).__init__(*args, **kwargs)

        config = get_config()
        section_name = 'cybersource:' + self.merchant_id
        self.host = config.get(section_name, 'host')
        self.username = config.get(section_name, 'username')
        self.password = config.get(section_name, 'password')
        self.interval_start = luigi.DateParameter().parse(config.get(section_name, 'interval_start'))

        self.merchant_close_date = None
        merchant_close_date = config.get(section_name, 'merchant_close_date', '')
        if merchant_close_date:
            self.merchant_close_date = luigi.DateParameter().parse(merchant_close_date)


class DailyPullFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
    """
    A task that reads out of a remote Cybersource account and writes to a file.

    A complication is that this needs to be performed with more than one account
    (or merchant_id), with potentially different credentials.  If possible, create
    the same credentials (username, password) for each account.

    Pulls are made for only a single day.  This is what Cybersource
    supports for these reports, and it allows runs to performed
    incrementally on a daily tempo.

    """
    # Date to fetch Cybersource report.
    run_date = luigi.DateParameter(
        default=datetime.date.today(),
        description='Default is today.',
    )

    # This is the table that we had been using for gathering and
    # storing historical Cybersource data.  It adds one additional
    # column over the 'PaymentBatchDetailReport' format.
    REPORT_NAME = 'PaymentSubmissionDetailReport'
    REPORT_FORMAT = 'csv'

    def requires(self):
        pass

    def run(self):
        self.remove_output_on_overwrite()
        auth = (self.username, self.password)
        response = requests.get(self.query_url, auth=auth)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to Cybersource for {}".format(response.status_code, self.run_date)
            raise Exception(msg)

        # if there are no transactions in response, there will be no merchant id.
        if self.merchant_id not in response.content and not self.is_empty_transaction_allowed:
            raise Exception('No transactions to process.')

        with self.output().open('w') as output_file:
            output_file.write(response.content)

    def output(self):
        """Output is in the form {output_root}/cybersource/{CCYY-mm}/cybersource_{merchant}_{CCYYmmdd}.csv"""
        month_year_string = self.run_date.strftime('%Y-%m')  # pylint: disable=no-member
        date_string = self.run_date.strftime('%Y%m%d')  # pylint: disable=no-member
        filename = "cybersource_{merchant_id}_{date_string}.{report_format}".format(
            merchant_id=self.merchant_id,
            date_string=date_string,
            report_format=self.REPORT_FORMAT,
        )
        url_with_filename = url_path_join(self.output_root, "cybersource", month_year_string, filename)
        return get_target_from_url(url_with_filename)

    @property
    def query_url(self):
        """Generate the url to download a report from a Cybersource account."""
        slashified_date = self.run_date.strftime('%Y/%m/%d')  # pylint: disable=no-member
        url = 'https://{host}/DownloadReport/{date}/{merchant_id}/{report_name}.{report_format}'.format(
            host=self.host,
            date=slashified_date,
            merchant_id=self.merchant_id,
            report_name=self.REPORT_NAME,
            report_format=self.REPORT_FORMAT
        )
        return url


TRANSACTION_TYPE_MAP = {
    'ics_bill': 'sale',
    'ics_credit': 'refund'
}


class DailyProcessFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily Cybersource pull, and writes to a TSV file.

    The output file should be readable by Hive, and be in a common format across
    other payment accounts.

    """
    run_date = luigi.DateParameter(
        default=datetime.date.today(),
        description='Date to fetch Cybersource report. Default is today.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )

    def requires(self):
        args = {
            'run_date': self.run_date,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
            'merchant_id': self.merchant_id,
            'is_empty_transaction_allowed': self.is_empty_transaction_allowed
        }
        return DailyPullFromCybersourceTask(**args)

    def run(self):
        # Read from input and reformat for output.
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            # Skip the first line, which provides information about the source
            # of the file.  The second line should define the column headings.
            _download_header = input_file.readline()
            reader = csv.DictReader(input_file, delimiter=',')
            with self.output().open('w') as output_file:
                for row in reader:
                    # Output most of the fields from the original source.
                    # The values not included are:
                    #   batch_id: CyberSource batch in which the transaction was sent.
                    #   payment_processor: code for organization that processes the payment.
                    result = [
                        # Date
                        row['batch_date'],
                        # Name of system.
                        'cybersource',
                        # CyberSource merchant ID used for the transaction.
                        row['merchant_id'],
                        # Merchant-generated order reference or tracking number.
                        # For shoppingcart or otto, this should equal order_id,
                        # though sometimes it is basket_id.
                        row['merchant_ref_number'],
                        # ISO currency code used for the transaction.
                        row['currency'],
                        row['amount'],
                        # Transaction fee
                        '\\N',
                        TRANSACTION_TYPE_MAP[row['transaction_type']],
                        # We currently only process credit card transactions with Cybersource
                        'credit_card',
                        # Type of credit card used
                        row['payment_method'].lower().replace(' ', '_'),
                        # Identifier for the transaction.
                        row['request_id'],
                    ]
                    output_file.write('\t'.join(result))
                    output_file.write('\n')

    def output(self):
        """
        Output is set up so it can be read in as a Hive table with partitions.

        The form is {output_root}/payments/dt={CCYY-mm-dd}/cybersource_{merchant}.tsv
        """
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "cybersource_{}.tsv".format(self.merchant_id)
        url_with_filename = url_path_join(self.output_root, "payments", partition_path_spec, filename)
        return get_target_from_url(url_with_filename)


class IntervalPullFromCybersourceTask(PullFromCybersourceTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """Determines a set of dates to pull, and requires them."""

    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='Default is today, UTC.',
    )

    # Overwrite parameter definition to make it optional.
    output_root = luigi.Parameter(
        default=None,
        description='URL of location to write output.',
    )

    def __init__(self, *args, **kwargs):
        super(IntervalPullFromCybersourceTask, self).__init__(*args, **kwargs)
        # Provide default for output_root at this level.
        if self.output_root is None:
            self.output_root = self.warehouse_path

        path = url_path_join(self.warehouse_path, 'payments')
        file_pattern = '*cybersource_{}.tsv'.format(self.merchant_id)
        path_targets = PathSetTask([path], include=[file_pattern], include_zero_length=True).output()
        paths = list(set([os.path.dirname(target.path) for target in path_targets]))
        dates = [path.rsplit('/', 2)[-1] for path in paths]
        latest_date = sorted(dates)[-1]

        latest_completion_date = datetime.datetime.strptime(latest_date, "dt=%Y-%m-%d").date()
        run_date = latest_completion_date + datetime.timedelta(days=1)

        # Limit intervals to merchant account close date(if any).
        if self.merchant_close_date:
            run_date = min(run_date, self.merchant_close_date)
            self.interval_end = min(self.interval_end, self.merchant_close_date)

        self.selection_interval = date_interval.Custom(self.interval_start, run_date)
        self.run_interval = date_interval.Custom(run_date, self.interval_end)

    def requires(self):
        """Internal method to actually calculate required tasks once."""

        yield PathSelectionByDateIntervalTask(
            source=[url_path_join(self.warehouse_path, 'payments')],
            interval=self.selection_interval,
            pattern=['.*dt=(?P<date>\\d{{4}}-\\d{{2}}-\\d{{2}})/cybersource_{}\\.tsv'.format(self.merchant_id)],
            expand_interval=datetime.timedelta(0),
            date_pattern='%Y-%m-%d',
        )

        for run_date in self.run_interval:
            yield DailyProcessFromCybersourceTask(
                merchant_id=self.merchant_id,
                output_root=self.output_root,
                run_date=run_date,
                overwrite=self.overwrite,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            )

    def output(self):
        return [task.output() for task in self.requires()]


class CybersourceDataValidationTask(WarehouseMixin, luigi.WrapperTask):

    import_date = luigi.DateParameter()

    cybersource_merchant_ids = luigi.ListParameter(
        config_path={'section': 'payment', 'name': 'cybersource_merchant_ids'},
    )

    def requires(self):
        config = get_config()
        for merchant_id in self.cybersource_merchant_ids:
            section_name = 'cybersource:' + merchant_id
            interval_start = luigi.DateParameter().parse(config.get(section_name, 'interval_start'))
            interval_end = self.import_date

            merchant_close_date = config.get(section_name, 'merchant_close_date', '')
            if merchant_close_date:
                parsed_date = luigi.DateParameter().parse(merchant_close_date)
                interval_end = min(self.import_date, parsed_date)

            cybersource_interval = date_interval.Custom(interval_start, interval_end)

            for date in cybersource_interval:
                filename = "cybersource_{}.tsv".format(merchant_id)
                url = url_path_join(self.warehouse_path, 'payments', 'dt=' + date.isoformat(), filename)
                yield ExternalURL(url=url)
