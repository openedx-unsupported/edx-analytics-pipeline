"""
Tasks to support pulling Affiliate Window reports from their REST API to the data warehouse.
"""
from __future__ import absolute_import, print_function

import csv
import datetime
import json
import logging
import os

import luigi
import requests
from luigi import date_interval

from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.retry import retry
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

logger = logging.getLogger(__name__)


DEFAULT_RETRY_STATUS_CODES = (
    requests.codes.internal_server_error,   # 500
    requests.codes.request_timeout,         # 408
    requests.codes.too_many_requests,       # 429
    requests.codes.service_unavailable,     # 503
    requests.codes.gateway_timeout          # 504
)
DEFAULT_TIMEOUT_SECONDS = 300


class AffiliateWindowTaskMixin(OverwriteOutputMixin):
    """
    The parameters needed to run Affiliate Window reports.
    """

    output_root = luigi.Parameter(
        description='The parent folder to write the Affiliate Window report data to.',
    )
    run_date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='The date to generate a report for. Default is today, UTC.',
    )
    advertiser_id = luigi.Parameter(
        config_path={'section': 'affiliate_window', 'name': 'advertiser_id'},
        description='The Affiliate Window advertiser id, usually a number.',
    )
    api_token = luigi.Parameter(
        config_path={'section': 'affiliate_window', 'name': 'api_token'},
        description='The Affiliate Window OAuth2 token.',
    )
    host = luigi.Parameter(
        config_path={'section': 'affiliate_window', 'name': 'host'},
        description='The Affiliate Window API host name.',
    )
    interval_start = luigi.DateParameter(
        config_path={'section': 'affiliate_window', 'name': 'interval_start'},
        description='The earliest date we can pull Affiliate Window reports from.',
    )


class IntervalPullFromAffiliateWindowTask(AffiliateWindowTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """
    Determines a set of dates to pull, and requires them.
    """
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
        super(IntervalPullFromAffiliateWindowTask, self).__init__(*args, **kwargs)

        # Provide default for output_root at this level.
        if self.output_root is None:
            self.output_root = url_path_join(self.warehouse_path, 'fees', 'affiliate_window')

        path = self.output_root
        file_pattern = '*affiliate_window.tsv'
        path_targets = PathSetTask([path], include=[file_pattern], include_zero_length=True).output()

        if path_targets:
            paths = list(set([os.path.dirname(target.path) for target in path_targets]))
            dates = [path.rsplit('/', 2)[-1] for path in paths]
            latest_date = sorted(dates)[-1]
            latest_completion_date = datetime.datetime.strptime(latest_date, "dt=%Y-%m-%d").date()
            self.interval_start = latest_completion_date + datetime.timedelta(days=1)
            print(("Found previous reports to {}".format(latest_date)))
        else:
            # If this is the first run, start from the beginning
            print(("Couldn't find last completed date, defaulting to start date: {}".format(self.interval_start)))

        self.run_interval = date_interval.Custom(self.interval_start, self.interval_end)
        print(("Running reports from interval {}".format(self.run_interval)))

    def requires(self):
        """
        Internal method to actually calculate required tasks once.
        """
        for run_date in self.run_interval:
            print(run_date)
            yield DailyProcessFromAffiliateWindowTask(
                output_root=self.output_root,
                run_date=run_date,
                advertiser_id=self.advertiser_id,
                api_token=self.api_token,
                host=self.host,
                interval_start=self.interval_start
            )

    def output(self):
        return [task.output() for task in self.requires()]


def should_retry(error):
    """
    Retry the request if the response status code is in the set of status codes that are retryable.
    """
    error_response = getattr(error, 'response', None)
    if error_response is None:
        return False

    return error_response.status_code in DEFAULT_RETRY_STATUS_CODES


class DailyPullFromAffiliateWindowTask(AffiliateWindowTaskMixin, luigi.Task):
    """
    A task that pulls from the Affiliate Window REST API and writes to a file.

    Pulls are made for only a single day.
    """
    query_date = luigi.DateParameter(
        default=datetime.date.today(),
        description='Default is today.',
    )

    REPORT_NAME = 'AffiliateWindowReport'
    REPORT_FORMAT = 'json'

    def requires(self):
        pass

    @retry(should_retry=should_retry, timeout=DEFAULT_TIMEOUT_SECONDS)
    def fetch_report(self):
        params = (
            ('startDate', self.query_date.strftime('%Y-%m-%dT00:00:00')),
            ('endDate', self.query_date.strftime('%Y-%m-%dT23:59:59')),
            ('timezone', 'UTC'),
            ('accessToken', self.api_token),
        )
        url = 'https://{}/advertisers/{}/transactions/'.format(self.host, self.advertiser_id)

        response = requests.get(url, params=params)
        response.raise_for_status()

        return response.json()

    def run(self):
        print(("Fetching report from Affiliate Window for {}.".format(self.query_date)))
        transactions = self.fetch_report()

        print(("{} transactions found from Affiliate Window.".format(len(transactions))))

        # if there are no transactions in response something is wrong.
        if not transactions:
            raise Exception('No transactions to process.')

        with self.output().open('w') as output_file:
            json.dump(transactions, output_file)

    def output(self):
        """
        Output is in the form {output_root}/affiliate_window/{CCYY-mm}/affiliate_window_{CCYYmmdd}.json
        """
        month_year_string = self.query_date.strftime('%Y-%m')  # pylint: disable=no-member
        date_string = self.query_date.strftime('%Y%m%d')  # pylint: disable=no-member
        filename = "affiliate_window_{date_string}.{report_format}".format(
            date_string=date_string,
            report_format=self.REPORT_FORMAT,
        )
        url_with_filename = url_path_join(self.output_root, month_year_string, filename)
        return get_target_from_url(url_with_filename)


class DailyProcessFromAffiliateWindowTask(AffiliateWindowTaskMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily Affiliate Window pull, and writes to a TSV file.

    The output file should be readable by Hive.
    """
    run_date = luigi.DateParameter(
        default=datetime.date.today(),
        description='Date to fetch Affiliate Window report. Default is today.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )

    def requires(self):
        args = {
            'query_date': self.run_date,
            'output_root': self.output_root,
            'advertiser_id': self.advertiser_id,
            'api_token': self.api_token,
            'host': self.host,
            'interval_start': self.interval_start,
        }
        return DailyPullFromAffiliateWindowTask(**args)

    def run(self):
        print(("Processing Affiliate Window report for {}".format(self.run_date)))
        with self.input().open('r') as input_file:
            reader = json.load(input_file)

            print(("{} transactions found in JSON loaded from disk.".format(len(reader))))

            with self.output().open('w') as output_file:
                writer = csv.writer(output_file, delimiter="\t")
                found_ids = set()

                for row in reader:
                    if row['id'] in found_ids:
                        raise Exception("Found duplicate id: {}!".format(row['id']))

                    print(("Writing id {}".format(row['id'])))
                    found_ids.add(row['id'])

                    # Break out commonly used fields, put entire row blob into last column
                    result = [
                        row['id'],
                        row['url'],
                        row['publisherId'],
                        row['commissionSharingPublisherId'],
                        row['siteName'],
                        row['commissionStatus'],
                        row['commissionAmount']['amount'],
                        row['saleAmount']['amount'],
                        row['customerCountry'],
                        row['clickDate'],
                        row['transactionDate'],
                        row['validationDate'],
                        row['type'],
                        row['declineReason'],
                        row['voucherCodeUsed'],
                        row['voucherCode'],
                        row['amended'],
                        row['amendReason'],
                        row['oldSaleAmount']['amount'] if row['oldSaleAmount'] else None,
                        row['oldCommissionAmount']['amount'] if row['oldCommissionAmount'] else None,
                        row['publisherUrl'],
                        row['orderRef'],
                        row['paidToPublisher'],
                        row['paymentId'],
                        row['transactionQueryId'],
                        row['originalSaleAmount']['amount'] if row['originalSaleAmount'] else None,
                        json.dumps(row)
                    ]

                    result = [col if col is not None else '\N' for col in result]
                    writer.writerow(result)

    def output(self):
        """
        The form is {output_root}/fees/affiliate_window/dt={CCYY-mm-dd}/affiliate_window.tsv
        """
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        filename = "affiliate_window.tsv"
        url_with_filename = url_path_join(self.output_root, "dt=" + date_string, filename)
        return get_target_from_url(url_with_filename)
