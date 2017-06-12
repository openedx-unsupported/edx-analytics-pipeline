import csv
import datetime
import json
import logging
from Queue import PriorityQueue
import requests
import time
from StringIO import StringIO

import luigi
# from luigi.configuration import get_config
from luigi import date_interval
from sailthru.sailthru_client import SailthruClient

from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


# Examples:

# DailyPullFromCybersourceTask
# class DailyProcessFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
# class IntervalPullFromCybersourceTask(PullFromCybersourceTaskMixin, WarehouseMixin, luigi.WrapperTask):


# class PaypalTransactionsByDayTask(PaypalTaskMixin, luigi.Task):
# class PaypalTransactionsIntervalTask(PaypalTaskMixin, WarehouseMixin, luigi.WrapperTask):


# # This is not incremental.
# class DailyPullCatalogTask(PullCatalogMixin, luigi.Task):
# class DailyProcessFromCatalogSubjectTask(PullCatalogMixin, luigi.Task):
# class DailyLoadSubjectsToVerticaTask(PullCatalogMixin, VerticaCopyTask):
# class CourseCatalogWorkflow(PullCatalogMixin, VerticaCopyTaskMixin, luigi.WrapperTask):




class PullFromSailthruTaskMixin(OverwriteOutputMixin):
    """Define common parameters for Sailthru pull and downstream tasks."""

    api_key = luigi.Parameter(
        default_from_config={'section': 'sailthru', 'name': 'api_key'},
        significant=False,
        description='Sailthru API key.',
    )
    api_secret = luigi.Parameter(
        default_from_config={'section': 'sailthru', 'name': 'api_secret'},
        significant=False,
        description='Sailthru API secret.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )
    interval = luigi.DateIntervalParameter(
        default=None,
        description='Interval to pull data from Sailthru.',
    )


class DailyPullFromSailthruTask(PullFromSailthruTaskMixin, luigi.Task):
    """
    A task that reads out of a remote Sailthru account and writes to a file.

    """
    # Date to fetch Sailthru report.
    run_date = luigi.DateParameter(
        default=datetime.date.today(),
        description='Default is today.',
    )

    REPORT_FORMAT = 'json'

    def requires(self):
        pass

    def run(self):
        self.remove_output_on_overwrite()
        sailthru_client = SailthruClient(self.api_key, self.api_secret)

        with self.output().open('w') as output_file:

            for requested_date in self.interval:
                end_date = requested_date + datetime.timedelta(days=1)
                request_data = {
                    'status': 'sent',
                    'start_date': requested_date.isoformat(),
                    'end_date': end_date.isoformat(),
                }
                response = sailthru_client.api_get('blast', request_data)
                
                if not response.is_ok():
                    msg = "Encountered status {} on request to Sailthru for {}".format(
                        response.get_status_code(), requested_date
                    )
                    raise Exception(msg)

                # TODO: decide whether to insert additional information about when the record was pulled.
                output_file.write(response.get_body(as_dictionary=False))
                output_file.write('\n')

    def output(self):
        """Output is in the form {output_root}/sailthru_raw/{CCYY-mm}/sailthru_blast_{CCYYmmdd}.json"""
        # month_year_string = self.run_date.strftime('%Y-%m')  # pylint: disable=no-member
        requesting_date_string = self.run_date.strftime('%Y%m%d')  # pylint: disable=no-member
        filename = "sailthru_{type}_{date_string}_{interval}.{report_format}".format(
            type='blast',
            date_string=requesting_date_string,
            interval=str(self.interval),
            report_format=self.REPORT_FORMAT,
        )
        # url_with_filename = url_path_join(self.output_root, "sailthru_raw", month_year_string, filename)
        url_with_filename = url_path_join(self.output_root, "sailthru_raw", filename)
        return get_target_from_url(url_with_filename)


class SailthruBlastStatsRecord(Record):
    blast_id = IntegerField(nullable=False, description='Blast identifier.')
    email_subject = StringField(length=564, nullable=False, description='Blast identifier.')
    email_list = StringField(length=564, nullable=False, description='Blast identifier.')
    email_campaign_name = StringField(length=564, nullable=False, description='Blast identifier.')
    email_abtest_name = StringField(length=564, nullable=True, description='Blast identifier.')
    email_abtest_segment = StringField(length=564, nullable=True, description='Blast identifier.')
    email_start_time = StringField(length=564, nullable=False, description='Blast identifier.')
    email_sent_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_unsubscribe_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_open_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_click_cnt = IntegerField(nullable=False, description='Blast identifier.')


class DailyStatsFromSailthruTask(PullFromSailthruTaskMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily Sailthru pull, and writes to a TSV file.

    The output file should be readable by Hive.

    """
    run_date = luigi.DateParameter(
        default=datetime.date.today(),
        description='Date to fetch Sailthru report. Default is today.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )

    def requires(self):
        args = {
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'interval': self.interval,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
        }
        return DailyPullFromSailthruTask(**args)

    def run(self):
        # Read from input and reformat for output.
        self.remove_output_on_overwrite()
        with self.output().open('w') as output_file:
            with self.input().open('r') as input_file:
                for line in input_file:
                    info = json.loads(line)
                    output_lines = self.get_output_from_info(info)
                    for output_line in output_lines:
                        output_file.write(output_line)
                        output_file.write('\n')

    def get_output_from_info(self, info):
        output_lines = []
        blasts = info.get('blasts')
        for blast in blasts:
            output_entry = {}

            output_entry['blast_id'] = blast.get('blast_id')  # or 'final_blast_id'?  Looks like copy_blast_id is different.
            output_entry['email_subject'] = blast.get('subject')
            output_entry['email_list'] = blast.get('list')
            output_entry['email_campaign_name'] = blast.get('name')
            output_entry['email_abtest_name'] = blast.get('abtest')
            output_entry['email_abtest_segment'] = blast.get('abtest_segment')
            output_entry['email_start_time'] = blast.get('start_time')

            stats = blast.get('stats', {}).get('total', {})
            # ISSUE: don't these change over time?  And if so, do we need separate entries for them, by date when
            # they were fetched?
            output_entry['email_sent_cnt'] = stats.get('count', 0)
            output_entry['email_unsubscribe_cnt'] = stats.get('optout', 0)
            output_entry['email_open_cnt'] = stats.get('open_total', 0)
            output_entry['email_click_cnt'] = stats.get('click_total', 0)

            record = SailthruBlastStatsRecord(**output_entry)

            output_lines.append(record.to_separated_values())

        return output_lines

    def output(self):
        """
        Output is set up so it can be read in as a Hive table with partitions.

        The form is {output_root}/sailthru_blast_stats/dt={CCYY-mm-dd}/sailthru_blast.tsv
        """
        # date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        # partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "sailthru_blast.tsv"
        # url_with_filename = url_path_join(self.output_root, "sailthru_blast_stats", partition_path_spec, filename)
        url_with_filename = url_path_join(self.output_root, "sailthru_blast_stats", filename)
        return get_target_from_url(url_with_filename)


class EmailInfoPerBlastFromSailthruTask(PullFromSailthruTaskMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily Sailthru pull, and writes to a TSV file.

    The output file should be readable by Hive, and be in a common format across
    other payment accounts.

    """

    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )

    def requires(self):
        args = {
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'interval': self.interval,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
        }
        return DailyPullFromSailthruTask(**args)

    def submit_blast_query_request(self, blast_id):
        request_data = {
            'job': 'blast_query',
            'blast_id': blast_id,
        }
        job_response = self.sailthru_client.api_post('job', request_data)              
        if not job_response.is_ok():
            msg = "Encountered status {} on blast_query request to Sailthru for {}".format(
                job_response.get_status_code(), blast_id,
            )
            raise Exception(msg)

        job_status = job_response.get_body()
        return job_status

    def get_datetime(self, datetime_string):
        # Note that the times are local, and we're hardcoding the timezone, because
        # %z doesn't work.
        datetime_format = '%a, %d %b %Y %H:%M:%S -0400'
        return datetime.datetime.strptime(datetime_string, datetime_format)

    def get_estimated_end_time(self, start_time, sent_cnt):
        num_seconds = sent_cnt / 400
        delta = datetime.timedelta(0, num_seconds, 0)
        estimated_end_time = start_time + delta
        return estimated_end_time

    def get_timestamp(self, date_time):
        timestamp = (date_time - datetime.datetime(1970, 1, 1)).total_seconds()
        return timestamp

    def run(self):
        # Read from input and reformat for output.
        self.remove_output_on_overwrite()

        self.sailthru_client = SailthruClient(self.api_key, self.api_secret)

        # Queue up all blasts asynchronously.  And only write output
        # once each blast completes.

        queue = PriorityQueue()
        with self.input().open('r') as input_file:
            for line in input_file:
                info = json.loads(line)
                blasts = info.get('blasts')
                for blast in blasts:
                    blast_id = blast.get('blast_id')  # or 'final_blast_id'?  Looks like copy_blast_id is different.
                    stats = blast.get('stats', {}).get('total', {})
                    sent_cnt = stats.get('count', 0)
                    # FOR NOW, just skip big jobs, for testing.
                    if sent_cnt > 1000:
                        continue
                    job_status = self.submit_blast_query_request(blast_id)
                    job_id = job_status.get('job_id')
                    # start_time_string = job_status.get('start_time')
                    # start_time = self.get_datetime(start_time_string)
                    start_time = datetime.datetime.now()
                    estimated_end_time = self.get_estimated_end_time(start_time, sent_cnt)
                    data = {
                        'job_id': job_id,
                        'blast_id': blast_id,
                        'sent_cnt': sent_cnt,
                        'start_time': start_time,
                        'estimated_end_time': estimated_end_time,
                    }
                    priority = self.get_timestamp(estimated_end_time)
                    print "Queuing up:  priority '{}' data '{}'".format(priority, data)
                    queue.put((priority, data))

        # Now everything is queued, so we wait for each job to finish, in the order
        # that they were projected.  It is assumed that smaller blasts will complete first.
        with self.output().open('w') as output_file:
            while not queue.empty():
                item = queue.get()
                priority, data = item
                print "Waiting for completion:  priority '{}' data '{}'".format(priority, data)
                output_url = self.get_output_url_from_blast_query(data)
                blast_id = data.get('blast_id')
                reader = self.get_output_reader(output_url)
                for output_row in reader:
                    output_line = "{}\t{}\n".format(blast_id, output_row.get('email hash'))
                    output_file.write(output_line)

    def get_output_url_from_blast_query(self, data):
        output_lines = []
        job_id = data.get('job_id')
        blast_id = data.get('blast_id')
        sent_cnt = data.get('sent_cnt')
        start_time = data.get('start_time')
        estimated_end_time = data.get('estimated_end_time')
        
        job_response = self.sailthru_client.api_get('job', {'type': 'status', 'job_id': job_id})
        if not job_response.is_ok():
            msg = "Encountered status {} on blast_query request to Sailthru for {}".format(
                job_response.get_status_code(), blast_id,
            )
            raise Exception(msg)
        job_status = job_response.get_body()
        retry_count = 0

        # TODO: Maybe the polling should depend on how long we think the request will take.
        # In particular, perhaps blasts with more email addresses will take longer to process.
        poll_seconds = (estimated_end_time - start_time).seconds / 10
        if poll_seconds < 10:
            poll_seconds = 10
        print "Polling every {} seconds...".format(poll_seconds)
        while job_status.get('status') == 'pending' and retry_count < 60:
            time.sleep(poll_seconds)
            job_id = job_status.get('job_id')
            job_response = self.sailthru_client.api_get('job', {'type': 'status', 'job_id': job_id})
            if not job_response.is_ok():
                msg = "Encountered status {} on blast_query request to Sailthru for {}".format(
                    job_response.get_status_code(), blast_id,
                )
                raise Exception(msg)
            
            job_status = job_response.get_body()
            retry_count += 1

        if job_status.get('status') != 'completed':
            msg = "Encountered status {} on blast_query request to Sailthru for {}".format(
                job_status.get('status'), blast_id,
            )
            raise Exception(msg)

        # If we get here, we assume the request completed.
        end_time_string = job_status.get('end_time')
        end_time = self.get_datetime(end_time_string)
        end_diff = end_time - estimated_end_time
        print "Found completion for job '{}' blast '{}' count {}:  start {} estimated {} actual {} diff '{}'".format(
            job_id, blast_id, sent_cnt, stasrt_date, estimated_end_time, end_time, end_diff
        )
        export_url = job_status.get('export_url')
        return export_url
    
        return self.get_contents_of_batch_query(batch_id, export_url)

    def get_output_reader(self, export_url):
        # Now fetch the contents, and parse.
        response = requests.get(export_url)
        if response.status_code != requests.codes.ok:  # pylint: disable=no-member
            msg = "Encountered status {} on request to Sailthru URL for {}".format(response.status_code, export_url)
            raise Exception(msg)

        buf = StringIO(response.content)
        reader = csv.DictReader(buf, delimiter=',')
        return reader
    
    def output(self):
        """
        Output is set up so it can be read in as a Hive table with partitions.

        The form is {output_root}/sailthru_blast_stats/dt={CCYY-mm-dd}/sailthru_blast.tsv
        """
        # date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        # partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "sailthru_blast_emails.tsv"
        # url_with_filename = url_path_join(self.output_root, "sailthru_blast_stats", partition_path_spec, filename)
        url_with_filename = url_path_join(self.output_root, "sailthru_blast_emails", filename)
        return get_target_from_url(url_with_filename)


class IntervalPullFromSailthruTask(PullFromSailthruTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """Determines a set of dates to pull, and requires them."""

    date = None
    # interval = luigi.DateIntervalParameter(default=None)
    interval_start = luigi.DateParameter(
        default_from_config={'section': 'sailthru', 'name': 'interval_start'},
        significant=False,
    )
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
        super(IntervalPullFromSailthruTask, self).__init__(*args, **kwargs)
        # Provide default for output_root at this level.
        if self.output_root is None:
            # self.output_root = self.warehouse_path
            date_string = datetime.datetime.utcnow().date().isoformat()
            partition_path_spec = HivePartition('dt', date_string).path_spec
            self.output_root = url_path_join(self.warehouse_path, partition_path_spec)
            
        if self.interval is None:
            self.interval = date_interval.Custom(self.interval_start, self.interval_end)

    def requires(self):
        """Internal method to actually calculate required tasks once."""
        args = {
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
            'interval': self.interval,
        }
        # yield DailyStatsFromSailthruTask(**args)
        yield EmailInfoPerBlastFromSailthruTask(**args)
        
# for run_date in self.interval:
        #     args['run_date'] = run_date
        #     yield DailyStatsFromSailthruTask(**args)

    def output(self):
        return [task.output() for task in self.requires()]
