import csv
import datetime
import dateutil.parser
import json
import logging
import pytz
from Queue import PriorityQueue, Queue
import requests
from StringIO import StringIO
import time

import luigi
from luigi import date_interval
from sailthru.sailthru_client import SailthruClient

from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, DateTimeField
from edx.analytics.tasks.util.retry import retry
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


def get_datetime_from_sailthru(datetime_string):
    """Convert datetime info from Sailthru APIs into UTC datetime objects.

    Sailthru dates are either of the form:
       datetime_format = '%a, %d %b %Y %H:%M:%S -0400'
    or:
       datetime_format = '%Y-%m-%d %H:%M:%S'

    The latter lack timezone information, but are UTC.
    """
    dt = dateutil.parser.parse(datetime_string)
    # if naive, then assume that it's UTC.
    if dt.tzinfo:
        return dt.astimezone(pytz.utc)
    else:
        return dt.replace(tzinfo=pytz.utc)


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
    A task that fetches daily Sailthru blast information and writes to a file.

    """
    # Date to fetch Sailthru report.
    # run_date = luigi.DateParameter(
    #     default=datetime.date.today(),
    #     description='Default is today.',
    # )

    REPORT_FORMAT = 'json'
    MAX_NUM_BLASTS = 200

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
                    'limit': self.MAX_NUM_BLASTS,
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
        """Output is NO LONGER in the form {output_root}/sailthru_raw/{CCYY-mm}/sailthru_blast_{CCYYmmdd}.json.

        Instead, we write to {output_root}/sailthru_raw/sailthru_blast_{CCYYmmdd}_{CCYY-mm-dd-CCYY-mm-dd}.json.
        At some point, we may clone this to generate an incrmental output *and* a complete output.  The former
        would be used for finding blast_ids to fetch email addresses for.  The latter would be used for fetching
        blast statistics.
        """
        # month_year_string = self.run_date.strftime('%Y-%m')  # pylint: disable=no-member
        # requesting_date_string = self.run_date.strftime('%Y%m%d')  # pylint: disable=no-member
        filename = "sailthru_{type}_{interval}.{report_format}".format(
            type='blast',
            # date_string=requesting_date_string,
            interval=str(self.interval),
            report_format=self.REPORT_FORMAT,
        )
        # url_with_filename = url_path_join(self.output_root, "sailthru_raw", month_year_string, filename)
        url_with_filename = url_path_join(self.output_root, "sailthru_raw", filename)
        return get_target_from_url(url_with_filename)


class SailthruBlastStatsRecord(Record):
    # TODO: update descriptions.
    blast_id = IntegerField(nullable=False, description='Blast identifier.')
    email_subject = StringField(length=564, nullable=False, description='Blast identifier.')
    email_list = StringField(length=564, nullable=False, description='Blast identifier.')
    email_campaign_name = StringField(length=564, nullable=False, description='Blast identifier.')
    email_abtest_name = StringField(length=564, nullable=True, description='Blast identifier.')
    email_abtest_segment = StringField(length=564, nullable=True, description='Blast identifier.')
    email_start_time = DateTimeField(nullable=False, description='Blast identifier.')
    email_sent_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_unsubscribe_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_open_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_click_cnt = IntegerField(nullable=False, description='Blast identifier.')


class DailyStatsFromSailthruTask(PullFromSailthruTaskMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily Sailthru pull, and writes to a TSV file.

    The output file should be readable by Hive.

    """
    # run_date = luigi.DateParameter(
    #     default=datetime.date.today(),
    #     description='Date to fetch Sailthru report. Default is today.',
    # )

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
            start_time_string = blast.get('start_time')
            if start_time_string:
                output_entry['email_start_time'] = get_datetime_from_sailthru(start_time_string)
            else:
                output_entry['email_start_time'] = None
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

        The form should eventually be {output_root}/sailthru_blast_stats/dt={CCYY-mm-dd}/sailthru_blast.tsv.

        For now it is {output_root}/sailthru_blast_stats/sailthru_blast.tsv, where the date is included
        as part of output_root.  This was just a shortcut.
        """
        # TODO:  At some point, restore this so that output_root no longer needs today's date,
        # and can again be warehouse_path or its replacement.
        # date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        # partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "sailthru_blast.tsv"
        # url_with_filename = url_path_join(self.output_root, "sailthru_blast_stats", partition_path_spec, filename)
        url_with_filename = url_path_join(self.output_root, "sailthru_blast_stats", filename)
        return get_target_from_url(url_with_filename)


class RequestEmailInfoPerBlastFromSailthruTask(PullFromSailthruTaskMixin, luigi.Task):
    """
    A task that reads in a list of blast IDs, and creates a list of job IDs for requests for blast info.

    A separate job will read the list of job IDs and monitor their progress.
    """

    # Choose a value that exceeds the maximum number of recipients of a blast.
    # This is used to reverse the order of blasts by send_cnt, though were it negative,
    # it would probably work as well.
    MAX_COUNT = 100000000

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

    def run(self):
        # Read from input and reformat for output.
        self.remove_output_on_overwrite()

        self.sailthru_client = SailthruClient(self.api_key, self.api_secret)

        # Queue up all blasts asynchronously.  And only write output
        # once each blast completes. They will run 10 at a time, in the order
        # they are queued.  Of course, if we want to finish most quickly,
        # we would schedule the large jobs first, and work backwards.
        schedule_queue = PriorityQueue()
        with self.input().open('r') as input_file:
            for line in input_file:
                info = json.loads(line)
                blasts = info.get('blasts')
                for blast in blasts:
                    blast_id = blast.get('blast_id')  # or 'final_blast_id'?  Looks like copy_blast_id is different.
                    stats = blast.get('stats', {}).get('total', {})
                    sent_count = stats.get('count', 0)
                    # Insert blasts in reverse order by size, so that the biggest blasts
                    # will be pulled first.
                    # priority = self.MAX_COUNT - sent_count
                    # NOPE.  Problem with that is because the large jobs take so long,
                    # the export_url links for smaller jobs expire while the task is waiting
                    # for the larger jobs to finish.  With an expiration of 24 hours we wouldn't
                    # need to do this.  But expiration seemed to take place after 2 hours.
                    # On the other hand, if outputting the results for a blast take anywhere near
                    # two hours (e.g. for the larger jobs), then we're doomed either way.
                    priority = sent_count
                    data = {
                        'blast_id': blast_id,
                        'sent_count': sent_count,
                    }
                    schedule_queue.put((priority, data))

        # Submit jobs in order of priority, from smallest priority value to largest.
        # Submit them all.
        queue = Queue()
        while not schedule_queue.empty():
            item = schedule_queue.get()
            priority, data = item
            print "Scheduling:  priority '{}' data '{}'".format(priority, data)
            blast_id = data.get('blast_id')
            sent_count = data.get('sent_count', 0)
            # For testing, comment this out to skip over bigger jobs.
            # if sent_count > 10000:
            #     continue
            job_status = self.submit_blast_query_request(blast_id)
            job_id = job_status.get('job_id')
            submit_time = datetime.datetime.utcnow()
            start_time_string = job_status.get('start_time')
            if start_time_string:
                # TODO: Figure out if this ever happens.  It may be that such jobs start, but never right away.
                print "Job {} (blast {}) began execution at {}".format(job_id, blast_id, start_time_string)
            else:
                print "Job {} (blast {}) queued up for future execution".format(job_id, blast_id)
            data = {
                'job_id': job_id,
                'blast_id': blast_id,
                'sent_count': sent_count,
                'submit_time': submit_time.isoformat(),
                'start_time': start_time_string,
            }
            queue.put(data)

        # Now everything is queued, so wait for each job to finish, in the order
        # that they were originally queued, FIFO.
        with self.output().open('w') as output_file:
            while not queue.empty():
                data = queue.get()
                output_line = json.dumps(data)
                output_file.write(output_line + '\n')

    def output(self):
        """
        Output is a log file, listing job IDs for requests for email information about blasts.

        The form is {output_root}/sailthru_blast_email_jobs/sailthru_blast_email_jobs.log

        At some point, this may be made incremental, so that we only pull blast emails for
        the most recent blasts.
        """
        # date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        # partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "sailthru_blast_emails_jobs.log"
        # url_with_filename = url_path_join(self.output_root, "sailthru_blast_stats", partition_path_spec, filename)
        url_with_filename = url_path_join(self.output_root, "sailthru_blast_email_jobs", filename)
        return get_target_from_url(url_with_filename)


class SailthruBlastEmailRecord(Record):
    # TODO: update descriptions.
    blast_id = IntegerField(nullable=False, description='Blast identifier.')
    email_hash = StringField(length=564, nullable=False, description='Blast identifier.')

    # These are not needed right now, and it may end up slowing down our output.
    # So commenting these out for now to speed up output.
    # profile_id = StringField(length=564, nullable=False, description='Blast identifier.')
    # send_time = DateTimeField(nullable=False, description='Blast identifier.')
    # open_time = DateTimeField(nullable=True, description='Blast identifier.')
    # click_time = DateTimeField(nullable=True, description='Blast identifier.')
    # purchase_time = DateTimeField(nullable=True, description='Blast identifier.')
    # device = StringField(length=564, nullable=True, description='Blast identifier.')

    # This can get very long, as urls are appended, delimited by a space.  Skipping for now.
    # first_ten_clicks = StringField(length=564, nullable=True, description='Blast identifier.')
    # This comes in with more than one datetime, pipe-delimited.  Not sure how to store it, so just skipping it.
    # first_ten_clicks_time = DateTimeField(nullable=True, description='Blast identifier.')


class EmailInfoPerBlastFromSailthruTask(PullFromSailthruTaskMixin, luigi.Task):
    """
    A task that polls a job ID to see if email information about a blast is ready, and writes to a TSV file.

    The output file should be readable by Hive, and structured according to SailthruBlastEmailRecord.

    """

    # The polling interval depends on how long we think the request will take.
    # In particular, blasts with more email addresses take longer to process.
    # Assume a rate of 200 per second.  This is a little high, but should work well enough for
    # an initial setting, to allow for later tuning.   This can eventually be made a parameter.
    EMAILS_LOGGED_PER_SECOND = 200

    # Based on a guess at the overall duration, set the polling interval so there
    # are about 20 polls before the request should complete.
    POLLS_PER_ESTIMATED_DURATION = 20

    # We don't need micro-second polling, so set a reasonable minimum, in seconds.
    MINIMUM_POLLING_INTERVAL = 10

    # Number of times to poll before giving up.
    MAXIMUM_NUMBER_OF_RETRIES = 60

    def requires(self):
        args = {
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'interval': self.interval,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
        }
        return RequestEmailInfoPerBlastFromSailthruTask(**args)

    def run(self):
        # Read from input and reformat for output.
        self.remove_output_on_overwrite()
        if self.overwrite:
            # remove the output directory, not just the marker file.
            output_target = self.output()
            if not self.complete() and output_target.exists():
                output_target.remove()

        self.sailthru_client = SailthruClient(self.api_key, self.api_secret)

        number_of_failures = 0
        # Wait for each job to finish, in the order that they were originally queued, FIFO.
        with self.input().open('r') as input_file:
            for line in input_file:
                data = json.loads(line)
                output_url = self.get_output_url_from_blast_query(data)
                blast_id = data.get('blast_id')
                if output_url:
                    output_filename = 'sailthru_emails_blast_{}.tsv'.format(blast_id)
                else:
                    # If it failed to return a URL, just output a file with an appropriate
                    # name and continue on.  We raise a failure at the very
                    # end, however, to indicate that the job failed.
                    number_of_failures += 1
                    output_filename = 'sailthru_emails_blast_{}.failure'.format(blast_id)
                output_path = url_path_join(self.output_root, 'sailthru_blast_emails', output_filename)
                output_target = get_target_from_url(output_path)
                with output_target.open('w') as output_file:
                    if output_url:
                        try:
                            reader = self.get_output_reader(output_url)
                        except Exception as exc:
                            # For now, we will skip these failures, and just move on to see
                            # how far we get.
                            print "Unable to access output_url '{}' : {}".format(output_url, exc)
                            reader = []
                    else:
                        reader = []
                    for output_row in reader:
                        output_entry = {'blast_id': blast_id, 'email_hash': output_row.get('email hash')}
                        # for key in ['profile_id', 'device']:
                        #     value = output_row.get(key)
                        #     if value:
                        #         output_entry[key] = value
                        #     else:
                        #         output_entry[key] = None
                        # for key in ['send_time', 'open_time', 'click_time', 'purchase_time']:
                        #     datetime_string = output_row.get(key)
                        #     if datetime_string:
                        #         output_entry[key] = get_datetime_from_sailthru(datetime_string)
                        #     else:
                        #         output_entry[key] = None
                        record = SailthruBlastEmailRecord(**output_entry)
                        output_file.write(record.to_separated_values())
                        output_file.write('\n')

        # We do as much as possible (while we're still trying to get this to run) above, and then fail here.
        if number_of_failures > 0:
            msg = "Failed to find export_url: {} times".format(number_of_failures)
            raise Exception(msg)

    def get_estimated_duration(self, sent_count):
        num_seconds = sent_count / self.EMAILS_LOGGED_PER_SECOND
        return num_seconds

    def get_polling_interval(self, sent_count):
        num_seconds = self.get_estimated_duration(sent_count)
        poll_seconds = num_seconds / self.POLLS_PER_ESTIMATED_DURATION
        if poll_seconds < self.MINIMUM_POLLING_INTERVAL:
            poll_seconds = self.MINIMUM_POLLING_INTERVAL
        return poll_seconds

    def get_estimated_end_time(self, start_time, sent_count):
        num_seconds = self.get_estimated_duration(sent_count)
        delta = datetime.timedelta(0, num_seconds, 0)
        estimated_end_time = start_time + delta
        return estimated_end_time

    def get_output_url_from_blast_query(self, data):
        print "Waiting for completion:  data '{}'".format(data)

        output_lines = []
        job_id = data.get('job_id')
        blast_id = data.get('blast_id')
        sent_count = data.get('sent_count')
        
        job_response = self.sailthru_client.api_get('job', {'type': 'status', 'job_id': job_id})
        if not job_response.is_ok():
            msg = "Encountered status {} on blast_query request to Sailthru for {}".format(
                job_response.get_status_code(), blast_id,
            )
            raise Exception(msg)
        job_status = job_response.get_body()
        retry_count = 0
        poll_seconds = self.get_polling_interval(sent_count)
        print "Polling every {} seconds...".format(poll_seconds)
        while job_status.get('status') == 'pending' and retry_count < self.MAXIMUM_NUMBER_OF_RETRIES:
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
            msg = "Failed to complete:  encountered status {} on blast_query request to Sailthru for job {} blast_id {}".format(
                job_status.get('status'), job_id, blast_id,
            )
            raise Exception(msg)

        # If we get here, we assume the request completed.
        start_time_string = job_status.get('start_time')
        start_time = get_datetime_from_sailthru(start_time_string)
        end_time_string = job_status.get('end_time')
        end_time = get_datetime_from_sailthru(end_time_string)
        estimated_end_time = self.get_estimated_end_time(start_time, sent_count)
        duration = end_time - start_time
        end_diff = end_time - estimated_end_time
        print "Found completion for job '{}' blast '{}' count {}:  start {} estimated {} actual {} duration {} diff '{}'".format(
            job_id, blast_id, sent_count, start_time, estimated_end_time, end_time, duration, end_diff
        )

        # Depending on when this is run, it's possible that the output from the original job has expired.
        export_url = job_status.get('export_url')
        if not export_url:
            msg = "Failed to find export_url:  encountered status {} on blast_query request to Sailthru for job {} blast_id {} end_time {} expired {}".format(
                job_status.get('status'), job_id, blast_id, end_time.isoformat(), job_status.get('expired'),
            )
            # For now, avoid failing here, and instead let it do as much as it can so we can analyze the problem.
            # (For example, it seems to expire links after two hours, instead of 24 hours as documented.)
            # raise Exception(msg)
            print msg
        
        return export_url
    
    @retry(timeout=520)
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

        FIXME: The form is NOT {output_root}/sailthru_blast_stats/dt={CCYY-mm-dd}/sailthru_blast.tsv
        """
        # date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        # partition_path_spec = HivePartition('dt', date_string).path_spec
        # filename = "sailthru_blast_emails.tsv"
        # url_with_filename = url_path_join(self.output_root, "sailthru_blast_stats", partition_path_spec, filename)
        # url_with_filename = url_path_join(self.output_root, "sailthru_blast_emails", filename)
        # return get_target_from_url(url_with_filename)
        output_url = url_path_join(self.output_root, "sailthru_blast_emails/")
        return get_target_from_url(output_url, marker=True)

    def on_success(self):  # pragma: no cover
        """Overload the success method to touch the _SUCCESS file.  Any class that uses a separate Marker file from the
        data file will need to override the base on_success() call to create this marker."""
        self.output().touch_marker()


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
            # date_string = datetime.datetime.utcnow().date().isoformat()
            date_string = self.interval_end.isoformat()
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
        yield DailyStatsFromSailthruTask(**args)
        yield EmailInfoPerBlastFromSailthruTask(**args)
        
        # for run_date in self.interval:
        #     args['run_date'] = run_date
        #     yield DailyStatsFromSailthruTask(**args)

    def output(self):
        return [task.output() for task in self.requires()]
