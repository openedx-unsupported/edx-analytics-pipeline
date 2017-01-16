import luigi
import os
import logging
import re
import datetime

from boto.s3.connection import S3Connection
from dateutil import parser

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin

log = logging.getLogger(__name__)


class ListS3FilesWithDateTask(OverwriteOutputMixin, luigi.Task):

    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )

    bucket = luigi.Parameter()

    key_prefix = luigi.Parameter(default='logs/tracking/')

    def requires(self):
        pass

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, "s3_file_list.tsv"))

    def run(self):
        self.remove_output_on_overwrite()
        conn = S3Connection()
        bucket = conn.get_bucket(self.bucket)
        with self.output().open('w') as output_file:
            for key in bucket.list(prefix=self.key_prefix):
                key_url = url_path_join('s3:////', bucket.name, key.name)
                last_modified = key.last_modified
                output_file.write('\t'.join((key_url, last_modified)))
                output_file.write('\n')


class CharacterizeEventsTask(OverwriteOutputMixin, EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    bucket = luigi.Parameter()

    key_prefix = luigi.Parameter(default='logs/tracking/')


    def requires_local(self):
        return ListS3FilesWithDateTask(
            bucket=self.bucket,
            key_prefix=self.key_prefix,
            output_root=self.output_root,
        )

    def init_local(self):
        super(CharacterizeEventsTask, self).init_local()

        self.url_to_timestamp_map = {}
        with self.input_local().open() as f_in:
            lines = f_in.readlines()
            for line in lines:
                file_url, timestamp = line.split()
                self.url_to_timestamp_map[file_url] = timestamp

    def mapper(self, line):
        input_file = os.environ['mapreduce_map_input_file']

        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, event_date = value
        yield event_date, self.url_to_timestamp_map[input_file]

    def reducer(self, key, values):
        date_string = key
        event_date = parser.parse(date_string)
        event_cutoff_time = (event_date + datetime.timedelta(days=1)).replace(hour=04, minute=0, second=0, microsecond=0)
        events_before_cutoff = 0
        events_after_cutoff = 0
        for value in values:
            event_upload_time = parser.parse(value).replace(tzinfo=None)
            if event_upload_time <= event_cutoff_time:
                events_before_cutoff += 1
            elif event_upload_time > event_cutoff_time:
                events_after_cutoff += 1

        yield date_string, events_before_cutoff, events_after_cutoff

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'characterize_events/'))


class EventsHistogramTask(OverwriteOutputMixin, EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    bucket = luigi.Parameter()

    key_prefix = luigi.Parameter(default='logs/tracking/')

    def requires_local(self):
        return ListS3FilesWithDateTask(
            bucket=self.bucket,
            key_prefix=self.key_prefix,
            output_root=self.output_root,
        )

    def init_local(self):
        super(EventsHistogramTask, self).init_local()

        self.url_to_timestamp_map = {}
        with self.input_local().open() as f_in:
            lines = f_in.readlines()
            for line in lines:
                file_url, timestamp = line.split()
                self.url_to_timestamp_map[file_url] = timestamp

    def mapper(self, line):
        input_file = os.environ['mapreduce_map_input_file']

        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, event_date = value
        event_time = parser.parse(self.get_event_time(event)).replace(tzinfo=None)

        event_upload_time = parser.parse(self.url_to_timestamp_map[input_file]).replace(tzinfo=None)

        delta = event_upload_time - event_time

        if delta <= datetime.timedelta(hours=1):
            yield ('1_HOUR'), 1
        elif delta <= datetime.timedelta(hours=2):
            yield ('1_2_HOUR'), 1
        elif delta <= datetime.timedelta(hours=3):
            yield ('2_3_HOUR'), 1
        elif delta <= datetime.timedelta(hours=4):
            yield ('3_4_HOUR'), 1
        elif delta <= datetime.timedelta(hours=5):
            yield ('4_5_HOUR'), 1
        elif delta <= datetime.timedelta(hours=10):
            yield ('5_10_HOUR'), 1
        elif delta <= datetime.timedelta(hours=24):
            yield ('10_24_HOUR'), 1
        elif delta > datetime.timedelta(hours=24):
            yield ('24_PLUS_HOUR'), 1

    def reducer(self, key, values):
        yield (key), sum(values)

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'events_histogram/'))
