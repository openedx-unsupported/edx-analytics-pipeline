"""Standardize the format of events and split them by day."""

import datetime
import gzip
from hashlib import md5
import logging
import os

import ciso8601   # for fast date parsing
import cjson
import luigi
import luigi.date_interval

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin


log = logging.getLogger(__name__)


class CanonicalizationTask(EventLogSelectionMixin, WarehouseMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    Standardize the format of events and split them by day.
    Reads raw tracking logs, keeps valid events for dates in a specified interval, adds some metadata, and outputs them
    back to S3, with one folder per event emitted date. The events in each folder are split into OUTPUT_BUCKETS files,
    to make it possible to parallelize downstream jobs.
    Any events that weren't emitted in the given interval will be dropped. In particular, this includes mobile events
    that are delivered long after they were emitted.
    Once the job finishes, it writes a _SUCCESS file for each date in the interval. If such a file is present on
    startup, those days are not processed again unless the overwrite flag is set.
    """

    interval = luigi.DateIntervalParameter(None)
    output_root = None
    date = luigi.DateParameter()

    VERSION = 1
    output_buckets = luigi.IntParameter(default=100)

    n_reduce_tasks = output_buckets

    def __init__(self, *args, **kwargs):
        super(CanonicalizationTask, self).__init__(*args, **kwargs)

        self.interval = luigi.date_interval.Date.from_date(self.date)
        self.output_root = url_path_join(self.warehouse_path, 'events', 'dt=' + self.date.isoformat()) + '/'
        self.current_time = datetime.datetime.utcnow().isoformat()
        self.n_reduce_tasks = self.output_buckets

    def event_from_line(self, line):
        """
        Convert a line to an event, or None if it's not valid.
        Args:
            line: json string, hopefully not malformed
        Return:
            an event dictionary, or None if the line isn't valid.
        """
        event = eventlog.parse_json_event(line)
        if not event:
            self.incr_counter('analytics.c14n.malformed', 1)
            return None

        if 'event_type' not in event:
            return None

        return event

    def standardize_time(self, event):
        """
        Args:
            event: an event dict, or None if something has gone wrong earlier.
        Returns:
            the event, with a time field in ISO8601 format, with UTC time, or None if we
            couldn't get the time info.
        """
        if event is None:
            return None

        timestr = eventlog.get_event_time_string(event)
        if timestr is None:
            return None

        standardized_time = timestr + 'Z'

        event['time'] = standardized_time
        return event

    def count_late_events(self, event):
        """
        Logs a datadog event if the event has a received_at context field that is more than 1 day late than its emission
        time.
        Also ensures that the event has a context dictionary.
        """
        if event is None:
            return None

        context = event.setdefault('context', {})

        received_at_string = context.get('received_at')
        if received_at_string:
            received_at = ciso8601.parse_datetime(received_at_string)
            time = ciso8601.parse_datetime(event['time'])
            if (received_at - time) > datetime.timedelta(days=1):
                self.incr_counter('analytics.c14n.late_events', 1)

        return event

    def ensure_in_date_range(self, event):
        """
        Drop events with dates outside the date interval for this task.
        """
        if event is None:
            return None

        date_string = event['time'].split("T")[0]
        if date_string < self.lower_bound_date_string or date_string >= self.upper_bound_date_string:
            return None

        return event

    def add_metadata(self, event, line):
        """
        Add a metadata dictionary to the event, with the following fields:
         - version
         - last_modified (when the event was processed by this task)
         - id (hash of the raw event format, i.e. the line)
         - original_file -- the source file where we found this event
        """
        if event is None:
            return None

        metadata = event.setdefault('metadata', {})
        metadata.setdefault('version', self.VERSION)
        metadata['last_modified'] = self.current_time
        if 'id' not in metadata:
            metadata['id'] = self.compute_hash(line)

        map_input_file = os.environ['map_input_file']
        metadata['original_file'] = map_input_file

        return event

    def decode_content(self, event):
        """
        If the event has a field 'event', and that field is a string, decode it as json, or replace with an empty
        dictionary.
        """
        if event is None:
            return None

        content = event.get('event')
        if content and isinstance(content, basestring):
            try:
                event['event'] = cjson.decode(content)
            except Exception:  # pylint: disable=broad-except
                event['event'] = {}

        return event

    def mapper(self, line):
        """
        Args:
            line: an event, hopefully, but not necessarily, proper json.
        Returns:
            (date, bucket), canonicalized_event
        """
        print line
        log.debug(line)
        event = self.event_from_line(line)
        event = self.standardize_time(event)
        event = self.count_late_events(event)
        event = self.ensure_in_date_range(event)
        event = self.add_metadata(event, line)
        event = self.decode_content(event)
        if event is None:
            return

        canonical_event_as_string = cjson.encode(event)

        yield self.get_map_output_key(event), canonical_event_as_string

    def reducer(self, _key, values):
        for value in values:
            yield (value,)

    def compute_hash(self, line):
        """
        Compute a hash of an event line.
        Returns:
            The hexdigest of the line, a hexadecimal string.
        """
        hasher = md5()
        hasher.update(line)
        return hasher.hexdigest()

    def get_map_output_key(self, event):
        """
        Generate the grouping key for an event.
        This will be a deterministically generated integer that evenly distributes the events into buckets.
        Returns:
            bucket
        """
        # pick a random bucket using the first 3 digits of the event hash
        string_of_hex_digits = event['metadata']['id'][:3]
        number = int(string_of_hex_digits, 16)
        bucket = number % self.output_buckets

        return bucket

    def output(self):
        return get_target_from_url(self.output_root, success_marked=True)

    def run(self):
        self.remove_output_on_overwrite()
        super(CanonicalizationTask, self).run()

    def jobconfs(self):
        jcs = super(CanonicalizationTask, self).jobconfs()
        jcs.extend([
            'mapred.output.compress=true',
            'mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec'
        ])
        return jcs