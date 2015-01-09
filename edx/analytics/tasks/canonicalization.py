"""Standardize the format of events and split them by day."""

import datetime
import gzip
from hashlib import md5
import logging
import os

import ciso8601   # for fast date parsing
import cjson
import luigi

from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask
from edx.analytics.tasks.url import url_path_join, get_target_from_url, IgnoredTarget
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin


log = logging.getLogger(__name__)


class CanonicalizationTask(EventLogSelectionMixin, WarehouseMixin, MultiOutputMapReduceJobTask):
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

    output_root = None
    overwrite = luigi.BooleanParameter(default=False)

    VERSION = 1
    OUTPUT_BUCKETS = 100

    def __init__(self, *args, **kwargs):
        super(CanonicalizationTask, self).__init__(*args, **kwargs)

        self.output_root = url_path_join(self.warehouse_path, 'events')
        self.current_time = datetime.datetime.utcnow().isoformat()
        self.complete_dates = frozenset()

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
            self.increment_counter('analytics.c14n.malformed')
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

    def skip_complete(self, event):
        """
        Ignore events for dates we already have data for.

        Assumes that the event time standardization has been done.
        """
        if event is None:
            return None

        date_string = event['time'].split("T")[0]

        if date_string in self.complete_dates:
            return None

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
                self.increment_counter('analytics.c14n.late_events')

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
        event = self.event_from_line(line)
        event = self.standardize_time(event)
        event = self.skip_complete(event)
        event = self.count_late_events(event)
        event = self.ensure_in_date_range(event)
        event = self.add_metadata(event, line)
        event = self.decode_content(event)
        if event is None:
            return

        canonical_event_as_string = cjson.encode(event)

        yield self.get_map_output_key(event), canonical_event_as_string

    def get_map_output_key(self, event):
        """
        Generate the grouping key for an event.

        This will be a tuple containing the date the event was emitted and an deterministically generated integer that
        evenly distributes the events into buckets.

        Returns:
            (date, bucket)
        """

        date_string = event['time'].split("T")[0]
        # pick a random bucket using the first 3 digits of the event hash
        string_of_hex_digits = event['metadata']['id'][:3]
        number = int(string_of_hex_digits, 16)
        bucket = number % self.OUTPUT_BUCKETS

        return (date_string, bucket)

    def compute_hash(self, line):
        """
        Compute a hash of an event line.

        Returns:
            The hexdigest of the line, a hexadecimal string.
        """
        hasher = md5()
        hasher.update(line)
        return hasher.hexdigest()

    def multi_output_reducer(self, _key, values, raw_output_file):
        """
        Writes the events to the file corresponding to the key, which is opened by the superclass.

        Args:
            key -- a (date, bucket) tuple
            values -- an iterable of event strings
            raw_output_file -- an open file
        """
        with gzip.GzipFile(mode='wb', fileobj=raw_output_file) as output_file:
            event_count = 0
            bytes_written = 0
            for value in values:
                output_file.write(value.strip())
                output_file.write('\n')
                bytes_written += len(value) + 1
                event_count += 1

                if bytes_written > 1e6:
                    # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
                    # Do not remove it.
                    self.increment_counter('analytics.c14n.bytes_written', bytes_written)
                    bytes_written = 0

            if bytes_written > 0:
                self.increment_counter('analytics.c14n.bytes_written', bytes_written)

        self.increment_counter('analytics.c14n.events', value=event_count)

    def marker_targets(self):
        """
        Yield each marker target for the provided date interval.
        """
        for date_obj in self.interval:
            date_string = date_obj.isoformat()
            yield self.get_marker_target(date_string)

    def get_marker_target(self, date_string):
        """
        Return the target for the success marker for the events on a particular date.
        """
        path = url_path_join(
            self.get_output_directory(date_string),
            '_SUCCESS'
        )
        return get_target_from_url(path)

    def complete(self):
        """
        Return True if there is a success marker for every date in the interval for this task.
        """
        if self.overwrite:
            return False

        for marker_target in self.marker_targets():
            if not marker_target.exists():
                return False
        return True

    def get_output_directory(self, date_string):
        """
        Return the directory to put output for a particular date. The resulting paths will be usable as hive partitions.
        """
        return url_path_join(
            self.output_root,
            'dt=' + date_string
        )

    def output_path_for_key(self, key):
        """
        Return a path for writing gzipped events for a particular mapper key (i.e. date and bucket).
        """
        date_string, bucket = key
        return url_path_join(
            self.get_output_directory(date_string),
            'events_{:02}.json.gz'.format(bucket)
        )

    def output(self):
        """
        This task produces no output that Hadoop/Luigi knows about. A dummy temp file is used as the output file since
        an output file must be declared.
        """
        return IgnoredTarget()

    def run(self):
        """
        Precompute the set of dates that have already been completed (and thus have success markers), so we can skip
        them in the mappers. If self.overwrite is set, instead deletes any pre-existing folders before starting.

        Once the main task runs, write a success marker for each date.
        """
        self.complete_dates = set()
        if self.overwrite:
            for date_obj in self.interval:
                output_folder_target = get_target_from_url(self.get_output_directory(date_obj.isoformat()) + '/')
                if output_folder_target.exists():
                    output_folder_target.remove()
        else:
            for date_obj in self.interval:
                date_string = date_obj.isoformat()
                marker_target = self.get_marker_target(date_string)
                if marker_target.exists():
                    self.complete_dates.add(date_string)

        super(CanonicalizationTask, self).run()

        # touch a success marker for each day
        for marker_target in self.marker_targets():
            with marker_target.open('w'):
                pass
