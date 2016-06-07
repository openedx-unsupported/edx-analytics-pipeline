"""
Load events for internal reporting purposes.
Combine segment events and tracking log events.
Define common (wide) representation for all events to share, sparsely.
Need to define a Record, that will also provide mapping of types.
"""

import logging
import os
import pytz

import ciso8601
import luigi
import luigi.task

from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.module_engagement import OverwriteFromDateMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.segment_event_type_dist import SegmentEventLogSelectionMixin
from edx.analytics.tasks.url import ExternalURL, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask,
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import SparseRecord, StringField, DateField  # , IntegerField, FloatField

log = logging.getLogger(__name__)


class EventRecord(SparseRecord):
    """Represents an event, either a tracking log event or segment event."""

    # Globals:
    project = StringField(length=255, nullable=False, description='blah.')
    event_type = StringField(length=255, nullable=False, description='The type of event.  Example: video_play.')
    event_source = StringField(length=255, nullable=False, description='blah.')
    event_category = StringField(length=255, nullable=True, description='blah.')

    # TODO: decide what type 'timestamp' should be.
    # Also make entries required (not nullable), once we have confidence.
    timestamp = StringField(length=255, nullable=True, description='Timestamp when event was emitted.')
    received_at = StringField(length=255, nullable=True, description='Timestamp when event was received.')
    # TODO: figure out why these have errors, and then make DateField.
    date = StringField(length=255, nullable=False, description='The learner interacted with the entity on this date.')

    # Common (but optional) values:
    course_id = StringField(length=255, nullable=True, description='Id of course.')
    username = StringField(length=30, nullable=True, description='Learner\'s username.')

    # Per-event values:
    # entity_type = StringField(length=10, nullable=True, description='Category of entity that the learner interacted'
    # ' with. Example: "video".')
    # entity_id = StringField(length=255, nullable=True, description='A unique identifier for the entity within the'
    # ' course that the learner interacted with.')


class EventRecordDownstreamMixin(WarehouseMixin, MapReduceJobTaskMixin, OverwriteFromDateMixin):
    """Common parameters and base classes used to pass parameters through the event record workflow."""

    # Required parameter
    date = luigi.DateParameter(
        description='Upper bound date for the end of the interval to analyze. Data produced before 00:00 on this'
                    ' date will be analyzed. This workflow is intended to run nightly and this parameter is intended'
                    ' to be set to "today\'s" date, so that all of yesterday\'s data is included and none of today\'s.'
    )

    # Override superclass to disable this parameter
    interval = None

    output_root = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)


class BaseEventRecordDataTask(EventRecordDownstreamMixin, MultiOutputMapReduceJobTask):
    """Base class for loading EventRecords from different sources."""

    # Create a DateField object to help with converting date_string
    # values for assignment to DateField objects.
    date_field_for_converting = DateField()

    # Override superclass to disable this parameter
    # TODO: check if this is redundant, if it's already in the mixin.
    interval = None

    # TODO: maintain support for info about events.  We may need something similar to identify events
    # that should -- or should not -- be included in the event dump.

    def requires_local(self):
        return ExternalURL(url=self.events_list_file_path)

    def init_local(self):
        super(BaseEventRecordDataTask, self).init_local()
        if self.events_list_file_path is None:
            self.known_events = {}
        else:
            self.known_events = self.parse_events_list_file()
        self.interval = luigi.date_interval.Date.from_date(self.date)

    def parse_events_list_file(self):
        """Read and parse the known events list file and populate it in a dictionary."""
        parsed_events = {}
        with self.input_local().open() as f_in:
            lines = f_in.readlines()
            for line in lines:
                if not line.startswith('#') and len(line.split("\t")) is 3:
                    parts = line.rstrip('\n').split("\t")
                    parsed_events[(parts[1], parts[2])] = parts[0]
        return parsed_events

    def get_map_input_file(self):
        """Returns path to input file from which event is being read, if available."""
        # TODO: decide if this is useful information.  (Share across all logs.  Add to a common base class?)
        try:
            # Hadoop sets an environment variable with the full URL of the input file. This url will be something like:
            # s3://bucket/root/host1/tracking.log.gz. In this example, assume self.source is "s3://bucket/root".
            return os.environ['map_input_file']
        except KeyError:
            log.warn('map_input_file not defined in os.environ, unable to determine input file path')
            return None

    def multi_output_reducer(self, _key, values, output_file):
        """
        Write values to the appropriate file as determined by the key.
        """
        for value in values:
            # Assume that the value is a dict containing the relevant sparse data,
            # either raw or encoded in a json string.
            # Either that, or we could ship the original event as a json string,
            # or ship the resulting sparse record as a tuple.
            # It should be a pretty arbitrary decision, since it all needs
            # to be done, and it's just a question where to do it.
            # For now, keep this simple, and assume it's tupled already.
            output_file.write(value)
            output_file.write('\n')
            # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
            # Do not remove it.
            self.incr_counter('Event Record Exports', 'Raw Bytes Written', len(value) + 1)

    def output_path_for_key(self, key):
        """
        Output based on date and something else.  What else?  Type?

        Mix them together by date, but identify with different files for each project/environment.

        Output is in the form {warehouse_path}/event_records/dt={CCYY-MM-DD}/{project}.tsv
        """
        # If we're only running now with a specific date, then there
        # is no reason to sort by date_received.
        _date_received, project = key

        # return url_path_join(
        #     self.output_root,
        #     'event_records',
        #     'dt={date}'.format(date=date_received),
        #     '{project}.tsv'.format(project=project),
        # )
        return url_path_join(
            self.output_root,
            '{project}.tsv'.format(project=project),
        )

    def normalize_time(self, event_time):
        """
        Convert time string to ISO-8601 format in UTC timezone.

        Returns None if string representation cannot be parsed.
        """
        return ciso8601.parse_datetime(event_time).astimezone(pytz.utc).isoformat()

    def convert_date(self, date_string):
        """Converts date from string format to date object, for use by DateField."""
        if date_string:
            try:
                # TODO: for now, return as a string.
                # When actually supporting DateField, then switch back to date.
                # ciso8601.parse_datetime(ts).astimezone(pytz.utc).date().isoformat()
                return self.date_field_for_converting.deserialize_from_string(date_string).isoformat()
            except ValueError:
                self.incr_counter('Event Record Exports', 'Cannot convert to date', 1)
                # Don't bother to make sure we return a good value
                # within the interval, so we can find the output for
                # debugging.  Should not be necessary, as this is only
                # used for the column value, not the partitioning.
                return "BAD: {}".format(date_string)
                # return self.lower_bound_date_string
        else:
            self.incr_counter('Event Record Exports', 'Missing date', 1)
            return date_string


class TrackingEventRecordDataTask(EventLogSelectionMixin, BaseEventRecordDataTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    def get_event_emission_time(self, event):
        return super(TrackingEventRecordDataTask, self).get_event_time(event)

    def get_event_arrival_time(self, event):
        try:
            return event['context']['received_at']
        except KeyError:
            return self.get_event_emission_time(event)

    def get_event_time(self, event):
        # Some events may emitted and stored for quite some time before actually being entered into the tracking logs.
        # The primary cause of this is mobile devices that go offline for a significant period of time. They will store
        # events locally and then when connectivity is restored transmit them to the server. We log the time that they
        # were received by the server and use that to batch them into exports since it is much simpler than trying to
        # inject them into past exports.
        return self.get_event_arrival_time(event)

    def mapper(self, line):
        event, date_received = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return
        if event_type.startswith('/'):
            # Ignore events that begin with a slash
            return

        username = event.get('username', '').strip()
        # if not username:
        #   return

        course_id = eventlog.get_course_id(event)
        # if not course_id:
        #   return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')
        if event_source is None:
            return

        if (event_source, event_type) in self.known_events:
            event_category = self.known_events[(event_source, event_type)]
        else:
            event_category = 'unknown'

        project = 'tracking_prod'

        event_dict = {
            'project': project,
            'event_type': event_type,
            'event_source': event_source,
            'event_category': event_category,

            'timestamp': self.get_event_emission_time(event),
            'received_at': self.get_event_arrival_time(event),
            'date': self.convert_date(date_received),

            'course_id': course_id,
            'username': username,
            # etc.
        }

        record = EventRecord(**event_dict)

        key = (date_received, project)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


class SegmentEventRecordDataTask(SegmentEventLogSelectionMixin, BaseEventRecordDataTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    def _get_time_from_segment_event(self, event, key):
        try:
            event_time = event[key]
            event_time = self.normalize_time(event_time)
            if event_time is None:
                log.error("Unparseable %s time from event: %r", key, event)
            self.incr_counter('Event', 'Unparseable {} Time Field'.format(key), 1)
        except KeyError:
            log.error("Missing %s time from event: %r", key, event)
            self.incr_counter('Event', 'Missing {} Time Field'.format(key), 1)
            return None

    def get_event_arrival_time(self, event):
        return self._get_time_from_segment_event(event, 'receivedAt')

    def get_event_emission_time(self, event):
        return self._get_time_from_segment_event(event, 'sentAt')

    def get_event_time(self, event):
        """
        Returns time information from event if present, else returns None.

        Overrides base class implementation to get correct timestamp
        used by get_event_and_date_string(line).

        """
        # TODO: clarify which value should be used.
        # "originalTimestamp" is almost "sentAt".  "timestamp" is
        # almost "receivedAt".  Order is (probably)
        # "originalTimestamp" < "sentAt" < "timestamp" < "receivedAt".
        return self.get_event_arrival_time(event)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_received = value
        self.incr_counter('Segment_Event_Dist', 'Inputs with Dates', 1)

        segment_type = event.get('type')
        self.incr_counter('Segment_Event_Dist', 'Type {}'.format(segment_type), 1)

        channel = event.get('channel')
        self.incr_counter('Segment_Event_Dist', 'Channel {}'.format(channel), 1)

        if segment_type == 'track':
            event_type = event.get('event')

            if event_type is None or date_received is None:
                # Ignore if any of the keys is None
                self.incr_counter('Segment_Event_Dist', 'Tracking with missing type', 1)
                return

            if event_type.startswith('/'):
                # Ignore events that begin with a slash.  How many?
                self.incr_counter('Segment_Event_Dist', 'Tracking with implicit type', 1)
                return

            # Not all 'track' events have event_source information.  In particular, edx.bi.XX events.
            # Their 'properties' lack any 'context', having only label and category.

            event_category = event.get('properties', {}).get('category')
            if channel == 'server':
                event_source = event.get('properties', {}).get('context', {}).get('event_source')
                if event_source is None:
                    event_source = 'track-server'
                elif (event_source, event_type) in self.known_events:
                    event_category = self.known_events[(event_source, event_type)]
                self.incr_counter('Segment_Event_Dist', 'Tracking server', 1)
            else:
                # expect that channel is 'client'.
                event_source = channel
                self.incr_counter('Segment_Event_Dist', 'Tracking non-server', 1)

        else:
            # 'page' or 'identify'
            event_category = segment_type
            event_type = segment_type
            event_source = channel

        self.incr_counter('Segment_Event_Dist', 'Output From Mapper', 1)

        project = event.get('projectId')

        event_dict = {
            'project': project,
            'event_type': event_type,
            'event_source': event_source,
            'event_category': event_category,

            'timestamp': self.get_event_emission_time(event),
            'received_at': self.get_event_arrival_time(event),
            'date': self.convert_date(date_received),

            # 'course_id': course_id,
            # 'username': username,
            # etc.
        }

        record = EventRecord(**event_dict)
        key = (date_received, project)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


class GeneralEventRecordDataTask(EventRecordDownstreamMixin, luigi.WrapperTask):
    """Runs all Event Record tasks for a given time interval."""

    def requires(self):
        kwargs = {
            'output_root': self.output_root,
            'events_list_file_path': self.events_list_file_path,
            'n_reduce_tasks': self.n_reduce_tasks,
            'interval': self.interval,
            # 'warehouse_path': self.warehouse_path,
        }
        yield (
            TrackingEventRecordDataTask(**kwargs),
            SegmentEventRecordDataTask(**kwargs),
        )


class EventRecordTableTask(BareHiveTableTask):
    """The hive table for event_record data."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'event_records'

    @property
    def columns(self):
        return EventRecord.get_hive_schema()


class EventRecordPartitionTask(EventRecordDownstreamMixin, HivePartitionTask):
    """The hive table partition for this engagement data."""

    @property
    def partition_value(self):
        """Use a dynamic partition value based on the date parameter."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return EventRecordTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return GeneralEventRecordDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite=self.overwrite,
            events_list_file_path=self.events_list_file_path,
        )


class EventRecordIntervalTask(EventRecordDownstreamMixin,
                              OverwriteOutputMixin, luigi.WrapperTask):
    """Compute engagement information over a range of dates and insert the results into Hive and Vertica and whatever else."""

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            should_overwrite = date >= self.overwrite_from_date
            yield EventRecordPartitionTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=should_overwrite,
                overwrite_from_date=self.overwrite_from_date,
                events_list_file_path=self.events_list_file_path,
            )
            # yield LoadEventRecordToVerticaTask(
            #     date=date,
            #     n_reduce_tasks=self.n_reduce_tasks,
            #     warehouse_path=self.warehouse_path,
            #     overwrite=should_overwrite,
            #     overwrite_from_date=self.overwrite_from_date,
            # )

    def output(self):
        return [task.output() for task in self.requires()]

    def get_raw_data_tasks(self):
        """
        A generator that iterates through all tasks used to generate the data in each partition in the interval.

        This can be used by downstream map reduce jobs to read all of the raw data.
        """
        for task in self.requires():
            if isinstance(task, EventRecordPartitionTask):
                yield task.data_task
