"""
Load events for internal reporting purposes.
Combine segment events and tracking log events.
Define common (wide) representation for all events to share, sparsely.
Need to define a Record, that will also provide mapping of types.
"""

from collections import defaultdict
import datetime
import logging
import random

import luigi
import luigi.task

from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.segment_event_type_dist import SegmentEventLogSelectionMixin, SegmentEventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import ExternalURL, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.record import SparseRecord, StringField, IntegerField, DateField, FloatField

log = logging.getLogger(__name__)


class EventRecord(SparseRecord):
    """Represents an event, either a tracking log event or segment event."""

    # Globals:
    # TODO: decide what type 'timestamp' should be.
    timestamp = StringField(length=255, nullable=False, description='Timestamp of event.')
    event_type = StringField(length=255, nullable=False, description='The type of event.  Example: video_play.')
    event_source = StringField(length=255, nullable=False, description='blah.')
    event_category = StringField(length=255, nullable=True, description='blah.')
    project = StringField(length=255, nullable=False, description='blah.')
    # TODO: figure out why these have errors, and then make DateField.
    date = StringField(length=255, nullable=False, description='The learner interacted with the entity on this date.')

    # Common (but optional) values:
    course_id = StringField(length=255, nullable=True, description='Id of course.')
    username = StringField(length=30, nullable=True, description='Learner\'s username.')

    # Per-event values:
    entity_type = StringField(length=10, nullable=True, description='Category of entity that the learner interacted'
                              ' with. Example: "video".')
    entity_id = StringField(length=255, nullable=True, description='A unique identifier for the entity within the'
                            ' course that the learner interacted with.')


class BaseEventRecordTask(MultiOutputMapReduceJobTask):

    output_root = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)

    # Create a DateField object to help with converting date_string
    # values for assignment to DateField objects.
    date_field_for_converting = DateField()

    # TODO: maintain support for info about events.  We may need something similar to identify events
    # that should -- or should not -- be included in the event dump.

    def requires_local(self):
        return ExternalURL(url=self.events_list_file_path)

    def init_local(self):
        super(BaseEventRecordTask, self).init_local()
        if self.events_list_file_path is None:
            self.known_events = {}
        else:
            self.known_events = self.parse_events_list_file()

    def parse_events_list_file(self):
        """ Read and parse the known events list file and populate it in a dictionary."""
        parsed_events = {}
        with self.input_local().open() as f_in:
            lines = f_in.readlines()
            for line in lines:
                if (not line.startswith('#') and len(line.split("\t")) is 3):
                    parts = line.rstrip('\n').split("\t")
                    parsed_events[(parts[1], parts[2])] = parts[0]
        return parsed_events

    def get_map_input_file(self):
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
        event_date, project = key

        return url_path_join(
            self.output_root,
            'event_records',
            'dt={date}'.format(date=event_date),
            '{project}.tsv'.format(project=project),
        )

    def convert_date(self, date_string):
        """Converts date from string format to date object, for use by DateField."""
        if date_string:
            try:
                # TODO: for now, return as a string.
                # When actually supporting DateField, then switch back to date.
                return self.date_field_for_converting.deserialize_from_string(date_string).isoformat()
            except ValueError as value_error:
                self.incr_counter('Event Record Exports', 'Cannot convert to date', 1)                
                return "BAD: {}".format(date_string)
        else:
            self.incr_counter('Event Record Exports', 'Missing date', 1)
            return date_string


class TrackingEventRecordTask(EventLogSelectionMixin, BaseEventRecordTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    def get_event_time(self, event):
        # Some events may emitted and stored for quite some time before actually being entered into the tracking logs.
        # The primary cause of this is mobile devices that go offline for a significant period of time. They will store
        # events locally and then when connectivity is restored transmit them to the server. We log the time that they
        # were received by the server and use that to batch them into exports since it is much simpler than trying to
        # inject them into past exports.

        # TODO: decide if we want to use this approach, and whether it
        # makes sense to also apply it to segment events as well.

        try:
            return event['context']['received_at']
        except KeyError:
            return super(TrackingEventRecordTask, self).get_event_time(event)

    def mapper(self, line):
        event, event_date = self.get_event_and_date_string(line) or (None, None)
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
            'timestamp': self.get_event_time(event),
            'course_id': course_id,
            'username': username,
            'event_type': event_type,
            'event_source': event_source,
            'event_category': event_category,
            'date': self.convert_date(event_date),
            'project': project,
            # etc.
        }

        record = EventRecord(**event_dict)

        key = (event_date, project)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


class SegmentEventRecordTask(SegmentEventLogSelectionMixin, BaseEventRecordTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    def get_event_time(self, event):
        """
        Returns time information from event if present, else returns None.

        Overrides base class implementation to get correct timestamp.

        """
        try:
            # TODO: clarify which value should be used.  "originalTimestamp" is almost "sentAt".  "timestamp" is almost "receivedAt".
            # Order is (probably) "originalTimestamp" < "sentAt" < "timestamp" < "receivedAt".
            return event['originalTimestamp']
        except KeyError:
            self.incr_counter('Event', 'Missing Time Field', 1)
            return None

    def mapper(self, line):
        # self.incr_counter('Segment_Event_Dist', 'Input Lines', 1)
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, event_date = value
        self.incr_counter('Segment_Event_Dist', 'Inputs with Dates', 1)

        segment_type = event.get('type')
        self.incr_counter('Segment_Event_Dist', 'Type {}'.format(segment_type), 1)

        channel = event.get('channel')
        self.incr_counter('Segment_Event_Dist', 'Channel {}'.format(channel), 1)

        exported = False

        if segment_type == 'track':
            event_type = event.get('event')

            if event_type is None or event_date is None:
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
        # property_keys = ','.join(sorted(event.get('properties', {}).keys()))
        # context_keys = ','.join(sorted(event.get('context', {}).keys()))

        project = event.get('projectId')

        event_dict = {
            'timestamp': self.get_event_time(event),
            # 'course_id': course_id,
            # 'username': username,
            'event_type': event_type,
            'event_source': event_source,
            'event_category': event_category,
            'date': self.convert_date(event_date),
            'project': project,
            # etc.
        }

        record = EventRecord(**event_dict)
        key = (event_date, project)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


class GeneralEventRecordTask(MapReduceJobTaskMixin, luigi.WrapperTask):

    # TODO: pull these out into a mixin.
    output_root = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)
    interval = luigi.DateIntervalParameter(
        description='The range of dates for which to load logs.',
    )

    def requires(self):
        kwargs = {
            'output_root': self.output_root,
            'events_list_file_path': self.events_list_file_path,
            'n_reduce_tasks': self.n_reduce_tasks,
            'interval': self.interval,
            # 'warehouse_path': self.warehouse_path,
        }
        yield (
            TrackingEventRecordTask(**kwargs),
            SegmentEventRecordTask(**kwargs),
        )


# TODO:  Add loading of events into Hive, and into Vertica.
# class LoadInternalReportingEventsToWarehouse(WarehouseMixin, VerticaCopyTask):
