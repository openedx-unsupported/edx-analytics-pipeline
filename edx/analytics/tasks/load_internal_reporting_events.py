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

log = logging.getLogger(__name__)

import luigi
import luigi.task

from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.segment_event_type_dist import SegmentEventLogSelectionMixin, SegmentEventLogSelectionDownstreamMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, DateField, FloatField


class EventRecord(Record):
    """Represents an event, either a tracking log event or segment event."""

    course_id = StringField(length=255, nullable=False, description='Id of course.')
    username = StringField(length=30, nullable=False, description='Learner\'s username.')
    date = DateField(nullable=False, description='The learner interacted with the entity on this date.')
    entity_type = StringField(length=10, nullable=False, description='Category of entity that the learner interacted'
                                                                     ' with. Example: "video".')
    entity_id = StringField(length=255, nullable=False, description='A unique identifier for the entity within the'
                                                                    ' course that the learner interacted with.')
    event_type = StringField(length=255, nullable=False, description='The type of event.  Example: video_play.')
    event_source = StringField(length=255, nullable=False, description='blah.')
    

class TrackingEventRecordTask(EventLogSelectionMixin, MapReduceJobTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""
    output_root = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)

    # TODO: maintain support for info about events.  We may need something similar to identify events
    # that should -- or should not -- be included in the event dump.
    
    def requires_local(self):
        return ExternalURL(url=self.events_list_file_path)

    def init_local(self):
        super(SegmentEventTypeDistributionTask, self).init_local()
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
            return super(EventExportTask, self).get_event_time(event)

    def get_map_input_file(self):
        # TODO: decide if this is useful information.  (Share across all logs.  Add to a common base class?)
        try:
            # Hadoop sets an environment variable with the full URL of the input file. This url will be something like:
            # s3://bucket/root/host1/tracking.log.gz. In this example, assume self.source is "s3://bucket/root".
            return os.environ['map_input_file']
        except KeyError:
            log.warn('map_input_file not defined in os.environ, unable to determine input file path')
            return None

    def mapper(self, line):
        event, date_string = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return
        
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')

        event_dict = {
            'event_type': event_type,
            'event_source': event_source,
            'course_id': course_id,
            'username': username,
            'date': date_string,
            'timestamp': self.get_event_time(event),
            # etc.
        }

        record = EventRecord(event_dict)

        project = 'tracking_prod'
        
        key = (date_string, project)

        yield key, record.to_string_tuple()

    def multi_output_reducer(self, _key, values, output_file):
        """
        Write values to the appropriate file as determined by the key.
        Write to the encrypted file by streaming through gzip, which compresses before encrypting
        """
        with gzip.GzipFile(mode='wb', fileobj=output_file) as outfile:
            try:
                for value in values:
                    # Assume that the value is a dict containing the relevant sparse data,
                    # either raw or encoded in a json string. 
                    # Either that, or we could ship the original event as a json string,
                    # or ship the resulting sparse record as a tuple.
                    # It should be a pretty arbitrary decision, since it all needs
                    # to be done, and it's just a question where to do it.
                    # For now, keep this simple, and assume it's tupled already.
                    outfile.write(value.strip())
                    outfile.write('\n')
                    # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
                    # Do not remove it.
                    self.incr_counter('Event Record Exports', 'Raw Bytes Written', len(value) + 1)
            finally:
                outfile.close()
        
    def output_path_for_key(self, key):
        """
        Output based on date and something else.  What else?  Type?  

        Mix them together by date, but identify with different files for each project/environment.

        Output is in the form {warehouse_path}/event_records/dt={CCYY-MM-DD}/{project}.tsv
        """
        date_string, project = key

        return url_path_join(
            self.output_root,
            'event_records',
            'dt={date}'.format(date=date_string),
            '{project}.tsv'.format(project=project),
        )


class SegmentEventRecordTask(SegmentEventLogSelectionMixin, MapReduceJobTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""
    output_root = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)

    # TODO: maintain support for info about events.  We may need something similar to identify events
    # that should -- or should not -- be included in the event dump.
    
    def requires_local(self):
        return ExternalURL(url=self.events_list_file_path)

    def init_local(self):
        super(SegmentEventTypeDistributionTask, self).init_local()
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
                    exported = True

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
        property_keys = ','.join(sorted(event.get('properties', {}).keys()))
        context_keys = ','.join(sorted(event.get('context', {}).keys()))
        yield (event_date, event_category, event_type, event_source, exported, property_keys, context_keys), 1

    def reducer(self, key, values):
        yield (key), sum(values)

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'segment_event_type_distribution/'))

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

    
