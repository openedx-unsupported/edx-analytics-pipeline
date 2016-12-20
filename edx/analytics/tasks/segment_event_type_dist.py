"""event_type and event_source values being encountered on each day in a given time interval"""

import logging
import luigi
import luigi.task
from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.vertica_load import VerticaCopyTask

log = logging.getLogger(__name__)


class SegmentEventLogSelectionDownstreamMixin(EventLogSelectionDownstreamMixin):
    """Defines parameters for passing upstream to tasks that use EventLogSelectionMixin."""

    source = luigi.Parameter(
        is_list=True,
        config_path={'section': 'segment-logs', 'name': 'source'},
        description='A URL to a path that contains log files that contain the events. (e.g., s3://my_bucket/foo/).   Segment-logs',
    )
    pattern = luigi.Parameter(
        is_list=True,
        config_path={'section': 'segment-logs', 'name': 'pattern'},
        description='A regex with a named capture group for the date that approximates the date that the events '
        'within were emitted. Note that the search interval is expanded, so events don\'t have to be in exactly '
        'the right file in order for them to be processed.  Segment-logs',
    )


class SegmentEventLogSelectionMixin(SegmentEventLogSelectionDownstreamMixin, EventLogSelectionMixin):
    pass


class SegmentEventTypeDistributionTask(SegmentEventLogSelectionMixin, MapReduceJobTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""
    output_root = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)

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
                if not line.startswith('#') and len(line.split("\t")) is 3:
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


class PushToVerticaSegmentEventTypeDistributionTask(SegmentEventLogSelectionDownstreamMixin, VerticaCopyTask):
    """Push the event type distribution task data to Vertica."""
    output_root = luigi.Parameter()
    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)

    @property
    def table(self):
        return "segment_event_type_distribution"

    @property
    def columns(self):
        return [
            ('event_date', 'DATETIME'),
            ('event_category', 'VARCHAR(255)'),
            ('event_type', 'VARCHAR(255)'),
            ('event_source', 'VARCHAR(255)'),
            ('exported', 'BOOLEAN'),
            ('property_keys', 'VARCHAR(255)'),
            ('context_keys', 'VARCHAR(255)'),
            ('event_count', 'INT'),
        ]

    @property
    def insert_source_task(self):
        return SegmentEventTypeDistributionTask(
            output_root=self.output_root,
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            events_list_file_path=self.events_list_file_path,
            pattern=self.pattern,
            source=self.source,
        )
