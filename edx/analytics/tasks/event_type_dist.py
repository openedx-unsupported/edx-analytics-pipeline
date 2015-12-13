"""event_type and event_source values being encountered on each day in a given time interval"""

import logging
import luigi
import luigi.task
from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.vertica_load import VerticaCopyTask

log = logging.getLogger(__name__)


class EventTypeDistributionTask(EventLogSelectionMixin, MapReduceJobTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""
    output_root = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)

    def requires_local(self):
        return ExternalURL(url=self.events_list_file_path)

    def init_local(self):
        super(EventTypeDistributionTask, self).init_local()
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
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, event_date = value
        event_type = event.get('event_type')
        event_source = event.get('event_source')
        exported = False
        if event_source is None or event_type is None or event_date is None:
            # Ignore if any of the keys is None
            return
        if event_type.startswith('/'):
            # Ignore events that begin with a slash
            return
        if (event_source, event_type) in self.known_events:
            event_category = self.known_events[(event_source, event_type)]
            exported = True
        else:
            event_category = 'unknown'
        yield (event_date, event_category, event_type, event_source, exported), 1

    def reducer(self, key, values):
        yield (key), sum(values)

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'event_type_distribution/'))


class PushToVerticaEventTypeDistributionTask(VerticaCopyTask):
    """Push the event type distribution task data to Vertica."""
    output_root = luigi.Parameter()
    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()
    events_list_file_path = luigi.Parameter(default=None)

    @property
    def table(self):
        return "event_type_distribution"

    @property
    def columns(self):
        return [
            ('event_date', 'DATETIME'),
            ('event_category', 'VARCHAR(255)'),
            ('event_type', 'VARCHAR(255)'),
            ('event_source', 'VARCHAR(255)'),
            ('exported', 'BOOLEAN'),
            ('event_count', 'INT'),
        ]

    @property
    def insert_source_task(self):
        return EventTypeDistributionTask(
            output_root=self.output_root,
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            events_list_file_path=self.events_list_file_path
        )
