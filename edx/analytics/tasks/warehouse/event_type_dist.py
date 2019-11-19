"""event_type and event_source values being encountered on each day in a given time interval"""

from __future__ import absolute_import

import logging

import luigi.task
import six

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.obfuscate_util import backslash_encode_value
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

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
        # Make sure that event_type doesn't have embedded newlines and such, but do so
        # after checking that it's not None.
        event_type = backslash_encode_value(six.text_type(event_type))
        yield (event_date, event_category, event_type, event_source, exported), 1

    def reducer(self, key, values):
        yield (key), sum(values)

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'event_type_distribution/'))


class PushToVerticaEventTypeDistributionTask(EventLogSelectionDownstreamMixin, VerticaCopyTask):
    """Push the event type distribution task data to Vertica."""
    output_root = luigi.Parameter()
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
            events_list_file_path=self.events_list_file_path,
            source=self.source,
        )
