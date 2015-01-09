"""Group all events into a single file per day."""

import datetime
from hashlib import md5
import os

import cjson
import luigi
import luigi.configuration

from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util import eventlog


class CanonicalizationTask(EventLogSelectionMixin, MultiOutputMapReduceJobTask):
    """
    Group all events into a single file per day.

    Standardize their format so that downstream tasks can make assumptions about their structure.
    """

    VERSION = "1"

    output_root = luigi.Parameter()

    def init_local(self):
        super(CanonicalizationTask, self).init_local()
        self.current_time = datetime.datetime.utcnow().isoformat()

    def mapper(self, line):
        result = self.get_event_and_date_string(line)
        if result is None:
            return

        event, date_string = result

        if 'event_type' not in event or '/' in event['event_type']:
            return

        metadata = event.setdefault('metadata', {})
        metadata.setdefault('version', self.VERSION)
        metadata['last_modified'] = self.current_time
        if 'id' not in metadata:
            metadata['id'] = self.compute_hash(line)

        if 'map_input_file' in os.environ:
            metadata['original_file'] = os.environ['map_input_file']

        event.setdefault('date', date_string)

        standardized_time = eventlog.get_event_time_string(event)
        if standardized_time:
            event['time'] = standardized_time

        event.setdefault('context', {})
        content = event.get('event')
        if isinstance(content, basestring):
            try:
                event['event'] = cjson.decode(content)
            except Exception:
                self.incr_counter('Canonicalization', 'Malformed JSON content string', 1)

        canonical_event = cjson.encode(event)

        yield date_string, canonical_event

    def compute_hash(self, line):
        hasher = md5()
        hasher.update(line)
        return hasher.hexdigest()

    def output_path_for_key(self, date):
        return url_path_join(
            self.output_root,
            'dt=' + date,
            'events.log'
        )

    def multi_output_reducer(self, _key, values, output_file):
        for value in values:
            output_file.write(value.strip())
            output_file.write('\n')

            # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
            # Do not remove it.
            self.incr_counter('Canonicalization', 'Raw Bytes Written', len(value) + 1)
