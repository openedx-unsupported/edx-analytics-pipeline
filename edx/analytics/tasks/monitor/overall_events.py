"""Compute overall event metrics"""

import logging

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.util.url import get_target_from_url

log = logging.getLogger(__name__)


class TotalEventsDailyTask(EventLogSelectionMixin, MapReduceJobTask):
    """Produce a dataset for total events within a given time period."""

    output_root = luigi.Parameter()

    def mapper(self, line):
        event, date_string = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return

        yield (date_string), 1

    def reducer(self, key, values):
        """Emit counts for each day."""

        count = sum(values)
        yield key, count

    # Take advantage of a combiner to optimize network hops
    combiner = reducer

    def output(self):
        return get_target_from_url(self.output_root)
