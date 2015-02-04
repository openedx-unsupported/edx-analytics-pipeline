
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.canonicalization import EventIntervalMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.url import get_target_from_url


class ParseEventLogPerformanceTask(EventIntervalMixin, MapReduceJobTask):
    """
    This represents the smallest possible task that parses events for a particular date range.

    Many of our tasks follow this pattern, so this represents the smallest amount of useful work. To maintain a short
    development cycle we want to make this as fast as possible.
    """

    output_root = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(ParseEventLogPerformanceTask, self).__init__(*args, **kwargs)
        target = self.output()
        if target.exists():
            target.remove()

    def mapper(self, line):
        try:
            event = eventlog.decode_json(line)
        except Exception:
            return

        yield (event['date'], line)

    def output(self):
        return get_target_from_url(self.output_root)
