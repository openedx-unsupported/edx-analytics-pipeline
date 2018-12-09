
import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.util.url import get_target_from_url


class ParseEventLogPerformanceTask(EventLogSelectionMixin, MapReduceJobTask):
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
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        yield (date_string, line)

    def output(self):
        return get_target_from_url(self.output_root)
