
from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import IgnoredTarget


class ParseEventLogPerformanceTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    This represents the smallest possible task that parses events for a particular date range.

    Many of our tasks follow this pattern, so this represents the smallest amount of useful work. To maintain a short
    development cycle we want to make this as fast as possible.
    """

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        yield (date_string, line)

    def output(self):
        return IgnoredTarget()
