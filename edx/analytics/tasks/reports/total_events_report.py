"""
Produces report of all events as provided by a total events task
"""
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, ExternalURL
from edx.analytics.tasks.overall_events import TotalEventsDailyTask

import logging
log = logging.getLogger(__name__)


class TotalEventsReport(luigi.Task):
    """
    Calculates TSV file containing count of events

    Parameters:
        report: Location of the resulting report. The output format is a
            excel csv file with date and count of events for that day.
        counts: Location for the map reduce output. It is a collection of output
            files that are consolidated to produce the report.
    """
    report = luigi.Parameter()
    counts = luigi.Parameter()

    def requires(self):
        return ExternalURL(self.counts)

    def output(self):
        return get_target_from_url(self.report)

    @classmethod
    def create_header(cls):
        """Generate a header for CSV output."""
        fields = ['date', 'event_count']
        return ','.join(fields)

    @classmethod
    def create_csv_entry(cls, date, event_count):
        """Generate a single entry in CSV format."""
        return '{date},{event_count}'.format(
            date=date, event_count=event_count
        )

    def run(self):
        """ Create a CSV file based on the (TSV-formatted) input """
        counts = []
        total = 0
        with self.input().open('r') as input_file:
            for line in input_file:
                date, event_count = line.split('\t')
                counts.append((date.strip(), event_count.strip()))
                total += int(event_count)

        # Write out the counts as a CSV, in reverse order by date.
        with self.output().open('w') as output_file:
            output_file.write(self.create_header())
            output_file.write('\n')
            for date, event_count in reversed(sorted(counts)):
                output_file.write(self.create_csv_entry(date, event_count))
                output_file.write('\n')


class TotalEventsReportWorkflow(MapReduceJobTaskMixin, TotalEventsReport, EventLogSelectionDownstreamMixin):
    """
    Generates report for an event count by date for all events.

    Parameters required for :py:class `EventLogSelectionDownstreamMixin`:

        source:  a URL to the root location of input tracking log files (e.g., s3://my_bucket/foo/).
        interval: events within this date range are counted.
        pattern: regex pattern for files to be included when counting total events.
        (See :py:class `EventLogSelectionTask` for more details.)

    Additional parameters are passed through to :py:class:`TotalEventsReport`:

        counts: Location of event counts per day. The format is a hadoop
            tsv file, with fields country, count, and date.
        report: Location of the resulting report. The output format is an
            excel csv file with country and count.

    The following optional parameters are passed through to :py:class:`MapReduceJobTask`:

        mapreduce_engine
        n_reduce_tasks
    """

    def requires(self):
        return TotalEventsDailyTask(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            output_root=self.counts,
            pattern=self.pattern,
            interval=self.interval
        )
