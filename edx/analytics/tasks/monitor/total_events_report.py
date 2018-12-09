"""
Produces report of all events as provided by a total events task
"""
import logging

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin
from edx.analytics.tasks.monitor.overall_events import TotalEventsDailyTask
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url

log = logging.getLogger(__name__)


class TotalEventsReport(luigi.Task):
    """
    Calculates TSV file containing count of events

    """
    report = luigi.Parameter(
        description='Location of the resulting report. The output format is a '
        'Excel CSV file with date and count of events for that day.',
    )
    counts = luigi.Parameter(
        description='Location of event counts per day. It is a collection of output '
        'files that are consolidated to produce the report. The format is a '
        'Hadoop TSV file, with fields "country", "count", and "date".',
    )

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
