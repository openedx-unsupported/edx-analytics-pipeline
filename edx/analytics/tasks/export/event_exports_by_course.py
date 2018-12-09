"""Group events by course and export them for research purposes"""

import gzip
import logging

import luigi.date_interval

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.common.mapreduce import MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class EventExportByCourseTask(EventLogSelectionMixin, MultiOutputMapReduceJobTask):
    """
    Group events by course and export them for research purposes.
    """

    output_root = luigi.Parameter(
        config_path={'section': 'event-export-course', 'name': 'output_root'}
    )

    course = luigi.ListParameter(default=[])

    def mapper(self, line):
        event, date_string = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return

        course_id = eventlog.get_course_id(event, from_url=True)

        if course_id is None:
            return

        if self.course and course_id not in self.course:
            return

        key = (date_string, course_id)
        yield tuple([value.encode('utf8') for value in key]), line.strip()

    def get_event_time(self, event):
        # Some events may emitted and stored for quite some time before actually being entered into the tracking logs.
        # The primary cause of this is mobile devices that go offline for a significant period of time. They will store
        # events locally and then when connectivity is restored transmit them to the server. We log the time that they
        # were received by the server and use that to batch them into exports since it is much simpler than trying to
        # inject them into past exports. The use of EventExportByCourseTask is not incremental, we still use this to be
        # consistent with research exports and to be consistent with date of tracking log from where the event came.
        try:
            return event['context']['received_at']
        except KeyError:
            return super(EventExportByCourseTask, self).get_event_time(event)

    def output_path_for_key(self, key):
        date, course_id = key
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(course_id)

        return url_path_join(
            self.output_root,
            filename_safe_course_id,
            "events",
            '{course}-events-{date}.log.gz'.format(
                course=filename_safe_course_id,
                date=date,
            )
        )

    def multi_output_reducer(self, _key, values, output_file):
        with gzip.GzipFile(mode='wb', fileobj=output_file) as outfile:
            try:
                for value in values:
                    outfile.write(value.strip())
                    outfile.write('\n')
                    # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
                    # Do not remove it.
                    self.incr_counter('Event Export', 'Raw Bytes Written', len(value) + 1)
            finally:
                outfile.close()
