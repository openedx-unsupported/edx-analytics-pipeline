"""
Tasks for collecting link click data per course, per day, and uploading that data to Vertica.
"""
import logging
from urlparse import urlparse

import luigi
import luigi.task


from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog, opaque_key_util
from edx.analytics.tasks.vertica_load import VerticaCopyTask


log = logging.getLogger(__name__)
LINK_CLICKED = 'edx.ui.lms.link_clicked'


class LMSCoursewareLinkClickedTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    Produces a data set that shows how many users clicked to a new page in each course,
    including whether the click was within the edX courseware or to an external site.
    """

    output_root = luigi.Parameter()

    enable_direct_output = True

    def mapper(self, line):
        # We only want to consider lines that include the type of event with which we are concerned.
        if LINK_CLICKED not in line:
            return

        value = self.get_event_and_date_string(line)

        if value is None:
            return
        event, date_string = value

        event_type = event.get('event_type')
        if not event_type:
            log.error("encountered event with no event_type: %s", event)
            return

        if event_type != LINK_CLICKED:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            log.error("encountered explicit link_clicked event with no event data: %s", event)
            return

        course_id = eventlog.get_course_id(event)
        if course_id is None:
            log.error("encountered explicit link_clicked event with invalid course_id: %s", event)
            return

        target_url = event_data.get('target_url')
        if not target_url:
            log.error("encountered explicit link_clicked event with no target_url: %s", event)
            return

        current_url = event_data.get('current_url')
        if not current_url:
            log.error("encountered explicit link_clicked event with no current_url: %s", event)
            return

        # A link is considered "internal" when it does not navigate away from the current host.
        # Some internal links exclude the host name entirely- they start with / so we account for that.
        current_loc = urlparse(current_url).netloc
        target_loc = urlparse(target_url).netloc

        is_external = 0
        if current_loc != target_loc and target_loc != "":
            is_external = 1

        yield (course_id, date_string), (is_external)

    def reducer(self, key, values):
        """
        Emits the number of clicks for each course for each day with a click, and how many of the clicks
        were external links.
        """
        course_id, date_string = key
        click_count = 0
        external_click_count = 0
        for is_external in values:
            click_count += 1
            external_click_count += is_external

        yield (course_id, date_string, external_click_count, click_count)

    def output(self):
        # the output_root must end with a forward slash to indicate to subsequent jobs that it is a directory
        if not self.output_root.endswith("/"):
            self.output_root += "/"
        return get_target_from_url(self.output_root)

    def complete(self):
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        output_target = self.output()
        if not self.complete() and output_target.exists():
            output_target.remove()

        super(LMSCoursewareLinkClickedTask, self).run()


class PushToVerticaLMSCoursewareLinkClickedTask(VerticaCopyTask):
    """Push the LMS courseware link clicked task data to Vertica."""
    output_root = luigi.Parameter()
    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()

    @property
    def table(self):
        return "lms_courseware_link_clicked_events"

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255)'),
            ('event_date', 'DATE'),
            ('external_link_clicked_events', 'INT'),
            ('link_clicked_events', 'INT')
        ]

    @property
    def insert_source_task(self):
        return LMSCoursewareLinkClickedTask(
            output_root=self.output_root,
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks
        )

    @property
    def auto_primary_key(self):
        """Use 'record_number' as primary key to match the schema"""
        return ('record_number', 'AUTO_INCREMENT')

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None
