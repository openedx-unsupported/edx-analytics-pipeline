"""
End to end test of the internal reporting events table loading task.
"""

from __future__ import absolute_import

import datetime
import logging
import os

from six.moves import range

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available
from edx.analytics.tasks.tests.acceptance.services import fs
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class InternalReportingEventsLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's events table."""

    # Borrow data from module-engagement acceptance test.  It provides logs with events on more
    # than one date.
    EMPTY_INPUT_FILE = 'module_engagement_acceptance_empty.log'
    INPUT_TRACKING_FILE = 'module_engagement_acceptance_tracking_{date}.log'
    INPUT_SEGMENT_FILE = 'load_events_acceptance_segment_{date}.log'
    NUM_REDUCERS = 1

    def setUp(self):
        super(InternalReportingEventsLoadAcceptanceTest, self).setUp()
        for day in range(13, 17):
            fake_date = datetime.date(2015, 4, day)
            # Create tracking logs for each date, so there is an input for each day's run, even if it's empty.
            if day in (13, 16):
                self.upload_tracking_log(self.INPUT_TRACKING_FILE.format(date=fake_date.strftime('%Y%m%d')), fake_date)
            else:
                self.upload_tracking_log(self.EMPTY_INPUT_FILE, fake_date)
            # We need to create a segment log as well, for each day, even if it's empty too.
            if day in (14, 15):
                self.upload_segment_log(self.INPUT_SEGMENT_FILE.format(date=fake_date.strftime('%Y%m%d')), fake_date)
            else:
                self.upload_segment_log(self.EMPTY_INPUT_FILE, fake_date)

    def upload_segment_log(self, input_file_name, file_date, template_context=None):
        """Define a segment log path on S3 that will be matched by the standard event-log pattern."""
        input_file_path = url_path_join(
            self.test_src,
            'SegmentProject',
            'segment.log-{0}.gz'.format(file_date.strftime('%Y%m%d'))
        )

        raw_file_path = os.path.join(self.data_dir, 'input', input_file_name)
        with fs.template_rendered_file(raw_file_path, template_context) as rendered_file_name:
            with fs.gzipped_file(rendered_file_name) as compressed_file_name:
                self.upload_file(compressed_file_name, input_file_path)

    @when_vertica_available
    def test_internal_reporting_events(self):
        """Tests the workflow for loading internal reporting events table."""

        # Assume that warehouse_path is set, and that events_list_file_path doesn't need to be.
        self.task.launch([
            'EventRecordIntervalTask',
            '--interval', '2015-04-13-2015-04-17',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.task.launch([
            'LoadEventsIntoWarehouseWorkflow',
            '--interval', '2015-04-13-2015-04-17',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, by checking the number of dates."""

        with self.vertica.cursor() as cursor:
            cursor.execute("SELECT DISTINCT(date) FROM {schema}.event_records".format(schema=self.vertica.schema_name))
            event_dates = cursor.fetchall()
            log.debug("Found the following event date values: %s", event_dates)
            self.assertEqual(len(event_dates), 3)
