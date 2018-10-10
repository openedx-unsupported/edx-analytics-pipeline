"""
End to end test of the internal reporting grading events table loading task.
"""

import datetime
import logging
import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available
from edx.analytics.tasks.tests.acceptance.services import fs
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class InternalReportingGradingEventsLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's events table."""

    # Borrow data from module-engagement acceptance test.  It provides logs with events on more
    # than one date.
    EMPTY_INPUT_FILE = 'module_engagement_acceptance_empty.log'
    INPUT_TRACKING_FILE = 'module_engagement_acceptance_tracking_{date}.log'
    NUM_REDUCERS = 1

    def setUp(self):
        super(InternalReportingGradingEventsLoadAcceptanceTest, self).setUp()
        for day in range(13, 17):
            fake_date = datetime.date(2015, 4, day)
            # Create tracking logs for each date, so there is an input for each day's run, even if it's empty.
            if day in (13, 16):
                self.upload_tracking_log(self.INPUT_TRACKING_FILE.format(date=fake_date.strftime('%Y%m%d')), fake_date)
            else:
                self.upload_tracking_log(self.EMPTY_INPUT_FILE, fake_date)

    def test_internal_reporting_grading_events(self):
        """Tests the workflow for loading internal reporting events table."""

        # Assume that warehouse_path is set, and that events_list_file_path doesn't need to be.
        self.task.launch([
            'BulkGradingEventRecordIntervalTask',
            '--interval', '2015-04-13-2015-04-17',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, by checking the number of dates."""
        pass
