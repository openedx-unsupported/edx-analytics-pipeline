"""
End-to-end test of the workflow to load the warehouse's lms_courseware_link_clicked_events table.

"""

from __future__ import absolute_import

import datetime
import logging
import os

import pandas

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available

log = logging.getLogger(__name__)


class LmsCoursewareLinkClickedAcceptanceTest(AcceptanceTestCase):
    """
    Runs the MapReduce job that uploads LMS courseware link click data to Vertica, then queries that data
    and compares it to the expected output.
    """

    INPUT_FILE = 'lms_courseware_link_clicked_acceptance_tracking.log'
    DATE = datetime.date(2016, 6, 13)

    @when_vertica_available
    def test_lms_courseware_link_clicked(self):
        """Tests the workflow for the lms_courseware_link_clicked_events table, end to end."""
        self.upload_tracking_log(self.INPUT_FILE, self.DATE)
        self.task.launch([
            'PushToVerticaLMSCoursewareLinkClickedTask',
            '--output-root', self.test_out,
            '--interval', str(2016),
            '--n-reduce-tasks', str(self.NUM_REDUCERS)
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(
                self.data_dir,
                'output',
                'acceptance_expected_lms_courseware_link_clicked_events.csv'
            )

            def convert_date(date_string):
                """Convert date string to a date object."""
                return datetime.datetime.strptime(date_string, '%Y-%m-%d').date()

            expected = pandas.read_csv(expected_output_csv, converters={'event_date': convert_date})

            cursor.execute(
                "SELECT * FROM {schema}.lms_courseware_link_clicked_events ORDER BY course_id, event_date"
                .format(schema=self.vertica.schema_name)
            )

            response = cursor.fetchall()
            lms_courseware_link_clicked_events = pandas.DataFrame(
                response,
                columns=[
                    'record_number',
                    'course_id',
                    'event_date',
                    'external_link_clicked_events',
                    'link_clicked_events'
                ]
            )

            for frame in (lms_courseware_link_clicked_events, expected):
                frame.sort(['record_number'], inplace=True, ascending=[True])
                frame.reset_index(drop=True, inplace=True)

            self.assert_data_frames_equal(lms_courseware_link_clicked_events, expected)
