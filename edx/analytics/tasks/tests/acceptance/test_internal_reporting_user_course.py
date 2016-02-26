"""
End to end test of the internal reporting user_course table loading task.
"""

import os
import logging
import datetime
import pandas
import luigi

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

log = logging.getLogger(__name__)


class InternalReportingUserCourseLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's user_course table."""

    DATE = '2014-07-01'

    def setUp(self):
        super(InternalReportingUserCourseLoadAcceptanceTest, self).setUp()
        self.upload_file(os.path.join(self.data_dir, 'input', 'course_enrollment'), url_path_join(self.warehouse_path, 'course_enrollment', 'dt=2014-07-01', 'course_enrollment'))

    def test_internal_reporting_user_course(self):
        """Tests the workflow for the internal reporting user course table, end to end."""

        self.task.launch([
            'LoadInternalReportingUserCourseToWarehouse',
            '--date', self.DATE,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--overwrite'
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_f_user_course.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=True)

            cursor.execute("SELECT * FROM {schema}.f_user_course".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            f_user_course = pandas.DataFrame(response, columns=['record_number', 'date', 'course_id',
                                                         'user_id', 'enrollment_is_active', 'enrollment_change',
                                                         'enrollment_mode'])

            try:  # A ValueError will be thrown if the column names don't match or the two data frames are not square.
                self.assertTrue(all(f_user_course == expected))
            except ValueError:
                self.fail("Expected and returned data frames have different shapes or labels.")
