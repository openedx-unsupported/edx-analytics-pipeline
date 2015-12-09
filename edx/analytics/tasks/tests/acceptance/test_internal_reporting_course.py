"""
End to end test of the internal reporting course table loading task.

Testing strategy: the mock results of the course structure API will contain 2 courses, while there will be another
                  course that has enrollments but does not appear in the course structure API.  All three should appear
                  in the final output.
"""

import os
import logging
import pandas

from datetime import date

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join


log = logging.getLogger(__name__)


class DCourseLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the warehouse's d_course table."""

    INPUT_FILE = 'course_structure.json'
    DATE = date(2014, 7, 21)

    def setUp(self):
        super(DCourseLoadAcceptanceTest, self).setUp()

        # Set up the mock LMS databases.
        self.execute_sql_fixture_file('load_student_courseenrollment_for_internal_reporting_course.sql')

        self.upload_data()

    def upload_data(self):
        """
        Puts the test course structure information where the processing task would look for it, bypassing
        calling the actual API
        """
        src = os.path.join(self.data_dir, 'input', self.INPUT_FILE)
        # IMPORTANT: this path should be of the same format as the path that DailyPullCatalogTask uses for output.
        dst = url_path_join(self.warehouse_path, "courses_raw", self.DATE.strftime('dt=%Y-%m-%d'),
                            self.INPUT_FILE)

        # Upload mocked results of the API call
        self.s3_client.put(src, dst)

    def test_internal_reporting_course(self):
        """Tests the workflow for the d_course table, end to end."""

        self.task.launch([
            'LoadInternalReportingCourseToWarehouse',
            '--run-date', self.DATE.strftime('%Y-%m-%d'),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--overwrite'
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_course.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=True)

            cursor.execute("SELECT * FROM {schema}.d_course".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            d_course = pandas.DataFrame(response, columns=['course_id', 'course_org_id', 'course_number', 'course_run',
                                                           'course_start', 'course_end', 'course_name'])

            try:  # A ValueError will be thrown if the column names don't match or the two data frames are not square.
                self.assertTrue(all(d_course == expected))
            except ValueError:
                self.fail("Expected and returned data frames have different shapes or labels.")
