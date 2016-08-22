"""
End to end test of the internal reporting d_program_course table loading task.
"""

import os
import logging
import pandas

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available
from edx.analytics.tasks.url import url_path_join

log = logging.getLogger(__name__)


class InternalReportingUserCourseLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's d_program_course table."""

    DATE = '2016-09-08'

    def setUp(self):
        super(InternalReportingUserCourseLoadAcceptanceTest, self).setUp()
        self.upload_file(
            os.path.join(self.data_dir, 'input', 'course_catalog.json'),
            url_path_join(self.warehouse_path, 'course_catalog_raw', 'dt=' + self.DATE, 'course_catalog.json')
        )

    @when_vertica_available
    def test_internal_reporting_user_course(self):
        """Tests the workflow for the internal reporting d_program_course table, end to end."""

        self.task.launch([
            'LoadInternalReportingProgramCourseToWarehouse',
            '--date', self.DATE
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_program_course.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=True)

            cursor.execute(
                "SELECT program_id,program_type,program_title,catalog_course,catalog_course_title,course_id,org_id,"
                "partner_short_code FROM {schema}.d_program_course ORDER BY course_id ASC".format(
                    schema=self.vertica.schema_name
                )
            )
            response = cursor.fetchall()
            d_program_course = pandas.DataFrame(response, columns=['program_id', 'program_type', 'program_title',
                                                                   'catalog_course', 'catalog_course_title',
                                                                   'course_id', 'org_id', 'partner_short_code'])

            try:  # A ValueError will be thrown if the column names don't match or the two data frames are not square.
                self.assertTrue(all(d_program_course == expected))
            except ValueError:
                self.fail("Expected and returned data frames have different shapes or labels.")
