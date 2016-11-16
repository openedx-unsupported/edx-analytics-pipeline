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
            'LoadInternalReportingCourseCatalogToWarehouse',
            '--date', self.DATE
        ])

        self.validate_program_course()
        self.validate_course_seat()
        self.validate_course()

    def validate_program_course(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_program_course.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=True)

            columns = [
                'program_id', 'program_type', 'program_title',
                'catalog_course', 'catalog_course_title',
                'course_id', 'org_id', 'partner_short_code'
            ]

            cursor.execute(
                "SELECT {columns} FROM {schema}.d_program_course ORDER BY course_id ASC".format(
                    columns=",".join(columns),
                    schema=self.vertica.schema_name
                )
            )
            response = cursor.fetchall()
            d_program_course = pandas.DataFrame(response, columns=columns)

            self.assert_data_frames_equal(d_program_course, expected)

    def validate_course_seat(self):
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_course_seat.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=[4])

            columns = [
                'course_id', 'course_seat_type', 'course_seat_price', 'course_seat_currency',
                'course_seat_upgrade_deadline', 'course_seat_credit_provider', 'course_seat_credit_hours',
            ]

            cursor.execute(
                "SELECT {columns} FROM {schema}.d_course_seat ORDER BY course_id ASC, course_seat_type ASC".format(
                    columns=",".join(columns),
                    schema=self.vertica.schema_name
                )
            )
            response = cursor.fetchall()
            d_course_seat = pandas.DataFrame(response, columns=columns)

            self.assert_data_frames_equal(d_course_seat, expected)

    def validate_course(self):
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_course.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=[3, 4, 5, 6])

            columns = [
                'course_id', 'catalog_course', 'catalog_course_title', 'start_time', 'end_time',
                'enrollment_start_time', 'enrollment_end_time', 'content_language', 'pacing_type',
                'level_type', 'availability', 'org_id', 'partner_short_code', 'marketing_url',
                'min_effort', 'max_effort',
            ]

            cursor.execute(
                "SELECT {columns} FROM {schema}.d_course ORDER BY course_id ASC".format(
                    columns=",".join(columns),
                    schema=self.vertica.schema_name
                )
            )
            response = cursor.fetchall()
            d_course = pandas.DataFrame(response, columns=columns)

            self.assert_data_frames_equal(d_course, expected)
