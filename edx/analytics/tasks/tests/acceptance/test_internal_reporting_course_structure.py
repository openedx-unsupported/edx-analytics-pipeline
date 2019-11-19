"""
End to end test of the internal reporting course_structure table loading task.
"""

from __future__ import absolute_import

import datetime
import logging
import os

import pandas
from six.moves import map

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, coerce_columns_to_string, when_vertica_available
from edx.analytics.tasks.util.url import url_path_join
from edx.analytics.tasks.warehouse.load_internal_reporting_course_structure import CourseBlockRecord

log = logging.getLogger(__name__)


class InternalReportingCourseStructureAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's d_program_course table."""

    DATE = '2016-09-08'

    def setUp(self):
        super(InternalReportingCourseStructureAcceptanceTest, self).setUp()
        # Pretend that the API pulls from the Course Blocks API have already succeeded.
        # This means populating the raw blocks directory with json files, one per course.
        for file_name in ('course1.json', 'course2.json', 'course3.json', 'course4.json', '_SUCCESS'):
            self.upload_file(
                os.path.join(self.data_dir, 'input', 'course_structure', file_name),
                url_path_join(self.warehouse_path, 'course_block_raw', 'dt=' + self.DATE, file_name)
            )

    @when_vertica_available
    def test_internal_reporting_course_structure(self):
        """Tests the workflow for the internal reporting course_structure table, end to end."""

        # First generate the records in S3.
        self.task.launch([
            'LoadCourseBlockRecordToVertica',
            '--date', self.DATE
        ])
        # Then load the records from S3 into the Warehouse (as an external URL).
        self.task.launch([
            'LoadInternalReportingCourseStructureToWarehouse',
            '--date', self.DATE
        ])
        self.validate_course_structure()

    def validate_course_structure(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""

        columns = list(CourseBlockRecord.get_fields().keys())
        columns.append('export_date')

        actual = None
        with self.vertica.cursor() as cursor:
            cursor.execute("SELECT * FROM {schema}.course_structure;".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            # Convert everything to strings, except for two integer columns, so that the None values can be more
            # easily compared with 'None' values in the expected output.
            actual = pandas.DataFrame(list(map(coerce_columns_to_string, response)), columns=columns)
            actual['depth'] = actual['depth'].astype('int64')
            actual['order_index'] = actual['order_index'].astype('int64')

        expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_course_structure.tsv')
        expected = pandas.read_csv(
            expected_output_csv,
            encoding='utf-8',
            sep='\t',
        )

        # Convert both data frames to use the same row ordering.
        for frame in (actual, expected):
            frame.sort(['block_id'], inplace=True, ascending=[True])
            frame.reset_index(drop=True, inplace=True)

        self.assert_data_frames_equal(actual, expected)
