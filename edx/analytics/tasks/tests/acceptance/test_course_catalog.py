"""
End to end test of the course catalog tasks.
"""

import os
import logging

import pandas

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available
from edx.analytics.tasks.url import url_path_join


log = logging.getLogger(__name__)


class BaseCourseCatalogAcceptanceTest(AcceptanceTestCase):
    """Base class for the end-to-end test of course catalog-based tasks."""

    INPUT_FILE = 'catalog.json'

    def setUp(self):
        super(BaseCourseCatalogAcceptanceTest, self).setUp()

        self.upload_data()

    def upload_data(self):
        """Puts the test course catalog where the processing task would look for it, bypassing calling the actual API"""
        src = os.path.join(self.data_dir, 'input', self.INPUT_FILE)
        # IMPORTANT: this path should be of the same format as the path that DailyPullCatalogTask uses for output.
        dst = url_path_join(self.warehouse_path, "course_catalog", "catalog", "dt=2015-06-29", self.INPUT_FILE)

        # Upload mocked results of the API call
        self.s3_client.put(src, dst)


class CourseSubjectsAcceptanceTest(BaseCourseCatalogAcceptanceTest):
    """End-to-end test of pulling the course subject data into Vertica."""

    @when_vertica_available
    def test_course_subjects(self):
        """Tests the workflow for the course subjects, end to end."""

        self.task.launch([
            'CourseCatalogWorkflow',
            '--date', '2015-06-29'
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'expected_subjects_for_acceptance.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=True)

            cursor.execute("SELECT * FROM {schema}.d_course_subjects;".format(schema=self.vertica.schema_name))
            database_subjects = cursor.fetchall()
            subjects = pandas.DataFrame(database_subjects, columns=['row_number', 'course_id', 'date', 'subject_uri',
                                                                    'subject_title', 'subject_language'])

            try:  # A ValueError will be thrown if the column names don't match or the two data frames are not square.
                self.assertTrue(all(subjects == expected))
            except ValueError:
                self.fail("Expected and returned data frames have different shapes or labels.")
