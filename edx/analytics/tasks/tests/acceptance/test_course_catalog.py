"""
End to end test of the course catalog tasks.
"""

import os
import logging
import datetime

import pandas

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join


log = logging.getLogger(__name__)


class BaseCourseCatalogAcceptanceTest(AcceptanceTestCase):
    """Base class for the end-to-end test of course catalog-based tasks."""

    INPUT_FILE = 'catalog.json'

    def setUp(self):
        super(BaseCourseCatalogAcceptanceTest, self).setUp()

        assert 'oddjob_jar' in self.config

        self.oddjob_jar = self.config['oddjob_jar']

        self.upload_data()

    def upload_data(self):
        """Puts the test course catalog where the processing task would look for it, bypassing calling the actual ALI"""
        src = os.path.join(self.data_dir, 'input', self.INPUT_FILE)
        dst = url_path_join(self.warehouse_path, 'course_catalog_api/catalog/dt=2015-06-29', self.INPUT_FILE)

        # Upload mocked results of the API call
        self.s3_client.put(src, dst)


class CourseSubjectsAcceptanceTest(BaseCourseCatalogAcceptanceTest):
    """End-to-end test of pulling the course subject data into Vertica."""

    def test_course_subjects(self):
        """Tests the workflow for the course subjects, end to end."""

        self.task.launch([
            'CourseCatalogWorkflow',
            '--credentials', self.vertica.vertica_creds_url,
            '--run-date', '2015-06-29',
            '--catalog-url', self.catalog_path
        ])

        self.validate_output()

        # Drop the table afterwards so that the next test starts with a clean table.
        with self.vertica.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS {schema}.d_course_subjects".format(schema=self.vertica.schema_name))

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM {schema}.d_course_subjects;"
                           .format(schema=self.vertica.schema_name))
            total_count = cursor.fetchone()[0]

            expected_total_count = 6
            self.assertEqual(total_count, expected_total_count)

            # Verify that all the subject data loaded into Vertica is correct.
            expected_output_csv = os.path.join(self.data_dir, 'output', 'expected_subjects_for_acceptance.csv')
            expected = pandas.read_csv(expected_output_csv, index_col=0)

            cursor.execute("SELECT * FROM {schema}.d_course_subjects;".format(schema=self.vertica.schema_name))
            subjects = cursor.fetchall()

            try:
                for subject_listing in subjects:
                    class_number = subject_listing[0]
                    expected_row = expected.loc[class_number]
                    for col_num in range(1, 6):
                        found_datum = subject_listing[col_num]
                        expected_datum = expected_row[col_num - 1]  # We subtract 1 since we removed the row number.
                        if col_num == 2:  # Pandas doesn't parse the dates as datetime.date objects, so manually cast.
                            expected_year, expected_month, expected_day = tuple(expected_datum.split('-'))
                            expected_datum = datetime.date(int(expected_year), int(expected_month), int(expected_day))
                        self.assertEqual(found_datum, expected_datum)
            except KeyError:
                self.fail("Row number didn't match expected row number.")
