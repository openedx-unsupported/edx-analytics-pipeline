"""
Run end-to-end acceptance tests regarding the import of courseware_studentmodule, the computation of histgrams,
and the exporting of that data into the analytics database.

The goal of these tests is to emulate (as closely as possible) user actions and validate user visible outputs.
"""

import logging
import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class CSMHistogramAcceptanceTest(AcceptanceTestCase):
    """Validate the courseware_studentmodule -> luigi -> analytics db data pipeline"""

    ENVIRONMENT = 'acceptance'
    SQL_FILE = 'courseware_studentmodule_for_histograms.sql'

    def setUp(self):
        super(CSMHistogramAcceptanceTest, self).setUp()

    def load_data_from_sql_file(self):
        """
        External Effect: Drops courseware_studentmodule table and loads it with data from a static file.
        """
        self.import_db.execute_sql_file(
            os.path.join(self.data_dir, 'input', self.SQL_FILE)
        )

    def test_grade_dist_db_to_db(self):
        """
        Tests the grade_dist data flow starting from courseware_studentmodule table in mysql, including running sqoop
        """
        self.load_data_from_sql_file()
        self.run_grade_dist_sqoop_workflow()
        self.validate_grade_dist_in_analytics_db()

    def run_grade_dist_sqoop_workflow(self):
        """
        Preconditions: Populated courseware_studentmodule table in the MySQL database.
        External Effect: Generates a sqoop dump with the contents of courseware_studentmodule from the MySQL
            database for the test course and stores it in S3.  Then populates the analytics database table
            grade_distribution with histogram data.
        """
        self.task.launch([
            'GradeDistFromSqoopToMySQLWorkflow',
            '--import-credentials', self.import_db.credentials_file_url,
            '--credentials', self.export_db.credentials_file_url,
            '--name', self.ENVIRONMENT,
            '--dest', url_path_join(self.test_out, self.ENVIRONMENT),
            '--num-mappers', str(self.NUM_MAPPERS),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--sqoop-overwrite',
        ])

    def validate_grade_dist_in_analytics_db(self):
        """
        Preconditions: Table grade_distribution has been populated by luigi workflows
        """
        expected_grade_dist_rows = (
            # (module_id, course_id, grade, max_grade, count)
            ("i4x://stanford/analytics/problem/test1", "stanford/analytics/tests", 0.0, 2.0, 3),
            ("i4x://stanford/analytics/problem/test1", "stanford/analytics/tests", 1.0, 2.0, 2),
            ("i4x://stanford/analytics/problem/test1", "stanford/analytics/tests", 2.0, 2.0, 1),
            ("i4x://stanford/analytics/problem/test2", "stanford/analytics/tests", 0.0, 1.0, 1),
            ("i4x://stanford/analytics/problem/test2", "stanford/analytics/tests", 1.0, 1.0, 1),
            ("i4x://stanford/analytics/problem/test2", "stanford/analytics/tests", 1.0, None, 1),  # max_grade = NULL
        )
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT `module_id`, `course_id`, `grade`, `max_grade`, `count` from grade_distribution')
            all_rows = cursor.fetchall()
            self.assertEqual(set(all_rows), set(expected_grade_dist_rows))

    def test_seq_open_dist_db_to_db(self):
        """
        Tests the seq_open_dist data flow starting from courseware_studentmodule table in mysql, including running sqoop
        """
        self.load_data_from_sql_file()
        self.run_seq_open_dist_sqoop_workflow()
        self.validate_seq_open_dist_in_analytics_db()

    def run_seq_open_dist_sqoop_workflow(self):
        """
        Preconditions: Populated courseware_studentmodule table in the MySQL database.
        External Effect: Generates a sqoop dump with the contents of courseware_studentmodule from the MySQL
            database for the test course and stores it in S3.  Then populates the analytics database table
            sequential_open_distribution with histogram data.
        """
        self.task.launch([
            'SeqOpenDistFromSqoopToMySQLWorkflow',
            '--import-credentials', self.import_db.credentials_file_url,
            '--credentials', self.export_db.credentials_file_url,
            '--name', self.ENVIRONMENT,
            '--dest', url_path_join(self.test_out, self.ENVIRONMENT),
            '--num-mappers', str(self.NUM_MAPPERS),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--sqoop-overwrite',
        ])

    def validate_seq_open_dist_in_analytics_db(self):
        """
        Preconditions: Table sequential_open_distribution has been populated by luigi workflows
        """
        expected_seq_open_dist_rows = (
            # (module_id, course_id, count)
            ("i4x://stanford/analytics/sequential/test1", "stanford/analytics/tests", 1),
            ("i4x://stanford/analytics/sequential/test2", "stanford/analytics/tests", 2),
        )
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT `module_id`, `course_id`, `count` from sequential_open_distribution')
            all_rows = cursor.fetchall()
            self.assertEqual(set(all_rows), set(expected_seq_open_dist_rows))
