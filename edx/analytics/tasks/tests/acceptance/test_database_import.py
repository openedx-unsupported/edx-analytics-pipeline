import logging
import os
from unittest import TestCase

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase

log = logging.getLogger(__name__)


class TestImportPersistentCourseGradeTask(AcceptanceTestCase):

    ENVIRONMENT = 'acceptance'
    TABLE = 'grades_persistentcoursegrade'

    def load_data_from_file(self):
        """
        External Effect: Drops grades_persistentcoursegrade table and loads it with data from a static file.
        """
        self.import_db.execute_sql_file(
            os.path.join(self.data_dir, 'input', 'load_{table}.sql'.format(table=self.TABLE))
        )

    def test_import_from_mysql(self):
        self.load_data_from_file()
        self.task.launch([
            'ImportPersistentCourseGradeTask',
            '--credentials', self.import_db.credentials_file_url,
            '--destination', self.warehouse_path,
            '--database', self.import_db.database_name,
        ])

        hive_output = self.hive.execute('SELECT * FROM grades_persistentcoursegrade;')
        expected_rows = [
            '\t'.join((
                '1', '1', 'edX/Open_DemoX/edx_demo_course', 'NULL', 'version-1', 'grading-policy-1',
                '0.7', 'C', '2017-01-31 00:05:00', '2017-02-01 00:00:00', '2017-02-01 00:00:00',
            )),
            '\t'.join((
                '2', '2', 'edX/Open_DemoX/edx_demo_course', 'NULL', 'version-1', 'grading-policy-1',
                '0.8', 'B', '2017-01-31 00:05:00', '2017-02-01 00:00:00', '2017-02-01 00:00:00',
            )),
            '\t'.join((
                '3', '3', 'edX/Open_DemoX/edx_demo_course', 'NULL', 'version-1', 'grading-policy-1',
                '0.2', 'Fail', 'NULL', '2017-02-01 00:00:00', '2017-02-01 00:00:00',
            )),
        ]
        for expected in expected_rows:
            if expected not in str(hive_output):
                log.error('Expected "%r" not in output: %r', expected, str(hive_output))
            self.assertTrue(expected in str(hive_output))
