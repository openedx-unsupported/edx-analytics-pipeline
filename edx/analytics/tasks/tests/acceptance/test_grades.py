"""
Tests that grade by enrollment mode Hive table is created.
"""

import datetime
import logging
import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class EnrollmentGradesAcceptanceTest(AcceptanceTestCase):
    """End to end test of the CourseGradeByModeDataTask."""

    CATALOG_DATE = '2016-09-08'
    INPUT_FILE = 'enrollment_trends_tracking.log'

    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None

    def setUp(self):
        """Loads enrollment and course catalog fixtures."""
        super(EnrollmentGradesAcceptanceTest, self).setUp()

        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 7, 30))
        self.execute_sql_fixture_file('load_auth_userprofile.sql')

        self.upload_file(
            os.path.join(self.data_dir, 'input', 'course_runs.json'),
            url_path_join(self.warehouse_path, 'discovery_api_raw', 'dt={}'.format(self.CATALOG_DATE),
                          'course_runs.json')
        )
        self.import_db.execute_sql_file(
            os.path.join(self.data_dir, 'input', 'load_grades_persistentcoursegrade.sql')
        )

    def test_course_grade_by_mode_data_task(self):
        """Should run the course enrollment table task, create a course_grade_by_mode hive table, and populate
        it with a count of passing users for each course/enrollment mode pair."""
        task_params = [
            'CourseGradeByModeDataTask',
            '--date', self.CATALOG_DATE,
            '--warehouse-path', self.warehouse_path,
            '--interval', '2014-07-30-2014-08-07',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--overwrite-n-days', '7',
        ]

        self.task.launch(task_params)

        hive_output = self.hive.execute('SELECT * FROM course_grade_by_mode;')

        expected_rows = [
            '\t'.join(('course-v1:edX+Open_DemoX+edx_demo_course2', 'honor', '0')),
            '\t'.join(('course-v1:edX+Open_DemoX+edx_demo_course2', 'verified', '0')),
            '\t'.join(('edX/Open_DemoX/edx_demo_course', 'audit', '0')),
            '\t'.join(('edX/Open_DemoX/edx_demo_course', 'honor', '2')),
            '\t'.join(('edX/Open_DemoX/edx_demo_course', 'verified', '0')),
        ]
        for expected in expected_rows:
            self.assertTrue(expected in hive_output)
