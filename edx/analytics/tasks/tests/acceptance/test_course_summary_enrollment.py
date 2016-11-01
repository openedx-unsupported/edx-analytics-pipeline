"""
End to end test of the enrollment summary workflow.
"""

import datetime
import logging
import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join


log = logging.getLogger(__name__)


class CourseEnrollmentSummaryAcceptanceTest(AcceptanceTestCase):
    """Ensure course enrollment summary is populated in the result store."""

    # TODO: refactor this -- a bit confusing with the date and the date time being weird...
    DATE = '2016-09-08'

    def setUp(self):
        ''' Loads enrollment and course catalog fixtures. '''
        super(CourseEnrollmentSummaryAcceptanceTest, self).setUp()

        self.upload_tracking_log('enrollment_trends_tracking.log', datetime.date(2014, 8, 1))
        self.execute_sql_fixture_file('load_auth_userprofile.sql')

        self.upload_file(
            os.path.join(self.data_dir, 'input', 'course_catalog.json'),
            url_path_join(self.warehouse_path, 'course_catalog_raw', 'dt={}'.format(self.DATE), 'course_catalog.json')
        )

    def test_table_generation(self):
        self.launch_task()
        self.validate_table()

    def launch_task(self):
        self.task.launch([
            'CourseSummaryEnrollmentWrapperTask',
            '--interval', '2014-08-01-2014-08-06',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--date', self.DATE,
        ])

    def validate_table(self):
        columns = ['course_id', 'catalog_course_title', 'program_id', 'program_title', 'catalog_course',
                   'start_time', 'end_time', 'pacing_type', 'availability', 'enrollment_mode', 'count',
                   'count_change_7_days', 'cumulative_count',]
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT {columns} FROM course_meta_summary_enrollment'.format(','.join(columns)) /
                ' ORDER BY course_id ASC'
            )
            results = cursor.fetchall()

        expected = [
            # the enrollment data
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 'audit', 1, 1),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'verified', 1, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'honor', 1, 1),
        ]
        self.assertItemsEqual(expected, results)
