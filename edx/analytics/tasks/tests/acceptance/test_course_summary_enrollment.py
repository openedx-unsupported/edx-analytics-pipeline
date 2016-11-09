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

    CATALOG_DATE = '2016-09-08'

    def setUp(self):
        ''' Loads enrollment and course catalog fixtures. '''
        super(CourseEnrollmentSummaryAcceptanceTest, self).setUp()

        self.upload_tracking_log('enrollment_trends_tracking.log', datetime.date(2014, 7, 30))
        self.execute_sql_fixture_file('load_auth_userprofile.sql')

        self.upload_file(
            os.path.join(self.data_dir, 'input', 'course_catalog.json'),
            url_path_join(self.warehouse_path, 'course_catalog_raw', 'dt={}'.format(self.CATALOG_DATE),
                          'course_catalog.json')
        )

    def test_table_generation(self):
        self.launch_task()
        self.validate_table()

    def launch_task(self):
        ''' Kicks off the summary task. '''
        self.task.launch([
            'CourseSummaryEnrollmentWrapperTask',
            '--interval', '2014-07-30-2014-08-06',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--date', self.CATALOG_DATE,
        ])

    def validate_table(self):
        ''' Assert the summary table is as expected. '''
        columns = ['course_id', 'catalog_course_title', 'program_id', 'program_title', 'catalog_course',
                   'start_time', 'end_time', 'pacing_type', 'availability', 'enrollment_mode', 'count',
                   'count_change_7_days', 'cumulative_count', ]
        with self.export_db.cursor() as cursor:
            cursor.execute(
                '''
                  SELECT {columns}
                  FROM course_meta_summary_enrollment
                '''.format(columns=','.join(columns))
            )
            results = cursor.fetchall()

        expected = [
            ('course-v1:edX+Open_DemoX+edx_demo_course2', None, None,
             None, None, datetime.datetime(2016, 6, 1), datetime.datetime(2016, 9, 1),
             'self_paced', 'Current', 'honor', 1, 1, 1),
            ('course-v1:edX+Open_DemoX+edx_demo_course2', None, None,
             None, None, datetime.datetime(2016, 6, 1), datetime.datetime(2016, 9, 1),
             'self_paced', 'Current', 'verified', 1, 1, 1),
            ('edX/Open_DemoX/edx_demo_course', 'All about acceptance testing!', 'acb243a0-1234-5abe-099e-ffcae2a340d4',
             'Testing', 'edX+Open_DemoX', datetime.datetime(2016, 9, 1), datetime.datetime(2016, 12, 1),
             'instructor_paced', 'Current', 'honor', 2, 1, 4),
            ('edX/Open_DemoX/edx_demo_course', 'All about acceptance testing!', 'acb243a0-1234-5abe-099e-ffcae2a340d4',
             'Testing', 'edX+Open_DemoX', datetime.datetime(2016, 9, 1), datetime.datetime(2016, 12, 1),
             'instructor_paced', 'Current', 'verified', 0, -1, 2),
        ]
        self.assertItemsEqual(expected, results)
