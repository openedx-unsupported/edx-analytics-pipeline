"""
End to end test of demographic trends.
"""

import datetime
import logging
import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class EnterpriseEnrollmentAcceptanceTest(AcceptanceTestCase):
    """End to end test of demographic trends."""

    CATALOG_DATE = '2016-09-08'
    DATE = '2016-09-08'

    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None

    def setUp(self):
        """Loads enrollment and course catalog fixtures."""
        super(EnterpriseEnrollmentAcceptanceTest, self).setUp()

        self.prepare_database()

        self.upload_file(
            os.path.join(self.data_dir, 'input', 'courses.json'),
            url_path_join(self.warehouse_path, 'discovery_api_raw', 'dt=' + self.DATE, 'courses.json')
        )
        self.upload_file(
            os.path.join(self.data_dir, 'input', 'course_runs.json'),
            url_path_join(self.warehouse_path, 'discovery_api_raw', 'dt=' + self.DATE, 'course_runs.json')
        )
        self.upload_file(
            os.path.join(self.data_dir, 'input', 'programs.json'),
            url_path_join(self.warehouse_path, 'discovery_api_raw', 'dt=' + self.DATE, 'programs.json')
        )

    def prepare_database(self):
        sql_fixture_base_url = url_path_join(self.data_dir, 'input', 'enterprise')
        for filename in os.listdir(sql_fixture_base_url):
            self.execute_sql_fixture_file(url_path_join(sql_fixture_base_url, filename))

    def test_enterprise_enrollment_table_generation(self):
        self.launch_task()
        self.validate_enterprise_enrollment_table()

    def launch_task(self):
        """Kicks off the summary task."""
        task_params = [
            'ImportEnterpriseEnrollmentsIntoMysql',
            '--date', self.CATALOG_DATE,
        ]

        self.task.launch(task_params)

    def expected_enterprise_enrollment_results(self):
        """Returns expected results"""
        expected = [
            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 12, 2, 'course-v1:edX+Open_DemoX+edx_demo_course2',
             datetime.datetime(2016, 3, 22, 20, 59, 12), 'verified', 1, '', 0, None, 'ron', 1,
             'All about acceptance testing!', datetime.datetime(2016, 6, 1, 0, 0), datetime.datetime(2016, 9, 1, 0, 0),
             'self_paced', 'Self Paced', 3, 5, datetime.datetime(2015, 2, 12, 23, 14, 35), 'test2@example.com',
             'test_user2'],

            ['03fc6c3a33d84580842576922275ca6f', '2nd Enterprise', 13, 3, 'course-v1:edX+Open_DemoX+edx_demo_course2',
             datetime.datetime(2016, 3, 22, 21, 2, 9), 'no-id-professional', 1, '', 0, None, 'hermione', 1,
             'All about acceptance testing!', datetime.datetime(2016, 6, 1, 0, 0), datetime.datetime(2016, 9, 1, 0, 0),
             'self_paced', 'Self Paced', 3, 5, datetime.datetime(2015, 2, 12, 23, 14, 35), 'test3@example.com',
             'test_user3'],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 11, 1, 'course-v1:edX+Open_DemoX+edx_demo_course2',
             datetime.datetime(2016, 3, 22, 20, 56, 9), 'verified', 1, '', 0, None, 'harry', 1,
             'All about acceptance testing!', datetime.datetime(2016, 6, 1, 0, 0), datetime.datetime(2016, 9, 1, 0, 0),
             'self_paced', 'Self Paced', 3, 5, datetime.datetime(2015, 2, 12, 23, 14, 35), 'test@example.com',
             'test_user'],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 12, 2, 'course-v1:edX+Testing102x+1T2017',
             datetime.datetime(2016, 3, 22, 21, 4, 8), 'no-id-professional', 1, 'Pass', 1,
             datetime.datetime(2017, 5, 9, 16, 27, 34), 'ron', 1, 'All about acceptance testing Part 3!',
             datetime.datetime(2016, 12, 1, 0, 0), datetime.datetime(2017, 2, 1, 0, 0), 'instructor_paced', '9', 2, 5,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test2@example.com', 'test_user2'],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 11, 1, 'course-v1:edX+Testing102x+1T2017',
             datetime.datetime(2016, 3, 22, 21, 8, 8), 'credit', 0, '', 0, None, 'harry', 1,
             'All about acceptance testing Part 3!', datetime.datetime(2016, 12, 1, 0, 0),
             datetime.datetime(2017, 2, 1, 0, 0), 'instructor_paced', '9', 2, 5,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test@example.com', 'test_user'],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 11, 1, 'edX/Open_DemoX/edx_demo_course',
             datetime.datetime(2014, 6, 27, 16, 2, 38), 'verified', 1, 'Pass', 1,
             datetime.datetime(2017, 5, 9, 16, 27, 35), 'harry', 1, 'All about acceptance testing!',
             datetime.datetime(2016, 9, 1, 0, 0), datetime.datetime(2016, 12, 1, 0, 0), 'instructor_paced', '13', 2, 4,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test@example.com', 'test_user'],
        ]

        return [tuple(row) for row in expected]

    def validate_enterprise_enrollment_table(self):
        """Assert the enterprise_enrollment table is as expected."""
        columns = ['enterprise_id', 'enterprise_name', 'lms_user_id', 'enterprise_user_id', 'course_id',
                   'enrollment_created_timestamp', 'user_current_enrollment_mode', 'consent_granted', 'letter_grade',
                   'has_passed', 'passed_timestamp', 'enterprise_sso_uid', 'enterprise_site_id', 'course_title',
                   'course_start', 'course_end', 'course_pacing_type', 'course_duration_weeks', 'course_min_effort',
                   'course_max_effort', 'user_account_creation_timestamp', 'user_email', 'user_username']
        with self.export_db.cursor() as cursor:
            cursor.execute(
                '''
                  SELECT {columns}
                  FROM   enterprise_enrollment
                '''.format(columns=','.join(columns))
            )
            results = cursor.fetchall()

        expected = self.expected_enterprise_enrollment_results()
        self.assertItemsEqual(expected, results)
