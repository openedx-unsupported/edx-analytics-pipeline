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

        self.prepare_database('lms', self.import_db)
        self.prepare_database('otto', self.otto_db)

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
        self.upload_file(
            os.path.join(self.data_dir, 'input', 'user_activity_by_user'),
            url_path_join(
                self.warehouse_path,
                'user_activity_by_user',
                'dt=' + self.DATE,
                'user_activity_by_user_' + self.DATE
            )
        )
        self.upload_file(
            os.path.join(self.data_dir, 'input', 'course_enrollment_summary'),
            url_path_join(
                self.warehouse_path,
                'course_enrollment_summary',
                'dt={}'.format(self.DATE),
                'course_enrollment_summary_{}'.format(self.DATE),
            )
        )

    def prepare_database(self, name, database):
        sql_fixture_base_url = url_path_join(self.data_dir, 'input', 'enterprise', name)
        for filename in sorted(os.listdir(sql_fixture_base_url)):
            self.execute_sql_fixture_file(url_path_join(sql_fixture_base_url, filename), database=database)

    def test_enterprise_enrollment_table_generation(self):
        self.launch_task()
        self.validate_enterprise_enrollment_table()

    def launch_task(self):
        """Kicks off the summary task."""
        task_params = [
            'ImportEnterpriseEnrollmentsIntoMysql',
            '--date', self.DATE,
        ]

        self.task.launch(task_params)

    def expected_enterprise_enrollment_results(self):
        """Returns expected results"""
        expected = [
            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 12, 2, 'course-v1:edX+Open_DemoX+edx_demo_course2',
             datetime.datetime(2016, 3, 22, 20, 59, 12), 'verified', 1, '', 0, None, 'ron', 1,
             'All about acceptance testing!', datetime.datetime(2016, 6, 1, 0, 0), datetime.datetime(2016, 9, 1, 0, 0),
             'self_paced', '15', 3, 5, datetime.datetime(2015, 2, 12, 23, 14, 35), 'test2@example.com',
             'test_user2', 'edX+Open_DemoX', 'US', None, 'ENT - Email domain restricted', 'QAFWBFZ26GYYYIJS', None, 0,
             200.00, 10.00, None],

            ['03fc6c3a33d84580842576922275ca6f', '2nd Enterprise', 13, 3, 'course-v1:edX+Open_DemoX+edx_demo_course2',
             datetime.datetime(2016, 3, 22, 21, 2, 9), 'no-id-professional', 1, '', 0, None, 'hermione', 1,
             'All about acceptance testing!', datetime.datetime(2016, 6, 1, 0, 0), datetime.datetime(2016, 9, 1, 0, 0),
             'self_paced', '15', 3, 5, datetime.datetime(2015, 2, 12, 23, 14, 35), 'test3@example.com',
             'test_user3', 'edX+Open_DemoX', 'US', datetime.date(2015, 9, 9), 'ENT - No restrictions',
             'ZSJHRVLCNTT6XFCJ', None, 0.03, 100.00, 20.00, datetime.datetime(2016, 9, 8, 1, 2, 0)],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 11, 1, 'course-v1:edX+Open_DemoX+edx_demo_course2',
             datetime.datetime(2016, 3, 22, 20, 56, 9), 'verified', 1, '', 0, None, 'harry', 1,
             'All about acceptance testing!', datetime.datetime(2016, 6, 1, 0, 0), datetime.datetime(2016, 9, 1, 0, 0),
             'self_paced', '15', 3, 5, datetime.datetime(2015, 2, 12, 23, 14, 35), 'test@example.com',
             'test_user', 'edX+Open_DemoX', 'US', None, 'Aviato', 'OTTO_VER_25_PCT_OFF', None, 0.4, 200.00, 192.00,
             None],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 12, 2, 'course-v1:edX+Testing102x+1T2017',
             datetime.datetime(2016, 3, 22, 21, 4, 8), 'no-id-professional', 1, 'Pass', 1,
             datetime.datetime(2017, 5, 9, 16, 27, 34), 'ron', 1, 'All about acceptance testing Part 3!',
             datetime.datetime(2016, 12, 1, 0, 0), datetime.datetime(2017, 2, 1, 0, 0), 'instructor_paced', '9', 2, 5,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test2@example.com', 'test_user2', 'edX+Testing102',
             'US', datetime.date(2015, 9, 9), 'ENT - Discount', 'CQHVBDLY35WSJRZ4', None, 0.98, 100.00, 60.00,
             datetime.datetime(2016, 9, 10, 0, 30, 5)],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 11, 1, 'course-v1:edX+Testing102x+1T2017',
             datetime.datetime(2016, 3, 22, 21, 8, 8), 'credit', 0, '', 0, None, 'harry', 1,
             'All about acceptance testing Part 3!', datetime.datetime(2016, 12, 1, 0, 0),
             datetime.datetime(2017, 2, 1, 0, 0), 'instructor_paced', '9', 2, 5,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test@example.com', 'test_user', 'edX+Testing102',
             'US', datetime.date(2015, 9, 9), 'Aviato', 'OTTO_VER_25_PCT_OFF', None, 0.64, 100.00, 56.00, None],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 11, 1, 'edX/Open_DemoX/edx_demo_course',
             datetime.datetime(2014, 6, 27, 16, 2, 38), 'verified', 1, 'Pass', 1,
             datetime.datetime(2017, 5, 9, 16, 27, 35), 'harry', 1, 'All about acceptance testing!',
             datetime.datetime(2016, 9, 1, 0, 0), datetime.datetime(2016, 12, 1, 0, 0), 'instructor_paced', '13', 2, 4,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test@example.com', 'test_user', 'edX+Open_DemoX',
             'US', None, 'Pied Piper Discount', 'PJS4LCU435W6KGBS', None, 0.81, 300.00, 156.00, None],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 15, 4, 'course-v1:edX+Open_DemoX+edx_demo_course2',
             datetime.datetime(2016, 3, 22, 20, 56, 9), 'verified', 1, '', 0, None, 'ginny', 1,
             'All about acceptance testing!', datetime.datetime(2016, 6, 1, 0, 0), datetime.datetime(2016, 9, 1, 0, 0),
             'self_paced', '15', 3, 5, datetime.datetime(2015, 2, 12, 23, 14, 35), 'test5@example.com',
             'test_user5', 'edX+Open_DemoX', 'US', None, None, None, 'Absolute, 100 (#7)', 0.5, 200.00, 20.00, None],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 15, 4, 'course-v1:edX+Testing102x+1T2017',
             datetime.datetime(2016, 3, 22, 21, 8, 8), 'credit', 1, '', 0, None, 'ginny', 1,
             'All about acceptance testing Part 3!', datetime.datetime(2016, 12, 1, 0, 0),
             datetime.datetime(2017, 2, 1, 0, 0), 'instructor_paced', '9', 2, 5,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test5@example.com', 'test_user5', 'edX+Testing102',
             'US', None, None, None, 'Percentage, 100 (#8)', 0.74, 100.00, 60.00, None],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 15, 4, 'edX/Open_DemoX/edx_demo_course',
             datetime.datetime(2014, 6, 27, 16, 2, 38), 'verified', 1, 'Pass', 1,
             datetime.datetime(2017, 5, 9, 16, 27, 35), 'ginny', 1, 'All about acceptance testing!',
             datetime.datetime(2016, 9, 1, 0, 0), datetime.datetime(2016, 12, 1, 0, 0), 'instructor_paced', '13', 2, 4,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test5@example.com', 'test_user5', 'edX+Open_DemoX',
             'US', None, None, None, 'Percentage, 100 (#6)', 0.85, 230.00, 57.00, None],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 16, 5, 'course-v1:edX+Testing102x+1T2017',
             datetime.datetime(2019, 3, 22, 20, 56, 9), 'verified', 1, '', 0,
             datetime.datetime(2019, 9, 4, 16, 27, 35), 'dory', 1, 'All about acceptance testing Part 3!',
             datetime.datetime(2016, 12, 1, 0, 0), datetime.datetime(2017, 2, 1, 0, 0), 'instructor_paced', '9', 2, 5,
             datetime.datetime(2019, 9, 3, 23, 14, 35), 'test6@example.com', 'test_user6', 'edX+Testing102',
             'US', None, None, None, None, 0.3, None, None, None],

            ['0381d3cb033846d48a5cb1475b589d7f', 'Enterprise 1', 15, 4, 'course-v1:edX+Open_DemoX+edx_demo_course3',
             datetime.datetime(2016, 3, 22, 20, 56, 9), 'verified', 1, '', 0,
             None, u'ginny', 1, u'All about acceptance testing!',
             datetime.datetime(2016, 6, 1, 0, 0), datetime.datetime(2016, 9, 1, 0, 0), 'self_paced', '15', 3, 5,
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test5@example.com', 'test_user5', 'edX+Open_DemoX',
             'US', None, None, None, None, 0.5,
             225.00, 225.00, None],

            [u'0381d3cb033846d48a5cb1475b589d7f', u'Enterprise 1', 15, 4, u'course-v1:edX+Testing102x+1T2017',
             datetime.datetime(2016, 3, 22, 21, 8, 8), u'credit', 1, u'', 0, None, u'ginny', 1,
             u'All about acceptance testing Part 3!', datetime.datetime(2016, 12, 1, 0, 0),
             datetime.datetime(2017, 2, 1, 0, 0), u'instructor_paced', u'9', 2, 5,
             datetime.datetime(2015, 2, 12, 23, 14, 35), u'test5@example.com', u'test_user5', u'edX+Testing102', u'US',
             None, None, None, u'Percentage, 100 (#6)', 0.74, 100.0, None, None]
        ]

        return [tuple(row) for row in expected]

    def validate_enterprise_enrollment_table(self):
        """Assert the enterprise_enrollment table is as expected."""
        columns = ['enterprise_id', 'enterprise_name', 'lms_user_id', 'enterprise_user_id', 'course_id',
                   'enrollment_created_timestamp', 'user_current_enrollment_mode', 'consent_granted', 'letter_grade',
                   'has_passed', 'passed_timestamp', 'enterprise_sso_uid', 'enterprise_site_id', 'course_title',
                   'course_start', 'course_end', 'course_pacing_type', 'course_duration_weeks', 'course_min_effort',
                   'course_max_effort', 'user_account_creation_timestamp', 'user_email', 'user_username', 'course_key',
                   'user_country_code', 'last_activity_date', 'coupon_name', 'coupon_code', 'offer', 'current_grade',
                   'course_price', 'discount_price', 'unenrollment_timestamp']
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
