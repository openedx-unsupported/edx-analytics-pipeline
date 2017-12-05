"""
End to end test of demographic trends.
"""

import datetime
import logging
import os
from cStringIO import StringIO

import pandas
from ddt import data, ddt

from edx.analytics.tasks.insights.enrollments import EnrollmentSummaryRecord
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


@ddt
class EnrollmentAcceptanceTest(AcceptanceTestCase):
    """End to end test of demographic trends."""

    CATALOG_DATE = '2016-09-08'
    INPUT_FILE = 'enrollment_trends_tracking.log'

    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None

    def setUp(self):
        """Loads enrollment and course catalog fixtures."""
        super(EnrollmentAcceptanceTest, self).setUp()

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

    @data(True, False)
    def test_table_generation(self, enable_course_catalog):
        self.launch_task(enable_course_catalog)
        self.validate_enrollment_summary_table(enable_course_catalog)
        self.validate_demographic_trends()

    def launch_task(self, enable_course_catalog):
        """Kicks off the summary task."""
        task_params = [
            'ImportEnrollmentsIntoMysql',
            '--interval', '2014-07-30-2014-08-07',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--date', self.CATALOG_DATE,
            '--overwrite-n-days', '7',
        ]

        if enable_course_catalog:
            task_params.append('--enable-course-catalog')

        self.task.launch(task_params)

    def expected_enrollment_summary_results(self, enable_course_catalog):
        """Returns expected results with course catalog data removed if enable_course_catalog is False."""
        expected = [
            ['course-v1:edX+Open_DemoX+edx_demo_course2', 'All about acceptance testing!', 'edX+Open_DemoX',
             datetime.datetime(2016, 6, 1), datetime.datetime(2016, 9, 1), 'self_paced', 'Archived',
             'honor', 1, 1, 1, 0],
            ['course-v1:edX+Open_DemoX+edx_demo_course2', 'All about acceptance testing!', 'edX+Open_DemoX',
             datetime.datetime(2016, 6, 1), datetime.datetime(2016, 9, 1), 'self_paced', 'Archived',
             'verified', 1, 1, 1, 0],
            ['edX/Open_DemoX/edx_demo_course', 'All about acceptance testing!', 'edX+Open_DemoX',
             datetime.datetime(2016, 9, 1), datetime.datetime(2016, 12, 1), 'instructor_paced', 'Current',
             'honor', 0, 0, 3, 2],
            ['edX/Open_DemoX/edx_demo_course', 'All about acceptance testing!', 'edX+Open_DemoX',
             datetime.datetime(2016, 9, 1), datetime.datetime(2016, 12, 1), 'instructor_paced', 'Current',
             'verified', 0, 0, 2, 0],
        ]
        if not enable_course_catalog:
            # remove catalog data
            catalog_indices = range(1, 7)
            for row in expected:
                for catalog_index in catalog_indices:
                    row[catalog_index] = None

        return [tuple(row) for row in expected]

    def validate_enrollment_summary_table(self, enable_course_catalog):
        """Assert the summary table is as expected."""
        columns = ['course_id', 'catalog_course_title', 'catalog_course', 'start_time', 'end_time',
                   'pacing_type', 'availability', 'enrollment_mode',
                   'count', 'count_change_7_days', 'cumulative_count', 'passing_users']
        with self.export_db.cursor() as cursor:
            cursor.execute(
                '''
                  SELECT {columns}
                  FROM   course_meta_summary_enrollment
                '''.format(columns=','.join(columns))
            )
            results = cursor.fetchall()

        expected = self.expected_enrollment_summary_results(enable_course_catalog)
        self.assertItemsEqual(expected, results)

    def validate_gender(self):
        """Ensure the gender breakdown is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT date, course_id, gender, count, cumulative_count FROM course_enrollment_gender_daily'
                ' ORDER BY date, course_id, gender ASC'
            )
            results = cursor.fetchall()

        expected = [
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', None, 2, 2),
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 'm', 1, 2),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', None, 2, 2),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 'm', 0, 2),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'm', 1, 1),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', None, 2, 2),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 'm', 1, 2),
            (datetime.date(2014, 8, 4), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'm', 1, 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', None, 2, 2),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', 'm', 0, 2),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'm', 2, 2),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', None, 1, 3),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 'm', 0, 2),
            (datetime.date(2014, 8, 6), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'm', 2, 2),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', None, 0, 3),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 'm', 0, 2),
        ]
        self.assertItemsEqual(expected, results)

    def validate_birth_year(self):
        """Ensure the birth year breakdown is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT date, course_id, birth_year, count, cumulative_count FROM course_enrollment_birth_year_daily'
                ' ORDER BY date, course_id, birth_year ASC'
            )
            results = cursor.fetchall()

        expected = [
            (datetime.date(2014, 8, 6), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1975, 1, 1),
            (datetime.date(2014, 8, 6), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1984, 1, 1),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 1975, 0, 1),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 1984, 0, 1),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 2000, 0, 2),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', None, 0, 1),
        ]
        self.assertItemsEqual(expected, results)

    def validate_education_level(self):
        """Ensure the education level breakdown is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT date, course_id, education_level, count, cumulative_count FROM course_enrollment_education_level_daily'
                ' ORDER BY date, course_id, education_level ASC'
            )
            results = cursor.fetchall()

        expected = [
            (datetime.date(2014, 8, 6), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'associates', 1, 1),
            (datetime.date(2014, 8, 6), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'bachelors', 1, 1),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', None, 0, 2),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 'associates', 0, 1),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 'bachelors', 0, 2),
        ]
        self.assertItemsEqual(expected, results)

    def validate_mode(self):
        """Ensure the mode breakdown is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT date, course_id, mode, count, cumulative_count FROM course_enrollment_mode_daily'
                ' ORDER BY date, course_id, mode ASC'
            )
            results = cursor.fetchall()

        expected = [
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 'audit', 1, 1),
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 'honor', 1, 2),
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 'verified', 1, 1),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 'audit', 0, 1),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 'honor', 1, 2),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 'verified', 1, 1),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'verified', 1, 1),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 'audit', 1, 1),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 'honor', 1, 2),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 'verified', 1, 1),
            (datetime.date(2014, 8, 4), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'verified', 1, 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', 'honor', 1, 3),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', 'verified', 1, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'honor', 1, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'verified', 1, 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 'honor', 1, 3),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 'verified', 0, 2),
            (datetime.date(2014, 8, 6), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'honor', 1, 1),
            (datetime.date(2014, 8, 6), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'verified', 1, 1),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 'honor', 0, 3),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 'verified', 0, 2),
        ]
        self.assertItemsEqual(expected, results)

    def validate_base(self):
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT date, course_id, count, cumulative_count FROM course_enrollment_daily ORDER BY date, course_id ASC')
            results = cursor.fetchall()

        self.assertItemsEqual(results, [
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 3, 4),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 2, 4),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1, 1),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 3, 4),
            (datetime.date(2014, 8, 4), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1, 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', 2, 4),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 2, 2),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 1, 5),
            (datetime.date(2014, 8, 6), 'course-v1:edX+Open_DemoX+edx_demo_course2', 2, 2),
            (datetime.date(2014, 8, 6), 'edX/Open_DemoX/edx_demo_course', 0, 5),
        ])

    def validate_summary(self):
        """Ensure the summary is correct."""
        data_path = url_path_join(self.warehouse_path, 'course_enrollment_summary', 'dt=2014-08-07')
        raw_output = self.read_dfs_directory(data_path)
        output = StringIO(raw_output.replace('\t\\N', '\t'))
        columns = EnrollmentSummaryRecord.get_fields().keys()
        data = pandas.read_table(output, header=None, names=columns, parse_dates=True)

        expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_user_course.csv')
        expected = pandas.read_csv(expected_output_csv, parse_dates=True)

        for frame in (data, expected):
            frame.sort('first_enrollment_time', inplace=True, ascending=True)
            frame.reset_index(drop=True, inplace=True)

        self.assert_data_frames_equal(data, expected)

    def validate_demographic_trends(self):
        """Assert all tables are as expected."""
        self.validate_summary()
        self.validate_base()
        self.validate_gender()
        self.validate_birth_year()
        self.validate_education_level()
        self.validate_mode()
