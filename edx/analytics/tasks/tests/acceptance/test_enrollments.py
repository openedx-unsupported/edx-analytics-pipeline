"""
End to end test of demographic trends.
"""

import datetime
import logging
import os

from cStringIO import StringIO

import pandas

from edx.analytics.tasks.enrollments import EnrollmentSummaryRecord
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

log = logging.getLogger(__name__)


class EnrollmentAcceptanceTest(AcceptanceTestCase):
    """End to end test of demographic trends."""

    INPUT_FILE = 'enrollment_trends_tracking.log'

    def test_demographic_trends(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 8, 1))
        self.execute_sql_fixture_file('load_auth_userprofile.sql')

        self.task.launch([
            'ImportEnrollmentsIntoMysql',
            '--interval', '2014-08-01-2014-08-06',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--overwrite-n-days', '5',
        ])

        self.maxDiff = None

        self.validate_summary()
        self.validate_base()
        self.validate_gender()
        self.validate_birth_year()
        self.validate_education_level()
        self.validate_mode()

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
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', None, 1, 2),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 'm', 0, 2),
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
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1975, 1, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1984, 1, 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 1975, 0, 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 1984, 0, 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 2000, 1, 2),
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
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'associates', 1, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'bachelors', 1, 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', None, 1, 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 'associates', 0, 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 'bachelors', 0, 2),
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
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 'verified', 0, 1),
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
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 1, 4),
        ])

    def validate_summary(self):
        """Ensure the summary is correct."""
        data_path = url_path_join(self.warehouse_path, 'course_enrollment_summary', 'dt=2014-08-06')
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
