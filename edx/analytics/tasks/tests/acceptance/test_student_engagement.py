"""
End to end test of student engagement.
"""

import datetime
import hashlib
import logging

from luigi.s3 import S3Target

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

import pandas as pd


log = logging.getLogger(__name__)


class DailyStudentEngagementAcceptanceTest(AcceptanceTestCase):
    """Acceptance test for the CSV-generating Student Engagement Task."""

    INPUT_FILE = 'student_engagement_acceptance_tracking.log'
    NUM_REDUCERS = 1

    COURSE_1 = "edX/DemoX/Demo_Course"
    COURSE_2 = "edX/DemoX/Demo_Course_2"
    COURSE_3 = "course-v1:edX+DemoX+Demo_Course_2015"

    ALL_COURSES = [COURSE_1, COURSE_2, COURSE_3]

    def test_student_engagement(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2015, 4, 10))
        self.execute_sql_fixture_file('load_student_engagement.sql')

        self.interval = '2015-04-06-2015-04-20'  # run for exactly two weeks

        self.test_daily = url_path_join(self.test_out, 'daily')
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', self.test_src,
            '--output-root', self.test_daily,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', self.interval,
        ])

        self.test_weekly = url_path_join(self.test_out, 'weekly')
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', self.test_src,
            '--output-root', self.test_weekly,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', self.interval,
            '--interval-type', 'weekly',
        ])

        self.test_all = url_path_join(self.test_out, 'daily')
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', self.test_src,
            '--output-root', self.test_all,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', self.interval,
            '--interval-type', 'all',
        ])

        #
        # Produce student_engagement file list
        # Eg:
        # s3://.../StudentEngagementAcceptanceTest/out/.../student_engagement_daily_2015-04-05.csv
        #
        for interval_type in ['daily', 'weekly', 'all']:

            date_column_name = "date" if interval_type == 'daily' else "end_date"

            for course_id in self.ALL_COURSES:
                hashed_course_id = hashlib.sha1(course_id).hexdigest()
                course_dir = url_path_join(self.test_out, interval_type, hashed_course_id)
                outputs = self.s3_client.list(course_dir)
                outputs = [url_path_join(self.test_daily, p) for p in outputs if p.endswith(".csv")]

                # There are 14 student_engagement files in the test data directory, and 3 courses.
                if interval_type == 'daily':
                    self.assertEqual(len(outputs), 14)
                elif interval_type == 'weekly':
                    self.assertEqual(len(outputs), 2)
                elif interval_type == 'all':
                    self.assertEqual(len(outputs), 1)

                # Check that the results have data
                for output in outputs:

                    # TODO: parse expected date from output.


                    df = []
                    with S3Target(output).open() as f:
                        # Construct dataframe from file to create more intuitive column handling
                        df = pd.read_csv(f)

                    columns = len(df.columns)
                    # rows = len(df)

                    self.validate_number_of_columns(columns)

                    for date in df[date_column_name]:
                        self.validate_date_cell_format(date)
                        self.assertEquals(date, df[date_column_name][0])

                    for row_course_id in df["course_id"]:
                        self.assertEquals(row_course_id, course_id)

                    for user_name in df["username"]:
                        self.validate_username_string_format(user_name)

                    for email in df["email"]:
                        self.validate_email_string_format(email)

                    for column_name in df.ix[:, 5:14]:
                        for r in df[column_name]:
                            self.validate_problems_videos_forums_textbook_values(r)

                    for cohort in df["cohort"]:
                        self.validate_cohort_format(cohort)

                    self.validate_within_rows(df)

    def validate_number_of_columns(self,num_columns):
        """Ensure each student engagement file has the correct number of columns (15)"""
        self.assertTrue(num_columns == 15, msg="Number of columns not equal to 15")

    def validate_date_cell_format(self, date):
        """Ensure date on each row is of the format yyyy-mm-dd"""
        self.assertRegexpMatches(date, '^\d\d\d\d-\d\d-\d\d$')

    def validate_course_id_string_format(self, course_id):
        """Ensure course_id on each row matches a course_id string"""
        self.assertTrue(opaque_key_util.is_valid_course_id(course_id))

    def validate_username_string_format(self, user_name):
        """Ensure user_name on each row matches a user_name string"""
        self.assertRegexpMatches(user_name, '^.{1,}$')

    def validate_email_string_format(self, email):
        """Ensure email address on each row matches an email address"""
        self.assertRegexpMatches(email, '^([^@|\s]+@[^@]+\.[^@|\s]+)$')

    def validate_cohort_format(self, cohort):
        """Cohort is present or not"""
        if cohort:
            self.assertRegexpMatches(str(cohort), '^.*$')

    def validate_problems_videos_forums_textbook_values(self, value):
        """Ensure problems,videos,forum and texbook column values are greater than or equal to 0"""
        self.assertTrue(int(value) >= 0, msg="Problems,Videos,Forums or Textbook fields are not greater or equal to 0.")

    def validate_within_rows(self, dataframe):
        # Validate various comparisons within a given row. Eg:
        # 1. problems correct gte to problems_attempted
        for index, row in dataframe.iterrows():
            # Number of correct problems should be equal to or lower than problems_attempted
            self.assertTrue(row["problems_correct"] <= row["problems_attempted"],
                            msg="Greater number of problems_correct than problems_attempted.")
