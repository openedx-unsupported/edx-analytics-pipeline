"""
End to end test of student engagement.
"""

import os
import datetime
import logging

from luigi.s3 import S3Target

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

import pandas as pd


log = logging.getLogger(__name__)

class DailyStudentEngagementAcceptanceTest(AcceptanceTestCase):
    # Acceptance test for the CSV-generating Student Engagement Task

    INPUT_FILE = 'student_engagement_acceptance_tracking.log'
    INPUT_FORMAT = 'oddjob.ManifestTextInputFormat'
    NUM_REDUCERS = 1

    def test_student_engagement(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2015, 4, 05))
        self.execute_sql_fixture_file('load_student_engagement.sql')

        self.test_daily = url_path_join(self.test_out, 'daily')
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', self.test_src,
            '--output-root', self.test_daily,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', '2015-04-06-2015-04-20',
        ])

        self.test_weekly = url_path_join(self.test_out, 'weekly')
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', self.test_src,
            '--output-root', self.test_weekly,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', '2015-04-06-2015-04-20',
            '--interval-type', 'all',
        ])

        self.test_all = url_path_join(self.test_out, 'daily')
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', self.test_src,
            '--output-root', self.test_all,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', '2015-04-06-2015-04-20',
            '--interval-type', 'all',
        ])

        #
        # Produce student_engagement file list
        # Eg:
        # s3://.../StudentEngagementAcceptanceTest/out/.../student_engagement_daily_2015-04-05.csv
        #
        outputs = self.s3_client.list(self.test_daily)
        outputs = [url_path_join(self.test_daily, p) for p in outputs]

        # There are 15 student_engagement files in the test data directory
        self.assertEqual(len(outputs), 45)

        # Check that the results have data
        for output in outputs:
            df = []
            try:
                with S3Target(output).open() as f:
                    # Construct dataframe from file to create more intuitive column handling
                    df = pd.read_csv(f)

            except IOError as exc:
                if exc.errno != errno.EISDIR: # Do not fail if a directory is found, just ignore it.
                    raise # Propagate other kinds of IOError.


            if output
            columns = len(df.columns)
            rows    = len(df)

            self.validate_number_of_columns(columns)

            for date in df["date"]:
                self.validate_date_cell_format(date)

            for course_id in df["course_id"]:
                self.validate_course_id_string_format(course_id)

            for user_name in df["username"]:
                self.validate_username_string_format(user_name)

            for email in df["email"]:
                self.validate_email_string_format(email)

            for column_name in df.ix[:,5:14]:
                for r in df[column_name]:
                    self.validate_problems_videos_forums_textbook_values(r)

            for cohort in df["cohort"]:
                self.validate_cohort_format(cohort)

            self.validate_within_rows(df)


    def validate_number_of_columns(self,num_columns):
        """Ensure each student engagement file has the correct number of columns (15)"""
        self.assertTrue(num_columns==15,msg="Number of columns not equal to 15")

    def validate_date_cell_format(self,date):
        """Ensure date on each row is of the format yyyy-mm-dd"""
        self.assertRegexpMatches(date,'^\d\d\d\d-\d\d-\d\d$')

    def validate_course_id_string_format(self,course_id):
        """Ensure course_id on each row matches a course_id string"""
        self.assertTrue(opaque_key_util.is_valid_course_id(course_id))

    def validate_username_string_format(self,user_name):
        """Ensure user_name on each row matches a user_name string"""
        self.assertRegexpMatches(user_name,'^.{1,}$')

    def validate_email_string_format(self,email):
        """Ensure email address on each row matches an email address"""
        self.assertRegexpMatches(email,'^([^@|\s]+@[^@]+\.[^@|\s]+)$')

    def validate_cohort_format(self,cohort):
        """Cohort is present or not"""
        if cohort:
            self.assertRegexpMatches(str(cohort),'^.*$')

    def validate_problems_videos_forums_textbook_values(self,value):
        """Ensure problems,videos,forum and texbook column values are greater than or equal to 0"""
        self.assertTrue(int(value) >= 0, msg="Problems,Videos,Forums or Textbook fields are not greater or equal to 0.")

    def validate_within_rows(self,dataframe):
        # Validate various comparisons within a given row. Eg:
        # 1. problems correct gte to problems_attempted
        for index, row in dataframe.iterrows():
            # Number of correct problems should be equal to or lower than problems_attempted
            self.assertTrue(row["problems_correct"] <= row["problems_attempted"],msg="Greater number of problems_correct than problems_attempted.")
