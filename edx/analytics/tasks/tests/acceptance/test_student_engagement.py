"""
End to end test of student engagement.
"""

import datetime
import hashlib
import logging
import re

from luigi.s3 import S3Target

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

import pandas as pd


log = logging.getLogger(__name__)


class StudentEngagementAcceptanceTest(AcceptanceTestCase):
    """Acceptance test for the CSV-generating Student Engagement Task."""

    INPUT_FILE = 'student_engagement_acceptance_tracking.log'
    NUM_REDUCERS = 1

    COURSE_1 = "edX/DemoX/Demo_Course"
    COURSE_2 = "edX/DemoX/Demo_Course_2"
    COURSE_3 = "course-v1:edX+DemoX+Demo_Course_2015"

    ALL_COURSES = [COURSE_1, COURSE_2, COURSE_3]

    # We only expect some of the generated files to have any counts at all, so enumerate them.
    NONZERO_OUTPUT = [
        (COURSE_1, '2015-04-13', 'daily'),
        (COURSE_1, '2015-04-16', 'daily'),
        (COURSE_2, '2015-04-13', 'daily'),
        (COURSE_2, '2015-04-16', 'daily'),
        (COURSE_3, '2015-04-09', 'daily'),
        (COURSE_3, '2015-04-12', 'daily'),
        (COURSE_3, '2015-04-13', 'daily'),
        (COURSE_3, '2015-04-16', 'daily'),
        (COURSE_1, '2015-04-19', 'weekly'),
        (COURSE_2, '2015-04-19', 'weekly'),
        (COURSE_3, '2015-04-12', 'weekly'),
        (COURSE_3, '2015-04-19', 'weekly'),
        (COURSE_1, '2015-04-19', 'all'),
        (COURSE_2, '2015-04-19', 'all'),
        (COURSE_3, '2015-04-19', 'all'),
    ]

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

        self.test_all = url_path_join(self.test_out, 'all')
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', self.test_src,
            '--output-root', self.test_all,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', self.interval,
            '--interval-type', 'all',
        ])

        for interval_type in ['daily', 'weekly', 'all']:

            date_column_name = "Date" if interval_type == 'daily' else "End Date"

            for course_id in self.ALL_COURSES:
                hashed_course_id = hashlib.sha1(course_id).hexdigest()
                course_dir = url_path_join(self.test_out, interval_type, hashed_course_id)
                csv_files = self.s3_client.list(course_dir)

                # There are 14 student_engagement files in the test data directory, and 3 courses.
                if interval_type == 'daily':
                    self.assertEqual(len(csv_files), 14)
                elif interval_type == 'weekly':
                    self.assertEqual(len(csv_files), 2)
                elif interval_type == 'all':
                    self.assertEqual(len(csv_files), 1)

                # Check that the results have data
                for csv_filename in csv_files:
                    output = url_path_join(course_dir, csv_filename)

                    # parse expected date from output.
                    if interval_type == 'all':
                        expected_date = '2015-04-19'
                    else:
                        csv_pattern = '.*student_engagement_.*_(\\d\\d\\d\\d-\\d\\d-\\d\\d)\\.csv'
                        match = re.match(csv_pattern, output)
                        expected_date = match.group(1)

                    """ Build dataframe from csv file generated from events """
                    generate_file_dataframe = []
                    with S3Target(output).open() as csvfile:
                        # Construct dataframe from file to create more intuitive column handling
                        generate_file_dataframe = pd.read_csv(csvfile)
                        generate_file_dataframe.fillna('', inplace=True)

                    """ Validate specific values: """
                    for date in generate_file_dataframe[date_column_name]:
                        self.assertEquals(date, expected_date)

                    for row_course_id in generate_file_dataframe["Course ID"]:
                        self.assertEquals(row_course_id, course_id)

                    if (course_id, expected_date, interval_type) in self.NONZERO_OUTPUT:
                        # Compare auto-generated student engagement files with associated fixture files

                        # Build fixture file dataframe
                        fixture_file = self.data_dir + "/output/student_engagement/expected/" + interval_type + \
                                       '/' + hashed_course_id + '/' + csv_filename
                        fixture_dataframe = pd.read_csv(fixture_file)
                        fixture_dataframe.fillna('', inplace=True)

                        """ Compare dataframes """
                        self.assertFrameEqual(fixture_dataframe, generate_file_dataframe)

                    else:
                        self.assert_zero_engagement(generate_file_dataframe)
                        # TODO: check username, email, and cohort names (if any).

    def assert_zero_engagement(self, dataframe):
        """Asserts that all counts are zero."""
        for column_name in dataframe.columns[5:14]:
            for column_value in dataframe[column_name]:
                self.assertEquals(column_value, 0)
        for column_value in dataframe['URL of Last Subsection Viewed']:
            self.assertEquals(len(column_value), 0)

    def assertFrameEqual(self, df1, df2, **kwds):
        """ Assert that two dataframes are equal, ignoring ordering of columns"""
        from pandas.util.testing import assert_frame_equal
        return assert_frame_equal(df1.sort(axis=1), df2.sort(axis=1), check_names=True, **kwds)
