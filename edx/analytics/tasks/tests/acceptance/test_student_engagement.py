"""
End to end test of student engagement.
"""

import datetime
import hashlib
import logging
import re

from luigi.s3 import S3Target

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

from pandas import read_csv
from pandas.util.testing import assert_frame_equal

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

    # Cohort mappings are static properties of course and username.
    # Users not present in this map are assumed to have no cohort assignment.
    EXPECTED_COHORT_MAP = {
        COURSE_1: {
            'audit': 'best-cohort',
            'honor': 'best-cohort',
            'staff': 'other-cohort',
        },
        COURSE_2: {
        },
        COURSE_3: {
            'audit': 'new-cohort',
            'verified': 'additional-cohort',
        },
    }

    def test_student_engagement(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2015, 4, 10))
        self.execute_sql_fixture_file('load_student_engagement.sql')

        self.interval = '2015-04-06-2015-04-20'  # run for exactly two weeks

        for interval_type in ['daily', 'weekly', 'all']:

            self.run_task(interval_type)

            for course_id in self.ALL_COURSES:
                hashed_course_id = hashlib.sha1(course_id).hexdigest()
                course_dir = url_path_join(self.test_out, interval_type, hashed_course_id)
                csv_filenames = self.s3_client.list(course_dir)

                # Check expected number of CSV files.
                if interval_type == 'daily':
                    self.assertEqual(len(csv_filenames), 14)
                elif interval_type == 'weekly':
                    self.assertEqual(len(csv_filenames), 2)
                elif interval_type == 'all':
                    self.assertEqual(len(csv_filenames), 1)

                # Check that the CSV files contain the expected data.
                for csv_filename in csv_filenames:

                    # Parse expected date from filename.
                    if interval_type == 'all':
                        expected_date = '2015-04-19'
                    else:
                        csv_pattern = '.*student_engagement_.*_(\\d\\d\\d\\d-\\d\\d-\\d\\d)\\.csv'
                        match = re.match(csv_pattern, csv_filename)
                        expected_date = match.group(1)

                    # Build dataframe from csv file generated from events.
                    actual_dataframe = []
                    with S3Target(url_path_join(course_dir, csv_filename)).open() as csvfile:
                        actual_dataframe = read_csv(csvfile)
                        actual_dataframe.fillna('', inplace=True)

                    # Validate specific values:
                    if (course_id, expected_date, interval_type) in self.NONZERO_OUTPUT:
                        self.check_nonzero_engagement_dataframe(actual_dataframe, interval_type, hashed_course_id, csv_filename)
                    else:
                        self.check_zero_engagement_dataframe(actual_dataframe, interval_type, course_id, expected_date)

    def run_task(self, interval_type):
        """Run the CSV-generating task."""
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', self.test_src,
            '--output-root', url_path_join(self.test_out, interval_type),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', self.interval,
            '--interval-type', interval_type,
        ])

    def check_nonzero_engagement_dataframe(self, actual_dataframe, interval_type, hashed_course_id, csv_filename):
        """Compare auto-generated student engagement files with associated fixture files."""
        fixture_file = url_path_join(
            self.data_dir,
            "output/student_engagement/expected",
            interval_type,
            hashed_course_id,
            csv_filename,
        )
        expected_dataframe = read_csv(fixture_file)
        expected_dataframe.fillna('', inplace=True)
        assert_frame_equal(actual_dataframe, expected_dataframe, check_names=True)

    def check_zero_engagement_dataframe(self, dataframe, interval_type, course_id, expected_date):
        """Check values in student engagement data that should have zero engagement counts."""

        date_column_name = "Date" if interval_type == 'daily' else "End Date"
        for date in dataframe[date_column_name]:
            self.assertEquals(date, expected_date)

        for row_course_id in dataframe["Course ID"]:
            self.assertEquals(row_course_id, course_id)

        self.assert_enrollment(dataframe, course_id, expected_date)
        self.assert_username_properties(dataframe, course_id)
        self.assert_zero_engagement(dataframe)

    def assert_zero_engagement(self, dataframe):
        """Asserts that all counts are zero."""
        for column_name in dataframe.columns[5:14]:
            for column_value in dataframe[column_name]:
                self.assertEquals(column_value, 0)
        for column_value in dataframe['URL of Last Subsection Viewed']:
            self.assertEquals(len(column_value), 0)

    def assert_enrollment(self, dataframe, course_id, date):
        """Asserts that enrollments are as expected, given the course and date."""
        actual = [value for value in dataframe['Username']]
        expected = ['audit', 'honor']
        if course_id == self.COURSE_1:
            if date >= '2015-04-14':
                expected.append('staff')
            if date >= '2015-04-08':
                expected.append('verified')
        elif course_id == self.COURSE_2:
            if (date >= '2015-04-12' and date < '2015-04-14') or date >= '2015-04-16':
                expected.append('staff')
            expected.append('verified')
        elif course_id == self.COURSE_3:
            if date >= '2015-04-14':
                expected.append('staff')
            expected.append('verified')
        self.assertEquals(expected, actual)

    def assert_username_properties(self, dataframe, course_id):
        """Asserts that email and cohort values match username's expectations."""
        for _, row in dataframe.iterrows():
            username = row['Username']
            email = row['Email']
            cohort = row['Cohort']
            self.assertEquals(email, "{}@example.com".format(username))
            self.assertEquals(cohort, self.EXPECTED_COHORT_MAP[course_id].get(username, ''))
