"""
End to end test of student engagement.
"""

import datetime
import hashlib
import logging
import os.path
import re

from pandas import read_csv
from pandas.util.testing import assert_frame_equal

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, as_list_param
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class StudentEngagementAcceptanceTest(AcceptanceTestCase):
    """Acceptance test for the CSV-generating Student Engagement Task."""

    INPUT_FILE = 'student_engagement_acceptance_tracking.log'
    NUM_REDUCERS = 1

    COURSE_1 = "edX/DemoX/Demo_Course"
    COURSE_2 = "edX/DemoX/Demo_Course_2"
    COURSE_3 = "course-v1:edX+DemoX+Demo_Course_2015"

    ALL_COURSES = [COURSE_1, COURSE_2, COURSE_3]

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
                csv_targets = self.get_targets_from_remote_path(course_dir)

                # Check expected number of CSV files.
                if interval_type == 'daily':
                    self.assertEqual(len(csv_targets), 14)
                elif interval_type == 'weekly':
                    self.assertEqual(len(csv_targets), 2)
                elif interval_type == 'all':
                    self.assertEqual(len(csv_targets), 1)

                # Check that the CSV files contain the expected data.
                for csv_target in csv_targets:

                    # Parse expected date from filename.
                    if interval_type == 'all':
                        expected_date = '2015-04-19'
                    else:
                        csv_pattern = '.*student_engagement_.*_(\\d\\d\\d\\d-\\d\\d-\\d\\d)\\.csv'
                        match = re.match(csv_pattern, csv_target.path)
                        expected_date = match.group(1)

                    # Build dataframe from csv file generated from events.
                    actual_dataframe = []
                    with csv_target.open('r') as csvfile:
                        actual_dataframe = read_csv(csvfile)
                        actual_dataframe.fillna('', inplace=True)

                    self.check_engagement_dataframe(actual_dataframe, interval_type, course_id, expected_date)

                    # Validate specific values:
                    csv_filename = os.path.basename(csv_target.path)
                    expected_dataframe = self.get_expected_engagement(interval_type, hashed_course_id, csv_filename)
                    if expected_dataframe is not None:
                        assert_frame_equal(actual_dataframe, expected_dataframe, check_names=True)
                    else:
                        self.assert_zero_engagement(actual_dataframe)

    def run_task(self, interval_type):
        """Run the CSV-generating task."""
        self.task.launch([
            'StudentEngagementCsvFileTask',
            '--source', as_list_param(self.test_src),
            '--output-root', url_path_join(self.test_out, interval_type),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', self.interval,
            '--interval-type', interval_type,
        ])

    def get_expected_engagement(self, interval_type, hashed_course_id, csv_filename):
        """Compare auto-generated student engagement files with associated fixture files."""
        fixture_file = url_path_join(
            self.data_dir,
            "output/student_engagement/expected",
            interval_type,
            hashed_course_id,
            csv_filename,
        )
        if not os.path.exists(fixture_file):
            return None

        expected_dataframe = read_csv(fixture_file)
        expected_dataframe.fillna('', inplace=True)
        return expected_dataframe

    def check_engagement_dataframe(self, dataframe, interval_type, course_id, expected_date):
        """Check values in student engagement data that should be present in all dataframes."""

        date_column_name = "Date" if interval_type == 'daily' else "End Date"
        for date in dataframe[date_column_name]:
            self.assertEquals(date, expected_date)

        for row_course_id in dataframe["Course ID"]:
            self.assertEquals(row_course_id, course_id)

        self.assert_enrollment(dataframe, course_id, expected_date)
        self.assert_username_properties(dataframe, course_id)

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


class PerStudentEngagementAcceptanceTest(AcceptanceTestCase):
    """
    Acceptance test for the MySQL Per-Student Engagement Data Task
    Focuses on forum events, which are not covered by the above test case.
    """

    INPUT_FILE = 'student_forum_activity.log'
    NUM_REDUCERS = 1

    COURSE_ID = "course-v1:ForumX+1+1"

    def test_forum_engagement(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2015, 9, 14))
        self.execute_sql_fixture_file('load_student_engagement.sql')

        for interval_type in ['daily', 'weekly']:
            self.run_and_check(interval_type)

    def run_and_check(self, interval_type):
        self.task.launch([
            'StudentEngagementToMysqlTask',
            '--source', as_list_param(self.test_src),
            '--credentials', self.export_db.credentials_file_url,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', '2015-09-01-2015-09-16',
            '--interval-type', interval_type,
        ])

        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT end_date, course_id, username, '
                'forum_posts, forum_responses, forum_comments, '
                'forum_upvotes_given, forum_upvotes_received '
                'FROM student_engagement_{interval_type} WHERE course_id="{course_id}" '
                'ORDER BY end_date, username;'
                .format(course_id=self.COURSE_ID, interval_type=interval_type)
            )
            results = cursor.fetchall()

        if interval_type == 'weekly':
            end_date_expected = datetime.date(2015, 9, 15)
        elif interval_type == 'daily':
            end_date_expected = datetime.date(2015, 9, 14)
        else:
            assert False, "Invalid interval type: {}".format(interval_type)

        self.assertItemsEqual(results, [
            (end_date_expected, self.COURSE_ID, 'audit', 1, 0, 0, 3, 1),
            (end_date_expected, self.COURSE_ID, 'honor', 1, 1, 0, 0, 2),
            (end_date_expected, self.COURSE_ID, 'staff', 2, 0, 0, 1, 2),
            (end_date_expected, self.COURSE_ID, 'verified', 0, 0, 1, 1, 0),
        ])
