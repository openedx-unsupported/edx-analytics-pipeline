__author__ = 'johnbaker'

import os
import re
import hashlib
import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

class StudentEngagementAcceptanceTest(unittest.TestCase):
# class StudentEngagementAcceptanceTest(AcceptanceTestCase):
    # Temporary
    data_dir = '/Users/johnbaker/code/edx-analytics-pipeline/edx/analytics/tasks/tests/acceptance/fixtures/'

    INPUT_FILE = 'student_engagement_acceptance_tracking.log'
    NUM_REDUCERS = 1

    HASHED_COURSE_IDS = ["931f2c258d234630b8518670f96b8f81e8e91410",
                         "8ced31a1fb68cfdf1994b0e15844a8da416649ae",
                         "38baa5e739b0e44b83811fe3121df97b436a772a"]

    def test_student_engagement(self):
        for interval_type in ['daily', 'weekly', 'all']:
            date_column_name = "date" if interval_type == 'daily' else "end_date"

            for hashed_course_id in self.HASHED_COURSE_IDS:
                # course_dir = url_path_join(self.test_out, interval_type, hashed_course_id)
                # outputs = self.s3_client.list(course_dir)
                # outputs = [url_path_join(course_dir, p) for p in outputs if p.endswith(".csv")]
                course_dir = '/tmp/student_engagement/' + interval_type + '/' + hashed_course_id
                csv_files = [f for f in os.listdir(course_dir) if re.match(r'.*\.csv', f)]

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

                    # Build dataframe from csv file generated from events
                    generate_file_dataframe = []
                    with open(output) as csvfile:
                        # with S3Target(output).open() as csvfile:
                        # Construct dataframe from file to create more intuitive column handling
                        generate_file_dataframe = pd.read_csv(csvfile)
                        generate_file_dataframe.fillna('', inplace=True)

                    # Validate specific values:
                    for date in generate_file_dataframe[date_column_name]:
                        self.assertEquals(date, expected_date)

                    # Build fixture file dataframe
                    fixture_file = self.data_dir + "/output/student_engagement/expected/" \
                                   + interval_type + '/' + hashed_course_id + '/' + csv_filename
                    fixture_dataframe = pd.read_csv(fixture_file)
                    fixture_dataframe.fillna('', inplace=True)

                    # Compare dataframes
                    self.assertFrameEqual(fixture_dataframe, generate_file_dataframe)

    def assertFrameEqual(self, expected_dataframe, actual_dataframe, **kwds):
        """ Assert that two dataframes are equal, ignoring ordering of columns"""
        return assert_frame_equal(expected_dataframe, actual_dataframe, check_names=True)

if __name__ == '__main__':
    unittest.main()