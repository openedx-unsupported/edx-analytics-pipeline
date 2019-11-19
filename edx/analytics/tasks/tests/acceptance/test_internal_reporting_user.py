"""
End to end test of the internal reporting user table loading task.
"""

from __future__ import absolute_import

import datetime
import logging
import os

import pandas
from six.moves import map

from edx.analytics.tasks.tests.acceptance import (
    AcceptanceTestCase, coerce_columns_to_string, read_csv_fixture_as_list, when_vertica_available
)

log = logging.getLogger(__name__)


class InternalReportingUserLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's user table."""

    INPUT_FILE = 'location_by_course_tracking.log'
    INTERVAL = '2014-07-21-2014-07-22'
    DATE = '2014-07-22'

    def setUp(self):
        super(InternalReportingUserLoadAcceptanceTest, self).setUp()

        # Set up the mock LMS databases.
        self.execute_sql_fixture_file('load_auth_user_for_internal_reporting_user.sql')
        self.execute_sql_fixture_file('load_auth_userprofile.sql')

        # Put up the mock tracking log for user locations.
        self.upload_tracking_log(self.INPUT_FILE, datetime.datetime(2014, 7, 21))

    @when_vertica_available
    def test_internal_reporting_user(self):
        """Tests the workflow for the internal reporting user table, end to end."""

        self.task.launch([
            'LastCountryOfUserPartitionTask',
            '--interval', self.INTERVAL,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.task.launch([
            'LoadInternalReportingUserToWarehouse',
            '--date', self.DATE,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""

        columns = ['user_id', 'user_year_of_birth', 'user_level_of_education', 'user_gender', 'user_email',
                   'user_username', 'user_account_creation_time', 'user_last_location_country_code']

        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_user.csv')

            expected_output_data = read_csv_fixture_as_list(expected_output_csv)
            expected = pandas.DataFrame(expected_output_data, columns=columns)

            cursor.execute("SELECT * FROM {schema}.d_user".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            d_user = pandas.DataFrame(list(map(coerce_columns_to_string, response)), columns=columns)

            for frame in (d_user, expected):
                frame.sort(['user_id'], inplace=True, ascending=[True])
                frame.reset_index(drop=True, inplace=True)

            self.assert_data_frames_equal(d_user, expected)
