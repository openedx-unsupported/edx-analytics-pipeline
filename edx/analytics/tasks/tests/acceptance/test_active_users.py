"""
End to end test of active_users_this_year loading task.
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


class ActiveUsersAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load active_users_this_year warehouse table."""

    DATE = '2017-07-24'

    def setUp(self):
        super(ActiveUsersAcceptanceTest, self).setUp()

        self.upload_tracking_log('active_users_tracking.log', datetime.datetime(2017, 7, 21))
        self.upload_tracking_log('active_users_tracking.log', datetime.datetime(2017, 7, 12))

    @when_vertica_available
    def test_active_users_this_year(self):
        self.task.launch([
            'ActiveUsersWorkflow',
            '--date', self.DATE,
            '--overwrite-n-weeks', '1',
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of expected output."""

        columns = ['start_date', 'end_date', 'username']

        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'expected_active_users_per_week.csv')

            expected_output_data = read_csv_fixture_as_list(expected_output_csv)
            expected = pandas.DataFrame(expected_output_data, columns=columns)

            cursor.execute("SELECT * FROM {schema}.f_active_users_per_week".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            f_active_users_this_year = pandas.DataFrame(list(map(coerce_columns_to_string, response)), columns=columns)

            for frame in (f_active_users_this_year, expected):
                frame.sort(['username'], inplace=True, ascending=[True])
                frame.reset_index(drop=True, inplace=True)

            self.assert_data_frames_equal(f_active_users_this_year, expected)
