"""
End to end test of active_users_this_year loading task.
"""

import logging
import os

import pandas

from edx.analytics.tasks.tests.acceptance import (
    AcceptanceTestCase, when_vertica_available, read_csv_fixture_as_list, coerce_columns_to_string
)
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class ActiveUsersAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load active_users_this_year warehouse table."""

    DATE = '2017-06-05'

    def setUp(self):
        super(ActiveUsersAcceptanceTest, self).setUp()

        self.upload_file(
            os.path.join(self.data_dir, 'input', 'active_users'),
            url_path_join(self.warehouse_path, 'active_users_this_year', 'dt=2017-06-05', 'active_users')
        )

    @when_vertica_available
    def test_active_users_this_year(self):
        self.task.launch([
            'LoadInternalReportingActiveUsersToWarehouse',
            '--date', self.DATE,
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of expected output."""

        columns = ['window_start_date', 'window_end_date', 'username']

        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'expected_active_users_this_year.csv')

            expected_output_data = read_csv_fixture_as_list(expected_output_csv)
            expected = pandas.DataFrame(expected_output_data, columns=columns)

            cursor.execute("SELECT * FROM {schema}.f_active_users_this_year".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            f_active_users_this_year = pandas.DataFrame(map(coerce_columns_to_string, response), columns=columns)

            for frame in (f_active_users_this_year, expected):
                frame.sort(['username'], inplace=True, ascending=[True])
                frame.reset_index(drop=True, inplace=True)

            self.assert_data_frames_equal(f_active_users_this_year, expected)
