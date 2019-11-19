"""
End to end test of the internal reporting user activity table loading task.
"""

from __future__ import absolute_import

import logging
import os

import pandas

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class InternalReportingUserActivityLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's user activity table."""

    DATE = '2014-07-01'

    def setUp(self):
        super(InternalReportingUserActivityLoadAcceptanceTest, self).setUp()

        self.upload_file(os.path.join(self.data_dir, 'input', 'internal_reporting_user_activity'), url_path_join(self.warehouse_path, 'user_activity_by_user', 'dt=2014-07-01', 'user_activity_2014-07-01'))

    @when_vertica_available
    def test_internal_reporting_user_activity(self):
        """Tests the workflow for the internal reporting user activity table, end to end."""

        self.task.launch([
            'LoadInternalReportingUserActivityToWarehouse',
            '--date', self.DATE,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--overwrite'
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            # Note that we don't require the output rows to be in any particular order for this test, so we only
            # care about setwise equality of rows, not exact dataframe equality, and thus throw out row order info.
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_f_user_activity.csv')
            expected_dataframe = pandas.read_csv(expected_output_csv, parse_dates=False)
            expected_f_user_activity = set()
            for row in expected_dataframe.values:
                expected_f_user_activity.add(tuple(row[1:]))

            cursor.execute("SELECT * FROM {schema}.f_user_activity".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()

            user_id_column = 1
            course_id_column = 2
            date_column = 3
            event_type_column = 4
            event_count_column = 5

            def row_mapper(row):
                return (
                    row[user_id_column],
                    str(row[course_id_column]),
                    row[date_column].strftime('%Y-%m-%d'),
                    str(row[event_type_column]),
                    row[event_count_column]
                )
            f_user_activity = set([row_mapper(row) for row in response])

            self.assertSetEqual(f_user_activity, expected_f_user_activity)
