"""
End to end test of the internal reporting country table loading task.
"""

import os
import logging
import datetime
import pandas
import luigi

from luigi.date_interval import Date

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available
from edx.analytics.tasks.url import url_path_join

log = logging.getLogger(__name__)


class InternalReportingCountryLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's country table."""

    DATE = '2014-07-01'

    def setUp(self):
        super(InternalReportingCountryLoadAcceptanceTest, self).setUp()
        self.upload_file(os.path.join(self.data_dir, 'input', 'internal_reporting_d_country'), url_path_join(self.warehouse_path, 'internal_reporting_d_country', 'dt=2014-07-01', 'internal_reporting_d_country'))

    @when_vertica_available
    def test_internal_reporting_country(self):
        """Tests the workflow for loading internal reporting country table."""

        self.task.launch([
            'LoadInternalReportingCountryToWarehouse',
            '--date', self.DATE,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--overwrite'
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_country.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=True)

            cursor.execute("SELECT * FROM {schema}.d_country".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            d_country = pandas.DataFrame(response, columns=['country_name', 'user_last_location_country_code'])

            try:  # A ValueError will be thrown if the column names don't match or the two data frames are not square.
                self.assertTrue(all(d_country == expected))
            except ValueError:
                self.fail("Expected and returned data frames have different shapes or labels.")
