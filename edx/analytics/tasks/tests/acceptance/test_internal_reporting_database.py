"""
End to end test for importing mysql database into Vertica task.
"""

import datetime
import logging
import os

import pandas
from pandas.util.testing import assert_frame_equal

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, as_list_param, when_vertica_available

log = logging.getLogger(__name__)


class DatabaseImportAcceptanceTest(AcceptanceTestCase):
    """Validate workflow to load mysql database into Vertica."""

    DATE = '2014-07-01'

    def setUp(self):
        super(DatabaseImportAcceptanceTest, self).setUp()
        self.execute_sql_fixture_file('load_database_import_test_table.sql')

    @when_vertica_available
    def test_database_import(self):
        self.task.launch([
            'ImportMysqlToVerticaTask',
            '--date', self.DATE,
            '--marker-schema', 'acceptance_marker',
            '--exclude-field', as_list_param('.*\\.field_to_exclude$'),
            '--overwrite',
        ])

        self.validate_output()

    def validate_output(self):
        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(
                self.data_dir,
                'output',
                'database_import',
                'expected_database_import_test_table.csv'
            )

            def convert_date(date_string):
                """Convert date string to a date object."""
                return datetime.datetime.strptime(date_string, '%Y-%m-%d').date()

            expected = pandas.read_csv(expected_output_csv, parse_dates=[6, 7], converters={9: convert_date})

            cursor.execute(
                "SELECT * FROM {schema}.database_import_test_table".format(schema=self.vertica.schema_name)
            )
            response = cursor.fetchall()
            database_import_test_table = pandas.DataFrame(response, columns=list(expected.columns))

            assert_frame_equal(database_import_test_table, expected, check_dtype=False)
