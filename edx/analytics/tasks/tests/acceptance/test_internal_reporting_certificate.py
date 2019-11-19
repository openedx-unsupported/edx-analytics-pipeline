"""
End to end test of the d_user_course_certificates table loading task.
"""

from __future__ import absolute_import

import logging
import os

import pandas
from six.moves import map

from edx.analytics.tasks.tests.acceptance import (
    AcceptanceTestCase, coerce_columns_to_string, read_csv_fixture_as_list, when_vertica_available
)

log = logging.getLogger(__name__)


class InternalReportingCertificateLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the internal reporting warehouse's certificate table."""

    DATE = '2014-07-01'

    def setUp(self):
        super(InternalReportingCertificateLoadAcceptanceTest, self).setUp()
        self.execute_sql_fixture_file('load_certificates_generatedcertificate.sql')

    @when_vertica_available
    def test_internal_reporting_certificate(self):
        self.task.launch([
            'LoadInternalReportingCertificatesToWarehouse',
            '--date', self.DATE,
            '--overwrite'
        ])

        self.validate_output()

    def validate_output(self):
        """Validates the output, comparing it to a csv of all the expected output from this workflow."""

        columns = ['user_id', 'course_id', 'is_certified', 'certificate_mode', 'final_grade', 'has_passed',
                   'created_date', 'modified_date']

        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'acceptance_expected_d_user_course_certificate.csv')

            expected_output_data = read_csv_fixture_as_list(expected_output_csv)
            expected = pandas.DataFrame(expected_output_data, columns=columns)

            cursor.execute("SELECT * FROM {schema}.d_user_course_certificate".format(schema=self.vertica.schema_name))
            response = cursor.fetchall()
            d_user_course_certificate = pandas.DataFrame(list(map(coerce_columns_to_string, response)), columns=columns)

            for frame in (d_user_course_certificate, expected):
                frame.sort(['user_id'], inplace=True, ascending=[True])
                frame.reset_index(drop=True, inplace=True)

            self.assert_data_frames_equal(d_user_course_certificate, expected)
