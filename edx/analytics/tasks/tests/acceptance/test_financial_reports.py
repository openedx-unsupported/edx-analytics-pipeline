"""
End to end test of the financial reporting workflow.
"""

import logging
import os
from cStringIO import StringIO

import pandas

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.reports.reconcile import LoadInternalReportingOrderTransactionsToWarehouse

log = logging.getLogger(__name__)


class FinancialReportsAcceptanceTest(AcceptanceTestCase):

    IMPORT_DATE = '2015-09-01'
    UPPER_BOUND_DATE = '2015-09-02'

    def setUp(self):
        super(FinancialReportsAcceptanceTest, self).setUp()

        for input_file_name in ('paypal.tsv', 'cybersource_test.tsv'):
            src = url_path_join(self.data_dir, 'input', input_file_name)
            dst = url_path_join(self.warehouse_path, "payments", "dt=" + self.IMPORT_DATE, input_file_name)
            self.upload_file(src, dst)

        empty_file_path = url_path_join(
            self.warehouse_path, "payments", "dt=" + self.IMPORT_DATE, 'cybersource_empty_test.tsv')
        self.upload_file_with_content(empty_file_path, '')

        self.prepare_database('lms', self.import_db)
        self.prepare_database('otto', self.otto_db)

    def prepare_database(self, name, database):
        sql_fixture_base_url = url_path_join(self.data_dir, 'input', 'finance_reports', name)
        for filename in os.listdir(sql_fixture_base_url):
            self.execute_sql_fixture_file(url_path_join(sql_fixture_base_url, filename), database=database)

    @when_vertica_available
    def test_end_to_end(self):
        self.task.launch([
            'BuildFinancialReportsTask',
            '--import-date', self.UPPER_BOUND_DATE,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        final_output_task = LoadInternalReportingOrderTransactionsToWarehouse(import_date=self.UPPER_BOUND_DATE)
        columns = [x[0] for x in final_output_task.columns]

        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'expected_financial_report.csv')
            expected = pandas.read_csv(expected_output_csv, parse_dates=True)

            cursor.execute("SELECT {columns} FROM {schema}.f_orderitem_transactions".format(
                columns=','.join(columns),
                schema=self.vertica.schema_name
            ))
            response = cursor.fetchall()
            f_orderitem_transactions = pandas.DataFrame(response, columns=columns)

            try:  # A ValueError will be thrown if the column names don't match or the two data frames are not square.
                self.assertTrue(all(f_orderitem_transactions == expected))
            except ValueError:
                buf = StringIO()
                f_orderitem_transactions.to_csv(buf)
                print 'Actual:'
                print buf.getvalue()
                buf.seek(0)
                expected.to_csv(buf)
                print 'Expected:'
                print buf.getvalue()
                self.fail("Expected and returned data frames have different shapes or labels.")
