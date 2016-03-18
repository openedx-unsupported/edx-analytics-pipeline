"""
End to end test of the financial reporting workflow.
"""

import logging
import os
from cStringIO import StringIO

import luigi
import pandas
from pandas.util.testing import assert_frame_equal, assert_series_equal

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_vertica_available, when_vertica_not_available
from edx.analytics.tasks.url import url_path_join, get_target_from_url
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

    @when_vertica_not_available
    def test_end_to_end_without_vertica(self):
        # Similar to test_end_to_end but it excludes the vertica part and it checks data values,
        # not just data shape.
        table_name = 'reconciled_order_transactions'
        output_root = url_path_join(
            self.warehouse_path, table_name, 'dt=' + self.UPPER_BOUND_DATE
        ) + '/'
        self.task.launch([
            'ReconcileOrdersAndTransactionsTask',
            '--import-date', self.UPPER_BOUND_DATE,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--output-root', output_root,
        ])
        final_output_task = LoadInternalReportingOrderTransactionsToWarehouse(
            import_date=luigi.DateParameter().parse(self.UPPER_BOUND_DATE)
        )
        columns = [x[0] for x in final_output_task.columns]
        target = get_target_from_url(output_root)
        files = target.fs.listdir(target.path)
        raw_output = ""
        for file_name in files:
            if file_name.startswith(target.path):
                file_name = file_name[len(target.path) + 1:]
            if file_name[0] != '_':
                raw_output += get_target_from_url(url_path_join(output_root, file_name)).open('r').read()

        expected_output_csv = os.path.join(self.data_dir, 'output', 'expected_financial_report.csv')
        expected = pandas.read_csv(expected_output_csv, parse_dates=True)

        output = StringIO(raw_output.replace('\t\\N', '\t'))
        data = pandas.read_table(output, header=None, names=columns, parse_dates=True)
        # Re-order dataframe for consistent comparison:
        for frame in (data, expected):
            frame.sort(['payment_ref_id', 'transaction_type'], inplace=True, ascending=[True, False])
            frame.reset_index(drop=True, inplace=True)
        try:
            assert_frame_equal(data, expected)
        except AssertionError:
            pandas.set_option('display.max_columns', None)
            print('----- The report generated this data: -----')
            print(data)
            print('----- vs expected: -----')
            print(expected)
            if data.shape != expected.shape:
                print("Data shapes differ.")
            else:
                for index, series in data.iterrows():
                    # Try to print a more helpful/localized difference message:
                    try:
                        assert_series_equal(data.iloc[index, :], expected.iloc[index, :])
                    except AssertionError:
                        print("First differing row: {index}".format(index=index))
            raise
