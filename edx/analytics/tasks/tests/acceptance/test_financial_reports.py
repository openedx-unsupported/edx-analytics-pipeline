"""
End to end test of the financial reporting workflow.
"""

import logging
import os
from cStringIO import StringIO

import luigi
import pandas

from edx.analytics.tasks.tests.acceptance import (
    AcceptanceTestCase, coerce_columns_to_string, read_csv_fixture_as_list, when_vertica_available,
    when_vertica_not_available
)
from edx.analytics.tasks.util.url import url_path_join
from edx.analytics.tasks.warehouse.financial.reconcile import LoadInternalReportingOrderTransactionsToWarehouse

log = logging.getLogger(__name__)


class FinancialReportsAcceptanceTest(AcceptanceTestCase):

    IMPORT_DATE = '2015-09-01'
    UPPER_BOUND_DATE = '2015-09-02'

    def setUp(self):
        super(FinancialReportsAcceptanceTest, self).setUp()

        if not self.should_reset_state:
            return

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

        final_output_task = LoadInternalReportingOrderTransactionsToWarehouse(
            import_date=luigi.DateParameter().parse(self.UPPER_BOUND_DATE)
        )
        columns = [x[0] for x in final_output_task.columns]

        with self.vertica.cursor() as cursor:
            expected_output_csv = os.path.join(self.data_dir, 'output', 'expected_financial_report.csv')

            expected_output_data = read_csv_fixture_as_list(expected_output_csv)

            expected = pandas.DataFrame(expected_output_data, columns=columns)

            cursor.execute("SELECT {columns} FROM {schema}.f_orderitem_transactions".format(
                columns=','.join(columns),
                schema=self.vertica.schema_name
            ))
            response = cursor.fetchall()

            f_orderitem_transactions = pandas.DataFrame(map(coerce_columns_to_string, response), columns=columns)

            for frame in (f_orderitem_transactions, expected):
                frame.sort(['payment_ref_id', 'transaction_type'], inplace=True, ascending=[True, False])
                frame.reset_index(drop=True, inplace=True)

            self.assert_data_frames_equal(f_orderitem_transactions, expected)

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

        expected_output_csv = os.path.join(self.data_dir, 'output', 'expected_financial_report.csv')
        expected = pandas.read_csv(expected_output_csv, parse_dates=True)

        raw_output = self.read_dfs_directory(output_root)
        output = StringIO(raw_output.replace('\t\\N', '\t'))
        data = pandas.read_table(output, header=None, names=columns, parse_dates=True)
        # Re-order dataframe for consistent comparison:
        for frame in (data, expected):
            frame.sort(['payment_ref_id', 'transaction_type'], inplace=True, ascending=[True, False])
            frame.reset_index(drop=True, inplace=True)

        self.assert_data_frames_equal(data, expected)
