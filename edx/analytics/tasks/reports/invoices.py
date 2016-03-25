import datetime

import luigi

from luigi import date_interval

from edx.analytics.tasks.database_imports import (
    DatabaseImportMixin, ImportCurrentOrderState, ImportInvoices, ImportShoppingCartInvoiceTransactions
)
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin


DEFAULT_PAYMENT_GATEWAY_ID = '\\N'
DEFAULT_PAYMENT_GATEWAY_ACCOUNT_ID = '\\N'
DEFAULT_TRANSACTION_FEE = '\\N'
DEFAULT_PAYMENT_METHOD = 'invoice'
DEFAULT_PAYMENT_METHOD_TYPE = '\\N'


class InvoiceTransactionsTaskMixin(OverwriteOutputMixin):
    """The parameters needed to run the paypal reports."""

    output_root = luigi.Parameter(
        description='Folder to write invoice transaction data to.',
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='The date to generate a report for. Default is today, UTC.',
    )


class InvoiceTransactionsIntervalTask(InvoiceTransactionsTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """
    Creates TSV files with information about invoice transactions for a specified interval.
    """

    date = None
    interval = luigi.DateIntervalParameter(default=None)
    interval_start = luigi.DateParameter(
        default_from_config={'section': 'invoices', 'name': 'interval_start'},
        significant=False,
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='Default is today, UTC.',
    )
    output_root = luigi.Parameter(
        default=None,
        description='Folder to write invoice transaction data to.',
    )

    def __init__(self, *args, **kwargs):
        super(InvoiceTransactionsIntervalTask, self).__init__(*args, **kwargs)
        # Provide default for output_root at this level.
        if self.output_root is None:
            self.output_root = self.warehouse_path

        if self.interval is None:
            self.interval = date_interval.Custom(self.interval_start, self.interval_end)

    def requires(self):
        for day in self.interval:
            yield ShoppingCartInvoiceTransactionsByDayTask(
                output_root=self.output_root,
                date=day,
                overwrite=self.overwrite,
            )
            yield OttoInvoiceTransactionsByDayTask(
                output_root=self.output_root,
                date=day,
                overwrite=self.overwrite,
            )

    def output(self):
        return [task.output() for task in self.requires()]


class ShoppingCartInvoiceTransactionsByDayTask(InvoiceTransactionsTaskMixin, MapReduceJobTaskMixin, MapReduceJobTask):
    """
    Creates TSV file with information about ShoppingCart invoices.
    """

    def requires(self):
        yield (
            ImportShoppingCartInvoiceTransactions(
                import_date=self.import_date
            )
        )

    def mapper(self, line):
        transaction_id, date_created, amount, currency, status, invoice_id = line.split('\t')
        transaction_type = 'sale' if amount >= 0 else 'refund'
        transaction_record = [
            date_created,
            DEFAULT_PAYMENT_GATEWAY_ID,
            DEFAULT_PAYMENT_GATEWAY_ACCOUNT_ID,
            invoice_id,
            currency,
            amount,
            DEFAULT_TRANSACTION_FEE,
            transaction_type,
            DEFAULT_PAYMENT_METHOD,
            DEFAULT_PAYMENT_METHOD_TYPE,
            transaction_id,
        ]
        yield transaction_id, transaction_record

    def reducer(self, _key, values):
        for transaction_record in values:
            with self.output().open('w') as output_tsv_file:
                output_tsv_file.write('\t'.join(transaction_record) + '\n')

    def output(self):
        return get_target_from_url(
            url_path_join(self.output_root, 'payments', 'dt=' + self.date.isoformat(), 'shoppingcart_invoices.tsv')
        )


class OttoInvoiceTransactionsByDayTask(InvoiceTransactionsTaskMixin, MapReduceJobTaskMixin, MapReduceJobTask):
    """
    Creates TSV file with information about Otto invoices.
    """

    def requires(self):
        yield (
            OttoInvoiceTransactionsTask(
                import_date=self.import_date
            )
        )

    def mapper(self, line):
        invoice_id, date_created, state, order_id, currency, amount = line.split('\t')
        transaction_type = 'sale' if amount >= 0 else 'refund'
        invoice_record = [
            date_created,
            DEFAULT_PAYMENT_GATEWAY_ID,
            DEFAULT_PAYMENT_GATEWAY_ACCOUNT_ID,
            order_id,
            currency,
            amount,
            DEFAULT_TRANSACTION_FEE,
            transaction_type,
            DEFAULT_PAYMENT_METHOD,
            DEFAULT_PAYMENT_METHOD_TYPE,
            invoice_id,
        ]
        yield invoice_id, invoice_record

    def reducer(self, _key, values):
        for invoice_record in values:
            with self.output().open('w') as output_tsv_file:
                output_tsv_file.write('\t'.join(invoice_record) + '\n')

    def output(self):
        return get_target_from_url(
            url_path_join(self.output_root, 'payments', 'dt=' + self.date.isoformat(), 'otto_invoices.tsv')
        )


class OttoInvoiceTransactionsTask(DatabaseImportMixin, HiveTableFromQueryTask):
    """
    Adds information about currency and amount charged to Otto invoices.
    """

    otto_credentials = luigi.Parameter(
        default_from_config={'section': 'otto-database-import', 'name': 'credentials'}
    )
    otto_database = luigi.Parameter(
        default_from_config={'section': 'otto-database-import', 'name': 'database'}
    )

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'credentials': self.otto_credentials,
            'overwrite': self.overwrite,
            'database': self.otto_database,
        }
        yield (
            # Otto Invoice Table
            ImportInvoices(**kwargs),
            # Otto Current State Table
            ImportCurrentOrderState(**kwargs),
        )

    @property
    def table(self):
        return 'invoice'

    @property
    def columns(self):
        return [
            ('invoice_id', 'INT'),
            ('date_created', 'TIMESTAMP'),
            ('state', 'STRING'),
            ('order_id', 'INT'),
            ('currency', 'STRING'),
            ('amount', 'DECIMAL'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
                i.id AS invoice_id,
                i.created AS date_created,
                i.state AS state,
                o.id AS order_id,
                o.currency AS currency,
                o.total_excl_tax AS amount
            FROM invoice_invoice i
            JOIN order_order o ON i.basket_id = o.basket_id;
        """
