import datetime

import luigi

from luigi import date_interval

from edx.analytics.tasks.database_imports import (
    DatabaseImportMixin, ImportCurrentOrderState, ImportInvoices, ImportShoppingCartInvoiceTransactions
)
from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin


DEFAULT_PAYMENT_GATEWAY_ID = '\\N'
DEFAULT_PAYMENT_GATEWAY_ACCOUNT_ID = '\\N'
DEFAULT_TRANSACTION_FEE = '\\N'
DEFAULT_PAYMENT_METHOD = 'invoice'
DEFAULT_PAYMENT_METHOD_TYPE = '\\N'


class InvoiceTask(luigi.WrapperTask):
    """
    Store invoice data from ShoppingCart and Otto in Hive tables.
    """
    import_date = luigi.DateParameter()

    def requires(self):
        yield (
            ShoppingCartInvoiceTransactionsTableTask(
                import_date=self.import_date
            ),
            OttoInvoiceTableTask(
                import_date=self.import_date
            )
        )

    def output(self):
        return [task.output() for task in self.requires()]


class ShoppingCartInvoiceTransactionsTableTask(DatabaseImportMixin, HiveTableFromQueryTask):
    """
    Store invoice data from ShoppingCart in Hive table.
    """
    def requires(self):
        kwargs = {
            'destination': self.destination,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
            'credentials': self.credentials,
            'database': self.database,
        }
        yield (
            # ShoppingCart Invoice Table
            ImportShoppingCartInvoiceTransactions(**kwargs),
        )

    @property
    def table(self):
        return 'invoicetransaction'

    @property
    def columns(self):
        return [
            ('date', 'TIMESTAMP'),
            ('payment_gateway_id', 'STRING'),
            ('payment_gateway_account_id', 'STRING'),
            ('payment_ref_id', 'INT'),
            ('iso_currency_code', 'STRING'),
            ('amount', 'DECIMAL'),
            ('transaction_fee', 'DECIMAL'),
            ('transaction_type', 'STRING'),
            ('payment_method', 'STRING'),
            ('payment_method_type', 'STRING'),
            ('transaction_id', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
                created AS date,
                "\\N" AS payment_gateway_id,
                "\\N" AS payment_gateway_account_id,
                invoice_id AS payment_ref_id,
                currency AS iso_currency_code,
                amount,
                "\\N" AS transaction_fee,
                CASE
                    WHEN amount >= 0 THEN "sale"
                    ELSE "refund"
                END AS transaction_type,
                "invoice" AS payment_method,
                "\\N" AS payment_method_type,
                id AS transaction_id
            FROM shoppingcart_invoicetransaction;
        """


class OttoInvoiceTableTask(DatabaseImportMixin, HiveTableFromQueryTask):
    """
    Store invoice data from Otto in Hive table.
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
            'overwrite': self.overwrite,
            'credentials': self.otto_credentials,
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
            ('date', 'TIMESTAMP'),
            ('payment_gateway_id', 'STRING'),
            ('payment_gateway_account_id', 'STRING'),
            ('payment_ref_id', 'INT'),
            ('iso_currency_code', 'STRING'),
            ('amount', 'DECIMAL'),
            ('transaction_fee', 'DECIMAL'),
            ('transaction_type', 'STRING'),
            ('payment_method', 'STRING'),
            ('payment_method_type', 'STRING'),
            ('transaction_id', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
                i.created AS date,
                "\\N" AS payment_gateway_id,
                "\\N" AS payment_gateway_account_id,
                o.id AS payment_ref_id,
                o.currency AS iso_currency_code,
                o.total_excl_tax AS amount,
                "\\N" AS transaction_fee,
                CASE
                    WHEN o.total_excl_tax >= 0 THEN "sale"
                    ELSE "refund"
                END AS transaction_type,
                "invoice" AS payment_method,
                "\\N" AS payment_method_type,
                i.id AS transaction_id
            FROM invoice_invoice i
            JOIN order_order o ON i.basket_id = o.basket_id;
        """


# ===


class InvoiceTransactionsTaskMixin(OverwriteOutputMixin):
    """The parameters needed to run the paypal reports."""

    output_root = luigi.Parameter(
        description='Folder to write invoice transaction data to.',
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='The date to generate a report for. Default is today, UTC.',
    )

    def extra_modules(self):
        """edx.analytics.tasks is required by all tasks that load this file."""
        import edx.analytics.tasks.mapreduce
        return [edx.analytics.tasks.mapreduce]


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
    n_reduce_tasks = luigi.Parameter(
        default=1,
        significant=False,
        description='Number of reducer tasks to use in upstream tasks.  Scale this to your cluster size.',
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
                n_reduce_tasks=self.n_reduce_tasks
            )
            yield OttoInvoiceTransactionsByDayTask(
                output_root=self.output_root,
                date=day,
                overwrite=self.overwrite,
            )

    def output(self):
        return [task.output() for task in self.requires()]


class ShoppingCartInvoiceTransactionsByDayTask(InvoiceTransactionsTaskMixin, MapReduceJobTask):
    """
    Creates TSV file with information about ShoppingCart invoices.
    """

    def requires(self):
        yield (
            ShoppingCartInvoiceTransactionsTask(
                import_date=self.date
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


class OttoInvoiceTransactionsByDayTask(InvoiceTransactionsTaskMixin, MapReduceJobTask):
    """
    Creates TSV file with information about Otto invoices.
    """

    def requires(self):
        yield (
            OttoInvoiceTransactionsTask(
                import_date=self.date
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


class ShoppingCartInvoiceTransactionsTask(DatabaseImportMixin, HiveTableFromQueryTask):
    def requires(self):
        kwargs = {
            'destination': self.destination,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'credentials': self.credentials,
            'overwrite': self.overwrite,
            'database': self.database,
        }
        yield (
            # ShoppingCart Invoice Table
            ImportShoppingCartInvoiceTransactions(**kwargs),
        )

    @property
    def table(self):
        return 'invoicetransaction'

    @property
    def columns(self):
        return [
            ('transaction_id', 'INT'),
            ('date_created', 'TIMESTAMP'),
            ('amount', 'DECIMAL'),
            ('currency', 'STRING'),
            ('status', 'STRING'),
            ('invoice_id', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        import_date = self.import_date.isoformat()
        return """
            SELECT
                id AS transaction_id,
                created AS date_created,
                amount,
                currency,
                status,
                invoice_id
            FROM shoppingcart_invoicetransaction
            WHERE TO_DATE(created) = TO_DATE('{import_date}');
        """.format(import_date=import_date)


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
        import_date = self.import_date.isoformat()
        return """
            SELECT
                i.id AS invoice_id,
                i.created AS date_created,
                i.state AS state,
                o.id AS order_id,
                o.currency AS currency,
                o.total_excl_tax AS amount
            FROM invoice_invoice i
            JOIN order_order o ON i.basket_id = o.basket_id
            WHERE TO_DATE(created) = TO_DATE('{import_date}');
        """.format(import_date=import_date)
