import luigi

from edx.analytics.tasks.database_imports import (
    DatabaseImportMixin, ImportCurrentOrderState, ImportInvoices
)
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, HivePartition


class InvoiceTask(luigi.WrapperTask):
    """
    Store invoice data from Otto in Hive table.
    """
    import_date = luigi.DateParameter()

    def requires(self):
        yield OttoInvoiceTableTask(
            import_date=self.import_date
        )

    def output(self):
        return [task.output() for task in self.requires()]


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
            ('date', 'STRING'),
            ('payment_gateway_id', 'STRING'),
            ('payment_gateway_account_id', 'STRING'),
            ('payment_ref_id', 'STRING'),
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
                TO_DATE(i.created) AS date,
                "otto_invoice" AS payment_gateway_id,
                "\\\\N" AS payment_gateway_account_id,
                o.number AS payment_ref_id,
                o.currency AS iso_currency_code,
                o.total_incl_tax AS amount,
                "\\N" AS transaction_fee,
                CASE
                    WHEN o.total_incl_tax >= 0 THEN "sale"
                    ELSE "refund"
                END AS transaction_type,
                "invoice" AS payment_method,
                "\\\\N" AS payment_method_type,
                i.id AS transaction_id
            FROM invoice_invoice i
            JOIN order_order o ON i.order_id = o.id;
        """
