"""Provide entry-point for generating finance reports."""
from __future__ import absolute_import

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.common.vertica_load import VerticaCopyTaskMixin
from edx.analytics.tasks.warehouse.financial.ed_services_financial_report import (
    LoadInternalReportingEdServicesReportToWarehouse
)
from edx.analytics.tasks.warehouse.financial.reconcile import (
    LoadInternalReportingFullOrderTransactionsToWarehouse, LoadInternalReportingFullOttoOrdersToWarehouse,
    LoadInternalReportingFullShoppingcartOrdersToWarehouse, LoadInternalReportingOrderTransactionsToWarehouse,
    TransactionReportTask
)


class BuildFinancialReportsTask(MapReduceJobTaskMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Provide entry-point for generating finance reports."""

    # Instead of importing all of DatabaseImportMixin at this level, we just define
    # what we need and are willing to pass through.  That way the use of "credentials"
    # for the output of the report data is not conflicting.
    import_date = luigi.DateParameter()

    # Redefine the overwrite parameter to change its default to True.
    # This will cause the reports to reload when loading into internal reporting.
    overwrite = luigi.BoolParameter(default=True)

    is_empty_transaction_allowed = luigi.BoolParameter(
        default=False,
        description='Allow empty transactions from payment processors to be processsed, default is False.'
    )

    def requires(self):
        yield (
            TransactionReportTask(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
            ),
            LoadInternalReportingOrderTransactionsToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=self.overwrite,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            ),
            LoadInternalReportingEdServicesReportToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=self.overwrite,
            ),
            # The following task performs transaction reconciliation on a more complete order record.
            # Rather than hunt down all the places where the current table is being used, we instead
            # output a separate one.
            LoadInternalReportingFullOrderTransactionsToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=self.overwrite,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            ),
            # The following tasks output the order tables that were used in the above reconciliation.
            LoadInternalReportingFullShoppingcartOrdersToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=self.overwrite,
            ),
            LoadInternalReportingFullOttoOrdersToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=self.overwrite,
            ),
        )
