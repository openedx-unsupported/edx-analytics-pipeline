import luigi
import luigi.hdfs
import luigi.date_interval

from edx.analytics.tasks.reports.reconcile import TransactionReportTask, LoadInternalReportingOrderTransactionsToWarehouse
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import LoadInternalReportingEdServicesReportToWarehouse
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTaskMixin


class BuildFinancialReportsTask(MapReduceJobTaskMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    # Instead of importing all of DatabaseImportMixin at this level, we just define
    # what we need and are willing to pass through.  That way the use of "credentials"
    # for the output of the report data is not conflicting.
    import_date = luigi.DateParameter()

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
                overwrite=True,
            ),
            LoadInternalReportingEdServicesReportToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=True,
            ),
        )
