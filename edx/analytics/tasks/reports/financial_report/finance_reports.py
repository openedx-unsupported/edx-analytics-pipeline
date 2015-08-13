import luigi
import luigi.hdfs
import luigi.date_interval

from edx.analytics.tasks.reports.reconcile import TransactionReportTask
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask
from edx.analytics.tasks.database_imports import DatabaseImportMixin
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin


class BuildFinancialReportsTask(DatabaseImportMixin, MapReduceJobTaskMixin, luigi.WrapperTask):

    def requires(self):
        yield (
            BuildEdServicesReportTask(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
            ),
            TransactionReportTask(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
            )
        )
