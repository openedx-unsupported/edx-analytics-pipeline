import luigi
import luigi.hdfs
import luigi.date_interval

from edx.analytics.tasks.reports.reconcile import TransactionReportTask
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask
from edx.analytics.tasks.database_imports import DatabaseImportMixin


class BuildFinancialReportsTask(DatabaseImportMixin, luigi.WrapperTask):

    def requires(self):
        yield (
            BuildEdServicesReportTask(
                import_date=self.import_date,
            ),
            TransactionReportTask(
                import_date=self.import_date,
            )
        )
