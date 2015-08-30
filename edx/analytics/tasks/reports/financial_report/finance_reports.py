import luigi
import luigi.hdfs
import luigi.date_interval
import datetime
from edx.analytics.tasks.reports.reconcile import ReconcileOrdersAndTransactionsDownstreamMixin
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask
from edx.analytics.tasks.database_imports import (
    DatabaseImportMixin, ImportCourseModeTask, ImportStudentCourseEnrollmentTask
)
from edx.analytics.tasks.reports.reconcile import ReconciledOrderTransactionTableTask


class BuildFinancialReportsMixin(DatabaseImportMixin):

    interval = luigi.DateIntervalParameter(
        default=luigi.date_interval.Custom.parse("2014-01-01-{}".format(
            datetime.datetime.utcnow().date().isoformat()
        ))
    )

class BuildFinancialReportsTask(
    BuildFinancialReportsMixin,
    ReconcileOrdersAndTransactionsDownstreamMixin,
    luigi.WrapperTask):

    def requires(self):
        yield (
            # Import Course Information: Mainly Course Mode & Suggested Prices
            ImportCourseModeTask(),
            # Import Student Enrollment Information
            ImportStudentCourseEnrollmentTask(),
            # Import Reconciled Orders and Transactions
            ReconciledOrderTransactionTableTask(),
        )

    def output(self):
        return [task.output() for task in self.requires()]

    def requires(self):
        kwargs = {
            'interval': self.interval,
        }
        return BuildEdServicesReportTask(**kwargs)