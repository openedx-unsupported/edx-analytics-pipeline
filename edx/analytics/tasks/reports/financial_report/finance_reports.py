import luigi
import logging
import luigi.hdfs
import luigi.date_interval
import datetime
from edx.analytics.tasks.reports.reconcile import ReconcileOrdersAndTransactionsDownstreamMixin, TransactionReportTask
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import (
    BuildEdServicesReportTask, ImportCourseAndEnrollmentTablesTask)
from edx.analytics.tasks.database_imports import (
    DatabaseImportMixin, ImportCourseModeTask, ImportStudentCourseEnrollmentTask
)
from edx.analytics.tasks.reports.reconcile import ReconciledOrderTransactionTableTask
from edx.analytics.tasks.reports.orders_import import OrderTableTask
from edx.analytics.tasks.reports.paypal import PaypalTransactionsByDayTask
from edx.analytics.tasks.reports.cybersource import IntervalPullFromCybersourceTask


class BuildFinancialReportsMixin(DatabaseImportMixin):
    #
    # # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    # overwrite = luigi.BooleanParameter(default=True)
    #
    start_date = luigi.DateParameter(
        default_from_config={'section': 'enrollments', 'name': 'interval_start'},
        significant=False,
    )

    output_root = luigi.Parameter(default_from_config={'section': 'payment-reconciliation', 'name': 'destination'})

    merchant_id = luigi.Parameter(default_from_config={'section': 'cybersource', 'name': 'merchant_id'})

    interval = luigi.DateIntervalParameter(
        default=luigi.date_interval.Custom.parse("2014-01-01-{}".format(
            datetime.datetime.utcnow().date().isoformat()
        ))
    )

class TransactionReport(BuildFinancialReportsMixin,luigi.WrapperTask):

    def requires(self):
        # Ingest required data into HIVE needed to build the financial reports
        yield (
            TransactionReportTask()
        )

    def output(self):
        return [task.output() for task in self.requires()]


class EdServicesReport(BuildFinancialReportsMixin, luigi.WrapperTask):

    def requires(self):
        # Ingest required data into HIVE needed to build the financial reports
        yield (
            BuildEdServicesReportTask(interval=self.interval),
        )

    def output(self):
        return [task.output() for task in self.requires()]




class BuildFinancialReportsTaskOrig(
    BuildFinancialReportsMixin,
    ReconcileOrdersAndTransactionsDownstreamMixin,
    luigi.WrapperTask):

    def requires(self):
        kwargs = {
            'interval': self.interval,
        }
        # Ingest required data into HIVE needed to build the financial reports
        yield (
            # Import Order data
            log.debug('Importing order data.'),
            # OrderTableTask(),

            # Import payment provider data: PayPal
            # log.debug('Importing PayPal data')
            # PaypalTransactionsByDayTask(
            #     start_date=self.start_date,
            #     output_root=self.output_root,
            #     **kwargs
            # ),
            #
            # # Import payment provider data: CyberSource - edx.org
            # # log.debug('Importing cybersource - edx.org data.')
            # IntervalPullFromCybersourceTask(
            #     merchant_id=self.merchant_id,
            #     output_root=self.output_root,
            #     **kwargs
            # ),

            # Import payment provider data: CyberSource - MIT Corp edX
            # IntervalPullFromCybersourceTask(
            #     merchant_id='mit_corp_edx',
            #     output_root=self.output_root,
            #     **kwargs
            # ),

            # Import Course Information: Mainly Course Mode & Suggested Prices
            log.debug('Importing course mode & suggested price data.'),
            ImportCourseModeTask(),

            # Import Student Enrollment Information
            log.debug('Importing student course enrollment data.'),
            ImportStudentCourseEnrollmentTask(),

            # Import Reconciled Orders and Transactions
            log.debug('Reconciling orders and transactions.'),
            ReconciledOrderTransactionTableTask(**kwargs),

            # log.debug('Building ed services report.'),
            # BuildEdServicesReportTask(**kwargs),

            # log.debug('Building transaction report.')
            # TransactionReportTask(**kwargs)
        )

    # def run(self):
    #     return BuildEdServicesReportTask(interval=self.interval)

    def output(self):
        return [task.output() for task in self.requires()], BuildEdServicesReportTask(interval=self.interval)