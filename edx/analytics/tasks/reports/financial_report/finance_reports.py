# import datetime
import luigi
import luigi.hdfs
import luigi.date_interval

from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask

# from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
# from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, HivePartition
# from edx.analytics.tasks.reports.reconcile import ReconciledOrderTransactionTableTask
# from edx.analytics.tasks.database_imports import (
#     DatabaseImportMixin, ImportStudentCourseEnrollmentTask, ImportCourseModeTask
# )

class BuildFinancialReportsTask(luigi.WrapperTask):

    def requires(self):
        # kwargs = {
        #     'num_mappers': self.num_mappers,
        #     'verbose': self.verbose,
        #     'import_date': self.import_date,
        #     'overwrite': self.overwrite,
        #     'mapreduce_engine': self.mapreduce_engine,
        #     'n_reduce_tasks': self.n_reduce_tasks,
        #     'transaction_source': self.transaction_source,
        #     'order_source': self.order_source,
        #     'interval': self.interval,
        #     'pattern': self.pattern,
        #     'output_root': self.partition_location,
        # }

    # Transaction Report Requires
    # def requires(self):
    #     return ReconcileOrdersAndTransactionsTask(
    #         mapreduce_engine=self.mapreduce_engine,
    #         n_reduce_tasks=self.n_reduce_tasks,
    #         transaction_source=self.transaction_source,
    #         order_source=self.order_source,
    #         interval=self.interval,
    #         pattern=self.pattern,
    #         output_root=self.partition_location,
    #         # overwrite=self.overwrite,
    #     )

        kwargs = {
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
            'database': self.database,
            'credentials': self.credentials,
        }
        yield (
            BuildEdServicesReportTask(
                destination=self.destination,
                credentials=self.credentials,
                database=self.database,
                **kwargs
            ),
        )