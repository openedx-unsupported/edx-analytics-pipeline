import luigi
import luigi.hdfs
import luigi.date_interval
import datetime
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin

# from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
# from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, HivePartition
from edx.analytics.tasks.reports.reconcile import ReconcileOrdersAndTransactionsDownstreamMixin
# from edx.analytics.tasks.database_imports import (
# DatabaseImportMixin, ImportStudentCourseEnrollmentTask, ImportCourseModeTask
# )

class BuildFinancialReportsMixin(MapReduceJobTaskMixin):
    database = luigi.Parameter(default_from_config={'section': 'database-export', 'name': 'database'})
    credentials = luigi.Parameter(default_from_config={'section': 'database-export', 'name': 'credentials'})
    destination = luigi.Parameter(default_from_config={'section': 'database-export', 'name': 'destination'})
    output_root = luigi.Parameter(default_from_config={'section': 'database-export', 'name': 'output_root'})
    verbose = luigi.BooleanParameter(default=False)
    num_mappers = luigi.Parameter(default=None)

    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BooleanParameter(default=True)

    order_source = luigi.Parameter(default_from_config={'section': 'payment-reconciliation', 'name': 'order_source'})
    transaction_source = luigi.Parameter(
        default_from_config={'section': 'payment-reconciliation', 'name': 'transaction_source'}
    )

    import_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    start_date = luigi.DateParameter(default="2014-01-01")
    end_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    pattern = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'pattern'}
    )


class BuildFinancialReportsTask(
    BuildFinancialReportsMixin,
    ReconcileOrdersAndTransactionsDownstreamMixin,
    luigi.WrapperTask):

    @property
    def requires(self):

        format_interval = str(self.start_date) + "-" + str(self.end_date)

        print "FORMATTTTTT INTERVAL:", format_interval

        interval = luigi.DateIntervalParameter(
            default=luigi.date_interval.Custom.parse(format_interval)
            )

        kwargs = {
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            #'mapreduce_engine': self.mapreduce_engine,
            #'n_reduce_tasks': self.n_reduce_tasks,
            'interval': interval,
            # 'pattern': self.pattern,
        }

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


        # yield (
        #     BuildEdServicesReportTask(
        #         destination=self.destination,
        #         credentials=self.credentials,
        #         database=self.database,
        #         **kwargs
        #     ),
        # )

        return BuildEdServicesReportTask(
            destination=self.destination,
            credentials=self.credentials,
            database=self.database,
            **kwargs
        )
