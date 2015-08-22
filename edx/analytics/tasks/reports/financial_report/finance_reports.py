import luigi
import luigi.hdfs
import luigi.date_interval
import datetime
from edx.analytics.tasks.database_imports import DatabaseImportMixin
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.reports.reconcile import ReconcileOrdersAndTransactionsDownstreamMixin
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask


class BuildFinancialReportsMixin(DatabaseImportMixin):

    output_root = luigi.Parameter(default_from_config={'section': 'database-export', 'name': 'output_root'})

    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BooleanParameter(default=True)

    destination = luigi.DateParameter(
        default_from_config={'section': 'payment-reconciliation', 'name': 'destination'},
        significant=False,
    )
    order_source = luigi.Parameter(
        default_from_config={'section': 'payment-reconciliation', 'name': 'order_source'})
    transaction_source = luigi.Parameter(
        default_from_config={'section': 'payment-reconciliation', 'name': 'transaction_source'}
    )
    pattern = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'pattern'}
    )
    interval_start = luigi.DateParameter(
        default_from_config={'section': 'enrollments', 'name': 'interval_start'},
        significant=False,
    )
    interval_end = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    def __init__(self, *args, **kwargs):
        super(BuildFinancialReportsMixin, self).__init__(*args, **kwargs)

        if not self.interval:
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)


class BuildFinancialReportsTask(
    BuildFinancialReportsMixin,
    ReconcileOrdersAndTransactionsDownstreamMixin,
    luigi.WrapperTask):

    interval = luigi.DateIntervalParameter(default=None)

    def requires(self):
        kwargs = {
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'interval': self.interval,
            'destination': self.destination,
        }
        return BuildEdServicesReportTask(
            credentials=self.credentials,
            database=self.database,
            **kwargs
        )