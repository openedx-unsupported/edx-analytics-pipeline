import luigi
import luigi.hdfs
import luigi.date_interval
import datetime
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.reports.reconcile import ReconcileOrdersAndTransactionsDownstreamMixin


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
    interval_start = luigi.DateParameter(default="2014-01-01")
    interval_end = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    pattern = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'pattern'}
    )

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
        }
        return BuildEdServicesReportTask(
            interval=self.interval,
            destination=self.destination,
            credentials=self.credentials,
            database=self.database,
            **kwargs
        )