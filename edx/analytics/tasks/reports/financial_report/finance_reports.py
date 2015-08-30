import luigi
import luigi.hdfs
import luigi.date_interval
import datetime
from edx.analytics.tasks.database_imports import DatabaseImportMixin
# from edx.analytics.tasks.reports.financial_report.finance_reports import BuildFinancialReportsMixin
from edx.analytics.tasks.reports.reconcile import ReconcileOrdersAndTransactionsDownstreamMixin
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask



class BuildFinancialReportsMixin(DatabaseImportMixin):

    output_root = luigi.Parameter(default_from_config={'section': 'database-export', 'name': 'output_root'})

    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BooleanParameter(default=True)

    destination = luigi.Parameter(
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
    # interval_end = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    num_mappers = luigi.Parameter(default=None)

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
        kwargs = {
            # 'num_mappers': self.num_mappers,
            # 'verbose': self.verbose,
            # 'destination': self.destination,
            # 'import_date': self.import_date,
            # 'interval_end': self.interval_end,
            'interval': self.interval,
        }
        return BuildEdServicesReportTask(**kwargs)