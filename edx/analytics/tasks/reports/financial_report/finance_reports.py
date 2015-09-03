import luigi
import luigi.hdfs
import luigi.date_interval
import datetime
from edx.analytics.tasks.reports.reconcile import TransactionReportTask
from edx.analytics.tasks.reports.financial_report.ed_services_financial_report import BuildEdServicesReportTask
from edx.analytics.tasks.database_imports import DatabaseImportMixin


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

class BuildFinancialReportsTask(BuildFinancialReportsMixin, luigi.WrapperTask):
    def requires(self):
        # Ingest required data into HIVE needed to build the financial reports
        kwargs = {
            # 'output_root': self.output_root,
            'interval': self.interval,
        }
        yield (
            TransactionReportTask(),
            BuildEdServicesReportTask(),
        )