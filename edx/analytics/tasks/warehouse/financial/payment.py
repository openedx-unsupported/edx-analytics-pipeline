
import luigi

from edx.analytics.tasks.warehouse.financial.cybersource import IntervalPullFromCybersourceTask, CybersourceDataValidationTask
from edx.analytics.tasks.warehouse.financial.paypal import PaypalTransactionsIntervalTask, PaypalDataValidationTask


class PaymentTask(luigi.WrapperTask):

    import_date = luigi.DateParameter()
    cybersource_merchant_ids = luigi.Parameter(
        default_from_config={'section': 'payment', 'name': 'cybersource_merchant_ids'},
        is_list=True
    )

    def requires(self):
        yield PaypalTransactionsIntervalTask(
            interval_end=self.import_date
        )
        for merchant_id in self.cybersource_merchant_ids:
            yield IntervalPullFromCybersourceTask(
                interval_end=self.import_date,
                merchant_id=merchant_id
            )

    def output(self):
        return [task.output() for task in self.requires()]


class PaymentValidationTask(luigi.WrapperTask):
    """Task to validate existence of Payment data."""

    import_date = luigi.DateParameter()

    def requires(self):
        yield PaypalDataValidationTask(import_date=self.import_date)
        yield CybersourceDataValidationTask(import_date=self.import_date)
