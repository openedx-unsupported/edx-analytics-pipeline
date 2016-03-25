
import luigi

from edx.analytics.tasks.reports.cybersource import IntervalPullFromCybersourceTask
from edx.analytics.tasks.reports.invoices import InvoiceTransactionsIntervalTask
from edx.analytics.tasks.reports.paypal import PaypalTransactionsIntervalTask


class PaymentTask(luigi.WrapperTask):

    import_date = luigi.DateParameter()
    cybersource_merchant_ids = luigi.Parameter(
        default_from_config={'section': 'payment', 'name': 'cybersource_merchant_ids'},
        is_list=True
    )

    def requires(self):
        # Temporarily disable existing tasks to be able to run PaymentTask
        # yield PaypalTransactionsIntervalTask(
        #     interval_end=self.import_date
        # )
        # for merchant_id in self.cybersource_merchant_ids:
        #     yield IntervalPullFromCybersourceTask(
        #         interval_end=self.import_date,
        #         merchant_id=merchant_id
        #     )
        yield InvoiceTransactionsIntervalTask(
            interval_end=self.import_date
        )

    def output(self):
        return [task.output() for task in self.requires()]
