
import luigi

from edx.analytics.tasks.reports.adyen import ADYEN_CONFIG_SECTION, AdyenTransactionsIntervalTask
from edx.analytics.tasks.reports.cybersource import IntervalPullFromCybersourceTask
from edx.analytics.tasks.reports.paypal import PaypalTransactionsIntervalTask


class PaymentTask(luigi.WrapperTask):

    import_date = luigi.DateParameter()
    adyen_company_account = luigi.Parameter(
        default_from_config={'section': ADYEN_CONFIG_SECTION, 'name': 'company_account'},
    )
    adyen_merchants_list = luigi.Parameter(
        default_from_config={'section': ADYEN_CONFIG_SECTION, 'name': 'merchant_accounts_list'},
        is_list=True
    )
    cybersource_merchant_ids = luigi.Parameter(
        default_from_config={'section': 'payment', 'name': 'cybersource_merchant_ids'},
        is_list=True
    )

    def requires(self):
        for merchant in self.adyen_merchants_list:
            yield AdyenTransactionsIntervalTask(
                interval_end=self.import_date,
                company_account=self.adyen_company_account,
                merchant_account=merchant,
            )
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
