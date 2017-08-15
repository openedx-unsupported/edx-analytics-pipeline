
import luigi
from luigi import date_interval
from luigi.configuration import get_config

from edx.analytics.tasks.warehouse.financial.cybersource import IntervalPullFromCybersourceTask
from edx.analytics.tasks.warehouse.financial.paypal import PaypalTransactionsIntervalTask
from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from edx.analytics.tasks.util.hive import WarehouseMixin


class PaymentTaskMixin(object):
    import_date = luigi.DateParameter()
    cybersource_merchant_ids = luigi.Parameter(
        default_from_config={'section': 'payment', 'name': 'cybersource_merchant_ids'},
        is_list=True
    )


class PaymentTask(PaymentTaskMixin, luigi.WrapperTask):

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


class PaymentValidationTask(WarehouseMixin, PaymentTaskMixin, luigi.WrapperTask):
    """Task to validate existence of Payment data."""

    paypal_interval_start = luigi.DateParameter(
        default_from_config={'section': 'paypal', 'name': 'interval_start'},
        significant=False,
    )

    def requires(self):
        paypal_interval = date_interval.Custom(self.paypal_interval_start, self.import_date)

        for date in paypal_interval:
            url = url_path_join(self.warehouse_path, 'payments', 'dt=' + date.isoformat(), 'paypal.tsv')
            yield ExternalURL(url=url)

        config = get_config()
        for merchant_id in self.cybersource_merchant_ids:
            section_name = 'cybersource:' + merchant_id
            interval_start = luigi.DateParameter().parse(config.get(section_name, 'interval_start'))
            cybersource_interval = date_interval.Custom(interval_start, self.import_date)

            for date in cybersource_interval:
                filename = "cybersource_{}.tsv".format(merchant_id)
                url = url_path_join(self.warehouse_path, 'payments', 'dt=' + date.isoformat(), filename)
                yield ExternalURL(url=url)
