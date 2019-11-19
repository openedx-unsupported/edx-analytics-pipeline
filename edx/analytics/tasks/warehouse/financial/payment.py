
from __future__ import absolute_import

import luigi

from edx.analytics.tasks.warehouse.financial.cybersource import (
    CybersourceDataValidationTask, IntervalPullFromCybersourceTask
)
from edx.analytics.tasks.warehouse.financial.paypal import PaypalDataValidationTask, PaypalTransactionsIntervalTask


class PaymentTask(luigi.WrapperTask):

    import_date = luigi.DateParameter()
    cybersource_merchant_ids = luigi.ListParameter(
        config_path={'section': 'payment', 'name': 'cybersource_merchant_ids'},
    )
    is_empty_transaction_allowed = luigi.BoolParameter(
        default=False,
        description='Allow empty transactions from payment processors to be processsed, default is False.'
    )

    def requires(self):
        yield PaypalTransactionsIntervalTask(
            interval_end=self.import_date,
            is_empty_transaction_allowed=self.is_empty_transaction_allowed
        )
        for merchant_id in self.cybersource_merchant_ids:
            yield IntervalPullFromCybersourceTask(
                interval_end=self.import_date,
                merchant_id=merchant_id,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            )

    def output(self):
        return [task.output() for task in self.requires()]


class PaymentValidationTask(luigi.WrapperTask):
    """Task to validate existence of Payment data."""

    import_date = luigi.DateParameter()

    def requires(self):
        yield PaypalDataValidationTask(import_date=self.import_date)
        yield CybersourceDataValidationTask(import_date=self.import_date)
