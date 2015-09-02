import datetime
import logging
import luigi
import paypalrestsdk
import json

from luigi.date_interval import DateInterval

from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

# Tell urllib3 to switch the ssl backend to PyOpenSSL.
# see https://urllib3.readthedocs.org/en/latest/security.html#pyopenssl
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

log = logging.getLogger(__name__)


class PaypalTaskMixin(OverwriteOutputMixin):

    # start_date = luigi.DateParameter(
    #     default_from_config={'section': 'paypal', 'name': 'start_date'}
    # )
    client_mode = luigi.Parameter(
        default_from_config={'section': 'paypal', 'name': 'client_mode'}
    )
    client_id = luigi.Parameter(
        default_from_config={'section': 'paypal', 'name': 'client_id'},
        significant=False,
    )
    # Making this 'insignificant' means it won't be echoed in log files.
    client_secret = luigi.Parameter(
        default_from_config={'section': 'paypal', 'name': 'client_secret'},
        significant=False,
    )
    run_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    overwrite = luigi.BooleanParameter(default=True)

    interval = luigi.DateIntervalParameter()

    output_root = luigi.Parameter()

    import_date = luigi.DateParameter()


class RawPaypalTransactionLogTask(PaypalTaskMixin, luigi.Task):
    """
    A task that reads out of a remote Paypal account and writes to a file in raw JSON lines format.
    """

    start_date = luigi.DateParameter(
        default_from_config={'section': 'paypal', 'name': 'start_date'}
    )

    def initialize(self):
        log.debug('Initializing paypalrestsdk')
        paypalrestsdk.configure({
            'mode': self.client_mode,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        })

    def run(self):
        self.initialize()
        self.remove_output_on_overwrite()

        end_date = self.run_date + datetime.timedelta(days=1)
        request_args = {
            'start_time': "{}T00:00:00Z".format(self.run_date.isoformat()),  # pylint: disable=no-member
            'end_time': "{}T00:00:00Z".format(end_date.isoformat()),
            'count': 20
        }

        log.debug('Requesting payments')

        # Paypal REST payments API: https://developer.paypal.com/docs/integration/direct/rest-payments-overview/
        # API Reference: https://developer.paypal.com/docs/api/
        payment_history = paypalrestsdk.Payment.all(request_args)
        with self.output().open('w') as output_file:
            if payment_history.payments is None:
                log.error('No payments found')
            else:
                self.write_payments_from_response(payment_history, output_file)

            while payment_history.next_id is not None:
                request_args['start_id'] = payment_history.next_id
                payment_history = paypalrestsdk.Payment.all(request_args)
                self.write_payments_from_response(payment_history, output_file)

    def write_payments_from_response(self, payment_history, output_file):
        log.debug('Received %d payments from paypal', payment_history.count)
        if payment_history.count != len(payment_history.payments):
            log.error('Invalid response, payments do not match count: count=%d, len(payments)=%d', payment_history.count, len(payment_history.payments))

        for payment in payment_history.payments:
            output_file.write(json.dumps(payment.to_dict()))
            output_file.write('\n')

    def requires(self):
        pass

    def output(self):
        url_with_filename = url_path_join(self.output_root, 'paypal', '{0}.json'.format(self.run_date.isoformat()))
        return get_target_from_url(url_with_filename)


class PaypalTransactionsByDayTask(PaypalTaskMixin, luigi.Task):

    start_date = luigi.DateParameter(
        default_from_config={'section': 'paypal', 'name': 'start_date'}
    )
    marker = luigi.Parameter(
        default_from_config={'section': 'map-reduce', 'name': 'marker'},
        significant=False
    )

    def run(self):

        output_files = {}

        # interval_start_date_string = start_date
        interval_start_date_string = self.start_date.isoformat()
        interval_end_date_string = self.import_date.isoformat()

        for input_target in self.input():
            with input_target.open('r') as input_file:
                for line in input_file:
                    payment_record = json.loads(line)
                    for trans_with_items in payment_record.get('transactions', []):
                        for transaction in trans_with_items.get('related_resources', []):
                            trans_type = transaction.keys()[0]
                            details = transaction[trans_type]
                            created_date_string = details['create_time'].split('T')[0]

                            if created_date_string < interval_start_date_string or created_date_string >= interval_end_date_string:
                                continue

                            output_file = output_files.get(created_date_string)
                            if not output_file:
                                target = get_target_from_url(
                                    url_path_join(
                                        self.output_root, 'payments', 'dt=' + created_date_string, 'paypal-{}.tsv'.format(
                                            self.client_mode
                                        )
                                    )
                                )
                                output_file = target.open('w')
                                output_files[created_date_string] = output_file

                            payment_method = payment_record['payer']['payment_method']
                            if payment_method == 'paypal':
                                # The terminology doesn't match up here. We want the method represent the fact that it's a
                                # transfer instead of a credit card transaction, with the type of transfer being paypal.
                                # One could imagine other types of payment (google wallet for example) that would also be
                                # transfers, but would have a different payment type (google_wallet).
                                payment_method = 'instant_transfer'
                                payment_method_type = 'paypal'
                            else:
                                raise ValueError('Unhandled payment method "{}", update this mapping to assign proper'
                                                 ' payment method and payment method type values.'.format(payment_method))
                            record = [
                                # date
                                created_date_string,
                                # payment gateway
                                'paypal',
                                # payment gateway account ID
                                self.client_mode + '-' + self.client_id,
                                # payment reference number, this is used to join with orders
                                trans_with_items.get('invoice_number', '\\N'),
                                # currency
                                details['amount']['currency'],
                                # amount
                                details['amount']['total'],
                                # transaction fee
                                details.get('transaction_fee', {}).get('value', '\\N'),
                                # transaction type - either "sale" or "refund"
                                trans_type,
                                # payment method - currently this is only "instant_transfer"
                                payment_method,
                                # payment method type - currently this is only ever "paypal"
                                payment_method_type,
                                # identifier for the transaction
                                details['id'],
                            ]
                            output_file.write('\t'.join(record) + '\n')

        for output_file in output_files.values():
            output_file.close()

        with self.output().open('w') as output_file:
            output_file.write('OK')

    def requires(self):
        for run_date in DateInterval(self.start_date, self.import_date):
            yield RawPaypalTransactionLogTask(
                output_root=self.output_root,
                start_date=self.start_date,
                client_mode=self.client_mode,
                client_id=self.client_id,
                client_secret=self.client_secret,
                run_date=run_date,
                overwrite=self.overwrite,
            )

    def output(self):
        marker_url = url_path_join(self.marker, str(hash(self)))
        return get_target_from_url(marker_url)
