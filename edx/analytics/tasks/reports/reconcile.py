"""Perform reconciliation of transaction history against order history"""

from collections import namedtuple
import csv
import datetime
from decimal import Decimal
import logging

import luigi
import luigi.date_interval

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.hive import HiveTableTask, HivePartition
from edx.analytics.tasks.util.id_codec import encode_id
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course

log = logging.getLogger(__name__)


ORDERITEM_FIELDS = [
    'order_processor',   # "shoppingcart" or "otto"
    'user_id',
    'order_id',
    'line_item_id',
    'line_item_product_id',  # for "shoppingcart", this is the kind of orderitem table.
    'line_item_price',
    'line_item_unit_price',
    'line_item_quantity',
    'product_class',  # e.g. seat, donation
    'course_id',  # Was called course_key
    'product_detail',  # contains course mode
    'username',
    'user_email',
    'date_placed',
    'iso_currency_code',
    'status',
    'refunded_amount',
    'refunded_quantity',
    'payment_ref_id',  # This is the value to compare with the transactions.
]

OrderItemRecord = namedtuple('OrderItemRecord', ORDERITEM_FIELDS)  # pylint: disable=invalid-name

# These are cybersource-specific at the moment, until generalized
# for Paypal, etc.
# Generalization will include:
#  time = timestamp the transaction was recorded (in addition to the date)
#  transaction_type: needs to be generalized (cybersource-specific terms now).
#  transaction_fee:  not reported in cybersource reports.
#
TRANSACTION_FIELDS = [
    'date',
    'payment_gateway_id',
    'payment_gateway_account_id',
    'payment_ref_id',
    'iso_currency_code',
    'amount',
    'transaction_fee',
    'transaction_type',
    'payment_method',
    'payment_method_type',
    'transaction_id',
]

TransactionRecord = namedtuple('TransactionRecord', TRANSACTION_FIELDS)  # pylint: disable=invalid-name

LOW_ORDER_ID_SHOPPINGCART_ORDERS = (
    '1556',
    '1564',
    '1794',
    '9280',
    '9918',
)


class ReconcileOrdersAndTransactionsDownstreamMixin(MapReduceJobTaskMixin):

    transaction_source = luigi.Parameter(
        default_from_config={'section': 'payment-reconciliation', 'name': 'transaction_source'}
    )

    order_source = luigi.Parameter(
        default_from_config={'section': 'payment-reconciliation', 'name': 'order_source'}
    )

    # Create a dummy default for this parameter, since it is parsed by EventLogSelectionTask
    # but not actually used.
    # interval = luigi.DateIntervalParameter(
    #     default=luigi.date_interval.Custom.parse("2014-01-01-{}".format(
    #         datetime.datetime.utcnow().date().isoformat()
    #     ))
    # )

    interval_end = luigi.DateParameter()
    interval_start = luigi.DateParameter()

    pattern = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'pattern'}
    )

    # def __init__(self,interval_start,interval_end):
    #     self.interval = luigi.date_interval.Custom(interval_start, interval_end)

    interval = luigi.DateIntervalParameter(interval_start, interval_end)


    def extra_modules(self):
        """edx.analytics.tasks is required by all tasks that load this file."""
        import edx.analytics.tasks.mapreduce
        return [edx.analytics.tasks.mapreduce]

class ReconcileOrdersAndTransactionsDownstreamMixinHack(MapReduceJobTaskMixin):

    def extra_modules(self):
        """edx.analytics.tasks is required by all tasks that load this file."""
        import edx.analytics.tasks.mapreduce
        return [edx.analytics.tasks.mapreduce]

class ReconcileOrdersAndTransactionsTask(ReconcileOrdersAndTransactionsDownstreamMixin, MapReduceJobTask):
    """
    Compare orders and transactions.

    """
    output_root = luigi.Parameter()

    def requires(self):
        """Use EventLogSelectionTask to define inputs."""
        #partition_path_spec = HivePartition('dt', self.interval.date_b.isoformat()).path_spec  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', self.interval_end.isoformat()).path_spec  # pylint: disable=no-member
        order_partition = url_path_join(self.order_source, partition_path_spec)



        return EventLogSelectionTask(
            source=[self.transaction_source, order_partition],
            pattern=self.pattern,
            interval=self.interval
        )

    def mapper(self, line):
        fields = line.split('\t')
        # If we put the "payment_ref_id" in the front of all these fields, or
        # at least always in the same index, then we wouldn't this
        # ugly heuristic here.  (It would only need to be in the
        # reducer. :)
        if len(fields) > 11:
            # assume it's an order
            key = fields[-1]
        else:
            # assume it's a transaction
            key = fields[3]
            # Edx-only: if the transaction was within a time period when
            # Otto was storing basket-id values instead of payment_ref_ids in
            # its transactions, then apply a heuristic to the transactions
            # from that period to convert them to a payment_ref_id that should
            # work in most cases.
            if fields[0] > '2015-05-01' and fields[0] < '2015-06-14':
                if len(key) <= 4 and key not in LOW_ORDER_ID_SHOPPINGCART_ORDERS:
                    key = 'EDX-{}'.format(int(key) + 100000)

        yield key, fields

    def _orderitem_is_professional_ed(self, orderitem):
        return orderitem.order_processor == 'shoppingcart' and orderitem.line_item_product_id in ['2', '3']

    def _orderitem_status_is_consistent(self, orderitem):
        return (
            (orderitem.status == 'purchased' and Decimal(orderitem.refunded_amount) == 0.0) or
            (orderitem.status == 'refunded' and Decimal(orderitem.refunded_amount) > 0.0)
        )

    def _add_orderitem_status_to_code(self, orderitem, code):
        if self._orderitem_status_is_consistent(orderitem):
            return code
        else:
            return "ERROR_WRONGSTATUS_{}".format(code)

    def _get_code_for_nonmatch(self, orderitem, trans_balance):
        code = "ERROR_{}_BALANCE_NOT_MATCHING".format(orderitem.status.upper())
        if trans_balance == Decimal(orderitem.line_item_price):
            # If these are equal, then the refunded_amount must be non-zero,
            # and not have a matching transaction.
            code = "{}_REFUND_MISSING".format(code)
        elif trans_balance == 0.0:
            code = "{}_WAS_REFUNDED".format(code)
        elif trans_balance == -1 * Decimal(orderitem.line_item_price):
            code = "{}_WAS_REFUNDED_TWICE".format(code)
        elif trans_balance == 2 * Decimal(orderitem.line_item_price):
            code = "{}_WAS_CHARGED_TWICE".format(code)
        elif trans_balance < Decimal(orderitem.line_item_price):
            code = "ERROR_BALANCE_NOT_MATCHING_PARTIAL_REFUND"
        elif trans_balance > Decimal(orderitem.line_item_price):
            code = "ERROR_BALANCE_NOT_MATCHING_EXTRA_CHARGE"
        code = self._add_orderitem_status_to_code(orderitem, code)
        return code

    def reducer(self, _key, values):
        orderitems = []
        transactions = []
        for value in values:
            if len(value) > 17:
                # convert refunded_amount:
                if value[16] == '\\N':
                    value[16] = '0.0'
                # same for 'refunded_quantity':
                if value[17] == '\\N':
                    value[17] = '0'
                # same for 'product_detail'
                if value[10] == '\\N':
                    value[10] = ''

                record = OrderItemRecord(*value)

                orderitems.append(record)
            else:
                if value[6] == '\\N':
                    value[6] = None
                transactions.append(TransactionRecord(*value))

        if len(transactions) == 0:
            # We have an orderitem with no transaction.  This happens
            # when an order is begun but the user changes their mind.
            # But once those orders are filtered (based on status), we
            # don't expect there to be extras.

            # That said, there seem to be a goodly number of MITProfessionalX
            # entries that probably have transactions in a different account.

            # Also included are registrations that have no cost, so
            # having no transactions is actually a reasonable state.
            # These are dominated by DemoX registrations that
            # presumably demonstrate the process but have no cost.

            # And more are due to a difference in the timing of the
            # orders and the transaction extraction.  At present, the
            # orders are pulled at whatever time the task is run, and
            # they are dumped.  For transactions, the granularity is
            # daily: we only have up through yesterday's.  So there
            # may be orders from today that don't yet have
            # transactions downloaded.
            for orderitem in orderitems:
                code = "ERROR_NO_TRANSACTION"
                if self._orderitem_is_professional_ed(orderitem):
                    code = "NO_TRANS_PROFESSIONAL"
                elif Decimal(orderitem.line_item_unit_price) == 0.0:
                    code = "NO_TRANSACTION_NOCOST"
                code = self._add_orderitem_status_to_code(orderitem, code)
                yield self.format_transaction_table_output(code, None, orderitem)
            return

        if len(orderitems) == 0:
            # Same thing if we have transactions with no orderitems.
            # This is likely when the transaction pull is newer than the order pull,
            # or if a basket was charged that was not marked as a purchased order.
            # In the latter case, if the charge was later refunded and the current balance
            # is zero, then no further action is needed.  Otherwise either the order needs
            # to be updated (to reflect that they did actually receive what they ordered),
            # or the balance should be refunded (because they never received what they were charged for).
            trans_balance = sum([Decimal(transaction.amount) for transaction in transactions])
            code = "NO_ORDER_ZERO_BALANCE" if trans_balance == 0 else "ERROR_NO_ORDER_NONZERO_BALANCE"
            for transaction in transactions:
                yield self.format_transaction_table_output(code, transaction, None)
            return

        # This is the location for the main form of reconciliation.
        # Let's work through some of the easy cases, and work down from there.
        if len(orderitems) == 1:
            orderitem = orderitems[0]
            order_balance = Decimal(orderitem.line_item_price) - Decimal(orderitem.refunded_amount)
            trans_balance = sum([Decimal(transaction.amount) for transaction in transactions])
            if order_balance == trans_balance:
                code = "{}_BALANCE_MATCHING".format(orderitem.status.upper())
                if self._orderitem_is_professional_ed(orderitem):
                    code = "ERROR_PROFED_{}".format(code)
                # We have just compared independent of the status, but check that it's
                # consistent.
                code = self._add_orderitem_status_to_code(orderitem, code)
                for transaction in transactions:
                    yield self.format_transaction_table_output(code, transaction, orderitem)
            else:
                code = self._get_code_for_nonmatch(orderitem, trans_balance)
                for transaction in transactions:
                    yield self.format_transaction_table_output(code, transaction, orderitem)

        elif len(transactions) == 1:
            # If we have multiple orderitems and a single transaction, then we assume the single transaction
            # sums to the value of all the orderitems.
            # TODO: check more invariants:  e.g. same order_processor, same user(?).
            transaction = transactions[0]
            trans_balance = Decimal(transaction.amount)
            order_value = sum([Decimal(orderitem.line_item_price) - Decimal(orderitem.refunded_amount) for orderitem in orderitems])
            order_cost = sum([Decimal(orderitem.line_item_price) for orderitem in orderitems])
            if order_value == trans_balance:
                for orderitem in orderitems:
                    code = "PURCHASED_BALANCE_MATCHING"
                    if self._orderitem_is_professional_ed(orderitem):
                        code = "ERROR_PROFED_PURCHASED_BALANCE_MATCHING"
                    code = self._add_orderitem_status_to_code(orderitem, code)
                    # If we got here with a single transaction, we expect that refunded_amount must be zero.
                    # We just have to divide up the payment transaction over the order items.
                    item_amount = Decimal(orderitem.line_item_price)
                    yield self.format_transaction_table_output(code, transaction, orderitem, item_amount)
            elif order_cost == trans_balance:
                # We know that the refund is bogus, and we can more confidently distribute the
                # transaction across the orderitems.
                for orderitem in orderitems:
                    code = self._get_code_for_nonmatch(orderitem, trans_balance)
                    item_amount = Decimal(orderitem.line_item_price)
                    yield self.format_transaction_table_output(code, transaction, orderitem, item_amount)
            else:
                for index, orderitem in enumerate(orderitems):
                    code = self._get_code_for_nonmatch(orderitem, trans_balance)
                    # We need to come up with a value, which is complicated by the
                    # presence of a refund or a mismatch in order value and transaction.
                    # Arbitrarily put all the value into one of the order items.
                    item_amount = trans_balance if index == 1 else Decimal(0.0)
                    yield self.format_transaction_table_output(code, transaction, orderitem, item_amount)

        else:
            # for now, just get some transactions into the file, independent of the
            # orderitems involved.
            code = "MULTIPLE_ORDERITEMS"
            for transaction in transactions:
                yield self.format_transaction_table_output(code, transaction, orderitems[0])

    def output(self):
        return get_target_from_url(self.output_root)

    def format_transaction_table_output(self, audit_code, transaction, orderitem, transaction_amount_per_item=None):
        if transaction and transaction_amount_per_item is None:
            transaction_amount_per_item = transaction.amount

        transaction_fee_per_item = None
        if transaction and transaction.transaction_fee is not None:
            if transaction.amount == transaction_amount_per_item:
                transaction_fee_per_item = str(transaction.transaction_fee)
            else:
                proportion = Decimal(transaction_amount_per_item) / Decimal(transaction.amount)
                transaction_fee_per_item = str(Decimal(transaction.transaction_fee) * proportion)

        NULL = "\\N"  # pylint: disable=invalid-name

        org_id = None
        if orderitem:
            org_id = get_org_id_for_course(orderitem.course_id)

        result = [
            audit_code,
            orderitem.payment_ref_id if orderitem else transaction.payment_ref_id,
            orderitem.order_id if orderitem else NULL,
            encode_id(orderitem.order_processor, "order_id", orderitem.order_id) if orderitem else NULL,
            orderitem.date_placed if orderitem else NULL,
            # transaction information
            transaction.date if transaction else NULL,
            transaction.transaction_id if transaction else NULL,
            encode_id(transaction.payment_gateway_id, "transaction_id", transaction.transaction_id) if transaction else NULL,
            transaction.payment_gateway_id if transaction else NULL,
            transaction.payment_gateway_account_id if transaction else NULL,
            transaction.transaction_type if transaction else NULL,
            transaction.payment_method if transaction else NULL,
            transaction.amount if transaction else NULL,
            transaction.iso_currency_code if transaction else NULL,
            transaction.transaction_fee if transaction else NULL,
            # mapping information: part of transaction that applies to this orderitem
            str(transaction_amount_per_item) if transaction_amount_per_item is not None else NULL,
            str(transaction_fee_per_item) if transaction_fee_per_item is not None else NULL,
            # orderitem information
            orderitem.line_item_id if orderitem else NULL,
            encode_id(orderitem.order_processor, "line_item_id", orderitem.line_item_id) if orderitem else NULL,
            orderitem.line_item_product_id if orderitem else NULL,
            orderitem.line_item_price if orderitem else NULL,
            orderitem.line_item_unit_price if orderitem else NULL,
            orderitem.line_item_quantity if orderitem else NULL,
            orderitem.refunded_amount if orderitem else NULL,
            orderitem.refunded_quantity if orderitem else NULL,
            orderitem.user_id if orderitem else NULL,
            orderitem.username if orderitem else NULL,
            orderitem.user_email if orderitem else NULL,
            orderitem.product_class if orderitem else NULL,
            orderitem.product_detail if orderitem else NULL,
            orderitem.course_id if orderitem else NULL,
            org_id if org_id is not None else NULL,
            orderitem.order_processor if orderitem else NULL,
        ]
        return (OrderTransactionRecord(*result).to_tsv(),)


OrderTransactionRecordBase = namedtuple("OrderTransactionRecord", [  # pylint: disable=invalid-name
    "audit_code",
    "payment_ref_id",
    "order_id",
    "unique_order_id",
    "order_timestamp",
    "transaction_date",
    "transaction_id",
    "unique_transaction_id",
    "transaction_payment_gateway_id",
    "transaction_payment_gateway_account_id",
    "transaction_type",
    "transaction_payment_method",
    "transaction_amount",
    "transaction_iso_currency_code",
    "transaction_fee",
    "transaction_amount_per_item",
    "transaction_fee_per_item",
    "order_line_item_id",
    "unique_order_line_item_id",
    "order_line_item_product_id",
    "order_line_item_price",
    "order_line_item_unit_price",
    "order_line_item_quantity",
    "order_refunded_amount",
    "order_refunded_quantity",
    "order_user_id",
    "order_username",
    "order_user_email",
    "order_product_class",
    "order_product_detail",
    "order_course_id",
    "order_org_id",
    "order_processor",
])


class OrderTransactionRecord(OrderTransactionRecordBase):

    def to_tsv(self):
        return '\t'.join([str(v) if v is not None else "" for v in self])

    @staticmethod
    def from_job_output(tsv_str):
        record = tsv_str.split('\t')
        nulled_record = [v if v != "\\N" else None for v in record]
        return OrderTransactionRecord(*nulled_record)


class ReconciledOrderTransactionTableTask(ReconcileOrdersAndTransactionsDownstreamMixin, HiveTableTask):

    output_root = None

    @property
    def table(self):
        return 'reconciled_order_transactions'

    @property
    def columns(self):
        return [
            ('audit_code', 'STRING'),
            ('payment_ref_id', 'STRING'),
            ('order_id', 'INT'),
            ('unique_order_id', 'STRING'),
            ('order_timestamp', 'TIMESTAMP'),
            ('transaction_date', 'STRING'),
            ('transaction_id', 'STRING'),
            ('unique_transaction_id', 'STRING'),
            ('transaction_payment_gateway_id', 'STRING'),
            ('transaction_payment_gateway_account_id', 'STRING'),
            ('transaction_type', 'STRING'),
            ('transaction_payment_method', 'STRING'),
            ('transaction_amount', 'DECIMAL'),
            ('transaction_iso_currency_code', 'STRING'),
            ('transaction_fee', 'DECIMAL'),
            ('transaction_amount_per_item', 'DECIMAL'),
            ('transaction_fee_per_item', 'DECIMAL'),
            ('order_line_item_id', 'INT'),
            ('unique_order_line_item_id', 'STRING'),
            ('order_line_item_product_id', 'INT'),
            ('order_line_item_price', 'DECIMAL'),
            ('order_line_item_unit_price', 'DECIMAL'),
            ('order_line_item_quantity', 'INT'),
            ('order_refunded_amount', 'DECIMAL'),
            ('order_refunded_quantity', 'INT'),
            ('order_user_id', 'INT'),
            ('order_username', 'STRING'),
            ('order_user_email', 'STRING'),
            ('order_product_class', 'STRING'),
            ('order_product_detail', 'STRING'),
            ('order_course_id', 'STRING'),
            ('order_org_id', 'STRING'),
            ('order_processor', 'STRING'),
        ]

    @property
    def partition(self):
        # return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member
        return HivePartition('dt', self.interval_end.isoformat())  # pylint: disable=no-member

    def requires(self):
        return ReconcileOrdersAndTransactionsTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            transaction_source=self.transaction_source,
            order_source=self.order_source,
            pattern=self.pattern,
            output_root=self.partition_location,
            interval_end=self.interval_end,
            interval_start=self.interval_start,
            interval=self.interval,
        )


class TransactionReportTask(ReconcileOrdersAndTransactionsDownstreamMixin, luigi.Task):
    """
    Creates transactions.csv.
    """

    output_root = luigi.Parameter()

    COLUMNS = [
        'date',
        'transaction_id',
        'payment_gateway_id',
        'transaction_type',
        'payment_method',
        'transaction_amount',
        'line_item_transaction_fee',
        'line_item_id',
        'line_item_price',
        'product_class',
        'product_detail',
        'course_id',
        'org_id'
    ]

    def requires(self):
        return ReconcileOrdersAndTransactionsTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            transaction_source=self.transaction_source,
            order_source=self.order_source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=url_path_join(
                self.output_root,
                'reconciled_order_transactions',
                #'dt=' + self.interval.date_b.isoformat()  # pylint: disable=no-member
                'dt=' + self.interval_end.isoformat()  # pylint: disable=no-member
            ) + '/',
            # overwrite=self.overwrite,
        )

    def run(self):
        all_records = []

        # first load all records in memory so that we can sort them
        for record_str in self.input().open('r'):
            record = OrderTransactionRecord.from_job_output(record_str)

            if record.transaction_date is not None and record.transaction_date != '':  # pylint: disable=no-member
                all_records.append(record)

        with self.output().open('w') as output_file:
            writer = csv.DictWriter(output_file, self.COLUMNS)
            writer.writerow(dict((k, k) for k in self.COLUMNS))  # Write header

            def get_sort_key(record):
                return record.transaction_date, record.order_line_item_id, record.order_org_id

            for record in sorted(all_records, key=get_sort_key):
                writer.writerow({
                    'date': record.transaction_date,
                    'transaction_id': record.unique_transaction_id,
                    'payment_gateway_id': record.transaction_payment_gateway_id,
                    'transaction_type': record.transaction_type,
                    'payment_method': record.transaction_payment_method,
                    'transaction_amount': record.transaction_amount,
                    'line_item_transaction_fee': record.transaction_fee_per_item,
                    'line_item_id': record.unique_order_line_item_id,
                    'line_item_price': record.transaction_amount_per_item,
                    'product_class': record.order_product_class,
                    'product_detail': record.order_product_detail,
                    'course_id': record.order_course_id,
                    'org_id': record.order_org_id
                })

    def output(self):
        #return get_target_from_url(url_path_join(self.output_root, 'transaction', 'dt=' + self.interval.date_b.isoformat(), 'transactions.csv'))
        return get_target_from_url(url_path_join(self.output_root, 'transaction', 'dt=' + self.interval_end.isoformat(), 'transactions.csv'))