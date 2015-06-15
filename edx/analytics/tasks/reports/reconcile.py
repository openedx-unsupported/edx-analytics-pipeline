"""Perform reconciliation of transaction history against order history"""

from collections import namedtuple, defaultdict
from decimal import Decimal
import logging

import luigi
import luigi.date_interval

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
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

OrderItemRecord = namedtuple('OrderItemRecord', ORDERITEM_FIELDS)

# These are cybersource-specific at the moment, until generalized
# for Paypal, etc.
# Generalization will include:
#  time = timestamp the transaction was recorded (in addition to the date)
#  transaction_type: needs to be generalized (cybersource-specific terms now).
#  transaction_fee:  not reported in cybersource reports.
#
TRANSACTION_FIELDS = [
    'payment_gateway_id',
    'payment_gateway_account_id',
    'date',
    'payment_ref_id',
    'iso_currency_code',
    'amount',
    'transaction_type',
    'payment_method',
    'transaction_id',
    'request_id_for_now',
]

TransactionRecord = namedtuple('TransactionRecord', TRANSACTION_FIELDS)

LOW_ORDER_ID_SHOPPINGCART_ORDERS = (
    '1556',
    '1564',
    '1794',
    '9280',
    '9918',
)

class ReconcileOrdersAndTransactionsDownstreamMixin(MapReduceJobTaskMixin):

    source = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'source'}
    )

    # Create a dummy default for this parameter, since it is parsed by EventLogSelectionTask
    # but not actually used.
    interval = luigi.DateIntervalParameter(default=luigi.date_interval.Custom.parse("2014-01-01-2015-01-02"))

    pattern = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'pattern'}
    )

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
        return EventLogSelectionTask(
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
        )

    def mapper(self, line):
        fields = line.split('\t')
        # If we put the "payment_ref_id" in the front of all these fields, or
        # at least always in the same index, then we wouldn't this
        # ugly heuristic here.  (It would only need to be in the
        # reducer. :)
        if len(fields) > 10:
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
            if fields[2] > '2015-05-01' and fields[2] < '2015-06-14':
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
        code = self._add_orderitem_status_to_code(orderitem, code)
        return code

    def reducer(self, key, values):
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
                yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, None, orderitem))
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
                yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, None))
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
                    # proportion = 1.00  # we have one orderitem
                    yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, orderitem))
            else:
                code = self._get_code_for_nonmatch(orderitem, trans_balance)
                for transaction in transactions:
                    yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, orderitem))

        elif len(transactions) == 1:
            # If we have multiple orderitems and a single transaction, then we assume the single transaction
            # sums to the value of all the orderitems.
            # TODO: check more invariants:  e.g. same order_processor, same user(?).
            transaction = transactions[0]
            trans_balance = Decimal(transaction.amount)
            order_value = sum([Decimal(orderitem.line_item_price) - Decimal(orderitem.refunded_amount) for orderitem in orderitems])
            if order_value == trans_balance:
                for orderitem in orderitems:
                    code = "PURCHASED_BALANCE_MATCHING"
                    if self._orderitem_is_professional_ed(orderitem):
                        code = "ERROR_PROFED_PURCHASED_BALANCE_MATCHING"
                    code = self._add_orderitem_status_to_code(orderitem, code)
                    # TODO: what to do about refund value in this calculation?  This seems wrong.
                    item_amount = Decimal(orderitem.line_item_unit_price) * int(orderitem.line_item_quantity)
                    proportion = float(item_amount) / float(trans_balance)
                    yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, orderitem, proportion))
            else:
                # First partition orders by order_id.
                orderitem_partition = defaultdict(list)
                for orderitem in orderitems:
                    orderitem_partition[orderitem.order_id] = orderitem
                # TODO: distribute transactions to each of the separate orders, and reinvoke
                # the entire processing.  This should actually be done at the very top of all this.
                # For now we will just report this case.
                for orderitem in orderitems:
                    code = self._get_code_for_nonmatch(orderitem, trans_balance)
                    # For now, just append a suffix when we have multiple orders.
                    if len(orderitem_partition) > 1:
                        code = "{}_MULTIPLE_ORDERS".format(code)
                    # Because the balance doesn't match, we don't
                    # actually pass a "proportion" value on, but
                    # rather rely on the error condition to permit
                    # these to be filtered later.
                    yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, orderitem))

        else:
            # If we're here, we know we have multiple orderitems *and*
            # multiple transactions.
            orderitem_partition = defaultdict(list)
            for orderitem in orderitems:
                orderitem_partition[orderitem.order_id] = orderitem

            # TODO: figure out the mapping of multiple orders to multiple
            # transactions.  Easier step is to look at each single transaction
            # and see if it matches the sum of order items.  We're not yet
            # prepared to deal with partial transactions.
            if len(orderitem_partition) > 1:
                yield ("MULTIPLE_ORDERS", key, orderitems, transactions)
            else:
                yield ("MULTIPLE_ORDERITEMS", key, orderitems, transactions)

    def output(self):
        filename = u'reconcile_{reconcile_type}.tsv'.format(reconcile_type="all")
        output_path = url_path_join(self.output_root, filename)
        return get_target_from_url(output_path)

    def format_transaction_table_output(self, audit_code, transaction, orderitem, proportion = 1.0):
        # TODO: When calculating the transaction fee, apply the proportion to the
        # value provided by the transaction object.

        # TODO: it's not clear what the actual "value" of this entry is.  The
        # transaction.amount is the value of the entire transaction, not the part of the transaction
        # that is put towards the orderitem.  So if we have two orderitems of $50 and
        # a single transaction of $100, and then a refund of -$100, it's not clear where
        # the +$50 and -$50 should appear in the table below. Perhaps it should be made more
        # explicit - something like transaction_amount_for_orderitem.  And it would have to
        # be passed in, perhaps instead of the proportion.  The result should be a single column
        # that can be summed to get the total value of the original transactions (and broken down
        # by account too).

        transaction_fee_per_item = "0.0"
        if orderitem:
            org_id = get_org_id_for_course(orderitem.course_id) or ""
        else:
            org_id = ""

        result = [
            audit_code,
            orderitem.payment_ref_id if orderitem else transaction.payment_ref_id,
            orderitem.order_id if orderitem else "",
            orderitem.date_placed if orderitem else "",
            transaction.date if transaction else "",
            transaction.transaction_id if transaction else "",
            transaction.payment_gateway_id if transaction else "",
            transaction.payment_gateway_account_id if transaction else "",
            transaction.transaction_type if transaction else "",
            transaction.payment_method if transaction else "",
            transaction.amount if transaction else "",
            transaction.iso_currency_code if transaction else "",
            transaction_fee_per_item,
            orderitem.line_item_id if orderitem else "",
            orderitem.line_item_product_id if orderitem else "",
            # TODO: Should this information reflect the original orderitem's value, or the
            # end value after refund(s) have been applied?
            orderitem.line_item_price if orderitem else "",
            orderitem.line_item_unit_price if orderitem else "",
            # TODO: Should this information reflect the original quantity, or the
            # end quantity after refund(s) have been applied?
            orderitem.line_item_quantity if orderitem else "",
            # TODO: Assume we leave the above alone, and just add refund info here.
            orderitem.refunded_amount if orderitem else "",
            orderitem.refunded_quantity if orderitem else "",
            orderitem.username if orderitem else "",
            orderitem.user_email if orderitem else "",
            orderitem.product_class if orderitem else "",
            orderitem.product_detail if orderitem else "",
            orderitem.course_id if orderitem else "",
            org_id,
            orderitem.order_processor if orderitem else "",
        ]
        return '\t'.join(result)


class ReconciliationOutputTask(ReconcileOrdersAndTransactionsDownstreamMixin, MultiOutputMapReduceJobTask):

    def requires(self):
        return ReconcileOrdersAndTransactionsTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.output_root,
            # overwrite=self.overwrite,
        )

    def mapper(self, line):
        """
        Groups inputs by reconciliation type, writes all records with the same type to the same output file.
        """
        reconcile_type, content = line.split('\t', 1)
        yield (reconcile_type), content

    def output_path_for_key(self, key):
        """
        Create a different output file based on the type (or category) of reconciliation.
        """
        reconcile_type = key.lower()
        filename = u'reconcile_{reconcile_type}.tsv'.format(reconcile_type=reconcile_type)
        return url_path_join(self.output_root, filename)

    def format_value(self, reconcile_type, value):
        """
        Transform a value into the right format for the given reconcile_type.
        """
        return value

    def multi_output_reducer(self, key, values, output_file):
        """
        Dump all the values with the same reconcile_type to the same file.

        """
        for value in values:
            formatted_value = self.format_value(key, value)
            output_file.write(formatted_value)
            output_file.write('\n')
