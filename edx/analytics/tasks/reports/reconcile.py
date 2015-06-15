"""Perform reconciliation of transaction history against order history"""

from collections import namedtuple
from decimal import Decimal
import logging

import luigi


from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course

log = logging.getLogger(__name__)


HACKED_ORIGINAL_ORDERITEM_FIELDS = [
    'id',   # changes to line_item_id
    'order_id',
    'user_id',
    'status',
    'qty',   # changes to line_item_quantity
    'unit_cost',   # changes to line_item_unit_price
    'line_desc',  # changes to product_detail?  line_item_product_id
    'currency',  # changes to iso_currency_code
    'fulfilled_time',
    'report_comments',  # not kept...
    'refund_requested_time',
    'service_fee',  # not kept...
    'list_price',  # not kept...
    'created',  # not kept...
    'modified',  # not kept...
    'course_id',
    'course_enrollment_id',  # not kept...
    'mode',  # now a product_detail
    'donation_type',  # was used for product_class
]

ORDERITEM_FIELDS = [
    'order_processor',   # "shoppingcart" or "otto"
    'user_id',
    'order_id',
    'line_item_id',
    'line_item_product_id',
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
    # TODO: I don't understand how to use these...
    'refunded_amount',
    'refunded_quantity',
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
    'order_id',
    'iso_currency_code',
    'amount',
    'transaction_type',
    'payment_method',
    'transaction_id',
    'request_id_for_now',
]

TransactionRecord = namedtuple('TransactionRecord', TRANSACTION_FIELDS)


class ReconcileOrdersAndTransactionsDownstreamMixin(MapReduceJobTaskMixin):

    source = luigi.Parameter(
        is_list=True,
        default_from_config={'section': 'payment-reconciliation', 'name': 'source'}
    )

    interval = luigi.DateIntervalParameter(default="2014-01-01-2015-01-02")

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
        # interval = luigi.DateIntervalParameter.parse("2014-01-01-2015-01-02")
        return EventLogSelectionTask(
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
        )

    def mapper(self, line):
        fields = line.split('\t')
        # If we put the order_id in the front of all these fields, or
        # at least always in the same index, then we wouldn't this
        # ugly heuristic here.  (It would only need to be in the
        # reducer. :)
        if len(fields) > 10:
            # assume it's an order
            key = fields[2]
        else:
            # assume it's a transaction
            key = fields[3]

        yield key, fields

    def reducer(self, key, values):
        # count = len(list(values))
        # yield ("Found %d values for key %s" % (count, key),)
        orderitems = []
        transactions = []
        for value in values:
            if len(value) > 10:
                record = OrderItemRecord(*value)
                # We will have already filtered out those orderitems with
                # different status values, but for now, do it here.
                # if record.status in ['purchased', 'refunded']:
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
            for orderitem in orderitems:
                # yield ("NO_TRANSACTIONS", key, orderitem, None)
                yield ("TRANSACTION_TABLE", self.format_transaction_table_output("ERROR_NO_TRANSACTION", None, orderitem))
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
                # yield ("NO_ORDERITEMS", key, transaction, None)
                yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, None))
            return

        # This is the location for the main form of reconciliation.
        # Let's work through some of the easy cases, and work down from there.
        if len(orderitems) == 1:
            orderitem = orderitems[0]
            # order_value = Decimal(orderitem.line_item_unit_price) * int(orderitem.line_item_quantity)
            order_value = Decimal(orderitem.line_item_price)
            trans_balance = sum([Decimal(transaction.amount) for transaction in transactions])
            # Assume if the orderitem is not purchased, then it's refunded.
            order_balance = order_value if orderitem.status == 'purchased' else 0
            if order_balance == trans_balance:
                code = "PURCHASE_BALANCE_MATCHING" if orderitem.status == 'purchased' else "REFUND_ZERO_BALANCE"
                for transaction in transactions:
                    # proportion = 1.00  # we have one orderitem
                    # yield ("TRANSACTION_MATCHES", key, orderitem, transaction, proportion)
                    yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, orderitem))
            else:
                code = "ERROR_PURCHASE_BALANCE_NOT_MATCHING" if orderitem.status == 'purchased' else "ERROR_REFUND_NONZERO_BALANCE"
                for transaction in transactions:
                    yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, orderitem))
                # yield ("AMOUNTS_UNEQUAL", key, orderitems, transactions)

        elif len(transactions) == 1:
            # If we have multiple orderitems and a single transaction, then we assume the single transaction
            # sums to the value of all the orderitems.
            # TODO: check that all orderitems are actually purchases.  Assume for now....
            transaction = transactions[0]
            trans_balance = Decimal(transaction.amount)
            # order_value = sum([Decimal(orderitem.line_item_unit_price) * int(orderitem.line_item_quantity) for orderitem in orderitems])
            order_value = sum([Decimal(orderitem.line_item_price) for orderitem in orderitems])
            if order_value == trans_balance:
                code = "PURCHASE_BALANCE_MATCHING"
                for orderitem in orderitems:
                    item_amount = Decimal(orderitem.line_item_unit_price) * int(orderitem.line_item_quantity)
                    proportion = float(item_amount) / float(trans_balance)
                    # yield ("TRANSACTION_MATCHES", key, orderitem, transaction, proportion)
                    yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, orderitem, proportion))
            else:
                code = "ERROR_PURCHASE_BALANCE_NOT_MATCHING"
                # yield ("AMOUNTS_UNEQUAL", key, orderitems, transactions)
                # Because the balance doesn't match, we don't actually pass a "proportion" value on,
                # but rather rely on the error condition to permit these to be filtered later.
                for orderitem in orderitems:
                    yield ("TRANSACTION_TABLE", self.format_transaction_table_output(code, transaction, orderitem))
        else:
            # TODO: figure out the mapping of multiple orders to multiple
            # transactions.  Easier step is to look at each single transaction
            # and see if it matches the sum of order items.  We're not yet
            # prepared to deal with partial transactions.
            yield ("MULTIPLE_ORDERITEMS", key, orderitems, transactions)

    def output(self):
        filename = u'reconcile_{reconcile_type}.tsv'.format(reconcile_type="all")
        output_path = url_path_join(self.output_root, filename)
        return get_target_from_url(output_path)

    def format_transaction_table_output(self, audit_code, transaction, orderitem, proportion = 1.0):
        # TODO: When calculating the transaction fee, apply the proportion to the
        # value provided by the transaction object.
        transaction_fee_per_item = "0.0"
        if orderitem:
            org_id = get_org_id_for_course(orderitem.course_id) or ""
        else:
            org_id = ""

        result = [
            audit_code,
            orderitem.order_id if orderitem else transaction.order_id,
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
            orderitem.line_item_price if orderitem else "",
            orderitem.line_item_unit_price if orderitem else "",
            orderitem.line_item_quantity if orderitem else "",
            orderitem.username if orderitem else "",
            orderitem.user_email if orderitem else "",
            orderitem.product_class if orderitem else "",
            orderitem.product_detail if orderitem else "",
            orderitem.course_id if orderitem else "",
            org_id,
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
