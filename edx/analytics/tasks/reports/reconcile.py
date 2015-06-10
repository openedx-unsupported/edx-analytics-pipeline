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


ORDERITEM_FIELDS = [
    'id',
    'order_id',
    'user_id',
    'status',
    'qty',
    'unit_cost',
    'line_desc',
    'currency',
    'fulfilled_time',
    'report_comments',
    'refund_requested_time',
    'service_fee',
    'list_price',
    'created',
    'modified',
    'course_id',
    'course_enrollment_id',
    'mode',
    'donation_type',
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
            key = fields[1]
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
                if record.status in ['purchased', 'refunded']:
                    orderitems.append(record)
            else:
                transactions.append(TransactionRecord(*value))

        if len(transactions) == 0:
            # We have an orderitem with no transaction.  This happens
            # when an order is begun but the user changes their mind.
            # But once those orders are filtered (based on status), we
            # don't expect there to be extras.
            for orderitem in orderitems:
                yield ("NO_TRANSACTIONS", key, orderitem, None)
            return

        if len(orderitems) == 0:
            # Same thing if we have transactions with no orderitems.
            # This is likely when the transaction pull is newer than the order pull.
            for transaction in transactions:
                yield ("NO_ORDERITEMS", key, transaction, None)
            return

        # This is the location for the main form of reconciliation.
        # Let's work through some of the easy cases, and work down from there.
        if len(orderitems) == 1:
            orderitem = orderitems[0]
            amount = Decimal(orderitem.unit_cost) * int(orderitem.qty)
            trans_amount = sum([Decimal(transaction.amount) for transaction in transactions])
            if amount == trans_amount:
                for transaction in transactions:
                    proportion = 1.00  # we have one orderitem
                    yield ("TRANSACTION_MATCHES", key, orderitem, transaction, proportion)
            else:
                yield ("AMOUNTS_UNEQUAL", key, orderitems, transactions)

        elif len(transactions) == 1:
            transaction = transactions[0]
            amount = sum([Decimal(orderitem.unit_cost) * int(orderitem.qty) for orderitem in orderitems])
            if amount == Decimal(transaction.amount):
                for orderitem in orderitems:
                    amount = Decimal(orderitem.unit_cost) * int(orderitem.qty)
                    proportion = float(amount) / float(transaction.amount)
                    yield ("TRANSACTION_MATCHES", key, orderitem, transaction, proportion)
            else:
                yield ("AMOUNTS_UNEQUAL", key, orderitems, transactions)

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

    def format_match_output(self, value):
        # orderitem, transaction, proportion = value
        fields = value.split('\t')
        num_orderitem_fields = len(ORDERITEM_FIELDS)
        num_trans_fields = len(TRANSACTION_FIELDS)
        # order_id = fields[0]
        orderitem = OrderItemRecord(*fields[1:num_orderitem_fields + 1])
        transaction = TransactionRecord(*fields[num_orderitem_fields + 1:num_orderitem_fields + num_trans_fields + 1])
        # proportion = fields[-1]

        # Set or calculate values that we don't (yet) read directly.
        time = ""
        audit_codes = ""
        order_item_total_cost = str(Decimal(orderitem.unit_cost) * int(orderitem.qty))
        # when calculating the transaction fee, apply the proportion to the
        # value provided by the transaction object.
        transaction_fee_per_item = "0.0"
        line_item_product_id = ""
        org_id = get_org_id_for_course(orderitem.course_id) or ""

        result = [
            time,
            transaction.date,
            transaction.transaction_id,
            transaction.payment_gateway_id,
            transaction.payment_gateway_account_id,
            transaction.transaction_type,
            transaction.payment_method,
            transaction.amount,
            transaction.iso_currency_code,
            transaction_fee_per_item,
            orderitem.id,
            line_item_product_id,
            order_item_total_cost,
            orderitem.unit_cost,
            orderitem.qty,
            audit_codes,
            "username_of_{}".format(orderitem.user_id),
            "email_of_{}".format(orderitem.user_id),
            "donation" if orderitem.donation_type != "\\n" else "seat",
            orderitem.mode,
            orderitem.course_id,
            org_id,
            orderitem.order_id,
        ]
        return '\t'.join(result)

    def format_value(self, reconcile_type, value):
        """
        Transform a value into the right format for the given reconcile_type.
        """
        if reconcile_type == "TRANSACTION_MATCHES":
            return self.format_match_output(value)

        return value

    def multi_output_reducer(self, key, values, output_file):
        """
        Dump all the values with the same reconcile_type to the same file.

        """
        for value in values:
            formatted_value = self.format_value(key, value)
            output_file.write(formatted_value)
            output_file.write('\n')
