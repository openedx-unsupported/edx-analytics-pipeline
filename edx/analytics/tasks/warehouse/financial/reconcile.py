"""Perform reconciliation of transaction history against order history"""

import csv
import json
import logging
from collections import defaultdict, namedtuple
from decimal import Decimal
from operator import attrgetter

import luigi.date_interval

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.util.hive import HivePartition, HiveTableTask, WarehouseMixin, hive_decimal_type
from edx.analytics.tasks.util.id_codec import encode_id
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join
from edx.analytics.tasks.warehouse.financial.orders_import import (
    FullOttoOrderTableTask, FullShoppingcartOrderTableTask, OrderTableTask
)
from edx.analytics.tasks.warehouse.financial.payment import PaymentTask

log = logging.getLogger(__name__)

# Precision for money is assumed to be two places.
TWOPLACES = Decimal(10) ** -2       # same as Decimal('0.01')

ORDERITEM_FIELDS = [
    'order_processor',   # "shoppingcart" or "otto"
    'user_id',  # this is the order system's user_id, not the same as auth_user's user_id.
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
    'coupon_id',
    'discount_amount',
    'voucher_id',
    'voucher_code',
    'status',
    'refunded_amount',
    'refunded_quantity',
    'payment_ref_id',  # This is the value to compare with the transactions.
    'partner_short_code',
    'course_uuid',  # UUID of the course, NOT course run
    'expiration_date',
]

ORDERITEM_FIELD_INDICES = {field_name: index for index, field_name in enumerate(ORDERITEM_FIELDS)}

BaseOrderItemRecord = namedtuple('OrderItemRecord', ORDERITEM_FIELDS)  # pylint: disable=invalid-name


class OrderItemRecord(BaseOrderItemRecord):
    """Override vanilla namedtuple to redefine types."""
    def __new__(cls, *args, **kwargs):
        result = super(OrderItemRecord, cls).__new__(cls, *args, **kwargs)
        result = result._replace(  # pylint: disable=no-member,protected-access
            refunded_amount=Decimal(result.refunded_amount),  # pylint: disable=no-member
            line_item_price=Decimal(result.line_item_price),  # pylint: disable=no-member
            line_item_unit_price=Decimal(result.line_item_unit_price),  # pylint: disable=no-member
            discount_amount=Decimal(result.discount_amount),  # pylint: disable=no-member
        )
        return result


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


BaseTransactionRecord = namedtuple('TransactionRecord', TRANSACTION_FIELDS)  # pylint: disable=invalid-name


class TransactionRecord(BaseTransactionRecord):
    """Override vanilla namedtuple to redefine types."""
    def __new__(cls, *args, **kwargs):
        result = super(TransactionRecord, cls).__new__(cls, *args, **kwargs)
        result = result._replace(  # pylint: disable=no-member,protected-access
            amount=Decimal(result.amount),  # pylint: disable=no-member
            transaction_fee=Decimal(result.transaction_fee) if result.transaction_fee is not None else None,  # pylint: disable=no-member
        )
        return result


LOW_ORDER_ID_SHOPPINGCART_ORDERS = (
    '1556',
    '1564',
    '1794',
    '9280',
    '9918',
)


class ReconcileOrdersAndTransactionsDownstreamMixin(MapReduceJobTaskMixin):
    """Define parameters needed downstream for running ReconcileOrdersAndTransactionsTask."""

    import_date = luigi.DateParameter()

    is_empty_transaction_allowed = luigi.BoolParameter(
        default=False,
        description='Allow empty transactions from payment processors to be processsed, default is False.'
    )

    # TODO: determine if this is still needed, if we start always exporting everything all the time.
    # def extra_modules(self):
    #    """edx.analytics.tasks is required by all tasks that load this file."""
    # import edx.analytics.tasks.common.mapreduce
    # return [edx.analytics.tasks.common.mapreduce]


class ReconcileOrdersAndTransactionsTask(ReconcileOrdersAndTransactionsDownstreamMixin, MapReduceJobTask):
    """
    Compare orders and transactions.

    """

    output_root = luigi.Parameter()
    shoppingcart_partners = luigi.Parameter(
        config_path={'section': 'financial-reports', 'name': 'shoppingcart-partners'},
        description="JSON string containing a dictionary mapping organization IDs of White Label partners "
        "with data in ShoppingCart to the corresponding Otto partner short code.  The short code to be used "
        "for other organization IDs must be given as value for the key \"DEFAULT\", to have a default that "
        "is not NULL.",
    )

    def __init__(self, *args, **kwargs):
        super(ReconcileOrdersAndTransactionsTask, self).__init__(*args, **kwargs)
        if self.shoppingcart_partners:
            self.shoppingcart_partners_dict = json.loads(self.shoppingcart_partners)
        else:
            self.shoppingcart_partners_dict = {}
        self.default_partner_short_code = self.shoppingcart_partners_dict.get("DEFAULT")

    def requires(self):
        yield (
            OrderTableTask(
                import_date=self.import_date
            ),
            PaymentTask(
                import_date=self.import_date,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            )
        )

    def mapper(self, line):
        fields = line.split('\t')
        if len(fields) == len(ORDERITEM_FIELDS):
            # Assume it's an order.
            record_type = OrderItemRecord.__name__
            key_index = ORDERITEM_FIELDS.index('payment_ref_id')
            key = fields[key_index]

            # Convert Hive null values ('\\N') in fields like 'product_detail':
            defaults = (
                ('product_detail', ''),
                ('refunded_amount', '0.0'),
                ('refunded_quantity', '0'),
                ('discount_amount', '0.0'),
                ('coupon_id', None),
                ('voucher_id', None),
                ('voucher_code', ''),
                ('partner_short_code', ''),
                ('course_uuid', None),
                ('expiration_date', None),
            )
            for field_name, default_value in defaults:
                index = ORDERITEM_FIELD_INDICES[field_name]
                if fields[index] == '\\N':
                    fields[index] = default_value

        elif len(fields) == len(TRANSACTION_FIELDS):
            # Assume it's a transaction.
            record_type = TransactionRecord.__name__
            key = fields[3]  # payment_ref_id
            # Convert nulls in 'transaction_fee'.
            if fields[6] == '\\N':
                fields[6] = None

            # Edx-only: if the transaction was within a time period when
            # Otto was storing basket-id values instead of payment_ref_ids in
            # its transactions, then apply a heuristic to the transactions
            # from that period to convert them to a payment_ref_id that should
            # work in most cases.
            if fields[0] > '2015-05-01' and fields[0] < '2015-06-14':
                if len(key) <= 4 and key not in LOW_ORDER_ID_SHOPPINGCART_ORDERS:
                    key = 'EDX-{}'.format(int(key) + 100000)
        else:
            raise ValueError("ERROR: unrecognized line with {} fields:  {}".format(len(fields), line))

        yield key, (record_type, fields)

    def _orderitem_is_white_label(self, orderitem):
        """Identify white-label orders in shoppingcart by heuristic."""
        # Currently, only white-label courses have product_ids that are either 2 or 3.
        return orderitem.order_processor == 'shoppingcart' and orderitem.line_item_product_id in ['2', '3']

    def _orderitem_status_is_consistent(self, orderitem):
        """Check that orderitem's status matches its refunded_amount."""
        return (
            (orderitem.status == 'purchased' and orderitem.refunded_amount == 0.0) or
            (orderitem.status == 'refunded' and orderitem.refunded_amount > 0.0)
        )

    def _check_orderitem_wrongstatus(self, orderitem, status):
        """Add a prefix to orderitem's audit status to indicate inconsistency in status."""
        if self._orderitem_status_is_consistent(orderitem):
            return status
        else:
            return "ERROR_WRONGSTATUS_{}".format(status)

    def _get_partner(self, course_id):
        """Heuristic to determine the partner short code of order items from ShoppingCart."""
        org = get_org_id_for_course(course_id)
        return self.shoppingcart_partners_dict.get(org) or self.default_partner_short_code

    def extract_transactions(self, values):
        """
        Pulls orderitems and transactions out of input values iterable.
        """
        orderitems = []
        transactions = []
        for (record_type, fields) in values:
            if record_type == 'OrderItemRecord':
                if not fields[ORDERITEM_FIELD_INDICES['partner_short_code']]:
                    fields[ORDERITEM_FIELD_INDICES['partner_short_code']] = self._get_partner(
                        fields[ORDERITEM_FIELD_INDICES['course_id']]
                    )
                orderitems.append(OrderItemRecord(*fields))
            elif record_type == 'TransactionRecord':
                transactions.append(TransactionRecord(*fields))
        # Standardize the ordering.
        orderitems = sorted(orderitems, key=attrgetter('date_placed', 'line_item_id'))
        transactions = sorted(transactions, key=attrgetter('date', 'transaction_id'))
        return orderitems, transactions

    def _get_audit_code_for_orders_without_transactions(self, orderitems):
        """
        Return an audit_code for each orderitem when there are no transactions.

        Orders without transactions happens when an order is begun but
        the user changes their mind.  Also included are registrations
        that have no cost, so having no transactions is actually a
        reasonable state.

        More are due to a difference in the timing of the orders and
        the transaction extraction.  At present, the orders are
        pulled at whatever time the task is run, and they are dumped.
        For transactions, the granularity is daily: we only have up
        through yesterday's.  So there may be orders from today that
        don't yet have transactions downloaded.

        There are also some professional-ed entries that have
        transactions in a different account.

        """
        for orderitem in orderitems:
            order_audit_code = 'ERROR_ORDER_NOT_BALANCED'
            orderitem_audit_code = 'ERROR_NO_TRANSACTION'
            transaction_audit_code = 'NO_TRANSACTION'
            if self._orderitem_is_white_label(orderitem):
                # Missing white-label is not an error, even if it's not balanced.
                order_audit_code = 'ORDER_NOT_BALANCED'
                orderitem_audit_code = 'NO_TRANS_WHITE_LABEL'
            elif orderitem.line_item_price == 0.0:
                # The order is for a free course, or has been discounted 100%, or an enrollment
                # code has been used. We don't expect a transaction for such orders.
                order_audit_code = 'ORDER_BALANCED'
                orderitem_audit_code = 'NO_COST'

            # Note that we don't call "check_orderitem_wrongstatus" here, as the
            # existing status is generally sufficient.  In the case of "NO_COST"
            # honor enrollment orders, they may in fact be refunded when a user unenrolls,
            # but the refund is zero.
            audit_code = (order_audit_code, orderitem_audit_code, transaction_audit_code)
            yield audit_code, orderitem

    def _get_audit_code_for_transactions_without_orderitems(self, transactions, trans_balance):
        """
        Return an audit_code for each transaction when there are no orderitems.

        This is likely when the transaction pull is newer than the
        order pull, or if a shoppingcart basket was charged that was
        not marked as a purchased order.  In the latter case, if the
        charge was later refunded and the current balance is zero,
        then no further action is needed.  Otherwise either the order
        needs to be updated (to reflect that they did actually receive
        what they ordered), or the balance should be refunded (because
        they never received what they were charged for).

        """
        order_audit_code = 'NO_ORDER_ZERO_BALANCE' if trans_balance == 0 else 'ERROR_NO_ORDER_NONZERO_BALANCE'
        for transaction in transactions:
            trans_audit_code = 'PURCHASE' if transaction.amount >= 0.0 else 'REFUND'
            audit_code = (order_audit_code, 'NO_ORDERITEM', trans_audit_code)
            yield audit_code, transaction

    def _deduplicate_transactions(self, transactions):
        """
        Due to various bugs in external pulls, it seems possible to get the same transactions appearing in
        the transaction reports pulled on different days.  If we assume that the transaction ID is unique,
        then we can handle these bugs by deduplicating based on transaction ID.  We will treat the first
        transaction with a given ID as being the "real" transaction date, in the case of differing dates.
        Since we're doing this in the reducer, we already know the transactions all share the same payment_ref_id.
        We know the transactions are sorted by date, so we just take the first transaction we encounter
        for each transaction_id.
        """
        new_transactions = []
        seen_transaction_ids = set()
        for transaction in transactions:
            if transaction.transaction_id not in seen_transaction_ids:
                new_transactions.append(transaction)
                seen_transaction_ids.add(transaction.transaction_id)
        return new_transactions

    def reducer(self, _key, values):
        """Convert orderitems and transactions into orderitem-transaction records."""
        orderitems, transactions = self.extract_transactions(values)

        transactions = self._deduplicate_transactions(transactions)

        # Check to see that all orderitems belong to the same order.
        distinct_order_ids = set([orderitem.order_id for orderitem in orderitems])
        if len(distinct_order_ids) > 1:
            orderitem_ids = [orderitem.line_item_id for orderitem in orderitems]
            raise Exception("ERROR: orderitems {} encountered with different order_ids {}".format(
                orderitem_ids,
                distinct_order_ids,
            ))

        # Calculate common values.
        trans_balance = Decimal(0.0)
        if len(transactions) > 0:
            trans_balance = sum([transaction.amount for transaction in transactions])
        order_balance = Decimal(0.0)
        if len(orderitems):
            order_balance = sum(
                [orderitem.line_item_price - orderitem.refunded_amount for orderitem in orderitems]
            )

        if len(transactions) == 0:
            for audit_code, orderitem in self._get_audit_code_for_orders_without_transactions(orderitems):
                yield self.format_transaction_table_output(audit_code, None, orderitem)
        elif len(orderitems) == 0:
            audit_code_trans = self._get_audit_code_for_transactions_without_orderitems(transactions, trans_balance)
            for audit_code, transaction in audit_code_trans:
                yield self.format_transaction_table_output(audit_code, transaction, None)
        else:
            # This is the main case, where transactions are mapped to orderitems.
            order_audit_code = "ORDER_BALANCED" if trans_balance == order_balance else "ERROR_ORDER_NOT_BALANCED"
            orderitem_transactions = self._map_transactions_to_orderitems(orderitems, transactions)
            orderitems_notrans = set(orderitems)
            for orderitem in orderitem_transactions:
                trans_list = orderitem_transactions[orderitem]
                orderitems_notrans.remove(orderitem)
                # Get the audit_code of the orderitem:
                transaction_balance = Decimal(0.0)
                trans_audit_codes = []
                for trans_entry in trans_list:
                    transaction, value, trans_audit_code, _fee = trans_entry
                    transaction_balance += value
                    trans_audit_codes.append(trans_audit_code)
                orderitem_audit_code = self._get_orderitem_audit_code(orderitem, transaction_balance, trans_audit_codes)
                for trans_entry in trans_list:
                    transaction, value, trans_audit_code, fee = trans_entry
                    audit_code = (order_audit_code, orderitem_audit_code, trans_audit_code)
                    yield self.format_transaction_table_output(audit_code, transaction, orderitem, value, fee)

            # Finally output the orderitems that have no transactions.
            for audit_code, orderitem in self._get_audit_code_for_orders_without_transactions(orderitems_notrans):
                _, orderitem_audit_code, transaction_audit_code = audit_code
                audit_code = (order_audit_code, orderitem_audit_code, transaction_audit_code)
                yield self.format_transaction_table_output(audit_code, None, orderitem)

    def _get_orderitem_audit_code(self, orderitem, trans_balance, trans_audit_codes):
        """Infer audit_code of orderitem based on balance of associated transactions."""
        orderitem_balance = orderitem.line_item_price - orderitem.refunded_amount
        audit_code = "ERROR_{}_BALANCE_NOT_MATCHING".format(orderitem.status.upper())
        if trans_balance == orderitem_balance:
            audit_code = "{}_BALANCE_MATCHING".format(orderitem.status.upper())
        elif trans_balance == orderitem.line_item_price:
            # If these are equal, then the refunded_amount must be non-zero,
            # and not have a matching transaction.
            audit_code = "{}_REFUND_MISSING".format(audit_code)
        elif trans_balance == 0.0:
            audit_code = "{}_WAS_REFUNDED".format(audit_code)
        elif (trans_balance == -1 * orderitem.line_item_price and
              ('REFUND_AGAIN' in trans_audit_codes or 'REFUND_AGAIN_STATUS_NOT_REFUNDED' in trans_audit_codes)):
            audit_code = "{}_WAS_REFUNDED_TWICE".format(audit_code)
        elif trans_balance == 2 * orderitem.line_item_price and 'PURCHASE_AGAIN' in trans_audit_codes:
            audit_code = "{}_WAS_CHARGED_TWICE".format(audit_code)
        elif trans_balance < 0.0:
            audit_code = "{}_OVER_REFUND".format(audit_code)
        elif trans_balance < orderitem.line_item_price:
            if any([code.startswith('REFUND') for code in trans_audit_codes]):
                # Only mark as a partial refund if there were any refunds at all.
                audit_code = "{}_PARTIAL_REFUND".format(audit_code)
            else:
                audit_code = "{}_UNDER_CHARGE".format(audit_code)
        elif trans_balance > orderitem.line_item_price:
            audit_code = "{}_OVER_CHARGE".format(audit_code)

        # Prepend modifiers.
        audit_code = self._check_orderitem_wrongstatus(orderitem, audit_code)
        if self._orderitem_is_white_label(orderitem):
            audit_code = "ERROR_WHITE_LABEL_{}".format(audit_code)
        return audit_code

    def _get_purchase_audit_code(self, orderitem, transaction_amount, orderitem_purchases):
        """
        Return an audit_code string depending on whether the item is already purchased or refunded.

        Possible values:

          PURCHASE_ONE:  a regular purchase of an item for its cost, with no previous purchases.
          PURCHASE_MISCHARGE:  a regular purchase of an item for its cost, with no previous purchases.
          PURCHASE_AGAIN:  a purchase of an item for its cost, but with previous purchases of it already made.
          PURCHASE_AGAIN_MISCHARGE:  a purchase of an item for its cost, but with previous purchases of it already made.

        """
        orderitem_cost = orderitem.line_item_price
        if orderitem in orderitem_purchases:
            if transaction_amount == orderitem_cost:
                transaction_audit_code = 'PURCHASE_AGAIN'
            else:
                transaction_audit_code = 'PURCHASE_AGAIN_MISCHARGE'
        else:
            if transaction_amount == orderitem_cost:
                transaction_audit_code = 'PURCHASE_ONE'
            else:
                transaction_audit_code = 'PURCHASE_MISCHARGE'
        return transaction_audit_code

    def _get_simple_purchase_audit_code(self, orderitem, orderitem_purchases, _orderitem_refunds):
        """Return an audit_code string to be used when distributing a purchase transaction over orderitems."""
        return 'PURCHASE_AGAIN' if orderitem in orderitem_purchases else 'PURCHASE_ONE'

    def _get_orderitem_to_purchase(self, target_transaction_audit_code, transaction, orderitems, orderitem_purchases):
        """Loop through orderitems, and 'purchase' the first one with a matching transaction_audit_code."""
        for orderitem in orderitems:
            purchase_audit_code = self._get_purchase_audit_code(orderitem, transaction.amount, orderitem_purchases)
            if purchase_audit_code == target_transaction_audit_code:
                return orderitem
        return None

    def _split_transaction_over_orderitems(self, orderitems, transaction, get_audit_code, orderitem_purchases, orderitem_refunds=None):
        """
        When a transaction matches the value of all orderitems, split the transaction across all of them.
        Use the same code to handle purchases and refunds.

        Split the transaction fee across orderitems as well.
        """
        total_fees = Decimal(0.0)
        orderitem_map_to_update = orderitem_refunds if orderitem_refunds is not None else orderitem_purchases
        for index, orderitem in enumerate(orderitems):
            orderitem_cost = orderitem.line_item_price * -1 if orderitem_refunds is not None else orderitem.line_item_price
            transaction_audit_code = get_audit_code(orderitem, orderitem_purchases, orderitem_refunds)
            # Calculate transaction_fee_per_item if there is a transaction fee.
            transaction_fee_per_item = None
            if transaction.transaction_fee is not None:
                if index < (len(orderitems) - 1):
                    proportion = orderitem_cost / transaction.amount
                    transaction_fee_per_item = (transaction.transaction_fee * proportion).quantize(TWOPLACES)
                    total_fees += transaction_fee_per_item
                else:
                    # If it's the last orderitem, make sure we don't have roundoff error.
                    transaction_fee_per_item = (transaction.transaction_fee - total_fees).quantize(TWOPLACES)
            new_value = (transaction, orderitem_cost, transaction_audit_code, transaction_fee_per_item)
            orderitem_map_to_update[orderitem].append(new_value)

    def _map_purchases_to_orderitems(self, orderitems, purchase_transactions):
        """Distributes purchase transactions onto orderitems, splitting if necessary.

        Returns a dict, with orderitems as keys.  Each orderitem's value is a list
        of tuples.  Each tuple in turn contains four objects: the transaction, the
        value of the transaction that goes towards the purchase of the orderitem,
        the audit_code that annotates the transaction's "purchase", and the amount of
        a transaction_fee going to the orderitem.
        """
        orderitem_purchases = defaultdict(list)
        sorted_purchase_transactions = sorted(purchase_transactions, key=attrgetter('date', 'transaction_id'))
        order_cost = sum([orderitem.line_item_price for orderitem in orderitems])

        for transaction in sorted_purchase_transactions:
            if transaction.amount == order_cost:
                self._split_transaction_over_orderitems(orderitems, transaction, self._get_simple_purchase_audit_code, orderitem_purchases)
            else:
                # Try identifying a single orderitem that matches the current transaction in
                # one of the following ways, considering each in turn until a match is found.
                target_purchase_sequence = ['PURCHASE_ONE', 'PURCHASE_MISCHARGE', 'PURCHASE_AGAIN']
                found = False
                for target_transaction_audit_code in target_purchase_sequence:
                    orderitem = self._get_orderitem_to_purchase(
                        target_transaction_audit_code, transaction, orderitems, orderitem_purchases
                    )
                    if orderitem is not None:
                        transaction_fee = transaction.transaction_fee
                        new_value = (transaction, transaction.amount, target_transaction_audit_code, transaction_fee)
                        orderitem_purchases[orderitem].append(new_value)
                        found = True
                        break

                if not found:
                    # We have a payment that doesn't align with one or all orderitems,
                    # so just purchase the first orderitem.
                    # It could be for two out of three orderitems, for example, but that
                    # should be rarer.  More likely is an overpayment or underpayment, and
                    # that is a problem that needs to be flagged if it hasn't already
                    # been addressed.
                    transaction_audit_code = 'PURCHASE_FIRST'
                    transaction_fee = transaction.transaction_fee
                    new_value = (transaction, transaction.amount, transaction_audit_code, transaction_fee)
                    orderitem_purchases[orderitems[0]].append(new_value)

        return orderitem_purchases

    def _get_refund_audit_code(self, orderitem, orderitem_purchases, orderitem_refunds):
        """
        Return a transaction_audit_code string depending on whether the item is already purchased or refunded.

        Possible values:

            REFUND_ONE:  a regular refund of an item for its cost, with a previous purchase
                and no previous refunds.
            REFUND_AGAIN:  a refund of an item for its cost, with a previous purchase and with
                a previous refund already made.
            REFUND_ONE_STATUS_NOT_REFUNDED:  a regular refund of an item for its cost, with a
                previous purchase and no previous refunds, but with the status of the orderitem
                not reflecting that a refund transaction was expected.
            REFUND_AGAIN_STATUS_NOT_REFUNDED:  a refund of an item for its cost, with a previous
                purchase and with a previous refund already made, but with the status of the orderitem
                not reflecting that a refund transaction was expected.
            REFUND_NEVER_PURCHASED:  for any refund of an item for which no purchase was made.

        """
        if orderitem in orderitem_purchases:
            if orderitem in orderitem_refunds:
                transaction_audit_code = 'REFUND_AGAIN' if orderitem.status == 'refunded' else 'REFUND_AGAIN_STATUS_NOT_REFUNDED'
            else:
                transaction_audit_code = 'REFUND_ONE' if orderitem.status == 'refunded' else 'REFUND_ONE_STATUS_NOT_REFUNDED'
        else:
            transaction_audit_code = 'REFUND_NEVER_PURCHASED'
        return transaction_audit_code

    def _get_orderitem_to_refund(self, target_audit_code, transaction, orderitems, orderitem_purchases, orderitem_refunds):
        """Loop through orderitems, and refund the first one with a matching transaction_audit_code."""
        for orderitem in orderitems:
            orderitem_cost = orderitem.line_item_price * -1
            transaction_audit_code = self._get_refund_audit_code(orderitem, orderitem_purchases, orderitem_refunds)
            if transaction.amount == orderitem_cost and transaction_audit_code == target_audit_code:
                return orderitem
        return None

    def _map_refunds_to_orderitems(self, orderitems, refund_transactions, orderitem_purchases):
        """Distributes refund transactions onto orderitems, splitting if necessary.

        Returns a dict, with orderitems as keys.  Each orderitem's value is a list
        of tuples.  Each tuple in turn contains four objects: the transaction, the
        value of the transaction that goes towards the refund of the orderitem,
        the audit_code that annotates the transaction's "refund", and the amount of
        a transaction_fee going to the orderitem.
        """
        orderitem_refunds = defaultdict(list)
        sorted_refund_transactions = sorted(refund_transactions, key=attrgetter('date', 'transaction_id'))
        order_cost = sum([orderitem.line_item_price * -1 for orderitem in orderitems])

        for transaction in sorted_refund_transactions:
            if transaction.amount == order_cost:
                self._split_transaction_over_orderitems(
                    orderitems, transaction, self._get_refund_audit_code, orderitem_purchases, orderitem_refunds
                )
            else:
                # Transaction_amount != order_cost overall, so first try to find a particular
                # orderitem that has been paid for and should be refunded and has not yet been refunded.
                # Try identifying a single orderitem that matches the current transaction in
                # one of the following ways, considering each in turn until a match is found.
                # First refund those orderitems that have not been refunded yet, favoring those orderitems
                # that have their status set to indicate that the orderitem should be refunded.
                # Then repeat the search on those orderitems that have already been refunded.
                target_refund_sequence = [
                    'REFUND_ONE',
                    'REFUND_ONE_STATUS_NOT_REFUNDED',
                    'REFUND_AGAIN',
                    'REFUND_AGAIN_STATUS_NOT_REFUNDED',
                ]
                found = False
                for target_transaction_audit_code in target_refund_sequence:
                    orderitem = self._get_orderitem_to_refund(
                        target_transaction_audit_code, transaction, orderitems, orderitem_purchases, orderitem_refunds
                    )
                    if orderitem is not None:
                        transaction_fee = transaction.transaction_fee
                        new_value = (transaction, transaction.amount, target_transaction_audit_code, transaction_fee)
                        orderitem_refunds[orderitem].append(new_value)
                        found = True
                        break

                if not found:
                    # If we haven't found anything better, arbitrarily assign the refund to the first orderitem.
                    transaction_audit_code = 'REFUND_FIRST'
                    transaction_fee = transaction.transaction_fee
                    new_value = (transaction, transaction.amount, transaction_audit_code, transaction_fee)
                    orderitem_refunds[orderitems[0]].append(new_value)

        return orderitem_refunds

    def _map_transactions_to_orderitems(self, orderitems, transactions):
        """
        Maps transactions across the orderitems for a given order.

        First assigns all positive transactions to orderitems to find purchases, and then
        uses this information to map negative transactions to orderitems based on their
        purchase state and their status.

        Returns a dict keyed by orderitem objects, with values equal to lists of tuples.
        Each tuple contains a transaction, the amount of the transaction going to the orderitem,
        a transaction audit_code, and the amount of a transaction_fee going to the orderitem.

        """
        # First identify all transactions that are purchases, and apply them to orderitems.
        purchase_transactions = [transaction for transaction in transactions if transaction.amount >= 0]
        orderitem_purchases = self._map_purchases_to_orderitems(orderitems, purchase_transactions)

        # Only apply refunds once all purchases are known.
        refund_transactions = [transaction for transaction in transactions if transaction.amount < 0]
        orderitem_refunds = self._map_refunds_to_orderitems(orderitems, refund_transactions, orderitem_purchases)

        # Combine purchases and refunds back into a single map.
        orderitem_transactions = defaultdict(list)
        for orderitem_dict in [orderitem_purchases, orderitem_refunds]:
            for orderitem in orderitem_dict:
                trans_list = orderitem_dict[orderitem]
                orderitem_transactions[orderitem].extend(trans_list)

        return orderitem_transactions

    def output(self):
        return get_target_from_url(self.output_root)

    def format_transaction_table_output(self, audit_code, transaction, orderitem, transaction_amount_per_item=None,
                                        transaction_fee_per_item=None):
        """Generate an output row from an orderitem and transaction."""

        # Handle cases where per-item values are defaulted.
        if transaction:
            if transaction_amount_per_item is None:
                transaction_amount_per_item = transaction.amount
            if transaction_fee_per_item is None:
                transaction_fee_per_item = transaction.transaction_fee

        org_id = None
        if orderitem:
            org_id = get_org_id_for_course(orderitem.course_id)

        result = [
            audit_code[0],
            audit_code[1],
            audit_code[2],
            orderitem.partner_short_code if orderitem else self.default_partner_short_code,
            orderitem.payment_ref_id if orderitem else transaction.payment_ref_id,
            orderitem.order_id if orderitem else None,
            encode_id(orderitem.order_processor, "order_id", orderitem.order_id) if orderitem else None,
            orderitem.date_placed if orderitem else None,
            # transaction information
            transaction.date if transaction else None,
            transaction.transaction_id if transaction else None,
            encode_id(transaction.payment_gateway_id, "transaction_id", transaction.transaction_id) if transaction else None,
            transaction.payment_gateway_id if transaction else None,
            transaction.payment_gateway_account_id if transaction else None,
            transaction.transaction_type if transaction else None,
            transaction.payment_method if transaction else None,
            transaction.amount if transaction else None,
            transaction.iso_currency_code if transaction else None,
            transaction.transaction_fee if transaction else None,
            # mapping information: part of transaction that applies to this orderitem
            str(transaction_amount_per_item) if transaction_amount_per_item is not None else None,
            str(transaction_fee_per_item) if transaction_fee_per_item is not None else None,
            # orderitem information
            orderitem.line_item_id if orderitem else None,
            encode_id(orderitem.order_processor, "line_item_id", orderitem.line_item_id) if orderitem else None,
            orderitem.line_item_product_id if orderitem else None,
            orderitem.line_item_price if orderitem else None,
            orderitem.line_item_unit_price if orderitem else None,
            orderitem.line_item_quantity if orderitem else None,
            orderitem.coupon_id if orderitem else None,
            orderitem.discount_amount if orderitem else None,
            orderitem.voucher_id if orderitem else None,
            orderitem.voucher_code if orderitem else None,
            orderitem.refunded_amount if orderitem else None,
            orderitem.refunded_quantity if orderitem else None,
            orderitem.user_id if orderitem else None,
            orderitem.username if orderitem else None,
            None,  # TODO: remove this column from the table
            orderitem.product_class if orderitem else None,
            orderitem.product_detail if orderitem else None,
            orderitem.course_id if orderitem else None,
            org_id if org_id is not None else None,
            orderitem.order_processor if orderitem else None,
            orderitem.course_uuid if orderitem else None,
            orderitem.expiration_date if orderitem else None,
        ]
        return (OrderTransactionRecord(*result).to_tsv(),)


OrderTransactionRecordBase = namedtuple("OrderTransactionRecord", [  # pylint: disable=invalid-name
    "order_audit_code",
    "orderitem_audit_code",
    "transaction_audit_code",
    "partner_short_code",
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
    "order_coupon_id",
    "order_discount_amount",
    "order_voucher_id",
    "order_voucher_code",
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
    "course_uuid",
    "expiration_date",
])


class OrderTransactionRecord(OrderTransactionRecordBase):
    """Stores transaction-orderitem mapping output."""

    def to_tsv(self):
        """Serializes the record to a TSV-formatted string."""
        return '\t'.join([str(v) if v is not None else "\\N" for v in self])

    @staticmethod
    def from_job_output(tsv_str):
        """Constructor that reads format generated by to_tsv()."""
        record = tsv_str.split('\t')
        nulled_record = [v if v != "\\N" else None for v in record]
        return OrderTransactionRecord(*nulled_record)


class ReconciledOrderTransactionTableTask(ReconcileOrdersAndTransactionsDownstreamMixin, HiveTableTask):
    """Load reconciled order and transaction information into Hive table."""

    output_root = None

    @property
    def table(self):
        return 'reconciled_order_transactions'

    @property
    def columns(self):
        return [
            ('order_audit_code', 'STRING'),
            ('orderitem_audit_code', 'STRING'),
            ('transaction_audit_code', 'STRING'),
            ('partner_short_code', 'STRING'),
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
            ('transaction_amount', hive_decimal_type(12, 2)),
            ('transaction_iso_currency_code', 'STRING'),
            ('transaction_fee', hive_decimal_type(12, 2)),
            ('transaction_amount_per_item', hive_decimal_type(12, 2)),
            ('transaction_fee_per_item', hive_decimal_type(12, 2)),
            ('order_line_item_id', 'INT'),
            ('unique_order_line_item_id', 'STRING'),
            ('order_line_item_product_id', 'INT'),
            ('order_line_item_price', hive_decimal_type(12, 2)),
            ('order_line_item_unit_price', hive_decimal_type(12, 2)),
            ('order_line_item_quantity', 'INT'),
            ('order_coupon_id', 'INT'),
            ('order_discount_amount', hive_decimal_type(12, 2)),
            ('order_voucher_id', 'INT'),
            ('order_voucher_code', 'STRING'),
            ('order_refunded_amount', hive_decimal_type(12, 2)),
            ('order_refunded_quantity', 'INT'),
            ('order_user_id', 'INT'),
            ('order_username', 'STRING'),
            ('order_user_email', 'STRING'),
            ('order_product_class', 'STRING'),
            ('order_product_detail', 'STRING'),
            ('order_course_id', 'STRING'),
            ('order_org_id', 'STRING'),
            ('order_processor', 'STRING'),
            ('course_uuid', 'STRING'),
            ('expiration_date', 'TIMESTAMP'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    def requires(self):
        return ReconcileOrdersAndTransactionsTask(
            import_date=self.import_date,
            output_root=self.partition_location
        )


class LoadInternalReportingOrderTransactionsToWarehouse(ReconcileOrdersAndTransactionsDownstreamMixin, WarehouseMixin, VerticaCopyTask):
    """
    Loads order-transaction table from Hive into the Vertica data warehouse.
    """
    @property
    def insert_source_task(self):
        # This gets added to what requires() yields in VerticaCopyTask.

        return (
            ReconcileOrdersAndTransactionsTask(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                # Get the location of the Hive table, so it can be opened and read.
                output_root=url_path_join(
                    self.warehouse_path,
                    'reconciled_order_transactions',
                    'dt=' + self.import_date.isoformat()  # pylint: disable=no-member
                ) + '/',
                # DO NOT PASS OVERWRITE FURTHER.  We mean for overwrite here
                # to just apply to the writing to Vertica, not to anything further upstream.
                # overwrite=self.overwrite,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            )
        )

    @property
    def table(self):
        return 'f_orderitem_transactions'

    @property
    def auto_primary_key(self):
        """No automatic primary key here."""
        return None

    @property
    def columns(self):
        """
        Most values are mapped back to their original table definitions.
        """
        return [
            ('order_audit_code', 'VARCHAR(255)'),
            ('orderitem_audit_code', 'VARCHAR(255)'),
            ('transaction_audit_code', 'VARCHAR(255)'),
            ('partner_short_code', 'VARCHAR(8)'),
            ('payment_ref_id', 'VARCHAR(128)'),
            ('order_id', 'INTEGER'),
            ('unique_order_id', 'VARCHAR(255)'),
            ('order_timestamp', 'TIMESTAMP'),  # datetime seems to be interchangeable
            ('transaction_date', 'VARCHAR(128)'),
            ('transaction_id', 'VARCHAR(128)'),
            ('unique_transaction_id', 'VARCHAR(255)'),
            ('transaction_payment_gateway_id', 'VARCHAR(128)'),
            ('transaction_payment_gateway_account_id', 'VARCHAR(128)'),
            ('transaction_type', 'VARCHAR(255)'),
            ('transaction_payment_method', 'VARCHAR(128)'),
            ('transaction_amount', 'DECIMAL(12,2)'),
            ('transaction_iso_currency_code', 'VARCHAR(12)'),
            ('transaction_fee', 'DECIMAL(12,2)'),
            ('transaction_amount_per_item', 'DECIMAL(12,2)'),
            ('transaction_fee_per_item', 'DECIMAL(12,2)'),
            ('order_line_item_id', 'INTEGER'),
            ('unique_order_line_item_id', 'VARCHAR(255)'),
            ('order_line_item_product_id', 'INTEGER'),
            ('order_line_item_price', 'DECIMAL(12,2)'),
            ('order_line_item_unit_price', 'DECIMAL(12,2)'),
            ('order_line_item_quantity', 'INTEGER'),
            ('order_coupon_id', 'INTEGER'),
            ('order_discount_amount', 'DECIMAL(12,2)'),
            ('order_voucher_id', 'INTEGER'),
            ('order_voucher_code', 'VARCHAR(255)'),
            ('order_refunded_amount', 'DECIMAL(12,2)'),
            ('order_refunded_quantity', 'INTEGER'),
            ('order_user_id', 'INTEGER'),
            ('order_username', 'VARCHAR(30)'),
            ('order_user_email', 'VARCHAR(254)'),
            ('order_product_class', 'VARCHAR(128)'),
            ('order_product_detail', 'VARCHAR(255)'),  # originally longtext
            ('order_course_id', 'VARCHAR(255)'),  # originally longtext
            ('order_org_id', 'VARCHAR(128)'),  # pulled from course_id
            ('order_processor', 'VARCHAR(32)'),
            ('course_uuid', 'VARCHAR(255)'),
            ('expiration_date', 'TIMESTAMP'),
        ]


# =======
#
# Add support here for "new" "full" order information, intended to be used for the next round of
# "financial_reporting" SQL scripts.
#
# It creates a separate, parallel "full" order that changes some of the columns stored for orders but applies the same reconciliation logic.
#
# Changes include:
#  * Removed email.
#  * Added product_course_id, lms_user_id, and partner_sku fields.
#
# =======

FULLORDERITEM_FIELDS = [
    'order_processor',   # "shoppingcart" or "otto"
    'user_id',  # this is the order system's user_id, not the same as auth_user's user_id.
    'order_id',
    'line_item_id',
    'line_item_product_id',  # for "shoppingcart", this is the kind of orderitem table.
    'line_item_price',
    'line_item_unit_price',
    'line_item_quantity',
    'product_class',  # e.g. seat, donation
    'course_id',  # Was called course_key, but is explicitly referenced in reconcile code as course_id.
    'product_detail',  # contains course mode
    'username',
    'date_placed',
    'iso_currency_code',
    'discount_amount',
    'status',
    'refunded_amount',
    'refunded_quantity',
    'payment_ref_id',  # This is the value to compare with the transactions.
    'partner_short_code',
    'course_uuid',  # UUID of the course, NOT course run
    'lms_user_id',
    'partner_sku',
]

FULLORDERITEM_FIELD_INDICES = {field_name: index for index, field_name in enumerate(FULLORDERITEM_FIELDS)}

BaseFullOrderItemRecord = namedtuple('FullOrderItemRecord', FULLORDERITEM_FIELDS)  # pylint: disable=invalid-name


class FullOrderItemRecord(BaseFullOrderItemRecord):
    """Override vanilla namedtuple to redefine types."""
    def __new__(cls, *args, **kwargs):
        result = super(FullOrderItemRecord, cls).__new__(cls, *args, **kwargs)
        result = result._replace(  # pylint: disable=no-member,protected-access
            refunded_amount=Decimal(result.refunded_amount),  # pylint: disable=no-member
            line_item_price=Decimal(result.line_item_price),  # pylint: disable=no-member
            line_item_unit_price=Decimal(result.line_item_unit_price),  # pylint: disable=no-member
            discount_amount=Decimal(result.discount_amount),  # pylint: disable=no-member
        )
        return result


FullOrderTransactionRecordBase = namedtuple("FullOrderTransactionRecord", [  # pylint: disable=invalid-name
    "order_audit_code",
    "orderitem_audit_code",
    "transaction_audit_code",
    "partner_short_code",
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
    "order_discount_amount",
    "order_refunded_amount",
    "order_refunded_quantity",
    "order_user_id",
    "order_username",
    "order_product_class",
    "order_product_detail",
    "order_course_id",
    "order_org_id",
    "order_processor",
    "course_uuid",
    "lms_user_id",
    "partner_sku",
    "order_status",
])


class FullOrderTransactionRecord(FullOrderTransactionRecordBase):
    """Stores transaction-orderitem mapping output."""

    def to_tsv(self):
        """Serializes the record to a TSV-formatted string."""
        return '\t'.join([str(v) if v is not None else "\\N" for v in self])

    @staticmethod
    def from_job_output(tsv_str):
        """Constructor that reads format generated by to_tsv()."""
        record = tsv_str.split('\t')
        nulled_record = [v if v != "\\N" else None for v in record]
        return FullOrderTransactionRecord(*nulled_record)


class ReconcileFullOrdersAndTransactionsTask(ReconcileOrdersAndTransactionsTask):
    """
    Compare all orders and transactions.

    """

    def requires(self):
        yield (
            FullOttoOrderTableTask(
                import_date=self.import_date
            ),
            FullShoppingcartOrderTableTask(
                import_date=self.import_date
            ),
            PaymentTask(
                import_date=self.import_date,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            )
        )

    def mapper(self, line):
        fields = line.split('\t')
        if len(fields) == len(FULLORDERITEM_FIELDS):
            # Assume it's an order.
            record_type = FullOrderItemRecord.__name__
            key_index = FULLORDERITEM_FIELDS.index('payment_ref_id')
            key = fields[key_index]

            # Convert Hive null values ('\\N') in fields like 'product_detail':
            defaults = (
                ('product_detail', ''),
                ('refunded_amount', '0.0'),
                ('refunded_quantity', '0'),
                ('discount_amount', '0.0'),
                ('partner_short_code', ''),
                ('course_uuid', None),
                ('lms_user_id', None),
                ('partner_sku', ''),
            )
            for field_name, default_value in defaults:
                index = FULLORDERITEM_FIELD_INDICES[field_name]
                if fields[index] == '\\N':
                    fields[index] = default_value

        elif len(fields) == len(TRANSACTION_FIELDS):
            # Assume it's a transaction.
            record_type = TransactionRecord.__name__
            key = fields[3]  # payment_ref_id
            # Convert nulls in 'transaction_fee'.
            if fields[6] == '\\N':
                fields[6] = None

            # Edx-only: if the transaction was within a time period when
            # Otto was storing basket-id values instead of payment_ref_ids in
            # its transactions, then apply a heuristic to the transactions
            # from that period to convert them to a payment_ref_id that should
            # work in most cases.
            if fields[0] > '2015-05-01' and fields[0] < '2015-06-14':
                if len(key) <= 4 and key not in LOW_ORDER_ID_SHOPPINGCART_ORDERS:
                    key = 'EDX-{}'.format(int(key) + 100000)
        else:
            raise ValueError("ERROR: unrecognized line with {} fields:  {}".format(len(fields), line))

        yield key, (record_type, fields)

    def extract_transactions(self, values):
        """
        Pulls orderitems and transactions out of input values iterable.
        """
        orderitems = []
        transactions = []
        for (record_type, fields) in values:
            if record_type == 'FullOrderItemRecord':
                if not fields[FULLORDERITEM_FIELD_INDICES['partner_short_code']]:
                    fields[FULLORDERITEM_FIELD_INDICES['partner_short_code']] = self._get_partner(
                        fields[FULLORDERITEM_FIELD_INDICES['course_id']]
                    )
                orderitems.append(FullOrderItemRecord(*fields))
            elif record_type == 'TransactionRecord':
                transactions.append(TransactionRecord(*fields))
        # Standardize the ordering.
        orderitems = sorted(orderitems, key=attrgetter('date_placed', 'line_item_id'))
        transactions = sorted(transactions, key=attrgetter('date', 'transaction_id'))
        return orderitems, transactions

    def format_transaction_table_output(self, audit_code, transaction, orderitem, transaction_amount_per_item=None,
                                        transaction_fee_per_item=None):
        """Generate an output row from an orderitem and transaction."""

        # Handle cases where per-item values are defaulted.
        if transaction:
            if transaction_amount_per_item is None:
                transaction_amount_per_item = transaction.amount
            if transaction_fee_per_item is None:
                transaction_fee_per_item = transaction.transaction_fee

        org_id = None
        if orderitem:
            org_id = get_org_id_for_course(orderitem.course_id)

        result = [
            audit_code[0],
            audit_code[1],
            audit_code[2],
            orderitem.partner_short_code if orderitem else self.default_partner_short_code,
            orderitem.payment_ref_id if orderitem else transaction.payment_ref_id,
            orderitem.order_id if orderitem else None,
            encode_id(orderitem.order_processor, "order_id", orderitem.order_id) if orderitem else None,
            orderitem.date_placed if orderitem else None,
            # transaction information
            transaction.date if transaction else None,
            transaction.transaction_id if transaction else None,
            encode_id(transaction.payment_gateway_id, "transaction_id", transaction.transaction_id) if transaction else None,
            transaction.payment_gateway_id if transaction else None,
            transaction.payment_gateway_account_id if transaction else None,
            transaction.transaction_type if transaction else None,
            transaction.payment_method if transaction else None,
            transaction.amount if transaction else None,
            transaction.iso_currency_code if transaction else None,
            transaction.transaction_fee if transaction else None,
            # mapping information: part of transaction that applies to this orderitem
            str(transaction_amount_per_item) if transaction_amount_per_item is not None else None,
            str(transaction_fee_per_item) if transaction_fee_per_item is not None else None,
            # orderitem information
            orderitem.line_item_id if orderitem else None,
            encode_id(orderitem.order_processor, "line_item_id", orderitem.line_item_id) if orderitem else None,
            orderitem.line_item_product_id if orderitem else None,
            orderitem.line_item_price if orderitem else None,
            orderitem.line_item_unit_price if orderitem else None,
            orderitem.line_item_quantity if orderitem else None,
            orderitem.discount_amount if orderitem else None,
            orderitem.refunded_amount if orderitem else None,
            orderitem.refunded_quantity if orderitem else None,
            orderitem.user_id if orderitem else None,
            orderitem.username if orderitem else None,
            orderitem.product_class if orderitem else None,
            orderitem.product_detail if orderitem else None,
            orderitem.course_id if orderitem else None,
            org_id if org_id is not None else None,
            orderitem.order_processor if orderitem else None,
            orderitem.course_uuid if orderitem else None,
            orderitem.lms_user_id if orderitem else None,
            orderitem.partner_sku if orderitem else None,
            orderitem.status if orderitem else None,
        ]
        return (FullOrderTransactionRecord(*result).to_tsv(),)


class LoadInternalReportingFullOrderTransactionsToWarehouse(LoadInternalReportingOrderTransactionsToWarehouse):
    """
    Loads fullorder-transaction table from Hive into the Vertica data warehouse.
    """
    @property
    def insert_source_task(self):
        # This gets added to what requires() yields in VerticaCopyTask.

        return (
            ReconcileFullOrdersAndTransactionsTask(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                # Get the location of the Hive table, so it can be opened and read.
                output_root=url_path_join(
                    self.warehouse_path,
                    self.table,
                    'dt=' + self.import_date.isoformat()  # pylint: disable=no-member
                ) + '/',
                # DO NOT PASS OVERWRITE FURTHER.  We mean for overwrite here
                # to just apply to the writing to Vertica, not to anything further upstream.
                # overwrite=self.overwrite,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            )
        )

    @property
    def table(self):
        return 'f_orderline_transactions'

    @property
    def columns(self):
        """
        Most values are mapped back to their original table definitions.
        """
        return [
            ('order_audit_code', 'VARCHAR(255)'),
            ('orderitem_audit_code', 'VARCHAR(255)'),
            ('transaction_audit_code', 'VARCHAR(255)'),
            ('partner_short_code', 'VARCHAR(8)'),
            ('payment_ref_id', 'VARCHAR(128)'),
            ('order_id', 'INTEGER'),
            ('unique_order_id', 'VARCHAR(255)'),
            ('order_timestamp', 'TIMESTAMP'),  # datetime seems to be interchangeable
            ('transaction_date', 'VARCHAR(128)'),
            ('transaction_id', 'VARCHAR(128)'),
            ('unique_transaction_id', 'VARCHAR(255)'),
            ('transaction_payment_gateway_id', 'VARCHAR(128)'),
            ('transaction_payment_gateway_account_id', 'VARCHAR(128)'),
            ('transaction_type', 'VARCHAR(255)'),
            ('transaction_payment_method', 'VARCHAR(128)'),
            ('transaction_amount', 'DECIMAL(12,2)'),
            ('transaction_iso_currency_code', 'VARCHAR(12)'),
            ('transaction_fee', 'DECIMAL(12,2)'),
            ('transaction_amount_per_item', 'DECIMAL(12,2)'),
            ('transaction_fee_per_item', 'DECIMAL(12,2)'),
            ('order_line_item_id', 'INTEGER'),
            ('unique_order_line_item_id', 'VARCHAR(255)'),
            ('order_line_item_product_id', 'INTEGER'),
            ('order_line_item_price', 'DECIMAL(12,2)'),
            ('order_line_item_unit_price', 'DECIMAL(12,2)'),
            ('order_line_item_quantity', 'INTEGER'),
            ('order_discount_amount', 'DECIMAL(12,2)'),
            ('order_refunded_amount', 'DECIMAL(12,2)'),
            ('order_refunded_quantity', 'INTEGER'),
            ('order_user_id', 'INTEGER'),
            ('order_username', 'VARCHAR(30)'),
            ('order_product_class', 'VARCHAR(128)'),
            ('order_product_detail', 'VARCHAR(255)'),  # originally longtext
            ('order_course_id', 'VARCHAR(255)'),  # originally longtext
            ('order_org_id', 'VARCHAR(128)'),  # pulled from course_id
            ('order_processor', 'VARCHAR(32)'),
            ('course_uuid', 'VARCHAR(255)'),
            ('order_lms_user_id', 'INTEGER'),
            ('order_partner_sku', 'VARCHAR(128)'),
            ('order_status', 'VARCHAR(128)'),
        ]


class BaseLoadFullOrdersToWarehouse(ReconcileOrdersAndTransactionsDownstreamMixin, WarehouseMixin, VerticaCopyTask):
    """
    Loads full_order table from Hive into the Vertica data warehouse.
    """
    @property
    def insert_source_task(self):
        raise NotImplemented

    @property
    def table(self):
        raise NotImplemented

    @property
    def auto_primary_key(self):
        """No automatic primary key here."""
        return None

    @property
    def columns(self):
        """
        Most values are mapped back to their original table definitions.
        """
        return [
            ('order_processor', 'VARCHAR(32)'),
            ('order_user_id', 'INTEGER'),
            ('order_id', 'INTEGER'),
            ('order_line_id', 'INTEGER'),
            ('line_item_product_id', 'INTEGER'),
            ('line_item_price', 'DECIMAL(12,2)'),
            ('line_item_unit_price', 'DECIMAL(12,2)'),
            ('line_item_quantity', 'INTEGER'),
            ('line_product_class', 'VARCHAR(128)'),
            ('line_course_key', 'VARCHAR(255)'),  # originally longtext
            ('line_product_detail', 'VARCHAR(255)'),  # originally longtext
            ('order_username', 'VARCHAR(30)'),
            ('order_timestamp', 'TIMESTAMP'),  # datetime seems to be interchangeable
            ('order_iso_currency_code', 'VARCHAR(12)'),
            ('line_discount_amount', 'DECIMAL(12,2)'),  # Total discount in currency amount, i.e. unit_discount * qty
            ('line_status', 'VARCHAR(128)'),
            ('line_refunded_amount', 'DECIMAL(12,2)'),
            ('line_refunded_quantity', 'INTEGER'),
            ('order_number', 'VARCHAR(128)'),
            ('order_partner_short_code', 'VARCHAR(8)'),
            ('line_course_uuid', 'VARCHAR(255)'),
            ('order_lms_user_id', 'INTEGER'),
            ('line_partner_sku', 'VARCHAR(128)'),
        ]


class LoadInternalReportingFullOttoOrdersToWarehouse(BaseLoadFullOrdersToWarehouse):
    """
    Loads full_order table from Hive into the Vertica data warehouse.
    """
    @property
    def insert_source_task(self):
        # This gets added to what requires() yields in VerticaCopyTask.

        return (
            FullOttoOrderTableTask(
                import_date=self.import_date,
                # DO NOT PASS OVERWRITE FURTHER.  We mean for overwrite here
                # to just apply to the writing to Vertica, not to anything further upstream.
                # overwrite=self.overwrite,
            )
        )

    @property
    def table(self):
        return 'full_otto_orders'


class LoadInternalReportingFullShoppingcartOrdersToWarehouse(BaseLoadFullOrdersToWarehouse):
    """
    Loads full_order table from Hive into the Vertica data warehouse.
    """
    @property
    def insert_source_task(self):
        # This gets added to what requires() yields in VerticaCopyTask.

        return (
            FullShoppingcartOrderTableTask(
                import_date=self.import_date,
                # DO NOT PASS OVERWRITE FURTHER.  We mean for overwrite here
                # to just apply to the writing to Vertica, not to anything further upstream.
                # overwrite=self.overwrite,
            )
        )

    @property
    def table(self):
        return 'full_shoppingcart_orders'


class LoadInternalReportingPaymentsToWarehouse(ReconcileOrdersAndTransactionsDownstreamMixin, WarehouseMixin, VerticaCopyTask):
    """
    Loads full_order table from Hive into the Vertica data warehouse.
    """
    @property
    def insert_source_task(self):
        # This gets added to what requires() yields in VerticaCopyTask.

        return (
            # TODO: This is not the right task.  The level it points to is too high for it to be opened and streamed.
            # Not sure what the right way to do this is, so leavign it out of the "full" load for now.
            PaymentTask(
                import_date=self.import_date,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed,
                # DO NOT PASS OVERWRITE FURTHER.  We mean for overwrite here
                # to just apply to the writing to Vertica, not to anything further upstream.
                # overwrite=self.overwrite,
            )
        )

    @property
    def table(self):
        return 'payments'

    @property
    def auto_primary_key(self):
        """No automatic primary key here."""
        return None

    @property
    def columns(self):
        """
        Most values are mapped back to their original table definitions.
        """
        return [
            ('transaction_date', 'VARCHAR(128)'),
            ('payment_gateway_id', 'VARCHAR(128)'),
            ('payment_gateway_account_id', 'VARCHAR(128)'),
            ('payment_ref_id', 'VARCHAR(128)'),
            ('iso_currency_code', 'VARCHAR(12)'),
            ('transaction_amount', 'DECIMAL(12,2)'),
            ('transaction_fee', 'DECIMAL(12,2)'),
            ('transaction_type', 'VARCHAR(255)'),
            ('payment_method', 'VARCHAR(128)'),
            ('payment_method_type', 'VARCHAR(128)'),
            ('transaction_id', 'VARCHAR(128)'),
        ]


class LoadFullOrderRelatedTablesTask(ReconcileOrdersAndTransactionsDownstreamMixin, WarehouseMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Provide entry-point for generating full order data for financial reports."""

    def requires(self):
        yield (
            LoadInternalReportingFullOrderTransactionsToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=self.overwrite,
                is_empty_transaction_allowed=self.is_empty_transaction_allowed
            ),
            LoadInternalReportingFullShoppingcartOrdersToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=self.overwrite,
            ),
            LoadInternalReportingFullOttoOrdersToWarehouse(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                credentials=self.credentials,
                overwrite=self.overwrite,
            ),
        )
