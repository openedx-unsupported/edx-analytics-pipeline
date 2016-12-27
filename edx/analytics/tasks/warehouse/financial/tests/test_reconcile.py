"""Tests for Order-transaction reconciliation and reporting."""

from ddt import ddt, data, unpack

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.reports.reconcile import (
    ReconcileOrdersAndTransactionsTask,
    BaseOrderItemRecord,
    OrderItemRecord,
    BaseTransactionRecord,
    TransactionRecord,
    OrderTransactionRecord,
    LOW_ORDER_ID_SHOPPINGCART_ORDERS,
)

TEST_DATE = '2015-06-01'
TEST_LATER_DATE = '2015-06-10'
DEFAULT_REF_ID = "EDX-12345"
HIVE_NULL = '\\N'
FIRST_ORDER_ITEM = '2345678'
SECOND_ORDER_ITEM = '2345679'
FIRST_TRANSACTION = '123423453456'
SECOND_TRANSACTION = '212342345345'
THIRD_TRANSACTION = '312342345345'


class ReconciliationTaskMixin(object):
    """Mixin class to provide constructors for input data."""

    def create_orderitem(self, is_refunded=False, **kwargs):
        """Create an OrderItemRecord with default values."""
        params = {
            'order_processor': 'otto',   # "shoppingcart" or "otto"
            'user_id': '12345',
            'order_id': '1234567',
            'line_item_id': FIRST_ORDER_ITEM,
            'line_item_product_id': '1',  # for "shoppingcart", this is the kind of orderitem table.
            'line_item_price': '50.00',
            'line_item_unit_price': '50.00',
            'line_item_quantity': '1',
            'coupon_id': '',
            'discount_amount': '0',
            'voucher_id': '',
            'voucher_code': '',
            'product_class': 'seat',  # e.g. seat, donation
            'course_id': 'edx/demo_course/2014',  # Was called course_key
            'product_detail': 'verified',  # contains course mode
            'username': 'testuser',
            'user_email': 'test@example.com',
            'date_placed': TEST_DATE,
            'iso_currency_code': 'USD',
            'status': 'purchased',
            'refunded_amount': '0.0',
            'refunded_quantity': '0',
            'payment_ref_id': DEFAULT_REF_ID,
            'partner_short_code': 'edx' if kwargs.get('order_processor') != 'shoppingcart' else '',
        }
        if is_refunded:
            params.update(**{
                'refunded_amount': '50.00',
                'refunded_quantity': '1',
                'status': 'refunded',
            })
        params.update(**kwargs)
        return OrderItemRecord(**params)

    def create_transaction(self, **kwargs):
        """Create a TransactionRecord with default values."""
        params = {
            'date': TEST_DATE,
            'payment_gateway_id': 'cybersource',
            'payment_gateway_account_id': 'edx_account',
            'payment_ref_id': DEFAULT_REF_ID,
            'iso_currency_code': 'USD',
            'amount': '50.00',
            'transaction_fee': None,
            'transaction_type': 'sale',
            'payment_method': 'credit_card',
            'payment_method_type': 'visa',
            'transaction_id': FIRST_TRANSACTION,
        }
        params.update(**kwargs)
        return TransactionRecord(**params)

    def create_refunding_transaction(self, **kwargs):
        """Add default refund values to a default transaction."""
        params = {
            'date': TEST_LATER_DATE,
            'amount': '-50.00',
            'transaction_type': 'refund',
            'transaction_id': SECOND_TRANSACTION,
        }
        params.update(**kwargs)
        return self.create_transaction(**params)

    task_class = ReconcileOrdersAndTransactionsTask


@ddt
class ReconciliationTaskMapTest(ReconciliationTaskMixin, MapperTestMixin, unittest.TestCase):
    """Test financial order-transaction mapper"""

    def _convert_record_to_line(self, record):
        """Convert a record, substituting HIVE_NULL for None, for input to mapper."""
        return '\t'.join([str(v) if v is not None else HIVE_NULL for v in record])

    def _convert_record_to_expected_output(self, record):
        """Convert a record, preserving None, for comparison with mapper output."""
        return [str(v) if v is not None else None for v in record]

    def test_bad_line(self):
        line = 'arbitrary stuff'
        with self.assertRaisesRegexp(ValueError, "unrecognized line"):
            self.assert_no_map_output_for(line)

    def test_default_order(self):
        orderitem = self.create_orderitem()
        line = self._convert_record_to_line(orderitem)
        expected_key = DEFAULT_REF_ID
        expected_value = ('OrderItemRecord', self._convert_record_to_expected_output(orderitem))
        self.assert_single_map_output(line, expected_key, expected_value)

    def test_default_transaction(self):
        trans = self.create_transaction()
        line = self._convert_record_to_line(trans)
        expected_key = DEFAULT_REF_ID
        expected_value = ('TransactionRecord', self._convert_record_to_expected_output(trans))
        self.assert_single_map_output(line, expected_key, expected_value)

    @data(
        ('product_detail', ''),
        ('refunded_amount', '0.0'),
        ('refunded_quantity', '0'),
    )
    @unpack
    def test_orderitem_mapped_nulls(self, fieldname, expected_value):
        expected_orderitem = self.create_orderitem(**{fieldname: expected_value})
        params = self.create_orderitem()._asdict()  # pylint: disable=no-member,protected-access
        params[fieldname] = HIVE_NULL
        input_orderitem = BaseOrderItemRecord(**params)
        line = self._convert_record_to_line(input_orderitem)
        expected_key = DEFAULT_REF_ID
        expected_value = ('OrderItemRecord', self._convert_record_to_expected_output(expected_orderitem))
        self.assert_single_map_output(line, expected_key, expected_value)

    @data(
        ('transaction_fee', None),
    )
    @unpack
    def test_transaction_mapped_nulls(self, fieldname, expected_value):
        expected_transaction = self.create_transaction(**{fieldname: expected_value})
        params = self.create_transaction()._asdict()  # pylint: disable=no-member,protected-access
        params[fieldname] = HIVE_NULL
        input_transaction = BaseTransactionRecord(**params)
        line = self._convert_record_to_line(input_transaction)
        expected_key = DEFAULT_REF_ID
        expected_value = ('TransactionRecord', self._convert_record_to_expected_output(expected_transaction))
        self.assert_single_map_output(line, expected_key, expected_value)

    @data(
        ('1000', 'EDX-101000'),
        ('1200', 'EDX-101200'),
        ('3211', 'EDX-103211'),
    )
    @unpack
    def test_mapped_payment_ref_id(self, ref_id, expected_ref_id):
        trans = self.create_transaction(payment_ref_id=ref_id)
        line = self._convert_record_to_line(trans)
        expected_key = expected_ref_id
        expected_value = ('TransactionRecord', self._convert_record_to_expected_output(trans))
        self.assert_single_map_output(line, expected_key, expected_value)

    def test_mapped_payment_ref_id_exceptions(self):
        for ref_id in LOW_ORDER_ID_SHOPPINGCART_ORDERS:
            trans = self.create_transaction(payment_ref_id=ref_id)
            line = self._convert_record_to_line(trans)
            expected_key = ref_id
            expected_value = ('TransactionRecord', self._convert_record_to_expected_output(trans))
            self.assert_single_map_output(line, expected_key, expected_value)


@ddt
class ReconciliationTaskReducerTest(ReconciliationTaskMixin, ReducerTestMixin, unittest.TestCase):
    """Test financial order-transaction reducer"""

    def _check_output(self, inputs, column_values, **extra_values):
        """Compare generated with expected output."""
        # Namedtuple objects need to be converted to lists on input.
        inputs = [(type(inputval).__name__, list(inputval)) for inputval in inputs]
        output = self._get_reducer_output(inputs)
        if not isinstance(column_values, list):
            column_values = [column_values]
        self.assertEquals(len(output), len(column_values), '{0} != {1}'.format(output, column_values))

        def record_sort_key(record):
            """Sort function for records, by order then by transaction."""
            return record.order_line_item_id, record.transaction_id

        sorted_output = sorted([OrderTransactionRecord.from_job_output(output_tuple[0]) for output_tuple in output],
                               key=record_sort_key)

        for record, expected_columns in zip(sorted_output, column_values):
            # This reducer does the packing in a different way, converting to TSV *before*
            # putting into the output.  So unpack that here, and output as a dict,
            # so that column names can be used instead of numbers.
            output_dict = record._asdict()  # pylint: disable=no-member,protected-access
            expected_columns.update(**extra_values)
            for column_num, expected_value in expected_columns.iteritems():
                self.assertEquals(output_dict[column_num], expected_value)

    def test_no_transaction(self):
        inputs = [self.create_orderitem(), ]
        self._check_output(inputs, {
            'order_audit_code': 'ERROR_ORDER_NOT_BALANCED',
            'orderitem_audit_code': 'ERROR_NO_TRANSACTION',
            'transaction_audit_code': 'NO_TRANSACTION',
            'partner_short_code': 'edx',
            'transaction_date': None,
            'transaction_id': None,
            'unique_transaction_id': None,
            'transaction_payment_gateway_id': None,
            'transaction_payment_gateway_account_id': None,
            'transaction_type': None,
            'transaction_payment_method': None,
            'transaction_amount': None,
            'transaction_iso_currency_code': None,
            'transaction_fee': None,
            'transaction_amount_per_item': None,
            'transaction_fee_per_item': None,
        })

    @data(
        ('otto', 'course-v1:MITx+15.071x_3+1T2016', 'edx', 'edx'),
        ('otto', 'course-v1:MITx+15.071x_3+1T2016', '', 'edx'),
        ('otto', 'course-v1:OpenCraftX+12345+1T2016', 'OCX', 'OCX'),
        ('shoppingcart', 'course-v1:MITx+15.071x_3+1T2016', '', 'edx'),
        ('shoppingcart', 'edX/DemoX/Demo_Course', '', 'edx'),
        ('shoppingcart', 'course-v1:OpenCraftX+12345+1T2016', '', 'OCX'),
        ('shoppingcart', 'OpenCraftX/12345/1T2016', '', 'OCX'),
    )
    @unpack
    @with_luigi_config('financial-reports', 'shoppingcart-partners', '{"OpenCraftX": "OCX", "DEFAULT": "edx"}')
    def test_partner_short_code(self, order_processor, course_id, partner_short_code, expected_short_code):
        self.create_task()  # Recreate task to pick up overridden Luigi configuration
        orderitem = self.create_orderitem(
            order_processor=order_processor,
            course_id=course_id,
            partner_short_code=partner_short_code,
        )
        self._check_output([orderitem], {'partner_short_code': expected_short_code})

    @data(
        ('', None),
        ('OCX', 'OCX'),
    )
    @unpack
    @with_luigi_config('financial-reports', 'shoppingcart-partners', '{}')
    def test_partner_short_code_no_default(self, partner_short_code, expected_short_code):
        self.create_task()  # Recreate task to pick up overridden Luigi configuration
        orderitem = self.create_orderitem(partner_short_code=partner_short_code)
        self._check_output([orderitem], {'partner_short_code': expected_short_code})

    def test_honor_order(self):
        # The honor code is not actually important here, the zero price is.
        # But this happens most commonly when people enroll on Otto for honor mode.
        inputs = [self.create_orderitem(**{
            'line_item_price': '0.0',
            'line_item_unit_price': '0.0',
            'product_detail': 'honor',
        }), ]
        self._check_output(inputs, {
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'NO_COST',
            'transaction_audit_code': 'NO_TRANSACTION',
            'transaction_id': None,
        })

    def test_refunded_honor_order(self):
        # The honor code is not actually important here, the zero price is.
        # But this happens most commonly when people enroll on Otto for honor mode.
        inputs = [self.create_orderitem(**{
            'line_item_price': '0.0',
            'line_item_unit_price': '0.0',
            'product_detail': 'honor',
            'status': 'refunded',
            'refunded_amount': '0.0',
            'refunded_quantity': '1',
        }), ]
        self._check_output(inputs, {
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'NO_COST',
            'transaction_audit_code': 'NO_TRANSACTION',
            'transaction_id': None,
        })

    def test_white_label_order_no_transaction(self):
        inputs = [self.create_orderitem(**{
            'order_processor': 'shoppingcart',
            'line_item_product_id': '2',
        }), ]
        self._check_output(inputs, {
            'order_audit_code': 'ORDER_NOT_BALANCED',
            'orderitem_audit_code': 'NO_TRANS_WHITE_LABEL',
            'transaction_audit_code': 'NO_TRANSACTION',
            'transaction_id': None,
        })

    def test_orderitems_from_different_orders(self):
        inputs = [
            self.create_orderitem(order_id='order1'),
            self.create_orderitem(order_id='order2'),
        ]
        with self.assertRaisesRegexp(Exception, 'different order_ids'):
            self._check_output(inputs, {})

    def test_no_order(self):
        purchase = self.create_transaction()
        self._check_output([purchase], {
            'order_audit_code': 'ERROR_NO_ORDER_NONZERO_BALANCE',
            'orderitem_audit_code': 'NO_ORDERITEM',
            'transaction_audit_code': 'PURCHASE',
            'order_id': None,
            'unique_order_id': None,
            'order_timestamp': None,
            'order_line_item_id': None,
            'unique_order_line_item_id': None,
            'order_line_item_product_id': None,
            'order_line_item_price': None,
            'order_line_item_unit_price': None,
            'order_line_item_quantity': None,
            'order_coupon_id': None,
            'order_discount_amount': None,
            'order_voucher_id': None,
            'order_voucher_code': None,
            'order_refunded_amount': None,
            'order_refunded_quantity': None,
            'order_user_id': None,
            'order_username': None,
            'order_user_email': None,
            'order_product_class': None,
            'order_product_detail': None,
            'order_course_id': None,
            'order_org_id': None,
            'order_processor': None,
        })

    def test_no_order_refunded(self):
        purchase = self.create_transaction()
        refund = self.create_refunding_transaction()
        self._check_output([purchase, refund], [
            {'transaction_audit_code': 'PURCHASE'},
            {'transaction_audit_code': 'REFUND'},
        ], **{
            'order_audit_code': 'NO_ORDER_ZERO_BALANCE',
            'orderitem_audit_code': 'NO_ORDERITEM',
        })

    ###################################
    # Single ORDERITEM tests:
    ###################################

    # Single purchase #

    def test_normal_purchase(self):
        orderitem = self.create_orderitem()
        purchase = self.create_transaction()
        self._check_output([orderitem, purchase], {
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'PURCHASED_BALANCE_MATCHING',
            'transaction_audit_code': 'PURCHASE_ONE',
            'transaction_amount_per_item': '50.00',
            'transaction_fee_per_item': None,
        })

    def test_purchase_with_fee(self):
        orderitem = self.create_orderitem()
        purchase = self.create_transaction(transaction_fee='5.00')
        self._check_output([orderitem, purchase], {
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'PURCHASED_BALANCE_MATCHING',
            'transaction_audit_code': 'PURCHASE_ONE',
            'transaction_amount_per_item': '50.00',
            'transaction_fee_per_item': '5.00',
        })

    def test_white_label_purchase(self):
        orderitem = self.create_orderitem(**{
            'order_processor': 'shoppingcart',
            'line_item_product_id': '2',
        })
        purchase = self.create_transaction()
        self._check_output([orderitem, purchase], {
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'ERROR_WHITE_LABEL_PURCHASED_BALANCE_MATCHING',
            'transaction_audit_code': 'PURCHASE_ONE',
        })

    @data(
        (False, '100.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_CHARGE'),
        (False, '10.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_UNDER_CHARGE'),
        (True, '100.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_CHARGE'),
        (True, '50.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_REFUND_MISSING'),
        (True, '10.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_UNDER_CHARGE'),
    )
    @unpack
    def test_mischarge_purchase(self, is_refunded_order, amount, orderitem_status):
        orderitem = self.create_orderitem(is_refunded=is_refunded_order)
        purchase = self.create_transaction(amount=amount)
        self._check_output([orderitem, purchase], {
            'order_audit_code': 'ERROR_ORDER_NOT_BALANCED',
            'orderitem_audit_code': orderitem_status,
            'transaction_audit_code': 'PURCHASE_MISCHARGE' if amount != '50.00' else 'PURCHASE_ONE',
        })

    @data(
        (False, '-100.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (False, '-50.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (False, '-10.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (True, '-100.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (True, '-50.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (True, '-10.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_REFUND'),
    )
    @unpack
    def test_refund_without_purchase(self, is_refunded_order, amount, orderitem_status):
        orderitem = self.create_orderitem(is_refunded=is_refunded_order)
        purchase = self.create_transaction(amount=amount)
        self._check_output([orderitem, purchase], {
            'order_audit_code': 'ERROR_ORDER_NOT_BALANCED',
            'orderitem_audit_code': orderitem_status,
            'transaction_audit_code': 'REFUND_FIRST' if amount != '-50.00' else 'REFUND_NEVER_PURCHASED',
        })

    # Two purchases #

    @data(
        (False, '100.00', 'PURCHASE_FIRST', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_CHARGE'),
        (False, '50.00', 'PURCHASE_AGAIN', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_WAS_CHARGED_TWICE'),
        (False, '10.00', 'PURCHASE_FIRST', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_CHARGE'),
        (True, '100.00', 'PURCHASE_FIRST', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_CHARGE'),
        (True, '50.00', 'PURCHASE_AGAIN', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_WAS_CHARGED_TWICE'),
        (True, '10.00', 'PURCHASE_FIRST', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_CHARGE'),
    )
    @unpack
    def test_second_purchase(self, is_refunded_order, amount, transaction_status, orderitem_status):
        orderitem = self.create_orderitem(is_refunded=is_refunded_order)
        purchase1 = self.create_transaction()
        purchase2 = self.create_transaction(amount=amount)
        self._check_output([orderitem, purchase1, purchase2], [
            {'transaction_audit_code': 'PURCHASE_ONE'},
            {'transaction_audit_code': transaction_status},
        ], **{
            'order_audit_code': 'ERROR_ORDER_NOT_BALANCED',
            'orderitem_audit_code': orderitem_status,
        })

    def test_two_transactions_to_purchase(self):
        orderitem = self.create_orderitem()
        purchase1 = self.create_transaction(amount='25.00')
        purchase2 = self.create_transaction(amount='25.00', transaction_id=SECOND_TRANSACTION)
        self._check_output([orderitem, purchase1, purchase2], [
            {'transaction_audit_code': 'PURCHASE_MISCHARGE', 'transaction_id': FIRST_TRANSACTION},
            {'transaction_audit_code': 'PURCHASE_FIRST', 'transaction_id': SECOND_TRANSACTION},
        ], **{
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'PURCHASED_BALANCE_MATCHING',
        })

    # One purchase and one refund #

    def test_normal_refund(self):
        orderitem = self.create_orderitem(is_refunded=True)
        purchase = self.create_transaction()
        refund = self.create_refunding_transaction()
        self._check_output([orderitem, purchase, refund], [
            {'transaction_audit_code': 'PURCHASE_ONE'},
            {'transaction_audit_code': 'REFUND_ONE'},
        ], **{
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'REFUNDED_BALANCE_MATCHING',
        })

    @data(
        (True, '-100.00', 'REFUND_FIRST', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (True, '-10.00', 'REFUND_FIRST', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_PARTIAL_REFUND'),
        (False, '-100.00', 'REFUND_FIRST', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (False, '-10.00', 'REFUND_FIRST', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_PARTIAL_REFUND'),
        (False, '-50.00', 'REFUND_ONE_STATUS_NOT_REFUNDED', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_WAS_REFUNDED'),
    )
    @unpack
    def test_mischarge_refund(self, is_refunded_order, amount, transaction_status, orderitem_status):
        orderitem = self.create_orderitem(is_refunded=is_refunded_order)
        purchase = self.create_transaction()
        refund = self.create_refunding_transaction(amount=amount)
        self._check_output([orderitem, purchase, refund], [
            {'transaction_audit_code': 'PURCHASE_ONE'},
            {'transaction_audit_code': transaction_status},
        ], **{
            'order_audit_code': 'ERROR_ORDER_NOT_BALANCED',
            'orderitem_audit_code': orderitem_status
        })

    # One purchase and two refunds #

    @data(
        (True, '-100.00', 'REFUND_FIRST', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (True, '-50.00', 'REFUND_AGAIN', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_WAS_REFUNDED_TWICE'),
        (True, '-10.00', 'REFUND_FIRST', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (False, '-100.00', 'REFUND_FIRST', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_REFUND'),
        (False, '-50.00', 'REFUND_AGAIN_STATUS_NOT_REFUNDED', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_WAS_REFUNDED_TWICE'),
        (False, '-10.00', 'REFUND_FIRST', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_REFUND'),
    )
    @unpack
    def test_extra_refund(self, is_refunded_order, amount, transaction_status, orderitem_status):
        orderitem = self.create_orderitem(is_refunded=is_refunded_order)
        purchase = self.create_transaction()
        refund1 = self.create_refunding_transaction()
        refund2 = self.create_refunding_transaction(amount=amount)
        self._check_output([orderitem, purchase, refund1, refund2], [
            {'transaction_audit_code': 'PURCHASE_ONE'},
            {'transaction_audit_code': 'REFUND_ONE' if is_refunded_order else 'REFUND_ONE_STATUS_NOT_REFUNDED'},
            {'transaction_audit_code': transaction_status},
        ], **{
            'order_audit_code': 'ERROR_ORDER_NOT_BALANCED',
            'orderitem_audit_code': orderitem_status,
        })

    def test_double_refund_to_refund(self):
        orderitem = self.create_orderitem(is_refunded=True)
        purchase = self.create_transaction()
        refund1 = self.create_refunding_transaction(amount='-30.00')
        refund2 = self.create_refunding_transaction(amount='-20.00')
        self._check_output([orderitem, purchase, refund1, refund2], [
            {'transaction_audit_code': 'PURCHASE_ONE'},
            {'transaction_audit_code': 'REFUND_FIRST'},
            {'transaction_audit_code': 'REFUND_FIRST'},
        ], **{
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'REFUNDED_BALANCE_MATCHING',
        })

    ###################################
    # Multiple ORDERITEM tests:
    ###################################

    @data(
        (False, 'PURCHASED_BALANCE_MATCHING', 'ORDER_BALANCED'),
        (True, 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_REFUND_MISSING', 'ERROR_ORDER_NOT_BALANCED'),
    )
    @unpack
    def test_single_purchase_two_orderitems(self, is_refunded_order, orderitem_status, order_status):
        orderitem1 = self.create_orderitem(is_refunded=is_refunded_order)
        orderitem2 = self.create_orderitem(line_item_id=SECOND_ORDER_ITEM)
        purchase = self.create_transaction(amount='100.00')
        self._check_output([orderitem1, orderitem2, purchase], [
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'orderitem_audit_code': orderitem_status,
                'transaction_audit_code': 'PURCHASE_ONE',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'orderitem_audit_code': 'PURCHASED_BALANCE_MATCHING',
                'transaction_audit_code': 'PURCHASE_ONE',
            },
        ], **{
            'order_audit_code': order_status,
        })

    @data(
        (False, 'PURCHASED_BALANCE_MATCHING', 'ORDER_BALANCED'),
        (True, 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_REFUND_MISSING', 'ERROR_ORDER_NOT_BALANCED'),
    )
    @unpack
    def test_single_purchase_two_orderitems_with_fee(self, is_refunded_order, orderitem_status, order_status):
        orderitem1 = self.create_orderitem(is_refunded=is_refunded_order)
        orderitem2 = self.create_orderitem(line_item_id=SECOND_ORDER_ITEM)
        purchase = self.create_transaction(amount='100.00', transaction_fee='5.00')
        self._check_output([orderitem1, orderitem2, purchase], [
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'orderitem_audit_code': orderitem_status,
                'transaction_audit_code': 'PURCHASE_ONE',
                'transaction_amount_per_item': '50.00',
                'transaction_fee': '5.00',
                'transaction_fee_per_item': '2.50',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'orderitem_audit_code': 'PURCHASED_BALANCE_MATCHING',
                'transaction_audit_code': 'PURCHASE_ONE',
                'transaction_amount_per_item': '50.00',
                'transaction_fee': '5.00',
                'transaction_fee_per_item': '2.50',
            },
        ], **{
            'order_audit_code': order_status,
        })

    def test_single_purchase_two_orderitems_with_rounded_fee(self):
        orderitem1 = self.create_orderitem()
        orderitem2 = self.create_orderitem(line_item_id=SECOND_ORDER_ITEM)
        purchase = self.create_transaction(amount='100.00', transaction_fee='1.01')
        self._check_output([orderitem1, orderitem2, purchase], [
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_audit_code': 'PURCHASE_ONE',
                'transaction_amount_per_item': '50.00',
                'transaction_fee_per_item': '0.50',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'transaction_audit_code': 'PURCHASE_ONE',
                'transaction_amount_per_item': '50.00',
                'transaction_fee_per_item': '0.51',
            },
        ])

    @data(
        (False, '130.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_CHARGE', 'ERROR_ORDER_NOT_BALANCED'),
        (False, '50.00', 'PURCHASED_BALANCE_MATCHING', 'ERROR_ORDER_NOT_BALANCED'),
        (False, '10.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_UNDER_CHARGE', 'ERROR_ORDER_NOT_BALANCED'),
        (True, '130.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_CHARGE', 'ERROR_ORDER_NOT_BALANCED'),
        (True, '50.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_REFUND_MISSING', 'ORDER_BALANCED'),
        (True, '10.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_UNDER_CHARGE', 'ERROR_ORDER_NOT_BALANCED'),
    )
    @unpack
    def test_purchase_of_one_out_of_two(self, is_refunded_order, amount, orderitem_status, order_status):
        orderitem1 = self.create_orderitem(is_refunded=is_refunded_order)
        orderitem2 = self.create_orderitem(line_item_id=SECOND_ORDER_ITEM)
        purchase = self.create_transaction(amount=amount)
        self._check_output([orderitem1, orderitem2, purchase], [
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'orderitem_audit_code': orderitem_status,
                'transaction_audit_code': 'PURCHASE_MISCHARGE' if amount != '50.00' else 'PURCHASE_ONE',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'orderitem_audit_code': 'ERROR_NO_TRANSACTION',
                'transaction_audit_code': 'NO_TRANSACTION',
            },
        ], **{
            'order_audit_code': order_status,
        })

    @data(
        (False, '70.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_OVER_CHARGE', 'ERROR_ORDER_NOT_BALANCED'),
        (False, '50.00', 'PURCHASED_BALANCE_MATCHING', 'ERROR_ORDER_NOT_BALANCED'),
        (False, '10.00', 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_UNDER_CHARGE', 'ERROR_ORDER_NOT_BALANCED'),
        (True, '70.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_OVER_CHARGE', 'ERROR_ORDER_NOT_BALANCED'),
        (True, '50.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_REFUND_MISSING', 'ERROR_ORDER_NOT_BALANCED'),
        (True, '10.00', 'ERROR_REFUNDED_BALANCE_NOT_MATCHING_UNDER_CHARGE', 'ERROR_ORDER_NOT_BALANCED'),
    )
    @unpack
    def test_purchase_one_out_of_two_unequal(self, is_refunded_order, amount, orderitem_status, order_status):
        orderitem1 = self.create_orderitem(is_refunded=is_refunded_order)
        orderitem2 = self.create_orderitem(**{
            'line_item_id': SECOND_ORDER_ITEM,
            'line_item_price': '30.00',
            'line_item_unit_price': '30.00'
        })

        purchase = self.create_transaction(amount=amount)
        self._check_output([orderitem1, orderitem2, purchase], [
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'orderitem_audit_code': orderitem_status,
                'transaction_audit_code': 'PURCHASE_MISCHARGE' if amount not in ['50.00'] else 'PURCHASE_ONE',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'orderitem_audit_code': 'ERROR_NO_TRANSACTION',
                'transaction_audit_code': 'NO_TRANSACTION',
            },
        ], **{
            'order_audit_code': order_status,
        })

    @data(
        (False, 'ERROR_ORDER_NOT_BALANCED'),
        (True, 'ORDER_BALANCED'),
    )
    @unpack
    def test_purchase_matching_second_one_out_of_two_unequal(self, is_refunded_order, order_status):
        orderitem1 = self.create_orderitem(is_refunded=is_refunded_order)
        orderitem2 = self.create_orderitem(**{
            'line_item_id': SECOND_ORDER_ITEM,
            'line_item_price': '30.00',
            'line_item_unit_price': '30.00'
        })

        purchase = self.create_transaction(amount='30.00')
        self._check_output([orderitem1, orderitem2, purchase], [
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'orderitem_audit_code': 'ERROR_NO_TRANSACTION',
                'transaction_audit_code': 'NO_TRANSACTION',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'orderitem_audit_code': 'PURCHASED_BALANCE_MATCHING',
                'transaction_audit_code': 'PURCHASE_ONE',
            },
        ], **{
            'order_audit_code': order_status,
        })

    def test_two_orderitems_refund_one(self):
        orderitem1 = self.create_orderitem(is_refunded=True)
        orderitem2 = self.create_orderitem(line_item_id=SECOND_ORDER_ITEM)
        purchase = self.create_transaction(amount='100.00')
        refund = self.create_refunding_transaction()
        self._check_output([orderitem1, orderitem2, purchase, refund], [
            # Output is in order by orderitem, then transaction.
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_id': FIRST_TRANSACTION,
                'orderitem_audit_code': 'REFUNDED_BALANCE_MATCHING',
                'transaction_audit_code': 'PURCHASE_ONE',
            },
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_id': SECOND_TRANSACTION,
                'orderitem_audit_code': 'REFUNDED_BALANCE_MATCHING',
                'transaction_audit_code': 'REFUND_ONE',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'transaction_id': FIRST_TRANSACTION,
                'orderitem_audit_code': 'PURCHASED_BALANCE_MATCHING',
                'transaction_audit_code': 'PURCHASE_ONE',
            },
        ], **{
            'order_audit_code': 'ORDER_BALANCED',
        })

    def test_two_orderitems_refund_one_with_fee(self):
        orderitem1 = self.create_orderitem(
            is_refunded=True,
            line_item_price='100.00',
            line_item_unit_price='100.00',
            refunded_amount='100.00',
        )
        orderitem2 = self.create_orderitem(is_refunded=True, line_item_id=SECOND_ORDER_ITEM)
        purchase = self.create_transaction(amount='150.00', transaction_fee='10.00')
        refund = self.create_refunding_transaction(amount='-150.00', transaction_fee='-5.00')
        self._check_output([orderitem1, orderitem2, purchase, refund], [
            # Output is in order by orderitem, then transaction.
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_id': FIRST_TRANSACTION,
                'transaction_audit_code': 'PURCHASE_ONE',
                'transaction_fee': '10.00',
                'transaction_fee_per_item': '6.67',
            },
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_id': SECOND_TRANSACTION,
                'transaction_audit_code': 'REFUND_ONE',
                'transaction_fee': '-5.00',
                'transaction_fee_per_item': '-3.33',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'transaction_id': FIRST_TRANSACTION,
                'transaction_audit_code': 'PURCHASE_ONE',
                'transaction_fee': '10.00',
                'transaction_fee_per_item': '3.33',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'transaction_id': SECOND_TRANSACTION,
                'transaction_audit_code': 'REFUND_ONE',
                'transaction_fee': '-5.00',
                'transaction_fee_per_item': '-1.67',
            },
        ], **{
            'order_audit_code': 'ORDER_BALANCED',
            'orderitem_audit_code': 'REFUNDED_BALANCE_MATCHING',
        })

    def test_two_orderitems_extra_purchase(self):
        orderitem1 = self.create_orderitem()
        orderitem2 = self.create_orderitem(line_item_id=SECOND_ORDER_ITEM)
        purchase = self.create_transaction(amount='100.00')
        purchase2 = self.create_transaction(transaction_id=SECOND_TRANSACTION)
        self._check_output([orderitem1, orderitem2, purchase, purchase2], [
            # Output is in order by orderitem, then transaction.
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_id': FIRST_TRANSACTION,
                'orderitem_audit_code': 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_WAS_CHARGED_TWICE',
                'transaction_audit_code': 'PURCHASE_ONE',
            },
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_id': SECOND_TRANSACTION,
                'orderitem_audit_code': 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_WAS_CHARGED_TWICE',
                'transaction_audit_code': 'PURCHASE_AGAIN',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'transaction_id': FIRST_TRANSACTION,
                'orderitem_audit_code': 'PURCHASED_BALANCE_MATCHING',
                'transaction_audit_code': 'PURCHASE_ONE',
            },
        ], **{
            'order_audit_code': 'ERROR_ORDER_NOT_BALANCED',
        })

    @data(
        [0, 1, 2, 3, 4],
        [4, 3, 2, 1, 0],
        [0, 2, 4, 3, 1],
        [2, 4, 1, 3, 0]
    )
    def test_two_orderitems_refund_both_wrongstatus(self, permutation):
        orderitem1 = self.create_orderitem(is_refunded=True)
        orderitem2 = self.create_orderitem(line_item_id=SECOND_ORDER_ITEM)
        purchase = self.create_transaction(amount='100.00')
        refund = self.create_refunding_transaction()
        refund2 = self.create_refunding_transaction(transaction_id=THIRD_TRANSACTION)
        inputs = [orderitem1, orderitem2, purchase, refund, refund2]
        permuted_input = [inputs[index] for index in permutation]
        self._check_output(permuted_input, [
            # Output is in order by orderitem, then transaction.
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_id': FIRST_TRANSACTION,
                'orderitem_audit_code': 'REFUNDED_BALANCE_MATCHING',
                'transaction_audit_code': 'PURCHASE_ONE',
            },
            {
                'order_line_item_id': FIRST_ORDER_ITEM,
                'transaction_id': SECOND_TRANSACTION,
                'orderitem_audit_code': 'REFUNDED_BALANCE_MATCHING',
                'transaction_audit_code': 'REFUND_ONE',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'transaction_id': FIRST_TRANSACTION,
                'orderitem_audit_code': 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_WAS_REFUNDED',
                'transaction_audit_code': 'PURCHASE_ONE',
            },
            {
                'order_line_item_id': SECOND_ORDER_ITEM,
                'transaction_id': THIRD_TRANSACTION,
                'orderitem_audit_code': 'ERROR_PURCHASED_BALANCE_NOT_MATCHING_WAS_REFUNDED',
                'transaction_audit_code': 'REFUND_ONE_STATUS_NOT_REFUNDED',
            },
        ], **{'order_audit_code': 'ERROR_ORDER_NOT_BALANCED'}
        )
