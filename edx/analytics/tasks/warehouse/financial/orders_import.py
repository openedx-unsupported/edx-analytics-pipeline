"""Import Orders: Shopping Cart Tables from the LMS, Orders from Otto."""

from __future__ import absolute_import

import luigi

from edx.analytics.tasks.insights.database_imports import (
    DatabaseImportMixin, ImportAuthUserTask, ImportCouponVoucherIndirectionState, ImportCouponVoucherState,
    ImportCourseEntitlementTask, ImportCurrentOrderDiscountState, ImportCurrentOrderLineState, ImportCurrentOrderState,
    ImportCurrentRefundRefundLineState, ImportEcommercePartner, ImportEcommerceUser, ImportProductCatalog,
    ImportProductCatalogAttributes, ImportProductCatalogAttributeValues, ImportProductCatalogClass,
    ImportShoppingCartCertificateItem, ImportShoppingCartCoupon, ImportShoppingCartCouponRedemption,
    ImportShoppingCartCourseRegistrationCodeItem, ImportShoppingCartDonation, ImportShoppingCartOrder,
    ImportShoppingCartOrderItem, ImportShoppingCartPaidCourseRegistration, ImportStudentCourseEnrollmentTask
)
from edx.analytics.tasks.util.hive import HivePartition, HiveTableFromQueryTask, hive_decimal_type


class OrderTableTask(DatabaseImportMixin, HiveTableFromQueryTask):
    otto_credentials = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'credentials'}
    )
    otto_database = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'database'}
    )

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
            'credentials': self.otto_credentials,
            'database': self.otto_database,
        }
        yield (
            # Otto User Table
            ImportEcommerceUser(**kwargs),

            # Otto Product Tables.
            ImportProductCatalog(**kwargs),
            ImportProductCatalogClass(**kwargs),
            ImportProductCatalogAttributes(**kwargs),
            ImportProductCatalogAttributeValues(**kwargs),

            # Otto Current State, Line Item, and Coupon Tables.
            ImportCurrentOrderState(**kwargs),
            ImportCurrentOrderLineState(**kwargs),
            ImportCurrentOrderDiscountState(**kwargs),
            ImportCouponVoucherIndirectionState(**kwargs),
            ImportCouponVoucherState(**kwargs),

            # Otto Refund Tables.
            ImportCurrentRefundRefundLineState(**kwargs),

            # Otto Partner Information.
            ImportEcommercePartner(**kwargs),
        )

        kwargs['credentials'] = self.credentials
        kwargs['database'] = self.database
        yield (
            # Shopping cart tables.
            ImportShoppingCartOrder(**kwargs),
            ImportShoppingCartOrderItem(**kwargs),
            ImportShoppingCartCertificateItem(**kwargs),
            ImportShoppingCartPaidCourseRegistration(**kwargs),
            ImportShoppingCartDonation(**kwargs),
            ImportShoppingCartCourseRegistrationCodeItem(**kwargs),
            ImportShoppingCartCoupon(**kwargs),
            ImportShoppingCartCouponRedemption(**kwargs),

            # Other LMS tables.
            ImportAuthUserTask(**kwargs),
            ImportCourseEntitlementTask(**kwargs),
            ImportStudentCourseEnrollmentTask(**kwargs),
        )

    @property
    def table(self):
        return 'order'

    @property
    def columns(self):
        return [
            ('order_processor', 'STRING'),
            ('user_id', 'INT'),
            ('order_id', 'INT'),
            ('line_item_id', 'INT'),
            ('line_item_product_id', 'INT'),
            ('line_item_price', hive_decimal_type(12, 2)),
            ('line_item_unit_price', hive_decimal_type(12, 2)),
            ('line_item_quantity', 'INT'),
            ('product_class', 'STRING'),
            ('course_key', 'STRING'),
            ('product_detail', 'STRING'),
            ('username', 'STRING'),
            ('user_email', 'STRING'),
            ('date_placed', 'TIMESTAMP'),
            ('iso_currency_code', 'STRING'),
            ('coupon_id', 'INT'),
            ('discount_amount', hive_decimal_type(12, 2)),  # Total discount in currency amount, i.e. unit_discount * qty
            ('voucher_id', 'INT'),
            ('voucher_code', 'STRING'),
            ('status', 'STRING'),
            ('refunded_amount', hive_decimal_type(12, 2)),
            ('refunded_quantity', 'INT'),
            ('payment_ref_id', 'STRING'),
            ('partner_short_code', 'STRING'),
            ('course_uuid', 'STRING'),
            ('expiration_date', 'TIMESTAMP'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    def hiveconfs(self):
        # TODO: This shouldn't be necessary. Hive is throwing an error when running from the acceptance tests. It does
        # not throw an error when running with production data, but the presence of the error is concerning. The error
        # is generated by the map-join optimization, disabling that optimization allows the job to complete
        # successfully.
        jcs = super(OrderTableTask, self).hiveconfs()
        jcs['hive.auto.convert.join'] = 'false'
        return jcs

    @property
    def insert_query(self):
        # NOTE: Orders lines for course entitlement products should always have a quantity of 1.
        return """
            SELECT
                combined.*
            FROM (
                -- Otto Records
                SELECT
                    "otto" AS order_processor,
                    o.user_id AS user_id,
                    ol.order_id AS order_id,
                    ol.id AS line_item_id,
                    ol.product_id AS line_item_product_id,

                    -- Price charged to the customer after all surcharges and discounts
                    ol.line_price_incl_tax AS line_item_price,
                    -- List price of each item before discounts
                    ol.unit_price_incl_tax AS line_item_unit_price,
                    ol.quantity AS line_item_quantity,
                    cpc.slug AS product_class,
                    COALESCE(enrollments.course_id, ckval.value_text) AS course_key,
                    ctval.value_text AS product_detail,
                    u.username AS username,
                    u.email AS user_email,
                    o.date_placed AS date_placed,
                    o.currency AS iso_currency_code,

                    -- Discount/coupon/voucher information
                    vcv.coupon_id AS coupon_id,
                    (ol.line_price_before_discounts_incl_tax - ol.line_price_incl_tax) AS discount_amount,
                    od.voucher_id AS voucher_id,
                    od.voucher_code AS voucher_code,

                    -- If a refund was found, mark this order as refunded
                    CASE
                        WHEN r.order_line_id IS NOT NULL THEN "refunded"
                        ELSE "purchased"
                    END AS status,
                    r.refunded_amount AS refunded_amount,
                    r.refunded_quantity AS refunded_quantity,

                    -- The EDX-1XXXX identifier is used to find transactions associated with this order
                    o.number AS payment_ref_id,

                    partner.short_code AS partner_short_code,
                    cuval.value_text AS course_uuid,
                    entitlements.expired_at AS expiration_date

                FROM order_line ol
                JOIN order_order o ON o.id = ol.order_id
                JOIN ecommerce_user u ON u.id = o.user_id

                -- Order lines are associated with "child" products
                JOIN catalogue_product cp ON cp.id = ol.product_id

                -- Product classes are associated with "parent" products, so find the parent for this product
                JOIN catalogue_product parent ON parent.id = cp.parent_id
                JOIN catalogue_productclass cpc ON cpc.id = parent.product_class_id

                -- Product attributes are effectively a key value store. Each product class has a set of attributes
                -- associated with it that store additional data that is class-specific. For example, a T-shirt might
                -- store size information.

                -- For "seat" product class, line items will have a "course_key" attribute which will contain the course
                -- key for the course that the user purchased a seat for
                LEFT OUTER JOIN catalogue_productattribute ckat ON ckat.product_class_id = parent.product_class_id AND ckat.code = "course_key"
                LEFT OUTER JOIN catalogue_productattributevalue ckval ON ckval.attribute_id = ckat.id AND ckval.product_id = cp.id

                -- Seat and Course Entitlement products both have a certificate type attribute that corresponds to the type of certificate the
                -- learner will receive after course completion. Common values include honor, professional, and verified.
                LEFT OUTER JOIN catalogue_productattribute ctat ON ctat.product_class_id = parent.product_class_id AND ctat.code = "certificate_type"
                LEFT OUTER JOIN catalogue_productattributevalue ctval ON ctval.attribute_id = ctat.id AND ctval.product_id = cp.id

                -- Course Entitlement products have a UUID linking to the course
                LEFT OUTER JOIN catalogue_productattribute cuat ON cuat.product_class_id = parent.product_class_id AND cuat.code = "UUID"
                LEFT OUTER JOIN catalogue_productattributevalue cuval ON cuval.attribute_id = cuat.id AND cuval.product_id = cp.id

                -- If the quantity > 1 for a particular line item it is possible that multiple refunds might be issued
                -- against it. In this case just sum all of the complete refunds to figure out the number of items that
                -- have been refunded and the dollar amount of all of those refunds.
                LEFT OUTER JOIN (
                    SELECT
                        order_line_id,
                        SUM(line_credit_excl_tax) AS refunded_amount,
                        SUM(quantity) AS refunded_quantity
                    FROM refund_refundline
                    WHERE status = "Complete"
                    GROUP BY order_line_id
                ) r ON r.order_line_id = ol.id

                -- Get discount information. Each order may have zero or one voucher code applied.
                -- We are relying on the ecommerce restriction that each order can have no more than one voucher
                -- applied. If the order contains multiple line items, the AND condition below will join the discount
                -- information only to the order line item(s) that were actually discounted.
                LEFT OUTER JOIN order_orderdiscount od ON od.order_id = o.id AND (ol.line_price_incl_tax <> ol.line_price_before_discounts_incl_tax)
                LEFT OUTER JOIN voucher_couponvouchers_vouchers vcvv ON vcvv.voucher_id = od.voucher_id
                LEFT OUTER JOIN voucher_couponvouchers vcv ON vcv.id = vcvv.couponvouchers_id

                -- Partner information
                LEFT OUTER JOIN partner_partner partner ON partner.id = ol.partner_id

                -- Get course entitlement data
                LEFT OUTER JOIN entitlements_courseentitlement entitlements ON entitlements.order_number = o.number
                    AND entitlements.course_uuid = REPLACE(cuval.value_text, "-", "")
                LEFT OUTER JOIN student_courseenrollment enrollments ON enrollments.id = entitlements.enrollment_course_run_id

                -- Only process complete orders
                WHERE ol.status = "Complete"

                UNION ALL

                -- Legacy Shopping Cart Records
                SELECT
                    'shoppingcart' AS order_processor,
                    o.user_id AS user_id,
                    oi.order_id AS order_id,
                    oi.id AS line_item_id,

                    -- LMS product types are identified by the table pointing to the orderitem
                    -- Assign ID numbers based on the pointing table
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN 1
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN 2
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN 3
                        WHEN d.orderitem_ptr_id IS NOT NULL THEN 4
                    END AS line_item_product_id,

                    -- The total cost is not stored, so we compute it
                    -- Note that this is the amount charged to the credit card after all discounts
                    (oi.qty * oi.unit_cost) AS line_item_price,
                    -- line_item_unit_price is supposed to be the list price per item (before discount)
                    -- as is the case in Otto. So we use 'list_price' but since that may be null in some
                    -- cases ( see https://git.io/vVXqO ), we fall back to unit_cost.
                    CASE
                        WHEN oi.list_price IS NOT NULL THEN oi.list_price
                        ELSE oi.unit_cost
                    END AS line_item_unit_price,
                    oi.qty AS line_item_quantity,
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN 'seat'          -- verified certificate
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN 'seat'         -- single user registration
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN 'reg-code'     -- registration codes
                        WHEN d.orderitem_ptr_id IS NOT NULL THEN 'donation'       -- donation
                    END AS product_class,
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN ci.course_id    -- the course the certificate is for
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN pcr.course_id  -- the course the user registered in
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN crc.course_id  -- the course the registration codes were generated for
                        WHEN d.orderitem_ptr_id IS NOT NULL THEN d.course_id      -- the course that the user donated to (may be NULL)
                    END AS course_key,
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN ci.mode         -- always "verified"
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN pcr.mode       -- always "honor" even though the user paid for the seat
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN crc.mode       -- always "honor"
                    END AS product_detail,
                    au.username as username,
                    au.email as user_email,
                    o.purchase_time AS date_placed,
                    UPPER(o.currency) AS iso_currency_code,

                    -- Coupon information. Shopping cart has no distinction between voucher codes and coupons.
                    -- The tables also only store the percentage discount, not the discount amount, so calculate it:
                    NULL AS coupon_id,
                    ((oi.list_price - oi.unit_cost) * oi.qty) AS discount_amount, -- coupon.percentage_discount would have rounding issues
                    coupon.id AS voucher_id,
                    coupon.code AS voucher_code,

                    -- Either "purchased" or "refunded"
                    oi.status AS status,

                    -- We don't currently support partial refunds so the refund will always encompass
                    -- the complete line item quantity and amount
                    IF(oi.status = 'refunded', oi.qty * oi.unit_cost, NULL) AS refunded_amount,
                    IF(oi.status = 'refunded', oi.qty, NULL) AS refunded_quantity,
                    oi.order_id AS payment_ref_id,

                    -- The partner short code is extracted from the course ID during order reconciliation.
                    '' AS partner_short_code,

                    -- These fields are not relevant to shoppingcart orders
                    NULL AS course_uuid,
                    NULL AS expiration_date

                FROM shoppingcart_orderitem oi
                JOIN shoppingcart_order o ON o.id = oi.order_id
                JOIN auth_user au ON au.id = o.user_id

                -- These tables contain details pertaining to the particular type of product purchased
                -- exactly one of these joins should resolve.
                LEFT OUTER JOIN shoppingcart_certificateitem ci ON ci.orderitem_ptr_id = oi.id
                LEFT OUTER JOIN shoppingcart_paidcourseregistration pcr ON pcr.orderitem_ptr_id = oi.id
                LEFT OUTER JOIN shoppingcart_courseregcodeitem crc ON crc.orderitem_ptr_id = oi.id
                LEFT OUTER JOIN shoppingcart_donation d ON d.orderitem_ptr_id = oi.id

                -- Join coupon information. Need to use course_id because one order can contain multiple items,
                -- but the database tables only link coupons to the whole order and not the specific line item.
                LEFT OUTER JOIN shoppingcart_couponredemption couponred ON couponred.order_id = o.id
                LEFT OUTER JOIN shoppingcart_coupon coupon ON (coupon.id = couponred.coupon_id AND coupon.course_id = (
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN ci.course_id
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN pcr.course_id
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN crc.course_id
                        WHEN d.orderitem_ptr_id IS NOT NULL THEN d.course_id
                    END
                ))

                -- Ignore "cart", "defunct-cart" and "paying" statuses since they won't have corresponding transactions
                WHERE oi.status IN ('purchased', 'refunded')
            ) combined;
        """


class BaseFullOrderTableTask(DatabaseImportMixin, HiveTableFromQueryTask):

    @property
    def columns(self):
        return [
            ('order_processor', 'STRING'),
            ('user_id', 'INT'),
            ('order_id', 'INT'),
            ('line_item_id', 'INT'),
            ('line_item_product_id', 'INT'),
            ('line_item_price', hive_decimal_type(12, 2)),
            ('line_item_unit_price', hive_decimal_type(12, 2)),
            ('line_item_quantity', 'INT'),
            ('product_class', 'STRING'),
            ('course_run_key', 'STRING'),
            ('product_detail', 'STRING'),
            ('username', 'STRING'),
            ('date_placed', 'TIMESTAMP'),
            ('iso_currency_code', 'STRING'),
            ('discount_amount', hive_decimal_type(12, 2)),  # Total discount in currency amount, i.e. unit_discount * qty
            ('status', 'STRING'),
            ('refunded_amount', hive_decimal_type(12, 2)),
            ('refunded_quantity', 'INT'),
            ('payment_ref_id', 'STRING'),
            ('partner_short_code', 'STRING'),
            ('course_uuid', 'STRING'),
            ('lms_user_id', 'INT'),
            ('partner_sku', 'STRING'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    def hiveconfs(self):
        # TODO: This shouldn't be necessary. Hive is throwing an error when running from the acceptance tests. It does
        # not throw an error when running with production data, but the presence of the error is concerning. The error
        # is generated by the map-join optimization, disabling that optimization allows the job to complete
        # successfully.
        jcs = super(BaseFullOrderTableTask, self).hiveconfs()
        jcs['hive.auto.convert.join'] = 'false'
        return jcs


class FullOttoOrderTableTask(BaseFullOrderTableTask):

    otto_credentials = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'credentials'}
    )
    otto_database = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'database'}
    )

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
            'credentials': self.otto_credentials,
            'database': self.otto_database,
        }
        yield (
            # Otto User Table
            ImportEcommerceUser(**kwargs),

            # Otto Product Tables.
            ImportProductCatalog(**kwargs),
            ImportProductCatalogClass(**kwargs),
            ImportProductCatalogAttributes(**kwargs),
            ImportProductCatalogAttributeValues(**kwargs),

            # Otto Current State, Line Item, and Coupon Tables.
            ImportCurrentOrderState(**kwargs),
            ImportCurrentOrderLineState(**kwargs),

            # Otto Refund Tables.
            ImportCurrentRefundRefundLineState(**kwargs),

            # Otto Partner Information.
            ImportEcommercePartner(**kwargs),
        )

    @property
    def table(self):
        return 'full_order_otto'

    @property
    def insert_query(self):
        # NOTE: Orders lines for course entitlement products should always have a quantity of 1.
        return """
                -- Otto Records
                SELECT
                    "otto" AS order_processor,
                    o.user_id AS user_id,
                    ol.order_id AS order_id,
                    ol.id AS line_item_id,
                    ol.product_id AS line_item_product_id,

                    -- Price charged to the customer after all surcharges and discounts
                    ol.line_price_incl_tax AS line_item_price,
                    -- List price of each item before discounts
                    ol.unit_price_incl_tax AS line_item_unit_price,
                    ol.quantity AS line_item_quantity,
                    cpc.slug AS product_class,
                    ckval.value_text AS course_run_key,
                    ctval.value_text AS product_detail,
                    u.username AS username,
                    o.date_placed AS date_placed,
                    o.currency AS iso_currency_code,

                    (ol.line_price_before_discounts_incl_tax - ol.line_price_incl_tax) AS discount_amount,

                    -- If a refund was found, mark this order as refunded
                    CASE
                        -- Move the order status here as well, for those cases that are not "Complete" that were excluded before.
                        WHEN ol.status <> "Complete" THEN ol.status
                        WHEN r.order_line_id IS NOT NULL THEN "refunded"
                        ELSE "purchased"
                    END AS status,
                    r.refunded_amount AS refunded_amount,
                    r.refunded_quantity AS refunded_quantity,

                    -- The EDX-1XXXX identifier is used to find transactions associated with this order
                    o.number AS payment_ref_id,

                    partner.short_code AS partner_short_code,
                    cuval.value_text AS course_uuid,
                    u.lms_user_id AS lms_user_id,
                    ol.partner_sku AS partner_sku

                FROM order_line ol
                INNER JOIN order_order o ON o.id = ol.order_id
                INNER JOIN ecommerce_user u ON u.id = o.user_id

                -- Order lines are associated with "child" products OR "standalone" products
                LEFT JOIN catalogue_product cp ON cp.id = ol.product_id

                -- Some product classes are associated with "parent" products, so find the parent for this product if it exists.
                LEFT JOIN catalogue_product parent ON parent.id = cp.parent_id
                LEFT JOIN catalogue_productclass cpc ON cpc.id = COALESCE(parent.product_class_id, cp.product_class_id)

                -- Product attributes are effectively a key value store. Each product class has a set of attributes
                -- associated with it that store additional data that is class-specific. For example, a T-shirt might
                -- store size information.

                -- For "seat" product class, line items will have a "course_key" attribute which will contain the course
                -- key for the course that the user purchased a seat for
                LEFT OUTER JOIN catalogue_productattribute ckat
                    ON ckat.product_class_id = COALESCE(parent.product_class_id, cp.product_class_id) AND ckat.code = "course_key"
                LEFT OUTER JOIN catalogue_productattributevalue ckval ON ckval.attribute_id = ckat.id AND ckval.product_id = cp.id

                -- Seat and Course Entitlement products both have a certificate type attribute that corresponds to the type of certificate the
                -- learner will receive after course completion. Common values include honor, professional, and verified.
                LEFT OUTER JOIN catalogue_productattribute ctat
                    ON ctat.product_class_id = COALESCE(parent.product_class_id, cp.product_class_id) AND ctat.code = "certificate_type"
                LEFT OUTER JOIN catalogue_productattributevalue ctval ON ctval.attribute_id = ctat.id AND ctval.product_id = cp.id

                -- Course Entitlement products have a UUID linking to the course
                LEFT OUTER JOIN catalogue_productattribute cuat
                    ON cuat.product_class_id = COALESCE(parent.product_class_id, cp.product_class_id) AND cuat.code = "UUID"
                LEFT OUTER JOIN catalogue_productattributevalue cuval ON cuval.attribute_id = cuat.id AND cuval.product_id = cp.id

                -- If the quantity > 1 for a particular line item it is possible that multiple refunds might be issued
                -- against it. In this case just sum all of the complete refunds to figure out the number of items that
                -- have been refunded and the dollar amount of all of those refunds.
                LEFT OUTER JOIN (
                    SELECT
                        order_line_id,
                        SUM(line_credit_excl_tax) AS refunded_amount,
                        SUM(quantity) AS refunded_quantity
                    FROM refund_refundline
                    WHERE status = "Complete"
                    GROUP BY order_line_id
                ) r ON r.order_line_id = ol.id

                -- Partner information
                LEFT OUTER JOIN partner_partner partner ON partner.id = ol.partner_id

        """


class FullShoppingcartOrderTableTask(BaseFullOrderTableTask):

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
            'credentials': self.credentials,
            'database': self.database,
        }
        yield (
            # Shopping cart tables.
            ImportShoppingCartOrder(**kwargs),
            ImportShoppingCartOrderItem(**kwargs),
            ImportShoppingCartCertificateItem(**kwargs),
            ImportShoppingCartPaidCourseRegistration(**kwargs),
            ImportShoppingCartDonation(**kwargs),
            ImportShoppingCartCourseRegistrationCodeItem(**kwargs),
            ImportShoppingCartCoupon(**kwargs),
            ImportShoppingCartCouponRedemption(**kwargs),

            # Other LMS tables.
            ImportAuthUserTask(**kwargs),
        )

    @property
    def table(self):
        return 'full_order_shoppingcart'

    @property
    def insert_query(self):
        # NOTE: Orders lines for course entitlement products should always have a quantity of 1.
        return """
                -- Legacy Shopping Cart Records
                SELECT
                    'shoppingcart' AS order_processor,
                    o.user_id AS user_id,
                    oi.order_id AS order_id,
                    oi.id AS line_item_id,

                    -- LMS product types are identified by the table pointing to the orderitem
                    -- Assign ID numbers based on the pointing table
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN 1
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN 2
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN 3
                        WHEN d.orderitem_ptr_id IS NOT NULL THEN 4
                    END AS line_item_product_id,

                    -- The total cost is not stored, so we compute it
                    -- Note that this is the amount charged to the credit card after all discounts
                    (oi.qty * oi.unit_cost) AS line_item_price,
                    -- line_item_unit_price is supposed to be the list price per item (before discount)
                    -- as is the case in Otto. So we use 'list_price' but since that may be null in some
                    -- cases ( see https://git.io/vVXqO ), we fall back to unit_cost.
                    CASE
                        WHEN oi.list_price IS NOT NULL THEN oi.list_price
                        ELSE oi.unit_cost
                    END AS line_item_unit_price,
                    oi.qty AS line_item_quantity,
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN 'seat'          -- verified certificate
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN 'seat'         -- single user registration
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN 'reg-code'     -- registration codes
                        WHEN d.orderitem_ptr_id IS NOT NULL THEN 'donation'       -- donation
                    END AS product_class,
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN ci.course_id    -- the course the certificate is for
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN pcr.course_id  -- the course the user registered in
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN crc.course_id  -- the course the registration codes were generated for
                        WHEN d.orderitem_ptr_id IS NOT NULL THEN d.course_id      -- the course that the user donated to (may be NULL)
                    END AS course_run_key,
                    CASE
                        WHEN ci.orderitem_ptr_id IS NOT NULL THEN ci.mode         -- always "verified"
                        WHEN pcr.orderitem_ptr_id IS NOT NULL THEN pcr.mode       -- always "honor" even though the user paid for the seat
                        WHEN crc.orderitem_ptr_id IS NOT NULL THEN crc.mode       -- always "honor"
                    END AS product_detail,
                    au.username as username,
                    o.purchase_time AS date_placed,
                    UPPER(o.currency) AS iso_currency_code,

                    -- Calculate size of actual discount. Calculating using coupon.percentage_discount would have rounding issues.
                    ((oi.list_price - oi.unit_cost) * oi.qty) AS discount_amount,

                    -- Either "purchased" or "refunded"
                    oi.status AS status,

                    -- We don't currently support partial refunds so the refund will always encompass
                    -- the complete line item quantity and amount
                    IF(oi.status = 'refunded', oi.qty * oi.unit_cost, NULL) AS refunded_amount,
                    IF(oi.status = 'refunded', oi.qty, NULL) AS refunded_quantity,
                    oi.order_id AS payment_ref_id,

                    -- The partner short code is extracted from the course ID during order reconciliation.
                    '' AS partner_short_code,

                    -- These fields are not relevant to shoppingcart orders
                    NULL AS course_uuid,
                    o.user_id AS lms_user_id,
                    NULL AS partner_sku

                FROM shoppingcart_orderitem oi
                JOIN shoppingcart_order o ON o.id = oi.order_id
                JOIN auth_user au ON au.id = o.user_id

                -- These tables contain details pertaining to the particular type of product purchased
                -- exactly one of these joins should resolve.
                LEFT OUTER JOIN shoppingcart_certificateitem ci ON ci.orderitem_ptr_id = oi.id
                LEFT OUTER JOIN shoppingcart_paidcourseregistration pcr ON pcr.orderitem_ptr_id = oi.id
                LEFT OUTER JOIN shoppingcart_courseregcodeitem crc ON crc.orderitem_ptr_id = oi.id
                LEFT OUTER JOIN shoppingcart_donation d ON d.orderitem_ptr_id = oi.id

                -- Ignore "cart", "defunct-cart" and "paying" statuses since they won't have corresponding transactions
                WHERE oi.status IN ('purchased', 'refunded')

        """
