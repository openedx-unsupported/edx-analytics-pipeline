"""
Import data from external RDBMS databases specific to enterprise into Hive.
"""

import logging

from edx.analytics.tasks.insights.database_imports import ImportMysqlToHiveTableTask

log = logging.getLogger(__name__)


class ImportEnterpriseCustomerTask(ImportMysqlToHiveTableTask):
    """Imports the `enterprise_enterprisecustomer` table to S3/Hive."""

    @property
    def table_name(self):
        return 'enterprise_enterprisecustomer'

    @property
    def columns(self):
        return [
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
            ('uuid', 'STRING'),
            ('name', 'STRING'),
            ('active', 'BOOLEAN'),
            ('site_id', 'INT'),
            ('enable_data_sharing_consent', 'BOOLEAN'),
            ('enforce_data_sharing_consent', 'STRING'),
            ('enable_audit_enrollment', 'BOOLEAN'),
            ('enable_audit_data_reporting', 'BOOLEAN'),
        ]


class ImportEnterpriseCustomerUserTask(ImportMysqlToHiveTableTask):
    """Imports the `enterprise_enterprisecustomeruser` table to S3/Hive."""

    @property
    def table_name(self):
        return 'enterprise_enterprisecustomeruser'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
            ('user_id', 'INT'),
            ('enterprise_customer_id', 'STRING'),
            ('linked', 'BOOLEAN'),
        ]


class ImportEnterpriseCourseEnrollmentUserTask(ImportMysqlToHiveTableTask):
    """Imports the `enterprise_enterprisecourseenrollment` table to S3/Hive."""

    @property
    def table_name(self):
        return 'enterprise_enterprisecourseenrollment'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
            ('course_id', 'STRING'),
            ('enterprise_customer_user_id', 'INT'),
        ]


class ImportDataSharingConsentTask(ImportMysqlToHiveTableTask):
    """Imports the `consent_datasharingconsent` table to S3/Hive."""

    @property
    def table_name(self):
        return 'consent_datasharingconsent'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('created', 'TIMESTAMP'),
            ('modified', 'TIMESTAMP'),
            ('username', 'STRING'),
            ('granted', 'BOOLEAN'),
            ('course_id', 'STRING'),
            ('enterprise_customer_id', 'STRING'),
        ]


class ImportUserSocialAuthTask(ImportMysqlToHiveTableTask):
    """Imports the `social_auth_usersocialauth` table to S3/Hive."""

    @property
    def table_name(self):
        return 'social_auth_usersocialauth'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('user_id', 'INT'),
            ('provider', 'STRING'),
            ('uid', 'STRING'),
            ('extra_data', 'STRING'),
        ]


class ImportVoucherTask(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Imports the voucher_voucher table from the ecommerce
    database to a destination directory and a HIVE metastore.

    A voucher is a discount coupon that can be applied to ecommerce purchases.
    """
    @property
    def table_name(self):
        return 'voucher_voucher'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('code', 'STRING'),
            ('usage', 'STRING'),
            ('start_datetime', 'TIMESTAMP'),
            ('end_datetime', 'TIMESTAMP'),
            ('num_basket_additions', 'INT'),
            ('num_orders', 'INT'),
            ('total_discount', 'DECIMAL(12, 2)'),
            ('date_created', 'TIMESTAMP'),
        ]


class ImportStockRecordTask(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Imports the partner_stockrecord table from the ecommerce
    database to a destination directory and a HIVE metastore.

    A voucher is a discount coupon that can be applied to ecommerce purchases.
    """
    @property
    def table_name(self):
        return 'partner_stockrecord'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('partner_sku', 'STRING'),
            ('price_currency', 'STRING'),
            ('price_excl_tax', 'DECIMAL(12, 2)'),
            ('price_retail', 'DECIMAL(12, 2)'),
            ('cost_price', 'DECIMAL(12, 2)'),
            ('num_in_stock', 'INT'),
            ('num_allocated', 'INT'),
            ('low_stock_threshold', 'INT'),
            ('date_created', 'TIMESTAMP'),
            ('date_updated', 'TIMESTAMP'),
            ('partner_id', 'INT'),
            ('product_id', 'INT'),
        ]


class ImportConditionalOfferTask(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Imports conditional offer information from an ecommerce table to a
    destination directory and a HIVE metastore.
    """

    @property
    def table_name(self):
        return 'offer_conditionaloffer'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('slug', 'STRING'),
            ('description', 'STRING'),
            ('offer_type', 'STRING'),
            ('status', 'STRING'),
            ('priority', 'INT'),
            ('start_datetime', 'TIMESTAMP'),
            ('end_datetime', 'TIMESTAMP'),
            ('max_global_applications', 'INT'),
            ('max_user_applications', 'INT'),
            ('max_basket_applications', 'INT'),
            ('max_discount', 'DECIMAL(12, 2)'),
            ('total_discount', 'DECIMAL(12, 2)'),
            ('num_applications', 'INT'),
            ('num_orders', 'INT'),
            ('redirect_url', 'STRING'),
            ('date_created', 'TIMESTAMP'),
            ('benefit_id', 'INT'),
            ('condition_id', 'INT'),
            ('email_domains', 'TIMESTAMP'),
            ('site_id', 'INT'),
        ]


class ImportBenefitTask(ImportMysqlToHiveTableTask):
    """
    Ecommerce: Imports offer benefit information from an ecommerce table to a
    destination directory and a HIVE metastore.
    """

    @property
    def table_name(self):
        return 'offer_benefit'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('type', 'STRING'),
            ('value', 'DECIMAL(12, 2)'),
            ('max_affected_items', 'INT'),
            ('proxy_class', 'STRING'),
            ('range_id', 'INT'),
        ]
