"""
Ensure we can write from MySQL to Hive data sources.
"""

import datetime
import textwrap
from unittest import TestCase

from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportBenefitTask, ImportConditionalOfferTask, ImportDataSharingConsentTask,
    ImportEnterpriseCourseEnrollmentUserTask, ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask,
    ImportStockRecordTask, ImportUserSocialAuthTask, ImportVoucherTask
)
from edx.analytics.tasks.util.tests.config import with_luigi_config


class ImportEnterpriseCustomerTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportEnterpriseCustomerTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `enterprise_enterprisecustomer`;
            CREATE EXTERNAL TABLE `enterprise_enterprisecustomer` (
                `created` TIMESTAMP,`modified` TIMESTAMP,`uuid` STRING,`name` STRING,`active` BOOLEAN,`site_id` INT,`enable_data_sharing_consent` BOOLEAN,`enforce_data_sharing_consent` STRING,`enable_audit_enrollment` BOOLEAN,`enable_audit_data_reporting` BOOLEAN
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/enterprise_enterprisecustomer';
            ALTER TABLE `enterprise_enterprisecustomer` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)


class ImportEnterpriseCustomerUserTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportEnterpriseCustomerUserTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `enterprise_enterprisecustomeruser`;
            CREATE EXTERNAL TABLE `enterprise_enterprisecustomeruser` (
                `id` INT,`created` TIMESTAMP,`modified` TIMESTAMP,`user_id` INT,`enterprise_customer_id` STRING,`linked` BOOLEAN
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/enterprise_enterprisecustomeruser';
            ALTER TABLE `enterprise_enterprisecustomeruser` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)


class ImportEnterpriseCourseEnrollmentUserTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportEnterpriseCourseEnrollmentUserTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `enterprise_enterprisecourseenrollment`;
            CREATE EXTERNAL TABLE `enterprise_enterprisecourseenrollment` (
                `id` INT,`created` TIMESTAMP,`modified` TIMESTAMP,`course_id` STRING,`enterprise_customer_user_id` INT
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/enterprise_enterprisecourseenrollment';
            ALTER TABLE `enterprise_enterprisecourseenrollment` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)


class ImportDataSharingConsentTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportDataSharingConsentTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `consent_datasharingconsent`;
            CREATE EXTERNAL TABLE `consent_datasharingconsent` (
                `id` INT,`created` TIMESTAMP,`modified` TIMESTAMP,`username` STRING,`granted` BOOLEAN,`course_id` STRING,`enterprise_customer_id` STRING
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/consent_datasharingconsent';
            ALTER TABLE `consent_datasharingconsent` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)


class ImportUserSocialAuthTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportUserSocialAuthTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `social_auth_usersocialauth`;
            CREATE EXTERNAL TABLE `social_auth_usersocialauth` (
                `id` INT,`user_id` INT,`provider` STRING,`uid` STRING,`extra_data` STRING
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/social_auth_usersocialauth';
            ALTER TABLE `social_auth_usersocialauth` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)


class ImportVoucherTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportVoucherTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `voucher_voucher`;
            CREATE EXTERNAL TABLE `voucher_voucher` (
                `id` INT,`name` STRING,`code` STRING,`usage` STRING,`start_datetime` TIMESTAMP,`end_datetime` TIMESTAMP,`num_basket_additions` INT,`num_orders` INT,`total_discount` DECIMAL(12, 2),`date_created` TIMESTAMP
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/voucher_voucher';
            ALTER TABLE `voucher_voucher` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)


class ImportStockRecordTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportStockRecordTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `partner_stockrecord`;
            CREATE EXTERNAL TABLE `partner_stockrecord` (
                `id` INT,`partner_sku` STRING,`price_currency` STRING,`price_excl_tax` DECIMAL(12, 2),`price_retail` DECIMAL(12, 2),`cost_price` DECIMAL(12, 2),`num_in_stock` INT,`num_allocated` INT,`low_stock_threshold` INT,`date_created` TIMESTAMP,`date_updated` TIMESTAMP,`partner_id` INT,`product_id` INT
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/partner_stockrecord';
            ALTER TABLE `partner_stockrecord` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)


class ImportConditionalOfferTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportConditionalOfferTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `offer_conditionaloffer`;
            CREATE EXTERNAL TABLE `offer_conditionaloffer` (
                `id` INT,`name` STRING,`slug` STRING,`description` STRING,`offer_type` STRING,`status` STRING,`priority` INT,`start_datetime` TIMESTAMP,`end_datetime` TIMESTAMP,`max_global_applications` INT,`max_user_applications` INT,`max_basket_applications` INT,`max_discount` DECIMAL(12, 2),`total_discount` DECIMAL(12, 2),`num_applications` INT,`num_orders` INT,`redirect_url` STRING,`date_created` TIMESTAMP,`benefit_id` INT,`condition_id` INT,`email_domains` TIMESTAMP,`site_id` INT
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/offer_conditionaloffer';
            ALTER TABLE `offer_conditionaloffer` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)


class ImportBenefitTaskTestCase(TestCase):
    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        dt = '2014-07-01'
        kwargs = {'import_date': datetime.datetime.strptime(dt, '%Y-%m-%d').date()}
        task = ImportBenefitTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `offer_benefit`;
            CREATE EXTERNAL TABLE `offer_benefit` (
                `id` INT,`type` STRING,`value` DECIMAL(12, 2),`max_affected_items` INT,`proxy_class` STRING,`range_id` INT
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/offer_benefit';
            ALTER TABLE `offer_benefit` ADD PARTITION (dt = '{dt}');
            """.format(dt=dt)
        )
        self.assertEquals(query, expected_query)
