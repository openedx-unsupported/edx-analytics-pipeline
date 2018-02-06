"""
Ensure we can write from MySQL to Hive data sources.
"""

import datetime
import textwrap
from unittest import TestCase

from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask, ImportEnterpriseCourseEnrollmentUserTask,
    ImportDataSharingConsentTask, ImportUserSocialAuthTask
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
                `created` TIMESTAMP,`modified` TIMESTAMP,`uuid` STRING,`name` STRING,`active` BOOLEAN,`site_id` INT,`catalog` INT,`enable_data_sharing_consent` BOOLEAN,`enforce_data_sharing_consent` STRING,`enable_audit_enrollment` BOOLEAN,`enable_audit_data_reporting` BOOLEAN
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
                `id` INT,`created` TIMESTAMP,`modified` TIMESTAMP,`user_id` INT,`enterprise_customer_id` STRING
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