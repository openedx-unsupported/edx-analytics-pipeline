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
            ('catalog', 'INT'),
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