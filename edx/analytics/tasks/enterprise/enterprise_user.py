"""Gather data related to enterprise users"""

import logging

import luigi
import luigi.task

from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportBenefitTask, ImportConditionalOfferTask, ImportDataSharingConsentTask,
    ImportEnterpriseCourseEnrollmentUserTask, ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask,
    ImportStockRecordTask, ImportUserSocialAuthTask, ImportVoucherTask
)
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserProfileTask, ImportAuthUserTask, ImportCurrentOrderDiscountState, ImportCurrentOrderLineState,
    ImportCurrentOrderState, ImportEcommerceUser, ImportPersistentCourseGradeTask, ImportProductCatalog,
    ImportStudentCourseEnrollmentTask
)
from edx.analytics.tasks.insights.enrollments import OverwriteHiveAndMysqlDownstreamMixin
from edx.analytics.tasks.insights.user_activity import UserActivityTableTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, OverwriteAwareHiveQueryDataTask
from edx.analytics.tasks.util.record import (
    BooleanField, DateField, DateTimeField, FloatField, IntegerField, Record, StringField
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    CoursePartitionTask, LoadInternalReportingCourseCatalogMixin
)

log = logging.getLogger(__name__)


class EnterpriseUserRecord(Record):
    """Summarizes an enterprise user"""
    enterprise_id = StringField(length=32, nullable=False, description='')
    lms_user_id = IntegerField(nullable=False, description='')
    enterprise_user_id = IntegerField(nullable=False, description='')
    enterprise_sso_uid = StringField(length=255, description='')
    user_account_creation_timestamp = DateTimeField(description='')
    user_email = StringField(length=255, description='')
    user_username = StringField(length=255, description='')
    user_country_code = StringField(length=2, description='')
    last_activity_date = DateField(description='')


class EnterpriseUserHiveTableTask(BareHiveTableTask):
    """
    Creates the metadata for the enterprise_user hive table
    """
    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_user'

    @property
    def columns(self):
        return EnterpriseUserRecord.get_hive_schema()


class EnterpriseUserHivePartitionTask(HivePartitionTask):
    """
    Generates the enterprise_user hive partition.
    """
    date = luigi.DateParameter()

    @property
    def hive_table_task(self):  # pragma: no cover
        return EnterpriseUserHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.date.isoformat()  # pylint: disable=no-member


class EnterpriseUserDataTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    OverwriteAwareHiveQueryDataTask
):
    """
    Executes a hive query to gather enterprise user data and store it in the enterprise_user hive table.
    """

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        return """
            SELECT DISTINCT enterprise_user.enterprise_customer_id AS enterprise_id,
                enterprise_user.user_id AS lms_user_id,
                enterprise_user.id AS enterprise_user_id,
                SUBSTRING_INDEX(social_auth.uid_full, ':', -1) AS enterprise_sso_uid,
                auth_user.date_joined AS user_account_creation_timestamp,
                auth_user.email AS user_email,
                auth_user.username AS user_username,
                user_profile.country AS user_country_code,
                user_activity.latest_date AS last_activity_date
            FROM enterprise_enterprisecustomeruser enterprise_user
            JOIN auth_user auth_user
                    ON enterprise_user.user_id = auth_user.id
            JOIN auth_userprofile user_profile
                    ON enterprise_user.user_id = user_profile.user_id
            LEFT JOIN (
                SELECT
                    user_id,
                    MAX(`date`) AS latest_date
                FROM
                    user_activity_by_user
                GROUP BY
                    user_id
            ) user_activity
                ON enterprise_user.user_id = user_activity.user_id
            LEFT JOIN (
                SELECT user_id, uid AS uid_full
                    FROM social_auth_usersocialauth sauth_a
                    INNER JOIN (
                        SELECT MAX(id) AS id
                        FROM social_auth_usersocialauth
                        WHERE provider = 'tpa-saml'
                        GROUP BY user_id
                        ) sauth_b
                            ON sauth_a.id = sauth_b.id
            ) social_auth
                ON enterprise_user.user_id = social_auth.user_id
            WHERE enterprise_user.linked = 1
        """

    @property
    def hive_partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return EnterpriseUserHivePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(EnterpriseUserDataTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportAuthUserTask(),
            ImportAuthUserProfileTask(),
            ImportEnterpriseCustomerUserTask(),
            ImportUserSocialAuthTask(),
            UserActivityTableTask(
                warehouse_path=self.warehouse_path,
                overwrite_n_days=0,
                date=self.date
            )
        )


class EnterpriseUserMysqlTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    MysqlInsertTask
):
    """
    All of enterprise users
    """

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_user'

    @property
    def insert_source_task(self):  # pragma: no cover
        return EnterpriseUserDataTask(
            warehouse_path=self.warehouse_path,
            overwrite_hive=self.overwrite_hive,
            overwrite_mysql=self.overwrite_mysql,
            overwrite=self.overwrite,
            date=self.date,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
        )

    @property
    def columns(self):
        return EnterpriseUserRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id',),
        ]


@workflow_entry_point
class ImportEnterpriseUsersIntoMysql(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    luigi.WrapperTask
):
    """Import enterprise user data into MySQL."""

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'overwrite_hive': self.overwrite_hive,
            'overwrite_mysql': self.overwrite_mysql,
            'overwrite': self.overwrite_hive,
            'date': self.date,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
        }

        yield [
            EnterpriseUserMysqlTask(**kwargs),
        ]
