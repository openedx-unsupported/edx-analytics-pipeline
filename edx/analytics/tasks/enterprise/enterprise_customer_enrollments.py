"""Compute metrics related to user enrollments in courses"""
# TODO: Fix query , simplify to scope of this class
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


class EnterpriseCustomerRecord(Record):
    """Summarizes a customer's enrollment"""
    enterprise_id = StringField(length=32, nullable=False, description='')
    enterprise_name = StringField(length=255, nullable=False, description='')
    enterprise_site_id = IntegerField(description='')

class EnterpriseCustomerHiveTableTask(BareHiveTableTask):
    """
    Creates the metadata for the enterprise_customer_enrollment hive table
    """
    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_customer_enrollment'

    @property
    def columns(self):
        return EnterpriseCustomerRecord.get_hive_schema()


class EnterpriseCustomerHivePartitionTask(HivePartitionTask):
    """
    Generates the enterprise_customer_enrollment hive partition.
    """
    date = luigi.DateParameter()

    @property
    def hive_table_task(self):  # pragma: no cover
        return EnterpriseCustomerHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.date.isoformat()  # pylint: disable=no-member


class EnterpriseCustomerDataTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    OverwriteAwareHiveQueryDataTask
):
    """
    Executes a hive query to gather enterprise customer enrollment data and store it in the enterprise_customer_enrollment hive table.
    """

    otto_credentials = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'credentials'}
    )
    otto_database = luigi.Parameter(
        config_path={'section': 'otto-database-import', 'name': 'database'}
    )

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        return """
            SELECT DISTINCT enterprise_customer.uuid AS enterprise_id,
                enterprise_customer.name AS enterprise_name,
                enterprise_customer.site_id AS enterprise_site_id,
            FROM enterprise_enterprisecourseenrollment enterprise_course_enrollment
            JOIN enterprise_enterprisecustomer enterprise_customer
                ON enterprise_user.enterprise_customer_id = enterprise_customer.uuid
        """

    @property
    def hive_partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return EnterpriseCustomerHivePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(EnterpriseCustomerDataTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportEnterpriseCourseEnrollmentUserTask(),
            ImportEnterpriseCustomerTask(),
            CoursePartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                api_root_url=self.api_root_url,
                api_page_size=self.api_page_size,
            ),
            UserActivityTableTask(
                warehouse_path=self.warehouse_path,
                overwrite_n_days=0,
                date=self.date
            )
        )

        kwargs = {
            'credentials': self.otto_credentials,
            'database': self.otto_database,
        }
        yield (
            ImportProductCatalog(**kwargs),
            ImportCurrentOrderLineState(**kwargs),
            ImportCurrentOrderDiscountState(**kwargs),
            ImportVoucherTask(**kwargs),
            ImportStockRecordTask(**kwargs),
            ImportCurrentOrderState(**kwargs),
            ImportEcommerceUser(**kwargs),
            ImportConditionalOfferTask(**kwargs),
            ImportBenefitTask(**kwargs),
        )


class EnterpriseCustomerMysqlTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    MysqlInsertTask
):
    """
    All enrollments of enterprise customers
    """

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_customer_enrollment'

    @property
    def insert_source_task(self):  # pragma: no cover
        return EnterpriseCustomerDataTask(
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
        return EnterpriseCustomerRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id',),
        ]


@workflow_entry_point
class ImportEnterpriseCustomerIntoMysql(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    luigi.WrapperTask
):
    """Import enterprise customer data into MySQL."""

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
            EnterpriseCustomerMysqlTask(**kwargs),
        ]
