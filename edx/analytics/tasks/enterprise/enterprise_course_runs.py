"""Compute metrics related to user enrollments in courses"""

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


class EnterpriseCourseRunRecord(Record):
    """Summarizes a course."""
    course_id = StringField(length=255, nullable=False, description='The course the learner is enrolled in.')
    course_title = StringField(length=255, description='')
    course_start = DateTimeField(description='')
    course_end = DateTimeField(description='')
    course_pacing_type = StringField(length=32, description='')
    course_duration_weeks = StringField(length=32, description='')
    course_min_effort = IntegerField(description='')
    course_max_effort = IntegerField(description='')
    course_key = StringField(length=255, description='')

class EnterpriseCourseRunHiveTableTask(BareHiveTableTask):
    """
    Creates the metadata for the enterprise_course_run hive table
    """
    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_course_run'

    @property
    def columns(self):
        return EnterpriseCourseRunRecord.get_hive_schema()


class EnterpriseCourseRunHivePartitionTask(HivePartitionTask):
    """
    Generates the enterprise_course_run hive partition.
    """
    date = luigi.DateParameter()

    @property
    def hive_table_task(self):  # pragma: no cover
        return EnterpriseCourseRunHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.date.isoformat()  # pylint: disable=no-member


class EnterpriseCourseRunDataTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    OverwriteAwareHiveQueryDataTask
):
    """
    Executes a hive query to gather enterprise course run data and store it in the enterprise_course_run hive table.
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
            SELECT DISTINCT enterprise_course_enrollment.course_id,
                enterprise_course_enrollment.created AS enrollment_created_timestamp,
                course.catalog_course_title AS course_title,
                course.start_time AS course_start,
                course.end_time AS course_end,
                course.pacing_type AS course_pacing_type,
                CASE
                    WHEN course.pacing_type = 'self_paced' THEN 'Self Paced'
                    ELSE CAST(CEIL(DATEDIFF(course.end_time, course.start_time) / 7) AS STRING)
                END AS course_duration_weeks,
                course.min_effort AS course_min_effort,
                course.max_effort AS course_max_effort,
                course.catalog_course AS course_key,
            FROM enterprise_enterprisecourseenrollment enterprise_course_enrollment
            JOIN course_catalog course
                ON enterprise_course_enrollment.course_id = course.course_id
       """

    @property
    def hive_partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return EnterpriseCourseRunHivePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(EnterpriseCourseRunDataTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportEnterpriseCourseEnrollmentUserTask(),
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


class EnterpriseCourseRunMysqlTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    MysqlInsertTask
):
    """
    All enterprise course runs
    """

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_course_run'

    @property
    def insert_source_task(self):  # pragma: no cover
        return EnterpriseCourseRunDataTask(
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
        return EnterpriseCourseRunRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
        ]


@workflow_entry_point
class ImportEnterpriseCourseRunIntoMysql(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    luigi.WrapperTask
):
    """Import enterprise course run data into MySQL."""

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
            EnterpriseCourseRunMysqlTask(**kwargs),
        ]
