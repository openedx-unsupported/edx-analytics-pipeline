"""Compute metrics related to course enrollments"""

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


class EnterpriseCourseEnrollmentRecord(Record):
    """Summarizes a course's enrollment."""
    enterprise_id = StringField(length=32, nullable=False, description='')
    lms_user_id = IntegerField(nullable=False, description='')
    course_id = StringField(length=255, nullable=False, description='The course the learner is enrolled in.')
    enrollment_created_timestamp = DateTimeField(nullable=False, description='')
    user_current_enrollment_mode = StringField(length=32, nullable=False, description='')
    consent_granted = BooleanField(description='')
    letter_grade = StringField(length=32, description='')
    has_passed = BooleanField(description='')
    passed_timestamp = DateTimeField(description='')
    current_grade = FloatField(description='')
   
class EnterpriseCourseEnrollmentHiveTableTask(BareHiveTableTask):
    """
    Creates the metadata for the enterprise_course_enrollment hive table
    """
    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_course_enrollment'

    @property
    def columns(self):
        return EnterpriseCourseEnrollmentRecord.get_hive_schema()


class EnterpriseCourseEnrollmentHivePartitionTask(HivePartitionTask):
    """
    Generates the enterprise_course_enrollment hive partition.
    """
    date = luigi.DateParameter()

    @property
    def hive_table_task(self):  # pragma: no cover
        return EnterpriseCourseEnrollmentHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.date.isoformat()  # pylint: disable=no-member


class EnterpriseCourseEnrollmentDataTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    OverwriteAwareHiveQueryDataTask
):
    """
    Executes a hive query to gather enterprise course enrollment data and store it in the enterprise_course_enrollment hive table.
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
                enterprise_user.user_id AS lms_user_id,
                enterprise_course_enrollment.course_id,
                enterprise_course_enrollment.created AS enrollment_created_timestamp,
                enrollment.mode AS user_current_enrollment_mode,
                consent.granted AS consent_granted,
                grades.letter_grade,
                CASE
                    WHEN grades.passed_timestamp IS NULL THEN 0
                    ELSE 1
                END AS has_passed,
                grades.passed_timestamp,
                grades.percent_grade AS current_grade,
            FROM enterprise_enterprisecourseenrollment enterprise_course_enrollment
            JOIN student_courseenrollment enrollment
                ON enterprise_course_enrollment.course_id = enrollment.course_id
                AND enterprise_user.user_id = enrollment.user_id
            LEFT JOIN (
                SELECT
                    course_id,
                    MAX(`date`) AS latest_date
                FROM
                    user_activity_by_user
                GROUP BY
                    course_id
            ) user_activity
                ON enterprise_course_enrollment.course_id = user_activity.course_id
            LEFT JOIN consent_datasharingconsent consent
                ON auth_user.username =  consent.username
                AND enterprise_course_enrollment.course_id = consent.course_id
            LEFT JOIN grades_persistentcoursegrade grades
                ON enterprise_user.user_id = grades.user_id
                AND enterprise_course_enrollment.course_id = grades.course_id
        """

    @property
    def hive_partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return EnterpriseCourseEnrollmentHivePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(EnterpriseCourseEnrollmentDataTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportAuthUserTask(),
            ImportEnterpriseCourseEnrollmentUserTask(),
            ImportDataSharingConsentTask(),
            ImportStudentCourseEnrollmentTask(),
            ImportPersistentCourseGradeTask(),
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


class EnterpriseCourseEnrollmentMysqlTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    MysqlInsertTask
):
    """
    All course enrollments associated with their enterprise.
    """

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_course_enrollment'

    @property
    def insert_source_task(self):  # pragma: no cover
        return EnterpriseCourseEnrollmentDataTask(
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
        return EnterpriseCourseEnrollmentRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id',),
        ]


@workflow_entry_point
class ImportEnterpriseCourseEnrollmentsIntoMysql(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    luigi.WrapperTask
):
    """Import enterprise enrollment data into MySQL."""

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
            EnterpriseCourseEnrollmentMysqlTask(**kwargs),
        ]
