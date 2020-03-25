"""Compute metrics related to subsection grades"""

import logging

import luigi
import luigi.task

from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportEnterpriseCourseEnrollmentUserTask, ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask
)
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserTask, ImportPersistentSubsectionGradeTask
)
from edx.analytics.tasks.insights.enrollments import (
    OverwriteHiveAndMysqlDownstreamMixin
)
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, OverwriteAwareHiveQueryDataTask
from edx.analytics.tasks.util.record import (
    DateTimeField, FloatField, Record, StringField, IntegerField
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    LoadInternalReportingCourseCatalogMixin
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_structure import AllCourseBlockRecordsTask

log = logging.getLogger(__name__)


class EnterpriseSubsectionGrade(Record):
    """Summarizes subsections of course grades."""
    enterprise_id = StringField(length=32, nullable=False, description='')
    enterprise_name = StringField(length=255, nullable=False, description='')
    enterprise_user_id = StringField(length=255, description='')
    lms_user_id = StringField(length=255, description='')
    user_email = StringField(length=255, description='')
    username = StringField(length=255, description='')

    course_id = StringField(length=255, nullable=False)

    section_block_id = StringField(length=564, nullable=False)
    section_display_name = StringField(length=255, nullable=False)
    section_index = IntegerField(length=255, nullable=False)

    subsection_block_id = StringField(length=564, nullable=False)
    subsection_display_name = StringField(length=255, nullable=False)
    subsection_index = IntegerField(nullable=False)
    subsection_grade_created = DateTimeField(nullable=True, description='')
    first_attempted = DateTimeField(nullable=True, description='')
    earned_all = FloatField(description='')
    possible_all = FloatField(description='')
    earned_graded = FloatField(description='')
    possible_graded = FloatField(description='')


class EnterpriseSubsectionGradeHiveTableTask(BareHiveTableTask):
    """
    Creates the metadata for the enterprise_subsection_grade hive table
    """
    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_subsection_grade'

    @property
    def columns(self):
        return EnterpriseSubsectionGrade.get_hive_schema()


class EnterpriseSubsectionGradeHivePartitionTask(HivePartitionTask):
    """
    Generates the enterprise_enrollment hive partition.
    """
    date = luigi.DateParameter()

    @property
    def hive_table_task(self):  # pragma: no cover
        return EnterpriseSubsectionGradeHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.date.isoformat()  # pylint: disable=no-member


class EnterpriseSubsectionGradeDataTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    OverwriteAwareHiveQueryDataTask
):
    """
    Executes a hive query to gather EnterpriseSubsectionGrade and store it in the enterprise_subsection_grade hive
     table.
    """
    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        return """      
            SELECT
                enterprise_customer.uuid AS enterprise_id,
                enterprise_customer.name AS enterprise_name,
                enterprise_customer_user.id AS enterprise_user_id,
                auth_user.id AS lms_user_id,
                auth_user.username AS username,
                auth_user.email AS user_email,
            
                subsection_grade.course_id AS course_id,
            
                course_section.block_id AS section_block_id,
                course_section.display_name AS section_display_name,
                section_data.section_index AS section_index,
            
                course_subsection.block_id AS subsection_block_id,
                course_subsection.display_name AS subsection_display_name,
                subsection_data.subsection_index AS subsection_index,
                subsection_grade.created AS subsection_grade_created,
                subsection_grade.first_attempted AS first_attempted,
                subsection_grade.earned_all AS earned_all,
                subsection_grade.possible_all AS possible_all,
                subsection_grade.earned_graded AS earned_graded,
                subsection_grade.possible_graded AS possible_graded
            FROM
                grades_persistentsubsectiongrade AS subsection_grade
            JOIN auth_user AS auth_user
                ON auth_user.id = subsection_grade.user_id
            JOIN enterprise_enterprisecustomeruser AS enterprise_customer_user
                ON enterprise_customer_user.user_id = auth_user.id
            JOIN enterprise_enterprisecourseenrollment AS enterprise_course_enrollment
                ON enterprise_course_enrollment.enterprise_customer_user_id = enterprise_customer_user.id
                AND enterprise_course_enrollment.course_id = subsection_grade.course_id
            JOIN enterprise_enterprisecustomer AS enterprise_customer
                ON enterprise_customer.uuid = enterprise_customer_user.enterprise_customer_id
            LEFT JOIN course_block_records AS course_subsection
                ON course_subsection.block_id = subsection_grade.usage_key
            LEFT JOIN course_block_records AS course_section
                ON course_section.block_id = course_subsection.section_block_id
            LEFT JOIN (
                /*Create rank "section_index" that says "this is the 1st
                section in the course, the second section in the course,
                etc.*/
                WITH sub_table AS (
                    SELECT
                        course_block_id,
                        section_block_id,
                        MAX(order_index) AS order_index
                    FROM
                        course_block_records
                    WHERE
                        /*Filter out higher level blocks so indexing is not thrown off.*/
                        block_type NOT IN ('vertical', 'sequential', 'chapter', 'course')
                    GROUP BY 1,2
                )
                    SELECT
                        section_block_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY course_block_id
                            ORDER BY order_index
                        ) AS section_index
                    from
                        sub_table
            ) AS section_data
            ON
                course_section.block_id = section_data.section_block_id
            LEFT JOIN (
                /*Create rank "subsection_index" that says "this is the 1st
                  subsection in the section, the second subsection in the section,
                  etc.*/
                WITH sub_table AS (
                    SELECT
                        course_block_id,
                        section_block_id,
                        subsection_block_id,
                        MAX(order_index) AS order_index
                    FROM
                        course_block_records
                    WHERE
                        /*Filter out higher level blocks so indexing is not thrown off.*/
                        block_type NOT IN ('vertical', 'sequential', 'chapter', 'course')
                    GROUP BY 1,2,3
                )
                    SELECT
                        subsection_block_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY course_block_id, section_block_id
                            ORDER BY order_index
                        ) AS subsection_index
                    FROM
                        sub_table
            ) AS subsection_data
            ON
                course_subsection.block_id = subsection_data.subsection_block_id
            ORDER BY
                subsection_grade.course_id,
                auth_user.username,
                course_subsection.order_index
        """

    @property
    def hive_partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return EnterpriseSubsectionGradeHivePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(EnterpriseSubsectionGradeDataTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportAuthUserTask(),
            ImportEnterpriseCustomerTask(),
            ImportEnterpriseCustomerUserTask(),
            ImportEnterpriseCourseEnrollmentUserTask(),
            ImportPersistentSubsectionGradeTask(),
            AllCourseBlockRecordsTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
        )


class EnterpriseSubsectionGradeMysqlTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    MysqlInsertTask
):
    """
    All subsection grades of enterprise users enrolled in courses associated with their enterprise.
    """

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_subsection_grade'

    @property
    def insert_source_task(self):  # pragma: no cover
        return EnterpriseSubsectionGradeDataTask(
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
        return EnterpriseSubsectionGrade.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id',),
            ('user_email',),
            ('course_id',),
        ]


@workflow_entry_point
class ImportEnterpriseEnrollmentsIntoMysql(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    luigi.WrapperTask
):
    """Import enterprise_subsection_grade into MySQL."""

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
            EnterpriseSubsectionGradeMysqlTask(**kwargs),
        ]
