"""Compute metrics related to subsection grades"""

import logging

import luigi
import luigi.task

from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportDataSharingConsentTask,
    ImportEnterpriseCourseEnrollmentUserTask, ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask
)
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserProfileTask, ImportAuthUserTask, ImportPersistentCourseGradeTask
)
from edx.analytics.tasks.insights.enrollments import (
    OverwriteHiveAndMysqlDownstreamMixin
)
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, OverwriteAwareHiveQueryDataTask
from edx.analytics.tasks.util.record import (
    DateTimeField, FloatField, Record, StringField
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    LoadInternalReportingCourseCatalogMixin
)

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
    subsection_block_id = StringField(length=255, nullable=False)
    first_attempted = DateTimeField(nullable=True, description='')
    subsection_grade_created = DateTimeField(nullable=True, description='')
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
                grade.course_id,
                grade.usage_key as subsection_block_id,
                subsection_.subsection_index,
                grade.first_attempted,
                pdu.user_username,
                grade.earned_all,
                grade.possible_all,
                grade.earned_graded,
                grade.possible_graded,
            FROM
                grades_persistentsubsectiongrade AS grade
            JOIN (
                SELECT
                 enterprise_customer.name AS enterprise_name,
                 enterprise_uuid,
                 enterprise_customer_id,
                FROM enterprise_enterprisecourseenrollment enterprise_course_enrollment
                JOIN enterprise_enterprisecustomeruser enterprise_user
                    ON enterprise_course_enrollment.enterprise_customer_user_id = enterprise_user.id
                JOIN enterprise_enterprisecustomer enterprise_customer
                    ON enterprise_user.enterprise_customer_id = enterprise_customer.uuid
            ) as enterprise_data
            
            JOIN enterprise_enterprisecourseenrollment enrollment
                ON grade.user_id = enrollment.user_id 
                AND grade.course_id = 
                    /*Pull in metadata on the subsection.*/
            LEFT JOIN
                    production.course_structure as struct
            ON
                    grade.usage_key = struct.block_id
                    /*Pull in metadata on the section.*/
            LEFT JOIN
                    production.course_structure as sect
            ON
                    struct.section_block_id = sect.block_id
            LEFT JOIN (
                    /*Create rank "section_index" that says "this is the 1st
                      section in the course, the second section in the course,
                      etc.*/
                   with sub as (
                    select
                            course_block_id,
                            section_block_id,
                            MAX(order_index) as order_index
                    from 
                            production.course_structure
                    where
                            /*Filter out higher level blocks 
                            so indexing is not thrown off.*/
                            block_type not in (
                            'vertical',
                            'sequential',
                            'chapter',
                            'course')
                    group by
                            1,2)
                    select
                            *,
                            ROW_NUMBER() OVER 
                            (PARTITION BY course_block_id 
                            ORDER BY order_index) as section_index
                    from
                            sub) as section_
            ON
                    struct.section_block_id = section_.section_block_id
            LEFT JOIN (
                    /*Create rank "subsection_index" that says "this is the 1st
                      subsection in the section, the second subsection in the section,
                      etc.*/
                    WITH sub as (
                    SELECT
                            course_block_id,
                            section_block_id,
                            subsection_block_id,
                            MAX(order_index) as order_index
                    FROM 
                            production.course_structure
                    WHERE
                            /*Filter out higher level blocks 
                            so indexing is not thrown off.*/
                            block_type not in (
                            'vertical',
                            'sequential',
                            'chapter',
                            'course')
                    GROUP BY
                            1,2,3)
                    SELECT
                            *,
                            ROW_NUMBER() OVER 
                            (PARTITION BY course_block_id, section_block_id 
                            ORDER BY order_index) as subsection_index
                    FROM
                            sub) as subsection_
            ON
                    struct.block_id = subsection_.subsection_block_id
            JOIN
                    enterprise.base_enterprise_user as eu
            ON
                    pdu.user_id = eu.lms_user_id
            JOIN
                    enterprise.base_enterprise_customer as ec
            ON
                    eu.enterprise_customer_uuid = ec.enterprise_customer_uuid
            WHERE
                    /* Naive filter to have only Pearson records. Because there is future
                    potential of other customers have access this, good to build in such a
                    way that this can be generalized to any enterprise customer name.*/
                    ec.enterprise_customer_name = 'Pearson'
            ORDER BY
                    grade.course_id,
                    pdu.user_username,
                        
                
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
            ImportAuthUserProfileTask(),
            ImportEnterpriseCustomerTask(),
            ImportEnterpriseCustomerUserTask(),
            ImportEnterpriseCourseEnrollmentUserTask(),
            ImportDataSharingConsentTask(),
            ImportPersistentCourseGradeTask(),
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
