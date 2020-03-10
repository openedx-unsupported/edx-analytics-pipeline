"""Compute metrics related to enterprise engagement in courses"""

import datetime
import logging
import os

import luigi
import luigi.task

from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportBenefitTask, ImportConditionalOfferTask, ImportDataSharingConsentTask,
    ImportEnterpriseCourseEnrollmentUserTask, ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask,
    ImportUserSocialAuthTask, ImportVoucherTask
)
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserProfileTask, ImportAuthUserTask, ImportCourseEntitlementTask, ImportCurrentOrderDiscountState,
    ImportCurrentOrderLineState, ImportCurrentOrderState, ImportEcommerceUser, ImportPersistentCourseGradeTask,
    ImportProductCatalog, ImportProductCatalogAttributes, ImportProductCatalogAttributeValues,
    ImportStudentCourseEnrollmentTask
)
from edx.analytics.tasks.insights.enrollments import (
    CourseEnrollmentSummaryPartitionTask, OverwriteHiveAndMysqlDownstreamMixin
)
from edx.analytics.tasks.insights.module_engagement import ModuleEngagementPartitionTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, OverwriteAwareHiveQueryDataTask
from edx.analytics.tasks.util.record import (
    BooleanField, DateField, DateTimeField, FloatField, IntegerField, Record, StringField
)
from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    CoursePartitionTask, LoadInternalReportingCourseCatalogMixin
)

log = logging.getLogger(__name__)


class EnterpriseEngagementRecord(Record):
    """Summarizes an entrprise's learner engagement."""
    course_id = StringField(length=255, nullable=False, description='The course the learner is enrolled in.')
    section_block_id = StringField(length=564, nullable=False, description='Block identifier.')
    section_display_name = StringField(length=255, nullable=False, truncate=True, normalize_whitespace=True,
                               description='User-facing title of the section.')
    section_index = IntegerField(description='')
    subsection_block_id = StringField(length=564, nullable=False, description='Block identifier.')
    subsection_display_name = StringField(length=255, nullable=False, truncate=True, normalize_whitespace=True,
                               description='User-facing title of the subsection.')
    subsection_index = IntegerField(description='')
    component_name = StringField(length=255, nullable=False, truncate=True, normalize_whitespace=True, description='')
    username = StringField(length=255, description='')
    date = DateTimeField(description='')
    component_block_id = StringField(length=564, nullable=False, description='Block identifier.')
    event = StringField(length=30, nullable=False, description='The interaction the learner had with the entity.'
                                                               ' Example: "viewed".')
    count = IntegerField(nullable=False, description='Number of interactions the learner had with this entity on this'
                                                     ' date.')
    entity_type = StringField(length=30, nullable=False, description='Category of entity that the learner interacted'
                                                                     ' with. Example: "video".')
    lms_web_url = StringField(
        length=255, nullable=True, description='The URL for navigating to the current block from within the course.'
                                               '  These all use "jump_to" syntax (i.e. "https://courses.edx.org/courses/(course_id)/jump_to/(block_id)").'
    )
    enterprise_id = StringField(length=32, nullable=False, description='')
    enterprise_name = StringField(length=255, nullable=False, description='')


class EnterpriseEngagementHiveTableTask(BareHiveTableTask):
    """
    Creates the metadata for the enterprise_engagement hive table
    """
    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_engagement'

    @property
    def columns(self):
        return EnterpriseEngagementRecord.get_hive_schema()


class EnterpriseEngagementHivePartitionTask(HivePartitionTask):
    """
    Generates the enterprise_engagement hive partition.
    """
    date = luigi.DateParameter()

    @property
    def hive_table_task(self):  # pragma: no cover
        return EnterpriseEngagementHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.date.isoformat()  # pylint: disable=no-member


class EnterpriseEngagementDataTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    OverwriteAwareHiveQueryDataTask
):
    """
    Executes a hive query to gather enterprise engagement data and store it in the enterprise_engagement hive table.
    """

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        return """
            SELECT
                    me.course_id,
                    struct.section_block_id,
                    sect.display_name as section_display_name,
                    section_.section_index,
                    struct.subsection_block_id,
                    subsect.display_name as subsection_display_name,
                    subsection_.subsection_index,
                    struct.display_name as component_name,
                    me.username,
                    me.date,
                    me.entity_id as component_block_id,
                    me.event,
                    me.count,
                    me.entity_type,
                    struct.lms_web_url
            FROM
                    /*Get information on the component name.*/
                    production.course_structure as struct
            LEFT JOIN
                    production.course_structure as sect
            ON
                    struct.section_block_id = sect.block_id
            LEFT JOIN
                    production.course_structure as subsect
                    
            ON
                    struct.subsection_block_id = subsect.block_id 
            LEFT JOIN
                    /*Note: the Vertica version of this table does
                      not include all data. Production version built
                      on top of read replica should check that lifetime
                      data is piping through.*/
                    insights.module_engagement as me
            ON
                    struct.block_id = me.entity_id
            LEFT JOIN
                    production.d_user as pdu
            ON
                    me.username = pdu.user_username
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
                    struct.subsection_block_id = subsection_.subsection_block_id
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
            AND
                    /* Per edX legal review, throw out all discussion data 
                    from delivery.*/
                    me.entity_type != 'discussion'
            ORDER BY
                    me.course_id,
                    me.username,
                    struct.order_index{code}
        """

    @property
    def hive_partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return EnterpriseEngagementHivePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(EnterpriseEngagementDataTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportAuthUserTask(),
            ImportAuthUserProfileTask(),
            ImportEnterpriseCustomerTask(),
            ImportEnterpriseCustomerUserTask(),
            ModuleEngagementPartitionTask(
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite_hive,
            ),
            AllCourseBlockRecordsTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite_hive,
            )
        )


class EnterpriseEngagementMysqlTask(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    MysqlInsertTask
):
    """
    All engagements of enterprise users in courses associated with their enterprise.
    """

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_engagement'

    @property
    def insert_source_task(self):  # pragma: no cover
        return EnterpriseEngagementDataTask(
            warehouse_path=self.warehouse_path,
            overwrite_hive=self.overwrite_hive,
            overwrite_mysql=self.overwrite_mysql,
            overwrite=self.overwrite,
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
        )

    @property
    def columns(self):
        return EnterpriseEngagementRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id',),
        ]


@workflow_entry_point
class ImportEnterpriseEngagementIntoMysql(
    OverwriteHiveAndMysqlDownstreamMixin,
    LoadInternalReportingCourseCatalogMixin,
    luigi.WrapperTask
):
    """Import enterprise engagement data into MySQL."""

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'overwrite_hive': self.overwrite_hive,
            'overwrite_mysql': self.overwrite_mysql,
            'overwrite': self.overwrite_hive,
            'date': self.date,
            'n_reduce_tasks' = self.n_reduce_tasks,
        }

        yield [
            EnterpriseEngagementMysqlTask(**kwargs),
        ]
