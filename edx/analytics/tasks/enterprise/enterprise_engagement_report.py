"""Generates an enterprise learner engagement report"""
import luigi

from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.enterprise.enterprise_database_imports import (
    ImportEnterpriseCustomerTask, ImportEnterpriseCustomerUserTask
)
from edx.analytics.tasks.insights.database_imports import DatabaseImportMixin
from edx.analytics.tasks.insights.module_engagement import ModuleEngagementWorkflowTask

from edx.analytics.tasks.util.hive import HivePartition, HiveTableFromQueryTask
from edx.analytics.tasks.util.record import (
    DateTimeField, IntegerField, Record, StringField
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_structure import AllCourseBlockRecordsTask
from edx.analytics.tasks.warehouse.load_internal_reporting_user import AggregateInternalReportingUserTableHive


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


class BuildEnterpriseEngagementReportTask(DatabaseImportMixin, MapReduceJobTaskMixin, HiveTableFromQueryTask):
    """
    Builds the enterprise learner engagement report.
    """

    def requires(self):
        for requirement in super(BuildEnterpriseEngagementReportTask, self).requires():
            yield requirement

        yield (
            AggregateInternalReportingUserTableHive(
                n_reduce_tasks=self.n_reduce_tasks,
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite
            ),
            ImportEnterpriseCustomerTask(
                import_date=self.import_date
            ),
            ImportEnterpriseCustomerUserTask(
                import_date=self.import_date
            ),
            ModuleEngagementWorkflowTask(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks
            ),
            AllCourseBlockRecordsTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite
            )
        )

    @property
    def table(self):
        return 'enterprise_engagement_report'

    @property
    def columns(self):
        return EnterpriseEngagementRecord.get_hive_schema()

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
                    ec.enterprise_customer_uuid,
                    ec.enterprise_customer_name,
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
                    course_block_records as struct
            LEFT JOIN
                    course_block_records as sect
            ON
                    struct.section_block_id = sect.block_id
            LEFT JOIN
                    course_block_records as subsect
                    
            ON
                    struct.subsection_block_id = subsect.block_id 
            LEFT JOIN
                    /*Note: the Vertica version of this table does
                      not include all data. Production version built
                      on top of read replica should check that lifetime
                      data is piping through.*/
                    module_engagement as me
            ON
                    struct.block_id = me.entity_id
            LEFT JOIN
                    internal_reporting_user as pdu
            ON
                    me.username = pdu.username
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
                            course_block_records
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
                            course_block_records
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
                    enterprise_enterprisecustomeruser as eu
            ON
                    pdu.user_id = eu.user_id
            JOIN
                    enterprise_enterprisecustomer as ec
            ON
                    eu.enterprise_customer_id = ec.uuid
            WHERE
                    /* Per edX legal review, throw out all discussion data 
                    from delivery.*/
                    me.entity_type != 'discussion'
            ORDER BY
                    me.course_id,
                    me.username
            ;
        """


class EnterpriseEngagementMysqlTask(MysqlInsertTask):
    """
    All engagements of enterprise users in courses associated with their enterprise.
    """
    import_date = luigi.DateParameter()

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_learner_engagement'

    @property
    def insert_source_task(self):  # pragma: no cover
        return (
            BuildEnterpriseEngagementReportTask(
                import_date=self.import_date,
                n_reduce_tasks=self.n_reduce_tasks
            )
        )
    @property
    def columns(self):
        return EnterpriseEngagementRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id', 'course_id'),
        ]


class ImportEnterpriseEngagementsIntoMysql(luigi.WrapperTask):
    """Import enterprise engagement data into MySQL."""

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'date': self.date,
            'n_reduce_tasks' = self.n_reduce_tasks,
        }

        yield [
            EnterpriseEngagementMysqlTask(**kwargs),
        ]
