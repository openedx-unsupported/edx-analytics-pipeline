import datetime
import luigi

from edx.analytics.tasks.enrollments import (
    CourseEnrollmentDownstreamMixin,
    EnrollmentByModeTask,
)
from edx.analytics.tasks.load_internal_reporting_course_catalog import (
    ExtractCoursePartitionTask,
    ExtractProgramCoursePartitionTask,
    LoadInternalReportingCourseCatalogMixin,
)

from edx.analytics.tasks.util.hive import (
    BareHiveTableTask,
    hive_database_name,
    HivePartitionTask,
    HiveQueryToMysqlTask,
)
from edx.analytics.tasks.util.record import (
    DateTimeField,
    IntegerField,
    Record,
    StringField,
)


class CourseEnrollmentSummaryRecord(Record):
    """Recent enrollment summary and metadata for a course."""
    course_id = StringField(length=255, nullable=False)
    catalog_course_title = StringField(nullable=True, length=255,
                                       normalize_whitespace=True)
    start_time = DateTimeField(nullable=True)
    end_time = DateTimeField(nullable=True)
    count = IntegerField(nullable=True)
    count_change_7_days = IntegerField(nullable=True)
    cumulative_count = IntegerField(nullable=True)
    pacing_type = StringField(nullable=True, length=255)
    availability = StringField(nullable=True, length=255)
    enrollment_mode = StringField(
        length=100,
        nullable=False,
    )
    program_id = StringField(nullable=False, length=36)
    program_title = StringField(nullable=True, length=255,
                                normalize_whitespace=True)
    catalog_course = StringField(nullable=False, length=255)


class CourseEnrollmentSummaryDownstreamMixin(CourseEnrollmentDownstreamMixin, LoadInternalReportingCourseCatalogMixin):
    """Combines course enrollment and catalog parameters."""
    pass


class CourseEnrollmentSummaryWrapperTask(CourseEnrollmentSummaryDownstreamMixin,
                                         luigi.WrapperTask):
    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
            'overwrite': self.overwrite,
            'schema': self.schema,
            'credentials': self.credentials,
            'read_timeout': self.read_timeout,
            'marker_schema': self.marker_schema,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite_n_days': self.overwrite_n_days,
        }
        yield ImportCourseSummaryEnrollmentsIntoMysql(**kwargs)


class ImportCourseSummaryEnrollmentsIntoMysql(CourseEnrollmentSummaryDownstreamMixin,
                                              HiveQueryToMysqlTask):

    @property
    def table(self):
        return 'course_summary'

    @property
    def columns(self):
        return CourseEnrollmentSummaryRecord.get_sql_schema()

    def requires(self):
        yield CourseSummaryEnrollmentPartitionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
            date=self.date,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )


class CourseSummaryEnrollmentTableTask(BareHiveTableTask):
    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'course_meta_summary_enrollment'

    @property
    def columns(self):
        return CourseEnrollmentSummaryRecord.get_hive_schema()


class CourseSummaryEnrollmentPartitionTask(CourseEnrollmentSummaryDownstreamMixin, HivePartitionTask):

    def __init__(self, *args, **kwargs):
        super(CourseSummaryEnrollmentPartitionTask, self).__init__(*args, **kwargs)
        self.partition_value = self.date.isoformat()

    def query(self):
        """
        Returns query for course summary metadata, current enrollment counts for
        each enrollment mode, and the week difference in enrollment.
        """
        # TODO: end date closed?
        end_date = self.date
        start_date = self.date - datetime.timedelta(days=7)

        query = """
            USE {database_name};
            INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec}) {if_not_exists}
            SELECT
                enrollment_end.course_id,
                program.catalog_course_title,
                program.program_id,
                program.program_title,
                program.catalog_course,
                course.start_time,
                course.end_time,
                course.pacing_type,
                course.availability,
                enrollment_end.count,
                (enrollment_end.count - COALESCE(enrollment_start.count, 0)) as count_change_7_days
                enrollment_end.cumulative_count,
                enrollment_end.mode,
            FROM {enrollment_table} as enrollment_end
            LEFT JOIN {enrollment_table} as enrollment_start
                ON enrollment_start.course_id = enrollment_end.course_id
                AND enrollment_start.mode = enrollment_end.mode
            LEFT JOIN {program_table} as program
                ON program.course_id = ce_recent.course_id
            LEFT JOIN {course_table} as course
                ON course.course_id = ce_recent.course_id
            WHERE ce_recent.date = '{end_date}'
            AND ce_early.date = '{start_date}';
        """.format(
            database_name=hive_database_name(),
            enrollment_table='course_enrollment_mode_daily',
            program_table='program_course',
            course_table='course_catalog',
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        return query


    @property
    def hive_table_task(self):
        return CourseSummaryEnrollmentTableTask(
            warehouse_path=self.warehouse_path,
        )

    def requires(self):
        yield (
            self.hive_table_task,
            EnrollmentByModeTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
                overwrite_n_days=self.overwrite_n_days,
            ),
            ExtractProgramCoursePartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                api_root_url=self.api_root_url,
                api_page_size=self.api_page_size,
                overwrite=self.overwrite,
            ),
            ExtractCoursePartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                api_root_url=self.api_root_url,
                api_page_size=self.api_page_size,
                overwrite=self.overwrite,
            ),
        )
