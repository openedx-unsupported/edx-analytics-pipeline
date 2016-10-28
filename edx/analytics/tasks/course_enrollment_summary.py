import datetime
import luigi

from edx.analytics.tasks.enrollments import (
    CourseEnrollmentDownstreamMixin,
    EnrollmentByModeTask,
)
from edx.analytics.tasks.load_internal_reporting_course_catalog import (
    CoursePartitionTask,
    ProgramCoursePartitionTask,
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


class CourseSummaryEnrollmentRecord(Record):
    """Recent enrollment summary and metadata for a course."""
    course_id = StringField(length=255, nullable=False, description='A unique identifier of the course')
    catalog_course_title = StringField(nullable=True, length=255, normalize_whitespace=True,
                                       description='The name of the course')
    program_id = StringField(nullable=False, length=36, description='A unique identifier of the program')
    program_title = StringField(nullable=True, length=255, normalize_whitespace=True,
                                description='The display title for the program')
    catalog_course = StringField(nullable=False, length=255, description='Course identifier without run')
    start_time = DateTimeField(nullable=True, description='The date and time that the course begins')
    end_time = DateTimeField(nullable=True, description='The date and time that the course ends')
    pacing_type = StringField(nullable=True, length=255, description='The type of pacing for this course')
    availability = StringField(nullable=True, length=255, description='Availability status of the course')
    enrollment_mode = StringField(length=100, nullable=False, description='Enrollment mode for the enrollment counts')
    count = IntegerField(nullable=True, description='The count of currently enrolled learners')
    count_change_7_days = IntegerField(nullable=True,
                                       description='Difference in enrollment counts over the past 7 days')
    cumulative_count = IntegerField(nullable=True, description='The cumulative total of all users ever enrolled')


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
        return 'course_meta_summary_enrollment'

    @property
    def columns(self):
        return CourseSummaryEnrollmentRecord.get_sql_schema()

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
        return CourseSummaryEnrollmentRecord.get_hive_schema()


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
                enrollment_end.mode,
                enrollment_end.count,
                (enrollment_end.count - COALESCE(enrollment_start.count, 0)) as count_change_7_days
                enrollment_end.cumulative_count,
            FROM course_enrollment_mode_daily as enrollment_end
            LEFT JOIN course_enrollment_mode_daily as enrollment_start
                ON enrollment_start.course_id = enrollment_end.course_id
                AND enrollment_start.mode = enrollment_end.mode
            LEFT JOIN program_course as program
                ON program.course_id = enrollment_end.course_id
            LEFT JOIN course_catalog as course
                ON course.course_id = enrollment_end.course_id
            WHERE enrollment_end.date = '{end_date}'
            AND enrollment_start.date = '{start_date}';
        """.format(
            database_name=hive_database_name(),
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
            ProgramCoursePartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                api_root_url=self.api_root_url,
                api_page_size=self.api_page_size,
                overwrite=self.overwrite,
            ),
            CoursePartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                api_root_url=self.api_root_url,
                api_page_size=self.api_page_size,
                overwrite=self.overwrite,
            ),
        )
