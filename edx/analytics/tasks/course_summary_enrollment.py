"""
Luigi tasks for composing the course summary enrollment SQL table, which includes
course metadata and recent enrollment numbers.
"""
import datetime
import luigi

from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.enrollments import (
    CourseEnrollmentDownstreamMixin,
    EnrollmentByModeTask,
)
from edx.analytics.tasks.load_internal_reporting_course_catalog import (
    CoursePartitionTask,
    LoadInternalReportingCourseCatalogMixin,
    ProgramCoursePartitionTask,
)
from edx.analytics.tasks.util.hive import (
    HivePartition,
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
    program_id = StringField(nullable=True, length=36, description='A unique identifier of the program')
    program_title = StringField(nullable=True, length=255, normalize_whitespace=True,
                                description='The display title for the program')
    catalog_course = StringField(nullable=True, length=255, description='Course identifier without run')
    start_time = DateTimeField(nullable=True, description='The date and time that the course begins')
    end_time = DateTimeField(nullable=True, description='The date and time that the course ends')
    pacing_type = StringField(nullable=True, length=255, description='The type of pacing for this course')
    availability = StringField(nullable=True, length=255, description='Availability status of the course')
    enrollment_mode = StringField(length=100, nullable=False, description='Enrollment mode for the enrollment counts')
    count = IntegerField(nullable=True, description='The count of currently enrolled learners')
    count_change_7_days = IntegerField(nullable=True,
                                       description='Difference in enrollment counts over the past 7 days')
    cumulative_count = IntegerField(nullable=True, description='The cumulative total of all users ever enrolled')


class CourseSummaryEnrollmentDownstreamMixin(CourseEnrollmentDownstreamMixin, LoadInternalReportingCourseCatalogMixin):
    """Combines course enrollment and catalog parameters."""

    disable_course_catalog = luigi.BooleanParameter(
        config_path={'section': 'course-summary-enrollment', 'name': 'disable_course_catalog'},
        default=False,
        description="Disables course catalog data jobs and only creates empty tables for them."
    )


class ImportCourseSummaryEnrollmentsIntoMysql(CourseSummaryEnrollmentDownstreamMixin,
                                              HiveQueryToMysqlTask):
    """Creates the course summary enrollment sql table."""

    @property
    def indexes(self):
        return [('course_id',)]

    @property
    def query(self):
        end_date = self.interval.date_b - datetime.timedelta(days=1)
        start_date = self.interval.date_b - datetime.timedelta(days=7)

        query = """
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
                (enrollment_end.count - COALESCE(enrollment_start.count, 0)) AS count_change_7_days,
                enrollment_end.cumulative_count
            FROM course_enrollment_mode_daily enrollment_end
            LEFT OUTER JOIN course_enrollment_mode_daily enrollment_start
                ON enrollment_start.course_id = enrollment_end.course_id
                AND enrollment_start.mode = enrollment_end.mode
                AND enrollment_start.date = '{start_date}'
            LEFT OUTER JOIN program_course program
                ON program.course_id = enrollment_end.course_id
            LEFT OUTER JOIN course_catalog course
                ON course.course_id = enrollment_end.course_id
            WHERE enrollment_end.date = '{end_date}'
        """.format(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        return query

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def table(self):
        return 'course_meta_summary_enrollment'

    @property
    def columns(self):
        return CourseSummaryEnrollmentRecord.get_sql_schema()

    @property
    def required_table_tasks(self):
        yield EnrollmentByModeTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
        )

        catalog_tasks = [
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
        ]

        if self.disable_course_catalog:
            catalog_tasks = [task.hive_table_task for task in catalog_tasks]

        yield catalog_tasks


@workflow_entry_point
class CourseSummaryEnrollmentWrapperTask(CourseSummaryEnrollmentDownstreamMixin,
                                         luigi.WrapperTask):
    """Entry point for course summary enrollment task."""

    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
            'overwrite': self.overwrite,  # for enrollment
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite_n_days': self.overwrite_n_days,  # for course catalog
            'disable_course_catalog': self.disable_course_catalog
        }
        yield ImportCourseSummaryEnrollmentsIntoMysql(**kwargs)
