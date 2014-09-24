"""
Determine the number of users that are enrolled in each course over time
"""
import logging
import textwrap
import luigi
from edx.analytics.tasks.database_imports import ImportIntoHiveTableTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.course_enroll import CourseEnrollmentChangesPerDay, BaseCourseEnrollmentTaskDownstreamMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask

log = logging.getLogger(__name__)


class ImportDailyEnrollmentTrendsToHiveTask(BaseCourseEnrollmentTaskDownstreamMixin, ImportIntoHiveTableTask):
    """
    Creates a Hive Table that points to Hadoop output of CourseEnrollmentChangesPerDay task.

    Parameters are defined by :py:class:`BaseCourseEnrollmentTaskDownstreamMixin`.
    """

    @property
    def table_name(self):
        return 'course_enrollment_changes_per_day'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('date', 'STRING'),
            ('enrollment_delta', 'INT'),
        ]

    @property
    def table_location(self):
        output_name = 'course_enrollment_changes_per_day_{name}/'.format(name=self.name)
        return url_path_join(self.dest, output_name)

    @property
    def table_format(self):
        """Provides structure of Hive external table data."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"

    @property
    def partition_date(self):
        return str(self.run_date)

    def requires(self):
        return CourseEnrollmentChangesPerDay(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            name=self.name,
            src=self.src,
            dest=self.dest,
            include=self.include,
            manifest=self.manifest,
            overwrite=self.overwrite,
            run_date=self.run_date
        )


class ImportCourseEnrollmentOffsetsBlacklist(ImportIntoHiveTableTask):
    """
    Imports list of courses to blacklist from enrollment sums calculations.
    A new partition is created for every date where the blacklist is updated.

    The input file is a tsv file assumed to exist at
    s3://some-bucket/blacklist/enrollment/dt=<partition_date>

    The file copied into the original partition 2014-08-27 is
    s3://some-bucket/course_enrollment_offsets_with_2012_Spring.tsv.
    ex. DemoOrgX/CourseFoo/3T2013 2013-12-03  12533

     """

    blacklist_date = luigi.Parameter(
        default_from_config={'section': 'enrollments', 'name': 'blacklist_date'}
    )
    blacklist_path = luigi.Parameter(
        default_from_config={'section': 'enrollments', 'name': 'blacklist_path'}
    )

    @property
    def table_name(self):
        return 'course_enrollment_blacklist'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
        ]

    @property
    def table_location(self):
        return self.blacklist_path

    @property
    def partition_date(self):
        return self.blacklist_date

    @property
    def table_format(self):
        """Provides format of Hive external table data."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"


class SumEnrollmentDeltasTask(BaseCourseEnrollmentTaskDownstreamMixin, ImportIntoHiveTableTask):
    """Defines task to perform sum in Hive to find course enrollment counts."""

    def query(self):
        create_table_statements = super(SumEnrollmentDeltasTask, self).query()
        query_format = textwrap.dedent("""
            INSERT OVERWRITE TABLE {table_name}
            PARTITION (dt='{partition_date}')
            SELECT e.*
            FROM
            (
                SELECT course_id, date, sum(enrollment_delta)
                OVER (
                    partition by course_id
                    order by date ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                FROM {incremental_table_name} e
                WHERE date < '{run_date}'
            ) e
            LEFT OUTER JOIN {blacklist_table} b on (e.course_id = b.course_id)
            WHERE b.course_id IS NULL;

        """)

        insert_query = query_format.format(
            table_name=self.table_name,
            partition_date=self.partition_date,
            incremental_table_name="course_enrollment_changes_per_day",
            blacklist_table="course_enrollment_blacklist",
            run_date=str(self.run_date),
        )

        query = create_table_statements + insert_query
        log.debug('Executing hive query: %s', query)
        return query

    @property
    def table_name(self):
        return 'course_enrollment_daily'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('date', 'STRING'),
            ('count', 'INT'),
        ]

    @property
    def table_location(self):
        return url_path_join(self.dest, self.table_name)

    @property
    def table_format(self):
        """Provides format of Hive external table data."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"

    @property
    def partition_date(self):
        return str(self.run_date)

    def output(self):
        # partition_location is a property depending on table_location and partitions
        return get_target_from_url(self.partition_location)

    def requires(self):
        return ImportDailyEnrollmentTrendsToHiveTask(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            name=self.name,
            src=self.src,
            dest=self.dest,
            include=self.include,
            manifest=self.manifest,
            overwrite=self.overwrite,
            run_date=self.run_date
        ), ImportCourseEnrollmentOffsetsBlacklist()


class ImportCourseDailyFactsIntoMysql(BaseCourseEnrollmentTaskDownstreamMixin, MysqlInsertTask):
    """ Imports course_enrollment_daily table from hive to mysql """
    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('date', 'DATE NOT NULL'),
            ('count', 'INTEGER'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('date', 'course_id'),
        ]

    @property
    def table(self):
        return "course_enrollment_daily"

    def init_copy(self, connection):
        self.attempted_removal = True
        connection.cursor().execute("DELETE FROM " + self.table)

    @property
    def insert_source_task(self):
        return SumEnrollmentDeltasTask(
            mapreduce_engine=self.mapreduce_engine,
            lib_jar=self.lib_jar,
            n_reduce_tasks=self.n_reduce_tasks,
            name=self.name,
            src=self.src,
            dest=self.dest,
            include=self.include,
            manifest=self.manifest,
            overwrite=self.overwrite,
            run_date=self.run_date
        )
