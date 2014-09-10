"""
Determine the number of users that are enrolled in each course over time
"""
import logging

import luigi

from edx.analytics.tasks.util.hive import (
    ImportIntoHiveTableTask, HiveTableFromQueryTask, HivePartition, TABLE_FORMAT_TSV
)
from edx.analytics.tasks.url import url_path_join
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
        return TABLE_FORMAT_TSV

    @property
    def partition(self):
        return HivePartition('dt', str(self.run_date))

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
    """

    blacklist_date = luigi.Parameter(
        default_from_config={'section': 'enrollments', 'name': 'blacklist_date'}
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
    def partition(self):
        return HivePartition('dt', self.blacklist_date)

    @property
    def table_format(self):
        """Provides format of Hive external table data."""
        return TABLE_FORMAT_TSV


class SumEnrollmentDeltasTask(BaseCourseEnrollmentTaskDownstreamMixin, HiveTableFromQueryTask):
    """Defines task to perform sum in Hive to find course enrollment counts."""

    @property
    def insert_query(self):
        return """
            SELECT e.*
            FROM
            (
                SELECT course_id, date, sum(enrollment_delta)
                OVER (
                    partition by course_id
                    order by date ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
                FROM course_enrollment_changes_per_day e
            ) e
            LEFT OUTER JOIN course_enrollment_blacklist b ON e.course_id = b.course_id
            WHERE b.course_id IS NULL;
        """

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
    def partition(self):
        return HivePartition('dt', str(self.run_date))

    @property
    def table_format(self):
        """Provides structure of Hive external table data."""
        return TABLE_FORMAT_TSV

    def requires(self):
        return (
            ImportDailyEnrollmentTrendsToHiveTask(
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
            ),
            ImportCourseEnrollmentOffsetsBlacklist(),
        )


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
