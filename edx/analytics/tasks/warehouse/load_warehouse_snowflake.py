import datetime
import os

import luigi

from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.common.snowflake_load import SnowflakeLoadDownstreamMixin, SnowflakeLoadFromHiveTSVTask
from edx.analytics.tasks.insights.enrollments import EnrollmentSummaryRecord
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    CourseRecord, CourseSeatRecord, CourseSubjectRecord, ProgramCourseRecord
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_structure import CourseBlockRecord


def convert_datetime_to_timestamp_tz(coldefs):
    """Substitute TIMESTAMP_TZ for DATETIME in a column definition for tables on Snowflake."""
    # coldefs should be a list of tuples, each with a column name and column "type".
    # Note that "type" information may include " NOT NULL" at the end, so we need to perform
    # a replace within the string.
    return [(name, type.replace('DATETIME', 'TIMESTAMP_TZ')) for name, type in coldefs]


class LoadInternalReportingCertificatesToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    @property
    def table(self):
        return 'd_user_course_certificate'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return [
            ('user_id', 'INTEGER NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('is_certified', 'INTEGER'),
            ('certificate_mode', 'VARCHAR(200)'),
            ('final_grade', 'VARCHAR(5)'),
            ('has_passed', 'INTEGER'),
            ('created_date', 'TIMESTAMP_TZ'),
            ('modified_date', 'TIMESTAMP_TZ'),
        ]

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('internal_reporting_certificates', self.date))


class LoadInternalReportingCountryToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):
    """
    Loads the country table from Hive into the Vertica data warehouse.
    """

    @property
    def table(self):
        return 'd_country'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return [
            ('country_name', 'VARCHAR(45)'),
            ('user_last_location_country_code', 'VARCHAR(45) NOT NULL')
        ]

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('internal_reporting_d_country', self.date))


class LoadInternalReportingCourseToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    @property
    def insert_source_task(self):
        url = url_path_join(self.hive_partition_path('course_catalog', self.date), 'course_catalog.tsv')
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_course'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return convert_datetime_to_timestamp_tz(CourseRecord.get_sql_schema())


class LoadInternalReportingCourseSeatToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    @property
    def insert_source_task(self):
        url = url_path_join(self.hive_partition_path('course_seat', self.date), 'course_seat.tsv')
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_course_seat'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return convert_datetime_to_timestamp_tz(CourseSeatRecord.get_sql_schema())


class LoadInternalReportingCourseSubjectToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    @property
    def insert_source_task(self):
        url = url_path_join(self.hive_partition_path('course_subjects', self.date), 'course_subjects.tsv')
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_course_subjects'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return convert_datetime_to_timestamp_tz(CourseSubjectRecord.get_sql_schema())


class LoadInternalReportingProgramCourseToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    @property
    def insert_source_task(self):
        url = self.hive_partition_path('program_course_with_order', self.date)
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_program_course'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return convert_datetime_to_timestamp_tz(ProgramCourseRecord.get_sql_schema())


class LoadInternalReportingCourseCatalogToSnowflake(WarehouseMixin, SnowflakeLoadDownstreamMixin, luigi.WrapperTask):

    date = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'date': self.date,
            'credentials': self.credentials,
            'sf_database': self.sf_database,
            'schema': self.schema,
            'scratch_schema': self.scratch_schema,
            'run_id': self.run_id,
            'warehouse': self.warehouse,
            'role': self.role,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite,
        }
        yield LoadInternalReportingCourseToSnowflake(**kwargs)
        yield LoadInternalReportingCourseSeatToSnowflake(**kwargs)
        yield LoadInternalReportingCourseSubjectToSnowflake(**kwargs)
        yield LoadInternalReportingProgramCourseToSnowflake(**kwargs)

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))


class LoadInternalReportingCourseStructureToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    def __init__(self, *args, **kwargs):
        super(LoadInternalReportingCourseStructureToSnowflake, self).__init__(*args, **kwargs)
        path = url_path_join(self.warehouse_path, 'course_block_records')
        path_targets = PathSetTask([path]).output()
        paths = list(set([os.path.dirname(target.path) for target in path_targets]))
        dates = [path.rsplit('/', 2)[-1] for path in paths]
        latest_date = sorted(dates)[-1]

        self.load_date = datetime.datetime.strptime(latest_date, "dt=%Y-%m-%d").date()

    @property
    def insert_source_task(self):
        record_table_name = 'course_block_records'
        partition_location = self.hive_partition_path(record_table_name, self.load_date)
        return ExternalURL(url=partition_location)

    @property
    def table(self):
        return 'course_structure'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return convert_datetime_to_timestamp_tz(CourseBlockRecord.get_sql_schema())


class LoadUserCourseSummaryToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('course_enrollment_summary', self.date))

    @property
    def table(self):
        return 'd_user_course'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return convert_datetime_to_timestamp_tz(EnrollmentSummaryRecord.get_sql_schema())


class LoadInternalReportingUserActivityToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    @property
    def insert_source_task(self):
        hive_table = "user_activity_by_user"
        url = url_path_join(self.warehouse_path, hive_table)
        return ExternalURL(url=url)

    @property
    def pattern(self):
        return '.*dt=.*/.*'

    @property
    def truncate_columns(self):
        return "TRUE"

    @property
    def table(self):
        return 'f_user_activity'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return [
            ('user_id', 'INTEGER NOT NULL'),
            ('course_id', 'VARCHAR(256) NOT NULL'),
            ('date', 'DATE'),
            ('activity_type', 'VARCHAR(200)'),
            ('number_of_activities', 'INTEGER')
        ]


class LoadInternalReportingUserToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('internal_reporting_user', self.date))

    @property
    def table(self):
        return 'd_user'

    @property
    def file_format_name(self):
        return 'hive_tsv_format'

    @property
    def columns(self):
        return [
            ('user_id', 'INTEGER'),
            ('user_year_of_birth', 'INTEGER'),
            ('user_level_of_education', 'VARCHAR(200)'),
            ('user_gender', 'VARCHAR(45)'),
            # The edx-platform codebase supports emails up to 254 characters long.
            ('user_email', 'VARCHAR(254)'),
            # 54 is the maximum length in our data right now, all of which are retired_user_<sha-1>@retired.invalid.  An
            # interesting but less relevant fact is that the edx-platform code only supports registering new usernames
            # up to 30 characters:
            # https://github.com/edx/edx-platform/blob/6e3fe00/openedx/core/djangoapps/user_api/accounts/__init__.py#L18
            ('user_username', 'VARCHAR(54)'),
            ('user_account_creation_time', 'TIMESTAMP_TZ'),
            ('user_last_location_country_code', 'VARCHAR(45)')
        ]


class LoadWarehouseSnowflakeTask(SnowflakeLoadDownstreamMixin, WarehouseMixin, luigi.WrapperTask):

    date = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'date': self.date,
            'credentials': self.credentials,
            'sf_database': self.sf_database,
            'schema': self.schema,
            'scratch_schema': self.scratch_schema,
            'run_id': self.run_id,
            'warehouse': self.warehouse,
            'role': self.role,
            'overwrite': self.overwrite,
            'warehouse_path': self.warehouse_path,
        }

        yield LoadInternalReportingCertificatesToSnowflake(**kwargs)

        yield LoadInternalReportingCountryToSnowflake(**kwargs)

        yield LoadInternalReportingCourseCatalogToSnowflake(**kwargs)

        yield LoadInternalReportingCourseStructureToSnowflake(**kwargs)

        yield LoadUserCourseSummaryToSnowflake(**kwargs)

        yield LoadInternalReportingUserActivityToSnowflake(**kwargs)

        yield LoadInternalReportingUserToSnowflake(**kwargs)

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
