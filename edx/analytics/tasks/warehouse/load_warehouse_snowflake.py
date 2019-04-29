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
            ('created_date', 'TIMESTAMP'),
            ('modified_date', 'TIMESTAMP'),
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
        return CourseRecord.get_sql_schema()


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
        return CourseSeatRecord.get_sql_schema()


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
        return CourseSubjectRecord.get_sql_schema()


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
        return ProgramCourseRecord.get_sql_schema()


class LoadInternalReportingCourseCatalogToSnowflake(WarehouseMixin, SnowflakeLoadDownstreamMixin, luigi.WrapperTask):

    date = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'date': self.date,
            'credentials': self.credentials,
            'database': self.database,
            'schema': self.schema,
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
        return CourseBlockRecord.get_sql_schema()


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
        return EnrollmentSummaryRecord.get_sql_schema()


class LoadInternalReportingUserActivityToSnowflake(WarehouseMixin, SnowflakeLoadFromHiveTSVTask):

    def __init__(self, *args, **kwargs):
        super(LoadInternalReportingUserActivityToSnowflake, self).__init__(*args, **kwargs)

        # Find the most recent data for the source.
        path = url_path_join(self.warehouse_path, 'internal_reporting_user_activity')
        path_targets = PathSetTask([path]).output()
        paths = list(set([os.path.dirname(target.path) for target in path_targets]))
        dates = [path.rsplit('/', 2)[-1] for path in paths]
        latest_date = sorted(dates)[-1]

        self.load_date = datetime.datetime.strptime(latest_date, "dt=%Y-%m-%d").date()

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.load_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        hive_table = "internal_reporting_user_activity"
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)

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
            # We use VARCHAR(100) for vertica, but vertica truncates to 100 characters, whereas snowflake throws an error.
            ('user_email', 'VARCHAR(150)'),
            # We use VARCHAR(45) for vertica, but vertica truncates to 45 characters, whereas snowflake throws an error.
            # 54 is the maximum length in our data right now, all of which are retired_user_<sha-1>@retired.invalid
            ('user_username', 'VARCHAR(54)'),
            ('user_account_creation_time', 'TIMESTAMP'),
            ('user_last_location_country_code', 'VARCHAR(45)')
        ]


class LoadWarehouseSnowflakeTask(SnowflakeLoadDownstreamMixin, WarehouseMixin, luigi.WrapperTask):

    date = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'date': self.date,
            'credentials': self.credentials,
            'database': self.database,
            'schema': self.schema,
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
