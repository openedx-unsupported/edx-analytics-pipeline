import datetime
import logging
import os

import luigi
from luigi import configuration
from luigi.contrib.redshift import S3CopyToTable
from edx.analytics.tasks.insights.enrollments import EnrollmentSummaryRecord
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from edx.analytics.tasks.warehouse.load_internal_reporting_user import AggregateInternalReportingUserTableHive
from edx.analytics.tasks.warehouse.load_internal_reporting_certificates import LoadInternalReportingCertificatesTableHive
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    LoadInternalReportingCourseCatalogMixin, CoursePartitionTask, CourseSeatTask, ProgramCoursePartitionTask,
    CourseRecord, CourseSeatRecord, ProgramCourseRecord
)
from edx.analytics.tasks.warehouse.course_catalog import PullCatalogMixin, DailyProcessFromCatalogSubjectTask

log = logging.getLogger(__name__)


class RedshiftS3CopyToTable(OverwriteOutputMixin, S3CopyToTable):

    def requires(self):
        return {'insert_source': self.insert_source_task}

    @property
    def insert_source_task(self):
        raise NotImplementedError

    def credentials(self):
        config = configuration.get_config()
        section = 'redshift'
        return {
            'host': config.get(section, 'host'),
            'port': config.get(section, 'port'),
            'database': config.get(section, 'database'),
            'user': config.get(section, 'user'),
            'password': config.get(section, 'password'),
            'aws_account_id': config.get(section, 'account_id'),
            'aws_arn_role_name': config.get(section, 'role_name'),
        }

    def s3_load_path(self):
        return self.input()['insert_source'].path

    @property
    def aws_account_id(self):
        return self.credentials()['aws_account_id']

    @property
    def aws_arn_role_name(self):
        return self.credentials()['aws_arn_role_name']

    @property
    def host(self):
        return self.credentials()['host'] + ':' +  self.credentials()['port']

    @property
    def database(self):
        return self.credentials()['database']

    @property
    def user(self):
        return self.credentials()['user']

    @property
    def password(self):
        return self.credentials()['password']

    @property
    def aws_access_key_id(self):
        return ''

    @property
    def aws_secret_access_key(self):
        return ''

    @property
    def copy_options(self):
        return "DELIMITER '\t'"

    @property
    def table_attributes(self):
        return ''

    @property
    def table_type(self):
        return ''

    def create_table(self, connection):
        if len(self.columns[0]) == 1:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented "
                                      "for %r and columns types not "
                                      "specified" % self.table)
        elif len(self.columns[0]) == 2:
            # if columns is specified as (name, type) tuples
            coldefs = ','.join(
                '{name} {type}'.format(
                    name=name,
                    type=type) for name, type in self.columns
            )
            query = ("CREATE {type} TABLE IF NOT EXISTS "
                     "{table} ({coldefs}) "
                     "{table_attributes}").format(
                type=self.table_type,
                table=self.table,
                coldefs=coldefs,
                table_attributes=self.table_attributes)

            connection.cursor().execute(query)
        else:
            raise ValueError("create_table() found no columns for %r"
                             % self.table)

    def truncate_table(self, connection):
        query = "truncate %s" % self.table
        cursor = connection.cursor()
        try:
            cursor.execute(query)
        finally:
            cursor.close()

    def init_copy(self, connection):
        self.create_table(connection)

        self.attempted_removal = True
        if self.overwrite:
            self.truncate_table(connection)

    def init_touch(self, connection):
        if self.overwrite:
            marker_table = self.output().marker_table
            query = "DELETE FROM {marker_table} where target_table='{target_table}';".format(
                marker_table=marker_table,
                target_table=self.table,
            )
            log.debug(query)
            connection.cursor().execute(query)

    def run(self):
        if not (self.table):
            raise Exception("table need to be specified")

        path = self.s3_load_path()
        output = self.output()
        connection = output.connect()
        cursor = connection.cursor()

        self.init_copy(connection)
        self.copy(cursor, path)

        self.init_touch(connection)
        output.touch(connection)
        connection.commit()

        # commit and clean up
        connection.close()

    def copy(self, cursor, f):
        cursor.execute("""
         COPY {table} from '{source}'
         IAM_ROLE 'arn:aws:iam::{id}:role/{role}'
         {options}
         ;""".format(
            table=self.table,
            source=f,
            id=self.aws_account_id,
            role=self.aws_arn_role_name,
            options=self.copy_options)
        )

    def update_id(self):
        return str(self)


class LoadInternalReportingCertificatesToRedshift(WarehouseMixin, RedshiftS3CopyToTable):

    date = luigi.DateParameter()

    @property
    def table_attributes(self):
        return 'DISTKEY(user_id)'

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
    def table(self):
        return 'd_user_course_certificate'

    @property
    def insert_source_task(self):
        return (
            LoadInternalReportingCertificatesTableHive(
                overwrite=self.overwrite,
                warehouse_path=self.warehouse_path,
                date=self.date
            )
        )


class LoadInternalReportingCountryToRedshift(WarehouseMixin, RedshiftS3CopyToTable):

    date = luigi.DateParameter()

    @property
    def columns(self):
        return [
            ('country_name', 'VARCHAR(45)'),
            ('user_last_location_country_code', 'VARCHAR(45) NOT NULL')
        ]

    @property
    def table(self):
        return 'd_country'

    @property
    def insert_source_task(self):
        hive_table = "internal_reporting_d_country"
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member


class LoadInternalReportingCourseToRedshift(LoadInternalReportingCourseCatalogMixin, RedshiftS3CopyToTable):


    @property
    def table_attributes(self):
        return 'DISTKEY(course_id)'

    @property
    def columns(self):
        return CourseRecord.get_sql_schema()

    @property
    def table(self):
        return 'd_course'

    @property
    def insert_source_task(self):
        return CoursePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        ).data_task


class LoadInternalReportingCourseSeatToRedshift(LoadInternalReportingCourseCatalogMixin, RedshiftS3CopyToTable):

    @property
    def table_attributes(self):
        return 'DISTKEY(course_id)'

    @property
    def columns(self):
        return CourseSeatRecord.get_sql_schema()

    @property
    def table(self):
        return 'd_course_seat'

    @property
    def insert_source_task(self):
        return CourseSeatTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )


class LoadInternalReportingProgramCourseToRedshift(LoadInternalReportingCourseCatalogMixin, RedshiftS3CopyToTable):

    @property
    def table_attributes(self):
        return 'DISTKEY(course_id)'

    @property
    def columns(self):
        return ProgramCourseRecord.get_sql_schema()

    @property
    def table(self):
        return 'd_program_course'

    @property
    def insert_source_task(self):
        return ProgramCoursePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        ).data_task


class LoadInternalReportingCourseCatalogToRedshift(LoadInternalReportingCourseCatalogMixin, luigi.WrapperTask):

    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
            'overwrite': self.overwrite,
        }
        yield LoadInternalReportingCourseToRedshift(**kwargs)
        yield LoadInternalReportingCourseSeatToRedshift(**kwargs)
        yield LoadInternalReportingProgramCourseToRedshift(**kwargs)

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))


class LoadUserCourseSummaryToRedshift(WarehouseMixin, RedshiftS3CopyToTable):

    date = luigi.DateParameter()

    @property
    def table_attributes(self):
        return 'DISTKEY(user_id)'

    @property
    def columns(self):
        return EnrollmentSummaryRecord.get_sql_schema()

    @property
    def table(self):
        return 'd_user_course'

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('course_enrollment_summary', self.date))


class LoadInternalReportingUserActivityToRedshift(WarehouseMixin, RedshiftS3CopyToTable):

    date = luigi.DateParameter()

    @property
    def table_attributes(self):
        return 'DISTKEY(user_id)'

    def __init__(self, *args, **kwargs):
        super(LoadInternalReportingUserActivityToRedshift, self).__init__(*args, **kwargs)

        path = url_path_join(self.warehouse_path, 'internal_reporting_user_activity')
        path_targets = PathSetTask([path]).output()
        paths = list(set([os.path.dirname(target.path) for target in path_targets]))
        dates = [path.rsplit('/', 2)[-1] for path in paths]
        latest_date = sorted(dates)[-1]
        self.load_date = datetime.datetime.strptime(latest_date, "dt=%Y-%m-%d").date()

    @property
    def partition(self):
        return HivePartition('dt', self.load_date.isoformat())

    @property
    def insert_source_task(self):
        hive_table = "internal_reporting_user_activity"
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)

    @property
    def table(self):
        return 'f_user_activity'

    @property
    def columns(self):
        return [
            ('user_id', 'INTEGER NOT NULL'),
            ('course_id', 'VARCHAR(256) NOT NULL'),
            ('date', 'DATE'),
            ('activity_type', 'VARCHAR(200)'),
            ('number_of_activities', 'INTEGER')
        ]


class LoadInternalReportingUserToRedshift(WarehouseMixin, RedshiftS3CopyToTable):

    date = luigi.DateParameter()

    n_reduce_tasks = luigi.Parameter()

    @property
    def table_attributes(self):
        return 'DISTKEY(user_id)'

    @property
    def insert_source_task(self):
        return (
            AggregateInternalReportingUserTableHive(
                n_reduce_tasks=self.n_reduce_tasks,
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
        )

    @property
    def table(self):
        return 'd_user'

    @property
    def columns(self):
        return [
            ('user_id', 'INTEGER'),
            ('user_year_of_birth', 'INTEGER'),
            ('user_level_of_education', 'VARCHAR(200)'),
            ('user_gender', 'VARCHAR(45)'),
            ('user_email', 'VARCHAR(100)'),
            ('user_username', 'VARCHAR(45)'),
            ('user_account_creation_time', 'TIMESTAMP'),
            ('user_last_location_country_code', 'VARCHAR(45)')
        ]


class DailyLoadSubjectsToRedshift(PullCatalogMixin, RedshiftS3CopyToTable):

    @property
    def table_attributes(self):
        return 'DISTKEY(course_id)'

    @property
    def insert_source_task(self):
        return DailyProcessFromCatalogSubjectTask(date=self.date, catalog_path=self.catalog_path)

    @property
    def table(self):
        return 'd_course_subjects'

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(200)'),
            ('date', 'DATE'),
            ('subject_uri', 'VARCHAR(200)'),
            ('subject_title', 'VARCHAR(200)'),
            ('subject_language', 'VARCHAR(200)')
        ]


class LoadWarehouseRedshift(WarehouseMixin, luigi.WrapperTask):

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()
    overwrite = luigi.BooleanParameter(default=False, significant=False)

    def requires(self):
        kwargs = {
            'overwrite': self.overwrite,
            'warehouse_path': self.warehouse_path,
        }

        yield LoadInternalReportingCertificatesToRedshift(
            date=self.date,
            **kwargs
        )

        yield LoadInternalReportingCountryToRedshift(
            date=self.date,
            **kwargs
        )

        yield LoadInternalReportingCourseCatalogToRedshift(
            date=self.date,
            **kwargs
        )

        yield LoadUserCourseSummaryToRedshift(
            date=self.date,
            **kwargs
        )

        yield LoadInternalReportingUserActivityToRedshift(
            date=self.date,
            **kwargs
        )

        yield LoadInternalReportingUserToRedshift(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            **kwargs
        )

        yield DailyLoadSubjectsToRedshift(
            date=self.date,
            **kwargs
        )


# class LoadUserCourseSummaryToRedshift(RedshiftS3CopyToTable):
#     columns = EnrollmentSummaryRecord.get_sql_schema()
#
#     @property
#     def table_attributes(self):
#         return 'DISTKEY(user_id) SORTKEY(user_id)'
#
#     @property
#     def table(self):
#         return 'd_user_course'
