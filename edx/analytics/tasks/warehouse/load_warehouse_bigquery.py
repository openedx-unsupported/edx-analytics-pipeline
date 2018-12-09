import datetime
import os

import luigi
from google.cloud import bigquery

from edx.analytics.tasks.common.bigquery_load import BigQueryLoadDownstreamMixin, BigQueryLoadTask
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.insights.enrollments import EnrollmentSummaryRecord
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    CourseRecord, CourseSeatRecord, CourseSubjectRecord, ProgramCourseRecord
)


class LoadInternalReportingCertificatesToBigQuery(WarehouseMixin, BigQueryLoadTask):

    @property
    def table(self):
        return 'd_user_course_certificate'

    @property
    def schema(self):
        # Defined in load_internal_reporting_certificates, but not in a Record.
        return [
            bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('is_certified', 'INTEGER'),
            bigquery.SchemaField('certificate_mode', 'STRING'),
            bigquery.SchemaField('final_grade', 'STRING'),
            bigquery.SchemaField('has_passed', 'INTEGER'),
            bigquery.SchemaField('created_date', 'DATETIME'),
            bigquery.SchemaField('modified_date', 'DATETIME'),
        ]

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('internal_reporting_certificates', self.date))


class LoadInternalReportingCountryToBigQuery(WarehouseMixin, BigQueryLoadTask):

    @property
    def table(self):
        return 'd_country'

    @property
    def schema(self):
        # Defined in load_internal_reporting_country, but not in a Record.
        return [
            bigquery.SchemaField('country_name', 'STRING'),
            bigquery.SchemaField('user_last_location_country_code', 'STRING', mode='REQUIRED'),
        ]

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('internal_reporting_d_country', self.date))


class LoadInternalReportingCourseToBigQuery(WarehouseMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        url = url_path_join(self.hive_partition_path('course_catalog', self.date), 'course_catalog.tsv')
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_course'

    @property
    def schema(self):
        return CourseRecord.get_bigquery_schema()


class LoadInternalReportingCourseSeatToBigQuery(WarehouseMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        url = url_path_join(self.hive_partition_path('course_seat', self.date), 'course_seat.tsv')
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_course_seat'

    @property
    def schema(self):
        return CourseSeatRecord.get_bigquery_schema()


class LoadInternalReportingCourseSubjectToBigQuery(WarehouseMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        url = url_path_join(self.hive_partition_path('course_subjects', self.date), 'course_subjects.tsv')
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_course_subjects'

    @property
    def schema(self):
        return CourseSubjectRecord.get_bigquery_schema()


class LoadInternalReportingProgramCourseToBigQuery(WarehouseMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        url = self.hive_partition_path('program_course_with_order', self.date)
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_program_course'

    @property
    def schema(self):
        return ProgramCourseRecord.get_bigquery_schema()


class LoadInternalReportingCourseCatalogToBigQuery(WarehouseMixin, BigQueryLoadDownstreamMixin, luigi.WrapperTask):

    date = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'date': self.date,
            'dataset_id': self.dataset_id,
            'credentials': self.credentials,
            'max_bad_records': self.max_bad_records,
            'overwrite': self.overwrite,
            'warehouse_path': self.warehouse_path,
        }
        yield LoadInternalReportingCourseToBigQuery(**kwargs)
        yield LoadInternalReportingCourseSeatToBigQuery(**kwargs)
        yield LoadInternalReportingCourseSubjectToBigQuery(**kwargs)
        yield LoadInternalReportingProgramCourseToBigQuery(**kwargs)

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))


class LoadUserCourseSummaryToBigQuery(WarehouseMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('course_enrollment_summary', self.date))

    @property
    def table(self):
        return 'd_user_course'

    @property
    def schema(self):
        return EnrollmentSummaryRecord.get_bigquery_schema()


class LoadInternalReportingUserActivityToBigQuery(WarehouseMixin, BigQueryLoadTask):

    def __init__(self, *args, **kwargs):
        super(LoadInternalReportingUserActivityToBigQuery, self).__init__(*args, **kwargs)

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
    def schema(self):
        # Defined in load_internal_reporting_user_activity, but not in a Record.
        return [
            bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('activity_type', 'STRING'),
            bigquery.SchemaField('number_of_activities', 'INTEGER'),
        ]


class LoadInternalReportingUserToBigQuery(WarehouseMixin, BigQueryLoadTask):

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
    def schema(self):
        # Defined in load_internal_reporting_user, but not in a Record.
        # The names of fields here and Vertica is also different than in Hive, with 'user_' prepended to all but 'user_id'.
        return [
            bigquery.SchemaField('user_id', 'INTEGER'),
            bigquery.SchemaField('user_year_of_birth', 'INTEGER'),
            bigquery.SchemaField('user_level_of_education', 'STRING'),
            bigquery.SchemaField('user_gender', 'STRING'),
            bigquery.SchemaField('user_email', 'STRING'),
            bigquery.SchemaField('user_username', 'STRING'),
            bigquery.SchemaField('user_account_creation_time', 'DATETIME'),
            bigquery.SchemaField('user_last_location_country_code', 'STRING'),
        ]


class LoadWarehouseBigQueryTask(BigQueryLoadDownstreamMixin, WarehouseMixin, luigi.WrapperTask):

    date = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'date': self.date,
            'dataset_id': self.dataset_id,
            'credentials': self.credentials,
            'max_bad_records': self.max_bad_records,
            'overwrite': self.overwrite,
            'warehouse_path': self.warehouse_path,
        }

        yield LoadInternalReportingCertificatesToBigQuery(**kwargs)

        yield LoadInternalReportingCountryToBigQuery(**kwargs)

        yield LoadInternalReportingCourseCatalogToBigQuery(**kwargs)

        yield LoadUserCourseSummaryToBigQuery(**kwargs)

        yield LoadInternalReportingUserActivityToBigQuery(**kwargs)

        yield LoadInternalReportingUserToBigQuery(**kwargs)

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
