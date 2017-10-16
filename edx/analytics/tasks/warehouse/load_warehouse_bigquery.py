import datetime
import os

import luigi

from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.common.bigquery_load import BigQueryLoadTask
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from google.cloud import bigquery


class LoadInternalReportingCertificatesToBigQuery(WarehouseMixin, BigQueryLoadTask):

    date = luigi.DateParameter()

    @property
    def table(self):
        return 'd_user_course_certificate'

    @property
    def schema(self):
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

    date = luigi.DateParameter()

    @property
    def table(self):
        return 'd_country'

    @property
    def schema(self):
        return [
            bigquery.SchemaField('country_name', 'STRING'),
            bigquery.SchemaField('user_last_location_country_code', 'STRING', mode='REQUIRED'),
        ]

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('internal_reporting_d_country', self.date))


class LoadInternalReportingCourseToBigQuery(WarehouseMixin, BigQueryLoadTask):

    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        url = url_path_join(self.hive_partition_path('course_catalog', self.date), 'course_catalog.tsv')
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_course'

    @property
    def schema(self):
        return [
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('catalog_course', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('catalog_course_title', 'STRING'),
            bigquery.SchemaField('start_time', 'DATETIME'),
            bigquery.SchemaField('end_time', 'DATETIME'),
            bigquery.SchemaField('enrollment_start_time', 'DATETIME'),
            bigquery.SchemaField('enrollment_end_time', 'DATETIME'),
            bigquery.SchemaField('content_language', 'STRING'),
            bigquery.SchemaField('pacing_type', 'STRING'),
            bigquery.SchemaField('level_type', 'STRING'),
            bigquery.SchemaField('availability', 'STRING'),
            bigquery.SchemaField('org_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('partner_short_code', 'STRING'),
            bigquery.SchemaField('marketing_url', 'STRING'),
            bigquery.SchemaField('min_effort', 'INTEGER'),
            bigquery.SchemaField('max_effort', 'INTEGER'),
            bigquery.SchemaField('announcement_time', 'DATETIME'),
            bigquery.SchemaField('reporting_type', 'STRING'),
        ] 


class LoadInternalReportingCourseSeatToBigQuery(WarehouseMixin, BigQueryLoadTask):

    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        url = url_path_join(self.hive_partition_path('course_seat', self.date), 'course_seat.tsv')
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_course_seat'

    @property
    def schema(self):
        return [
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('course_seat_type', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('course_seat_price', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('course_seat_currency', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('course_seat_upgrade_deadline', 'DATETIME'),
            bigquery.SchemaField('course_seat_credit_provider', 'STRING'),
            bigquery.SchemaField('course_seat_credit_hours', 'INTEGER'),
        ]


class LoadInternalReportingProgramCourseToBigQuery(WarehouseMixin, BigQueryLoadTask):

    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        url = self.hive_partition_path('program_course_with_order', self.date)
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'd_program_course'

    @property
    def schema(self):
        return [
            bigquery.SchemaField('program_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('program_type', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('program_title', 'STRING'),
            bigquery.SchemaField('catalog_course', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('catalog_course_title', 'STRING'),
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('org_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('partner_short_code', 'STRING'),
            bigquery.SchemaField('program_slot_number', 'INTEGER'),
        ]


class LoadInternalReportingCourseCatalogToBigQuery(WarehouseMixin, OverwriteOutputMixin, luigi.WrapperTask):

    dataset_id = luigi.Parameter()
    credentials = luigi.Parameter()
    date = luigi.DateParameter()

    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite,
            'dataset_id': self.dataset_id,
            'credentials': self.credentials,
        }
        yield LoadInternalReportingCourseToBigQuery(**kwargs)
        yield LoadInternalReportingCourseSeatToBigQuery(**kwargs)
        yield LoadInternalReportingProgramCourseToBigQuery(**kwargs)

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))


class LoadUserCourseSummaryToBigQuery(WarehouseMixin, BigQueryLoadTask):

    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('course_enrollment_summary', self.date))

    @property
    def table(self):
        return 'd_user_course'

    @property
    def schema(self):
        return [
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('current_enrollment_mode', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('current_enrollment_is_active', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('first_enrollment_mode', 'STRING'),
            bigquery.SchemaField('first_enrollment_time', 'DATETIME'),
            bigquery.SchemaField('last_unenrollment_time', 'DATETIME'),
            bigquery.SchemaField('first_verified_enrollment_time', 'DATETIME'),
            bigquery.SchemaField('first_credit_enrollment_time', 'DATETIME'),
            bigquery.SchemaField('end_time', 'DATETIME', mode='REQUIRED'),
        ]


class LoadInternalReportingUserActivityToBigQuery(WarehouseMixin, BigQueryLoadTask):

    date = luigi.DateParameter()

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
        return [
            bigquery.SchemaField('user_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('activity_type', 'STRING'),
            bigquery.SchemaField('number_of_activities', 'INTEGER'),
        ]


class LoadInternalReportingUserToBigQuery(WarehouseMixin, BigQueryLoadTask):

    date = luigi.DateParameter()

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


class DailyLoadSubjectsToBigQueryTask(WarehouseMixin, BigQueryLoadTask):
    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        partition_path_spec = HivePartition('dt', self.date.isoformat()).path_spec
        url_with_filename = url_path_join(self.warehouse_path, "course_catalog", "subjects",
                                          partition_path_spec, "subjects.tsv")
        return ExternalURL(url=url_with_filename)

    @property
    def table(self):
        return "d_course_subjects"

    @property
    def schema(self):
        return [
            bigquery.SchemaField('course_id', 'STRING'),
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('subject_uri', 'STRING'),
            bigquery.SchemaField('subject_title', 'STRING'),
            bigquery.SchemaField('subject_language', 'STRING'),
        ]


class LoadWarehouseBigQueryTask(WarehouseMixin, luigi.WrapperTask):

    date = luigi.DateParameter()
    dataset_id = luigi.Parameter()
    credentials = luigi.Parameter()
    overwrite = luigi.BooleanParameter(default=False, significant=False)

    def requires(self):
        kwargs = {
            'dataset_id': self.dataset_id,
            'credentials': self.credentials,
            'overwrite': self.overwrite,
            'warehouse_path': self.warehouse_path,
        }

        yield LoadInternalReportingCertificatesToBigQuery(
            date=self.date,
            **kwargs
        )

        yield LoadInternalReportingCountryToBigQuery(
            date=self.date,
            **kwargs
        )

        yield LoadInternalReportingCourseCatalogToBigQuery(
            date=self.date,
            **kwargs
        )

        yield LoadUserCourseSummaryToBigQuery(
            date=self.date,
            **kwargs
        )

        yield LoadInternalReportingUserActivityToBigQuery(
            date=self.date,
            **kwargs
        )

        yield LoadInternalReportingUserToBigQuery(
            date=self.date,
            **kwargs
        )

        yield DailyLoadSubjectsToBigQueryTask(
            date=self.date,
            **kwargs
        )
