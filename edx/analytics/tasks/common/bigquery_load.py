import logging
import datetime
import os
import luigi
import json
import time
import subprocess
import urlparse
import uuid
from luigi import configuration

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.warehouse.load_internal_reporting_certificates import LoadInternalReportingCertificatesTableHive
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    LoadInternalReportingCourseCatalogMixin, CoursePartitionTask, CourseSeatTask, ProgramCoursePartitionTask
)
from edx.analytics.tasks.warehouse.load_internal_reporting_user import AggregateInternalReportingUserTableHive
from edx.analytics.tasks.warehouse.course_catalog import PullCatalogMixin, DailyProcessFromCatalogSubjectTask

log = logging.getLogger(__name__)


class BigQueryTarget(luigi.Target):

    def __init__(self, credentials_target, dataset_id, table, update_id):
        self.dataset_id = dataset_id
        self.table = table
        self.update_id = update_id
        with credentials_target.open('r') as credentials_file:
            json_creds = json.load(credentials_file)
            self.project_id = json_creds['project_id']
            credentials = service_account.Credentials.from_service_account_info(json_creds)
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)

    def touch(self):
        self.create_marker_table()
        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table('table_updates')
        table.reload() # Load the schema
        table.insert_data([
            (self.update_id, "{dataset}.{table}".format(dataset=self.dataset_id, table=self.table))
        ])

    def create_marker_table(self):
        marker_table_schema = [
            bigquery.SchemaField('update_id', 'STRING'),
            bigquery.SchemaField('target_table', 'STRING'),
        ]

        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table('table_updates', marker_table_schema)
        if not table.exists():
            table.create()

    def exists(self):
        query_string = "SELECT 1 FROM {dataset}.table_updates where update_id = '{update_id}'".format(
            dataset=self.dataset_id,
            update_id=self.update_id,
        )
        log.debug(query_string)

        query = self.client.run_sync_query(query_string)

        try:
            query.run()
        except NotFound:
            return False

        return len(query.rows) == 1


class BigQueryLoadTask(OverwriteOutputMixin, luigi.Task):

    dataset_id = luigi.Parameter()
    credentials = luigi.Parameter()

    output_target = None
    required_tasks = None

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'source': self.insert_source_task,
            }
        return self.required_tasks

    @property
    def insert_source_task(self):
        raise NotImplementedError

    @property
    def table(self):
        raise NotImplementedError

    @property
    def schema(self):
        raise NotImplementedError

    @property
    def field_delimiter(self):
        return "\t"

    @property
    def null_marker(self):
        return '\N'

    @property
    def quote_character(self):
        return ''

    def create_dataset(self, client):
        dataset = client.dataset(self.dataset_id)
        if not dataset.exists():
            dataset.create()

    def create_table(self, client):
        dataset = client.dataset(self.dataset_id)
        table = dataset.table(self.table, self.schema)
        if not table.exists():
            table.create()

    def init_touch(self, client):
        pass
        # if self.overwrite:
        #     query = client.run_sync_query("delete {dataset}.table_updates where target_table='{dataset}.{table}'".format(
        #         dataset=self.dataset_id, table=self.table
        #     ))
        #     query.use_legacy_sql = False
        #     query.run()

    def init_copy(self, client):
        self.attempted_removal = True
        if self.overwrite:
            dataset = client.dataset(self.dataset_id)
            table = dataset.table(self.table)
            if table.exists():
                table.delete()

    def run(self):
        client = self.output().client
        self.create_dataset(client)
        self.init_copy(client)
        self.create_table(client)

        dataset = client.dataset(self.dataset_id)
        table = dataset.table(self.table, self.schema)


        source_path = self.input()['source'].path
        parsed_url = urlparse.urlparse(source_path)
        destination_path = url_path_join('gs:////', parsed_url.netloc, parsed_url.path)
        log.debug(" ".join(['gsutil', '-m', 'rsync', source_path, destination_path]))
        return_code = subprocess.call(['gsutil', '-m', 'rsync', source_path, destination_path])

        # with self.input()['source'].open('r') as source_file:
        #     job = table.upload_from_file(
        #             source_file,
        #             source_format='text/csv',
        #             field_delimiter=self.field_delimiter,
        #             null_marker=self.null_marker,
        #             quote_character=self.quote_character
        #     )

        #job_name = str(uuid.uuid4())

        job = client.load_table_from_storage(
            'load_{table}_{timestamp}'.format(table=self.table, timestamp=int(time.time())),
            table,
            destination_path
        )
        job.field_delimiter = self.field_delimiter
        job.quote_character = self.quote_character
        job.null_marker = self.null_marker

        job.begin()

        while job.state != 'DONE':
            job.reload()

        if job.errors:
            raise Exception([error['message'] for error in job.errors])

        self.init_touch(client)
        self.output().touch()

    def output(self):
        if self.output_target is None:
            self.output_target = BigQueryTarget(
                credentials_target=self.input()['credentials'],
                dataset_id=self.dataset_id,
                table=self.table,
                update_id=self.update_id(),
            )

        return self.output_target

    def update_id(self):
        return '{task_name}(date={key})'.format(task_name=self.task_family, key=self.date.isoformat())


class LoadInternalReportingCertificatesToBigQuery(WarehouseMixin, BigQueryLoadTask):

    date = luigi.DateParameter()

    @property
    def table(self):
        return 'd_user_course_certificate'

    @property
    def schema(self):
        return [
            bigquery.SchemaField('user_id', 'INT64', mode='REQUIRED'),
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('is_certified', 'INT64'),
            bigquery.SchemaField('certificate_mode', 'STRING'),
            bigquery.SchemaField('final_grade', 'STRING'),
            bigquery.SchemaField('has_passed', 'INT64'),
            bigquery.SchemaField('created_date', 'DATETIME'),
            bigquery.SchemaField('modified_date', 'DATETIME'),
        ]

    @property
    def insert_source_task(self):
        return (
            LoadInternalReportingCertificatesTableHive(
                overwrite=self.overwrite,
                warehouse_path=self.warehouse_path,
                date=self.date
            )
        )


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
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        hive_table = "internal_reporting_d_country"
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)


class LoadInternalReportingCourseToBigQuery(LoadInternalReportingCourseCatalogMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        return CoursePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        ).data_task

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
            bigquery.SchemaField('min_effort', 'INT64'),
            bigquery.SchemaField('max_effort', 'INT64'),
            bigquery.SchemaField('announcement_time', 'DATETIME'),
            bigquery.SchemaField('reporting_type', 'STRING'),
        ] 


class LoadInternalReportingCourseSeatToBigQuery(LoadInternalReportingCourseCatalogMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        return CourseSeatTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return 'd_course_seat'

    @property
    def schema(self):
        return [
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('course_seat_type', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('course_seat_price', 'FLOAT64', mode='REQUIRED'),
            bigquery.SchemaField('course_seat_currency', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('course_seat_upgrade_deadline', 'DATETIME'),
            bigquery.SchemaField('course_seat_credit_provider', 'STRING'),
            bigquery.SchemaField('course_seat_credit_hours', 'INT64'),
        ]


class LoadInternalReportingProgramCourseToBigQuery(LoadInternalReportingCourseCatalogMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        return ProgramCoursePartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            api_root_url=self.api_root_url,
            api_page_size=self.api_page_size,
            overwrite=self.overwrite,
        ).data_task

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
        ]


class LoadInternalReportingCourseCatalogToBigQuery(LoadInternalReportingCourseCatalogMixin, luigi.WrapperTask):

    dataset_id = luigi.Parameter()
    credentials = luigi.Parameter()

    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
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
            bigquery.SchemaField('user_id', 'INT64', mode='REQUIRED'),
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
            bigquery.SchemaField('user_id', 'INT64', mode='REQUIRED'),
            bigquery.SchemaField('course_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('activity_type', 'STRING'),
            bigquery.SchemaField('number_of_activities', 'INT64'),
        ]


class LoadInternalReportingUserToBigQuery(WarehouseMixin, BigQueryLoadTask):
    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter(
        description='Number of reduce tasks',
    )

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return (
            # Get the location of the Hive table, so it can be opened and read.
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
    def schema(self):
        return [
            bigquery.SchemaField('user_id', 'INT64'),
            bigquery.SchemaField('user_year_of_birth', 'INT64'),
            bigquery.SchemaField('user_level_of_education', 'STRING'),
            bigquery.SchemaField('user_gender', 'STRING'),
            bigquery.SchemaField('user_email', 'STRING'),
            bigquery.SchemaField('user_username', 'STRING'),
            bigquery.SchemaField('user_account_creation_time', 'DATETIME'),
            bigquery.SchemaField('user_last_location_country_code', 'STRING'),
        ]


class DailyLoadSubjectsToBigQueryTask(PullCatalogMixin, BigQueryLoadTask):

    @property
    def insert_source_task(self):
        # Note: don't pass overwrite down from here.  Use it only for overwriting when copying to Vertica.
        return DailyProcessFromCatalogSubjectTask(date=self.date, catalog_path=self.catalog_path)

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
