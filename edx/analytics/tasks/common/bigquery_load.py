import json
import logging
import os
import subprocess
import tempfile
import time
import urlparse

import luigi

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from google.cloud.exceptions import NotFound
    bigquery_available = True  # pylint: disable=invalid-name
except ImportError:
    log.warn('Unable to import Bigquery libraries')
    # On hadoop slave nodes we don't have bigquery libraries installed,
    # so just fail noisily if we attempt to use these libraries there.
    bigquery_available = False  # pylint: disable=invalid-name


RETRY_LIMIT = 500
WAIT_DURATION = 5


def wait_for_job(job, check_error_result=True):
    counter = 0
    while True:
        if counter == RETRY_LIMIT:
            raise RuntimeError("Retry limit exceeded while waiting on job.")

        job.reload()
        if job.state == 'DONE':
            if check_error_result and job.error_result:
                raise RuntimeError(job.errors)
            return

        counter += 1
        time.sleep(WAIT_DURATION)


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
        table.reload()   # Load the schema

        # Use a tempfile for loading data into table_updates
        # We deliberately don't use table.insert_data as we cannot use delete on
        # a bigquery table with streaming inserts.
        tmp = tempfile.NamedTemporaryFile(delete=False)
        table_update_row = (self.update_id, "{dataset}.{table}".format(dataset=self.dataset_id, table=self.table))
        tmp.write(','.join(table_update_row))
        tmp.close()

        # table.upload_from_file requires the file to be opened in 'rb' mode.
        with open(tmp.name, 'rb') as source_file:
            job = table.upload_from_file(source_file, source_format='text/csv')

        try:
            wait_for_job(job)
        finally:
            tmp.close()
            os.unlink(tmp.name)

    def marker_table_exists(self):
        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table('table_updates')
        return table.exists()

    def create_marker_table(self):
        marker_table_schema = [
            bigquery.SchemaField('update_id', 'STRING'),
            bigquery.SchemaField('target_table', 'STRING'),
        ]

        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table('table_updates', marker_table_schema)
        if not table.exists():
            table.create()

    def delete_marker_table(self):
        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table('table_updates')
        if table.exists():
            table.delete()

    def clear_marker_table(self):
        if not self.marker_table_exists():
            return

        query_string = "DELETE {dataset}.table_updates WHERE target_table='{dataset}.{table}'".format(
            dataset=self.dataset_id, table=self.table
        )
        query = self.client.run_sync_query(query_string)
        query.use_legacy_sql = False
        query.run()

    def clear_marker_table_entry(self):
        if not self.marker_table_exists():
            return

        query_string = "DELETE {dataset}.table_updates WHERE update_id='{update_id}' AND target_table='{dataset}.{table}'".format(
            dataset=self.dataset_id, update_id=self.update_id, table=self.table
        )
        query = self.client.run_sync_query(query_string)
        query.use_legacy_sql = False
        query.run()

    def exists(self):
        if not self.marker_table_exists():
            return False

        query_string = "SELECT 1 FROM {dataset}.table_updates WHERE update_id='{update_id}' AND target_table='{dataset}.{table}'".format(
            dataset=self.dataset_id,
            update_id=self.update_id,
            table=self.table,
        )
        log.debug(query_string)

        query = self.client.run_sync_query(query_string)

        try:
            query.run()
        except NotFound:
            return False

        return len(query.rows) == 1


class BigQueryLoadDownstreamMixin(OverwriteOutputMixin):

    dataset_id = luigi.Parameter()
    credentials = luigi.Parameter()
    max_bad_records = luigi.IntParameter(
        default=0, description="Number of bad records ignored by BigQuery before failing a load job."
    )


class BigQueryLoadTask(BigQueryLoadDownstreamMixin, luigi.Task):

    # Regardless whether loading only a partition or an entire table,
    # we still need a date to use to mark the table.
    date = luigi.DateParameter()

    skip_clear_marker = luigi.BoolParameter(
        default=False,
        description='Set this when pre-deleting the marker table, to avoid quota issues with clearing entries.',
        significant=False,
    )

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
    def table_description(self):
        return ''

    @property
    def table_friendly_name(self):
        return ''

    @property
    def partitioning_type(self):
        """Set to 'DAY' in order to partition by day.  Default is to not partition at all."""
        return None

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
            if self.partitioning_type:
                table.partitioning_type = self.partitioning_type
            if self.table_description:
                table.description = self.table_description
            if self.table_friendly_name:
                table.friendly_name = self.table_friendly_name
            table.create()

    def init_copy(self, client):
        self.attempted_removal = True
        if self.overwrite:
            dataset = client.dataset(self.dataset_id)
            table = dataset.table(self.table)
            if self.partitioning_type:
                # Delete only the specific partition, and clear the marker only for the partition.
                # Note that there is no partition.exists() functionality that is useful.  Instead,
                # it returns table.exists().  Likewise, partition.delete() is a no-op if the actual
                # partition doesn't exist.  So if the table exists, we just try to delete the partition.
                if table.exists():
                    partition = self._get_table_partition(dataset, table)
                    partition.delete()
                if not self.skip_clear_marker:
                    self.output().clear_marker_table_entry()
            else:
                # Delete the entire table and all markers related to the table.
                if table.exists():
                    table.delete()
                if not self.skip_clear_marker:
                    self.output().clear_marker_table()

    def _get_destination_from_source(self, source_path):
        parsed_url = urlparse.urlparse(source_path)
        destination_path = url_path_join('gs://{}'.format(parsed_url.netloc), parsed_url.path)
        return destination_path

    def _get_table_partition(self, dataset, table):
        date_string = self.date.isoformat()
        stripped_date = date_string.replace('-', '')
        partition_name = '{}${}'.format(table.name, stripped_date)
        return dataset.table(partition_name)

    def _copy_data_to_gs(self, source_path, destination_path):
        if self.is_file(source_path):
            command = ['gsutil', 'cp', source_path, destination_path]
        else:
            # Exclude any files which should not be uploaded to
            # BigQuery.  It is easier to remove them here than in the
            # load steps.  The pattern is a Python regular expression.
            exclusion_pattern = ".*_SUCCESS$|.*_metadata$"
            command = ['gsutil', '-m', 'rsync', '-x', exclusion_pattern, source_path, destination_path]

        log.debug(" ".join(command))
        return_code = subprocess.call(command)
        if return_code != 0:
            raise RuntimeError('Error {code} while syncing {source} to {destination}'.format(
                code=return_code,
                source=source_path,
                destination=destination_path,
            ))

    def _get_load_url_from_destination(self, destination_path):
        if self.is_file(destination_path):
            return destination_path
        else:
            return url_path_join(destination_path, '*')

    def _run_load_table_job(self, client, job_id, table, load_uri):
        job = client.load_table_from_storage(job_id, table, load_uri)
        job.field_delimiter = self.field_delimiter
        job.quote_character = self.quote_character
        job.null_marker = self.null_marker
        if self.max_bad_records > 0:
            job.max_bad_records = self.max_bad_records
        log.debug("Starting BigQuery Load job.")
        job.begin()
        wait_for_job(job, check_error_result=False)

        try:
            log.debug(" Load job started: %s ended: %s input_files: %s output_rows: %s output_bytes: %s",
                      job.started, job.ended, job.input_files, job.output_rows, job.output_bytes)
        except KeyError as keyerr:
            log.debug(" Load job started: %s ended: %s No load stats.", job.started, job.ended)

        if job.error_result:
            for error in job.errors:
                log.debug("   Load error: %s", error)
            raise RuntimeError(job.errors)
        else:
            log.debug("   No errors encountered!")

    def run(self):
        self.check_bigquery_availability()

        client = self.output().client
        self.create_dataset(client)
        self.init_copy(client)
        self.create_table(client)

        dataset = client.dataset(self.dataset_id)
        table = dataset.table(self.table, self.schema)

        source_path = self.input()['source'].path
        destination_path = self._get_destination_from_source(source_path)
        self._copy_data_to_gs(source_path, destination_path)
        load_uri = self._get_load_url_from_destination(destination_path)

        if self.partitioning_type:
            partition = self._get_table_partition(dataset, table)
            partition.partitioning_type = self.partitioning_type
            job_id = 'load_{table}_{date_string}_{timestamp}'.format(
                table=self.table, date_string=self.date.isoformat(), timestamp=int(time.time())
            )
            self._run_load_table_job(client, job_id, partition, load_uri)
        else:
            job_id = 'load_{table}_{timestamp}'.format(table=self.table, timestamp=int(time.time()))
            self._run_load_table_job(client, job_id, table, load_uri)

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

    def is_file(self, path):
        if path.endswith('.tsv') or path.endswith('.csv') or path.endswith('.gz'):
            return True
        else:
            return False

    def check_bigquery_availability(self):
        """Call to ensure fast failure if this machine doesn't have the Bigquery libraries available."""
        if not bigquery_available:
            raise ImportError('Bigquery library not available')
