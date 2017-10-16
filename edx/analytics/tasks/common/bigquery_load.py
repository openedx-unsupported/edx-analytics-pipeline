import logging
import datetime
import json
import os
import subprocess
import tempfile
import time
import urlparse

import luigi

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

from edx.analytics.tasks.util.url import ExternalURL, url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import WarehouseMixin


log = logging.getLogger(__name__)


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
        table.reload() # Load the schema

        # Use a tempfile for loading data into table_updates
        # We deliberately don't use table.insert_data as we cannot use delete on
        # a bigquery table with streaming inserts.
        tmp = tempfile.NamedTemporaryFile(delete=False)
        table_update_row  = (self.update_id, "{dataset}.{table}".format(dataset=self.dataset_id, table=self.table))
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
        if self.overwrite:
            query = client.run_sync_query("delete {dataset}.table_updates where target_table='{dataset}.{table}'".format(
                dataset=self.dataset_id, table=self.table
            ))
            query.use_legacy_sql = False
            query.run()

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
        destination_path = url_path_join('gs://{}'.format(parsed_url.netloc), parsed_url.path)

        if self.is_file(source_path):
            return_code = subprocess.call(['gsutil', 'cp', source_path, destination_path])
        else:
            log.debug(" ".join(['gsutil', '-m', 'rsync', source_path, destination_path]))
            return_code = subprocess.call(['gsutil', '-m', 'rsync', source_path, destination_path])

        if return_code != 0:
            raise RuntimeError('Error while syncing {source} to {destination}'.format(
                source=source_path,
                destination=destination_path,
            ))

        if self.is_file(destination_path):
            load_uri = destination_path
        else:
            load_uri = url_path_join(destination_path, '*')

        job = client.load_table_from_storage(
            'load_{table}_{timestamp}'.format(table=self.table, timestamp=int(time.time())),
            table,
            load_uri
        )
        job.field_delimiter = self.field_delimiter
        job.quote_character = self.quote_character
        job.null_marker = self.null_marker

        log.debug("Starting BigQuery Load job.")
        job.begin()
        wait_for_job(job)
        try:
            log.debug(
                "Load job started: %s ended: %s input_files: %s output_rows: %s output_bytes: %s",
                job.started,
                job.ended,
                job.input_files,
                job.output_rows,
                job.output_bytes,
            )
        except KeyError:
            log.debug("Load job started: %s ended: %s No load stats.", job.started, job.ended)

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

    def is_file(self, path):
        if path.endswith('.tsv') or path.endswith('.csv') or path.endswith('.gz'):
            return True
        else:
            return False

