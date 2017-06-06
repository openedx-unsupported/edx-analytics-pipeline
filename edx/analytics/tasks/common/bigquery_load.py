import logging

import luigi
import json
import time
from luigi import configuration

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

from edx.analytics.tasks.util.url import ExternalURL
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

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
        query = self.client.run_sync_query(
            "SELECT 1 FROM {dataset}.table_updates where update_id = '{update_id}'".format(
                dataset=self.dataset_id,
                update_id=self.update_id,
            )
        )

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
        return ','

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

        with self.input()['source'].open('r') as source_file:
            job = table.upload_from_file(source_file, source_format='text/csv', field_delimiter=self.field_delimiter)

        # job = client.load_table_from_storage(
        #     'load_{table}_{timestamp}'.format(table=self.table, timestamp=int(time.time())),
        #     table,
        #     self.source
        # )
        # job.begin()

        while job.state != 'DONE':
            job.reload()

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
        return str(self)

class TestBigQueryLoad(BigQueryLoadTask):

    date = luigi.DateParameter()
    source = luigi.Parameter()

    @property
    def insert_source_task(self):
        ExternalURL(url=self.source)

    @property
    def table(self):
        return 'titanic'

    @property
    def schema(self):
        return [
            bigquery.SchemaField('passenger_id', 'INT64'),
            bigquery.SchemaField('survived', 'BOOL'),
            bigquery.SchemaField('pclass', 'INT64'),
            bigquery.SchemaField('name', 'STRING'),
            bigquery.SchemaField('sex', 'STRING'),
            bigquery.SchemaField('age', 'FLOAT64'),
            bigquery.SchemaField('sib_sp', 'INT64'),
            bigquery.SchemaField('parch', 'INT64'),
            bigquery.SchemaField('ticket', 'STRING'),
            bigquery.SchemaField('fare', 'FLOAT64'),
            bigquery.SchemaField('cabin', 'STRING'),
            bigquery.SchemaField('embarked', 'STRING'),
        ]
