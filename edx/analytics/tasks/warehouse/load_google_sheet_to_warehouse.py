"""
Tasks to load google spreadsheet data into the warehouse.
"""
import datetime
import json
import logging
import re

import luigi
from edx.analytics.tasks.common.vertica_load import VerticaCopyTaskMixin, VerticaCopyTask
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from gspread import client

log = logging.getLogger(__name__)


DATA_TYPE_MAPPING = {
    'string': 'VARCHAR',
    'float': 'FLOAT',
    'date': 'DATE',
    'datetime': 'DATETIME',
}

def identifier_from_string(s):
        return re.sub('[^a-zA-Z0-9_]+', '_', s) # Replace all other characters including spaces with underscore.


class PullWorksheetMixin(WarehouseMixin, OverwriteOutputMixin):
    google_credentials = luigi.Parameter(description='Path to the external access credentials file.')
    spreadsheet_key = luigi.Parameter(description='Google sheets key.')
    worksheet_name = luigi.Parameter(description='Worksheet name.', default=None)
    column_types_row = luigi.BoolParameter(default=False, description='Whether there is a row with column types.')

    def create_google_spreadsheet_cient(self, credentials_target):
        with credentials_target.open('r') as credentials_file:
            json_creds = json.load(credentials_file)
            credentials = service_account.Credentials.from_service_account_info(json_creds,
                            scopes=['https://www.googleapis.com/auth/drive.readonly'])

        authed_session = AuthorizedSession(credentials)
        return client.Client(None, authed_session)


class PullWorksheetDataTask(PullWorksheetMixin, luigi.Task):
    """
    Task to read data from a google worksheet and write to a tsv file.
    """

    date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    def requires(self):
        return {
            'credentials': ExternalURL(url=self.google_credentials),
        }

    def run(self):
        self.remove_output_on_overwrite()
        credentials_target = self.input()['credentials']
        gs = self.create_google_spreadsheet_cient(credentials_target)
        sheet = gs.open_by_key(self.spreadsheet_key)
        worksheet = sheet.worksheet(self.worksheet_name)
        all_values = worksheet.get_all_values()
        # Remove the header/column names.
        self.header = all_values.pop(0)
        if self.column_types_row:
            self.types = all_values.pop(0)
        else:
            self.types = ['string'] * len(self.header)

        with self.output().open('w') as output_file:
            for value in all_values:
                output_file.write('\t'.join([v.encode('utf-8') for v in value]))
                output_file.write('\n')

    @property
    def columns(self):
        return self.header

    @property
    def column_types(self):
        return self.types

    def output(self):
        partition_path_spec = HivePartition('dt', self.date).path_spec

        output_worksheet_name = self.worksheet_name
        output_url = url_path_join(self.warehouse_path, 'google_sheets', self.spreadsheet_key,
                        output_worksheet_name, partition_path_spec, '{}.tsv'.format(output_worksheet_name))
        return get_target_from_url(output_url)


class LoadWorksheetToWarehouse(PullWorksheetMixin, VerticaCopyTask):
    """
    Task to load data from a google sheet into the Vertica data warehouse.
    """

    def create_table(self, connection):
        # Drop the table in case of overwrite
        if self.overwrite:
            connection.cursor().execute("DROP TABLE IF EXISTS {schema}.{table}".format(
                                        schema=self.schema, table=self.table))
        super(LoadWorksheetToWarehouse, self).create_table(connection)

    def init_copy(self, connection):
        # We have already dropped the table, so we do away with the delete here.
        self.attempted_removal = True

    @property
    def insert_source_task(self):
        return PullWorksheetDataTask(
            warehouse_path=self.warehouse_path,
            google_credentials=self.google_credentials,
            spreadsheet_key=self.spreadsheet_key,
            worksheet_name=self.worksheet_name,
            column_types_row=self.column_types_row,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return self.worksheet_name

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return None

    @property
    def columns(self):
        columns = self.insert_source_task.columns
        column_types = self.insert_source_task.column_types
        mapped_types = [DATA_TYPE_MAPPING.get(column_type) for column_type in column_types]
        return zip(columns, mapped_types)
        # return [(column,'VARCHAR') for column in self.insert_source_task.columns]

class LoadGoogleSpreadsheetToWarehouseWorkflow(PullWorksheetMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """
    Provides entry point for loading a google spreadsheet into the warehouse.
    All worksheets within the spreadsheet are loaded as separate tables.
    """

    def requires(self):
        credentials_target = ExternalURL(url=google_credentials).output()
        gs = self.create_google_spreadsheet_cient(credentials_target)
        spreadsheet = gs.open_by_key(self.spreadsheet_key)
        worksheets = spreadsheet.worksheets()
        for worksheet in worksheets:
            yield LoadWorksheetToWarehouse(
                google_credentials=self.google_credentials,
                spreadsheet_key=self.spreadsheet_key,
                worksheet_name=worksheet.title,
                overwrite=self.overwrite,
                column_types_row=self.column_types_row,
                schema=self.schema,
            )

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
