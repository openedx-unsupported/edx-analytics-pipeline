"""
Tasks to load google sheet data into the warehouse.
"""
import datetime
import json
import logging
import re

import luigi
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from gspread import client

log = logging.getLogger(__name__)


def identifier_from_string(s):
        return re.sub('[^a-zA-Z0-9_]+', '_', s) # Replace all other characters including spaces with underscore.

class PullGoogleSheetMixin(WarehouseMixin, OverwriteOutputMixin):
    google_credentials = luigi.Parameter(description='Path to the external access credentials file.')
    spreadsheet_key = luigi.Parameter(description='Google sheets key.')
    worksheet_name = luigi.Parameter(description='Worksheet name.')
    skip_rows = luigi.IntParameter(default=0, description='Number of rows to skip from the start.')


class PullGoogleSheetDataTask(PullGoogleSheetMixin, luigi.Task):
    """
    Task to read data from a google worksheet and write to a tsv file.
    """

    date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    def requires(self):
        return {
            'credentials': ExternalURL(url=self.google_credentials),
        }

    def create_google_sheets_client(self):
        with self.input()['credentials'].open('r') as credentials_file:
            json_creds = json.load(credentials_file)
            credentials = service_account.Credentials.from_service_account_info(json_creds,
                            scopes=['https://www.googleapis.com/auth/drive.readonly'])

        authed_session = AuthorizedSession(credentials)
        return client.Client(None, authed_session)

    def run(self):
        self.remove_output_on_overwrite()
        gs = self.create_google_sheets_client()
        sheet = gs.open_by_key(self.spreadsheet_key)
        worksheet = sheet.worksheet(self.worksheet_name)
        all_values = worksheet.get_all_values()[self.skip_rows:]
        # Remove the header/column names.
        self.header = [identifier_from_string(column) for column in all_values.pop(0)]

        with self.output().open('w') as output_file:
            for value in all_values:
                output_file.write('\t'.join([v.encode('utf-8') for v in value]))
                output_file.write('\n')

    @property
    def columns(self):
        return self.header

    def output(self):
        partition_path_spec = HivePartition('dt', self.date).path_spec

        output_worksheet_name = identifier_from_string(self.worksheet_name)
        output_url = url_path_join(self.warehouse_path, 'google_sheets', self.spreadsheet_key,
                        output_worksheet_name, partition_path_spec, '{}.tsv'.format(output_worksheet_name))
        return get_target_from_url(output_url)


class LoadGoogleSheetToWarehouse(PullGoogleSheetMixin, VerticaCopyTask):
    """
    Task to load data from a google sheet into the Vertica datawarehouse.
    """

    @property
    def insert_source_task(self):
        return PullGoogleSheetDataTask(
            warehouse_path=self.warehouse_path,
            google_credentials=self.google_credentials,
            spreadsheet_key=self.spreadsheet_key,
            worksheet_name=self.worksheet_name,
            skip_rows=self.skip_rows,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return identifier_from_string(self.worksheet_name)

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        return None

    @property
    def columns(self):
        return [(column,'VARCHAR') for column in self.insert_source_task.columns]
