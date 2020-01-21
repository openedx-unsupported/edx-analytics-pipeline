"""
Tasks to load google spreadsheet data into the warehouse.

The spreadsheet should have the following appearance if column_types_row=True:

            A                 B          C        D
  +----------------------+-----------+--------+-------+-------------+
1 |      timestamp       | full_name | foo    | bar   | record_date |
  +----------------------+-----------+--------+-------+-------------+
2 |      datetime        | string    | float  | int   |    date     |
  +----------------------+-----------+--------+-------+-------------+
3 | 2019-06-13T10:36:20Z | Blah Blah | 123.45 | 12345 |  2019-06-13 |
  +----------------------+-----------+--------+-------+-------------+
4 | 2019-06-14T00:13:01Z | Blah Blah | 12.345 | 23456 |  2019-06-14 |
  +----------------------+-----------+--------+-------+-------------+
...

Where the first row contains column titles, the second row contains column
types, and the third row begins the data. If column_types_row=False, the
second row is omitted, and the data begins on the second row, and types are
assumed to be 'string'. Possible values include:
    * integer/int
    * string
    * boolean
    * float
    * decimal
    * date
    * datetime

The worksheet titles are used as table names in Vertica, they can be
128 characters long, beginning with an upper/lower alphabet or underscore,
subsequent characters can include upper/lower alphabets, underscores and digits.
"""
import datetime
import json
import logging

import luigi
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from gspread import client

from edx.analytics.tasks.common.snowflake_load import SnowflakeLoadFromHiveTSVTask
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


# Provides a mapping between the simple types provided by the spreadsheet author in a second header row, and the types
# as they should be loaded into BOTH Vertica and Snowflake.
DATA_TYPE_MAPPING = {
    'integer': 'INT',
    'int': 'INT',
    'string': 'VARCHAR(500)',
    'boolean': 'BOOLEAN',
    'float': 'FLOAT',
    'decimal': 'DECIMAL(12,2)',
    'date': 'DATE',
    'datetime': 'DATETIME',
}


def create_google_spreadsheet_client(credentials_target):
    with credentials_target.open('r') as credentials_file:
        json_creds = json.load(credentials_file)
        credentials = service_account.Credentials.from_service_account_info(
            json_creds,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )

    authed_session = AuthorizedSession(credentials)
    return client.Client(None, authed_session)


class PullWorksheetMixin(OverwriteOutputMixin):
    date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    google_credentials = luigi.Parameter(description='Path to the external access credentials file.')
    # spreadsheet key/id is a 44 characters alphanumeric including hyphen and underscore extracted from the URL.
    spreadsheet_key = luigi.Parameter(description='Google sheets key.')
    worksheet_name = luigi.Parameter(description='Worksheet name.', default=None)
    column_types_row = luigi.BoolParameter(default=False, description='Whether there is a row with column types.')


class PullWorksheetDataTask(PullWorksheetMixin, WarehouseMixin, luigi.Task):
    """
    Task to read data from a google worksheet and write to a tsv file.
    """

    def requires(self):
        return {
            'credentials': ExternalURL(url=self.google_credentials),
        }

    def run(self):
        self.remove_output_on_overwrite()
        credentials_target = self.input()['credentials']
        gs = create_google_spreadsheet_client(credentials_target)
        sheet = gs.open_by_key(self.spreadsheet_key)
        worksheet = sheet.worksheet(self.worksheet_name)
        all_values = worksheet.get_all_values()
        # Remove the header/column names.
        self.header = [v.strip() for v in all_values.pop(0)]
        if self.column_types_row:
            self.types = [v.strip() for v in all_values.pop(0)]
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


class LoadWorksheetToSnowflake(PullWorksheetMixin, SnowflakeLoadFromHiveTSVTask):
    """
    Task to load data from a google sheet into the Snowflake data warehouse.
    """

    @property
    def insert_source_task(self):
        return PullWorksheetDataTask(
            date=self.date,
            google_credentials=self.google_credentials,
            spreadsheet_key=self.spreadsheet_key,
            worksheet_name=self.worksheet_name,
            column_types_row=self.column_types_row,
            overwrite=self.overwrite,
        )

    @property
    def null_marker(self):
        """
        Treat the empty string as the NULL marker, so empty strings will become NULL in Snowflake.
        """
        return r''

    @property
    def table(self):
        return self.worksheet_name

    @property
    def columns(self):
        columns = self.insert_source_task.columns
        column_types = self.insert_source_task.column_types
        mapped_types = [DATA_TYPE_MAPPING.get(column_type) for column_type in column_types]
        return zip(columns, mapped_types)

    @property
    def file_format_name(self):
        return 'google_sheets_tsv_format'


class LoadWorksheetToVertica(PullWorksheetMixin, VerticaCopyTask):
    """
    Task to load data from a google sheet into the Vertica data warehouse.
    """

    @property
    def copy_null_sequence(self):
        """
        The null sequence in the data to be copied. Empty string is Vertica's default.
        """
        return "''"

    def create_table(self, connection):
        # Drop the table in case of overwrite
        if self.overwrite:
            connection.cursor().execute("DROP TABLE IF EXISTS {schema}.{table}".format(
                                        schema=self.schema, table=self.table))
        super(LoadWorksheetToVertica, self).create_table(connection)

    def init_copy(self, connection):
        # We have already dropped the table, so we do away with the delete here.
        self.attempted_removal = True

    @property
    def insert_source_task(self):
        return PullWorksheetDataTask(
            date=self.date,
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


class LoadGoogleSpreadsheetsToSnowflakeWorkflow(luigi.WrapperTask):
    """
    Provides entry point for loading a google spreadsheet into Snowflake.
    All worksheets within the spreadsheet are loaded as separate tables.
    """

    spreadsheets_config = luigi.DictParameter(
        config_path={'section': 'google-spreadsheets', 'name': 'snowflake_config'},
        description='A dictionary containing spreadsheets config where a key is the spreadsheet key/id extracted from '
                    'spreadsheet url, value is a dictionary containing at least the destination snowflake database and '
                    'schema for the new tables. Can also specify column_types_row key where the value is either true '
                    'or false specifying whether the worksheets in the spreadsheet contain a column types row as a '
                    'second header row.'
    )
    sf_credentials = luigi.Parameter(
        description='Path to the Snowflake credentials file.'
    )
    sf_run_id = luigi.Parameter(
        description='Id number to uniquely identify this run of table-copying.'
    )
    sf_warehouse = luigi.Parameter(
        description='Name of Snowflake virtual warehouse to use.'
    )
    sf_role = luigi.Parameter(
        description='Snowflake user role used to execute DDL/DML statements'
    )
    google_credentials = luigi.Parameter(
        description='Path to the external access credentials file.'
    )
    overwrite = luigi.BoolParameter(
        default=False,
        description='Whether or not to overwrite S3 outputs.',
        significant=False
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date()
    )

    def requires(self):
        credentials_target = ExternalURL(url=self.google_credentials).output()
        gs = create_google_spreadsheet_client(credentials_target)
        for spreadsheet_key, config in self.spreadsheets_config.items():
            schema = config['schema']
            scratch_schema = config['scratch_schema']
            database = config['database']
            column_types_row = config.get('column_types_row', False)

            spreadsheet = gs.open_by_key(spreadsheet_key)
            worksheets = spreadsheet.worksheets()

            for worksheet in worksheets:
                yield LoadWorksheetToSnowflake(
                    date=self.date,

                    # Snowflake-related params.
                    credentials=self.sf_credentials,
                    run_id=self.sf_run_id,
                    sf_database=database,
                    schema=schema,
                    scratch_schema=scratch_schema,
                    warehouse=self.sf_warehouse,
                    role=self.sf_role,
                    overwrite=self.overwrite,

                    # Google-related params.
                    google_credentials=self.google_credentials,
                    spreadsheet_key=spreadsheet_key,
                    worksheet_name=worksheet.title,
                    column_types_row=column_types_row,
                )


class LoadGoogleSpreadsheetsToVerticaWorkflow(luigi.WrapperTask):
    """
    Provides entry point for loading a google spreadsheet into the warehouse.
    All worksheets within the spreadsheet are loaded as separate tables.
    """

    spreadsheets_config = luigi.DictParameter(
        config_path={'section': 'google-spreadsheets', 'name': 'config'},
        description='A dictionary containing spreadsheets config where a key is the spreadsheet key/id extracted from '
                    'spreadsheet url, value is a dictionary containing atleast schema key/value pair which specifies '
                    'the vertica schema for the spreadsheet tables. Can also specify column_types_row key where the '
                    'value is either true or false specifying whether the worksheets in the spreadsheet contain a '
                    'column types row as a second header row.'
    )
    google_credentials = luigi.Parameter(
        description='Path to the external access credentials file.'
    )
    overwrite = luigi.BoolParameter(
        default=False,
        description='Whether or not to overwrite S3 outputs and the warehouse tables.',
        significant=False
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date()
    )

    def requires(self):
        credentials_target = ExternalURL(url=self.google_credentials).output()
        gs = create_google_spreadsheet_client(credentials_target)
        for spreadsheet_key, config in self.spreadsheets_config.items():
            schema = config['schema']
            column_types_row = config.get('column_types_row', False)

            spreadsheet = gs.open_by_key(spreadsheet_key)
            worksheets = spreadsheet.worksheets()

            for worksheet in worksheets:
                yield LoadWorksheetToVertica(
                    date=self.date,
                    schema=schema,
                    google_credentials=self.google_credentials,
                    spreadsheet_key=spreadsheet_key,
                    worksheet_name=worksheet.title,
                    column_types_row=column_types_row,
                    overwrite=self.overwrite,
                )
