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
    import snowflake.connector
    snowflake_available = True  # pylint: disable=invalid-name
except ImportError:
    log.warn('Unable to import Snowflake Python connector module.')
    # On hadoop slave nodes we don't have Snowflake Python connector module installed,
    # so just fail noisily if we attempt to use these libraries there.
    snowflake_available = False  # pylint: disable=invalid-name


# Snowflake constants
SNOWFLAKE_ACCOUNT_NAME = 'edx.us-east-1'
SNOWFLAKE_DB_NAME = 'edx-data-warehouse'
SNOWFLAKE_MARKER_TABLE_NAME = 'table_updates'
SNOWFLAKE_SQOOP_FILE_FORMAT = 'sqoop_to_snowflake_file_format'


class SnowflakeTarget(luigi.Target):

    def __init__(self, credentials_target, schema_name, table, update_id):
        self.schema_name = schema_name
        self.table = table
        self.update_id = update_id
        with credentials_target.open('r') as credentials_file:
            json_creds = json.load(credentials_file)
            self.conn = snowflake.connector.connect(
                user = json_creds['user'],
                password = json_creds['password'],
                account = SNOWFLAKE_ACCOUNT_NAME
            )

    def _use_db_and_schema(self, cursor):
        cursor.execute('USE DATABASE {}'.format(SNOWFLAKE_DB_NAME))
        cursor.execute('USE SCHEMA {}'.format(self.schema_name))

    def touch(self):
        cursor = self.conn.cursor()
        self._use_db_and_schema(cursor)
        self.create_marker_table(cursor)

        # Add a row to the updates table for this schema/table combo.
        cursor.execute(
            'INSERT INTO {marker_table} (update_id, target_table) VALUES({update_id}, {schema}.{table})'.format(
                marker_table = SNOWFLAKE_MARKER_TABLE_NAME,
                update_id = self.update_id,
                schema = self.schema_name,
                table = self.table
            )
        )

    def marker_table_exists(self):
        cursor = self.conn.cursor()
        self._use_db_and_schema(cursor)
        try:
            cursor.execute('SELECT 1 FROM {}'.format(SNOWFLAKE_MARKER_TABLE_NAME))
        except snowflake.connector.errors.ProgrammingError:
            # Raised when table does not exist.
            return False
        return True

    def create_marker_table(self, cursor):
        cursor.execute('CREATE TABLE IF NOT EXISTS {} (update_id VARCHAR, target_table VARCHAR)'.format(SNOWFLAKE_MARKER_TABLE_NAME))

    def delete_marker_table(self):
        cursor = self.conn.cursor()
        self._use_db_and_schema(cursor)
        cursor.execute('DELETE FROM {}'.format(SNOWFLAKE_MARKER_TABLE_NAME))

    def clear_marker_table(self):
        if not self.marker_table_exists():
            return

        cursor = self.conn.cursor()
        self._use_db_and_schema(cursor)
        query_string = "DELETE {marker_table} WHERE target_table='{schema}.{table}'".format(
            marker_table = SNOWFLAKE_MARKER_TABLE_NAME,
            schema = self.schema_name,
            table = self.table
        )
        cursor.execute(query_string)

    def clear_marker_table_entry(self):
        if not self.marker_table_exists():
            return

        cursor = self.conn.cursor()
        self._use_db_and_schema(cursor)
        query_string = "DELETE {marker_table} WHERE update_id='{update_id}' AND target_table='{schema}.{table}'".format(
            marker_table = SNOWFLAKE_MARKER_TABLE_NAME,
            update_id = self.update_id,
            schema = self.schema_name,
            table = self.table
        )
        cursor.execute(query_string)

    def exists(self):
        if not self.marker_table_exists():
            return False

        query_string = "SELECT 1 FROM {marker_table} WHERE update_id='{update_id}' AND target_table='{schema}.{table}'".format(
            marker_table = SNOWFLAKE_MARKER_TABLE_NAME,
            update_id = self.update_id,
            schema = self.schema_name,
            table = self.table
        )
        log.debug(query_string)
        result = cursor.execute(query_string)
        return result.rowcount == 1


class SnowflakeLoadDownstreamMixin(OverwriteOutputMixin):

    schema_name = luigi.Parameter()
    credentials = luigi.Parameter()
    max_bad_records = luigi.IntParameter(
        default=0, description="Number of bad records ignored by Snowflake before failing a load job."
    )


class SnowflakeLoadTask(SnowflakeLoadDownstreamMixin, luigi.Task):

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

    def create_db_and_schema(self, cursor):
        cursor.execute('CREATE DATABASE IF NOT EXISTS {}'.format(SNOWFLAKE_DB_NAME))
        cursor.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(self.schema_name))
        cursor.execute('USE DATABASE {db}.{schema}'.format(db=SNOWFLAKE_DB_NAME, schema=self.schema_name))

    def create_table(self, cursor):
        query_string = 'CREATE TABLE IF NOT EXISTS {table_name}'.format(self.table)
        if self.table_description:
            query_string += ' COMMENT = \'{description}\''
        cursor.execute(query_string)

    def init_copy(self, cursor):
        self.attempted_removal = True
        if self.overwrite:
            try:
                cursor.execute('DELETE FROM {}'.format(self.table))
            except snowflake.connector.errors.ProgrammingError:
                # Table already doesn't exist, so nothing to do here.
                pass
            if not self.skip_clear_marker:
                self.output().clear_marker_table()

    # def _get_destination_from_source(self, source_path):
    #     parsed_url = urlparse.urlparse(source_path)
    #     destination_path = url_path_join('gs://{}'.format(parsed_url.netloc), parsed_url.path)
    #     return destination_path

    # def _get_table_partition(self, dataset, table):
    #     date_string = self.date.isoformat()
    #     stripped_date = date_string.replace('-', '')
    #     partition_name = '{}${}'.format(table.name, stripped_date)
    #     return dataset.table(partition_name)

    # def _copy_data_to_gs(self, source_path, destination_path):
    #     if self.is_file(source_path):
    #         command = ['gsutil', 'cp', source_path, destination_path]
    #     else:
    #         # Exclude any files which should not be uploaded to
    #         # BigQuery.  It is easier to remove them here than in the
    #         # load steps.  The pattern is a Python regular expression.
    #         exclusion_pattern = ".*_SUCCESS$|.*_metadata$"
    #         command = ['gsutil', '-m', 'rsync', '-x', exclusion_pattern, source_path, destination_path]

    #     log.debug(" ".join(command))
    #     return_code = subprocess.call(command)
    #     if return_code != 0:
    #         raise RuntimeError('Error {code} while syncing {source} to {destination}'.format(
    #             code=return_code,
    #             source=source_path,
    #             destination=destination_path,
    #         ))

    # def _get_load_url_from_destination(self, destination_path):
    #     if self.is_file(destination_path):
    #         return destination_path
    #     else:
    #         return url_path_join(destination_path, '*')

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

    def _create_format_and_stage(self, cursor):
        query_string = ('CREATE OR REPLACE FILE FORMAT {} '
            'TYPE = \'CSV\' COMPRESSION = \'AUTO\' FIELD_DELIMITER = \'0x01\' '
            'RECORD_DELIMITER = \'\n\' SKIP_HEADER = 0 FIELD_OPTIONALLY_ENCLOSED_BY = \'NONE\' '
            'TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE '
            'ESCAPE = \'NONE\' ESCAPE_UNENCLOSED_FIELD = \'\134\' DATE_FORMAT = \'AUTO\' '
            'TIMESTAMP_FORMAT = \'AUTO\' NULL_IF = (\'\\N\')').format(SNOWFLAKE_SQOOP_FILE_FORMAT)
        cursor.execute(query_string)
        query_string = ('CREATE OR REPLACE STAGE {stage_name} '
            'URL = \'{s3_db_url}\' CREDENTIALS = (AWS_KEY_ID = \'{aws_key_id}\' '
            'AWS_SECRET_KEY = \'{aws_secret_key}\') COMMENT = \'{stage_comment}\' '
            'FILE_FORMAT = {file_format_name}').format(
            stage_name = SNOWFLAKE_SQOOP_FILE_FORMAT,
            s3_db_url = '',
            aws_key_id = '',
            aws_secret_key = '',
            stage_comment = '',
            file_format_name = ''
        )
        cursor.execute(query_string)


    def _copy_into_table(self):
        query_string = 'COPY INTO {table_name} FROM '


    def run(self):
        self.check_snowflake_availability()

        # client = self.output().client
        # self.create_dataset(client)

        conn = self.output().conn
        cursor = conn.cursor()
        self.create_db_and_schema(cursor)

        self.init_copy(client)
        self.create_table(client)

        dataset = client.dataset(self.dataset_id)
        table = dataset.table(self.table, self.schema)

        source_path = self.input()['source'].path
        destination_path = self._get_destination_from_source(source_path)
        # TODO: Change this Google Cloud-specific data copy to Snowflake-specific "COPY INTO" command.
        self._copy_data_to_gs(source_path, destination_path)
        load_uri = self._get_load_url_from_destination(destination_path)

        job_id = 'load_{table}_{timestamp}'.format(table=self.table, timestamp=int(time.time()))
        self._run_load_table_job(client, job_id, table, load_uri)

        self.output().touch()

    def output(self):
        if self.output_target is None:
            self.output_target = SnowflakeTarget(
                credentials_target=self.input()['credentials'],
                dataset_id=self.dataset_id,
                table=self.table,
                update_id=self.update_id(),
            )

        return self.output_target

    def update_id(self):
        return '{task_name}(date={key})'.format(task_name=self.task_family, key=self.date.isoformat())

    # def is_file(self, path):
    #     if path.endswith('.tsv') or path.endswith('.csv') or path.endswith('.gz'):
    #         return True
    #     else:
    #         return False

    def check_snowflake_availability(self):
        """Call to ensure fast failure if this machine doesn't have the Snowflake Python connector available."""
        if not snowflake_available:
            raise ImportError('Snowflake Python connector module not available')
