import json
import logging

import luigi
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import ProgrammingError

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL

log = logging.getLogger(__name__)


class SnowflakeTarget(luigi.Target):

    marker_table = 'table_updates'

    def __init__(self, credentials_target, database, schema, table, role, warehouse, update_id):
        with credentials_target.open('r') as credentials_file:
            creds = json.load(credentials_file)
            self.private_key = creds.get('private_key')
            self.passphrase = creds.get('passphrase')
            self.user = creds.get('user')
            self.account = creds.get('account')
            self.aws_key_id = creds.get('aws_key_id')
            self.aws_secret_key = creds.get('aws_secret_key')
            self.database = database
            self.schema = schema
            self.table = table
            self.role = role
            self.warehouse = warehouse
            self.update_id = update_id

    def connect(self, autocommit=False):
        p_key = serialization.load_pem_private_key(
            self.private_key.encode(),
            password=self.passphrase.encode(),
            backend=default_backend()
        )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

        connection = snowflake.connector.connect(
            user=self.user,
            account=self.account,
            private_key=pkb,
            autocommit=autocommit)

        # Switch to specified role.
        connection.cursor().execute("USE ROLE {}".format(self.role))

        return connection

    def create_warehouse(self, connection):
        cursor = connection.cursor()
        query = """
        CREATE WAREHOUSE IF NOT EXISTS {warehouse} WITH WAREHOUSE_SIZE = 'XSMALL'
        WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE
        MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD'
        INITIALLY_SUSPENDED = TRUE
        """.format(warehouse=self.warehouse)
        log.debug(query)
        cursor.execute(query)

    def suspend_warehouse(self, connection):
        cursor = connection.cursor()
        # Check if the warehouse is actually running before trying to suspend it.
        cursor.execute("SHOW WAREHOUSES LIKE '{}'".format(self.warehouse))
        row = cursor.fetchone()
        if row[1] == "STARTED":
            cursor.execute("ALTER WAREHOUSE {} SUSPEND".format(self.warehouse))

    def touch(self, connection):
        self.create_marker_table()

        connection.cursor().execute(
            """INSERT INTO {database}.{schema}.{marker_table} (update_id, target_table)
               VALUES (%s, %s)""".format(database=self.database, schema=self.schema, marker_table=self.marker_table),
            (self.update_id, "{database}.{schema}.{table}".format(database=self.database, schema=self.schema, table=self.table))
        )

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        close_connection = False
        if connection is None:
            # Luigi first checks for task completion by calling the exists() method. We create a new connection here
            # and also create the warehouse so that we can execute the SELECT query below.
            close_connection = True
            connection = self.connect()
            self.create_warehouse(connection)

        cursor = connection.cursor()
        try:
            cursor.execute(
                """SELECT 1 FROM {database}.{schema}.{marker_table}
                WHERE update_id = %s
                LIMIT 1""".format(
                    database=self.database, schema=self.schema, marker_table=self.marker_table),
                (self.update_id,)
            )
            row = cursor.fetchone()
        except ProgrammingError as e:
            if "does not exist" in e.msg:
                # If so then the query failed because the database or schema or table doesn't exist.
                row = None
            else:
                raise

        finally:
            if close_connection:
                self.suspend_warehouse(connection)
                connection.close()

        return row is not None

    def create_marker_table(self):
        connection = self.connect(autocommit=True)
        cursor = connection.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{marker_table} (
            id            INT AUTOINCREMENT,
            update_id     VARCHAR(4096)  NOT NULL,
            target_table  VARCHAR(128),
            inserted      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()::timestamp_ntz,
            PRIMARY KEY (update_id, id)
        )
        """.format(database=self.database, schema=self.schema, marker_table=self.marker_table)
        log.debug(query)
        cursor.execute(query)
        connection.close()


class SnowflakeLoadDownstreamMixin(OverwriteOutputMixin):

    credentials = luigi.Parameter(description='Path to the external access credentials file.')
    database = luigi.Parameter(description='The name of the database to which to write.')
    schema = luigi.Parameter(description='The name of the schema to which to write.')
    warehouse = luigi.Parameter(description='Name of virtual warehouse to use.')
    role = luigi.Parameter(description='User role used to execute DDL/DML statements')


class SnowflakeLoadTask(SnowflakeLoadDownstreamMixin, luigi.Task):

    date = luigi.DateParameter()
    output_target = None
    required_tasks = None

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'insert_source_task': self.insert_source_task,
            }
        return self.required_tasks

    @property
    def insert_source_task(self):
        raise NotImplementedError

    @property
    def table(self):
        raise NotImplementedError

    @property
    def columns(self):
        raise NotImplementedError

    @property
    def file_format_name(self):
        raise NotImplementedError

    @property
    def field_delimiter(self):
        return "\t"

    @property
    def null_marker(self):
        return "\\\\N"

    @property
    def pattern(self):
        return ".*"

    def create_database(self, connection):
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS {database}".format(database=self.database))

    def create_schema(self, connection):
        cursor = connection.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS {database}.{schema}".format(database=self.database, schema=self.schema))

    def create_table(self, connection):
        coldefs = ','.join(
            '{name} {definition}'.format(name=name, definition=definition) for name, definition in self.columns
        )
        query = "CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} ({coldefs})".format(
            database=self.database, schema=self.schema, table=self.table, coldefs=coldefs
        )
        connection.cursor().execute(query)

    def create_format(self, connection):
        query = """
        CREATE OR REPLACE FILE FORMAT {database}.{schema}.{file_format_name}
        TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = '{field_delimiter}'
        FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
        EMPTY_FIELD_AS_NULL = FALSE ESCAPE_UNENCLOSED_FIELD = 'NONE'
        NULL_IF = ('{null_marker}')
        """.format(
            database=self.database,
            schema=self.schema,
            file_format_name=self.file_format_name,
            field_delimiter=self.field_delimiter,
            null_marker=self.null_marker,
        )
        log.debug(query)
        connection.cursor().execute(query)

    def create_stage(self, connection):
        stage_url = self.input()['insert_source_task'].path
        query = """
        CREATE OR REPLACE STAGE {database}.{schema}.{table}_stage
            URL = '{stage_url}'
            CREDENTIALS = (AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
            FILE_FORMAT = {database}.{schema}.{file_format_name};
        """.format(
            database=self.database,
            schema=self.schema,
            table=self.table,
            stage_url=stage_url,
            aws_key_id=self.output().aws_key_id,
            aws_secret_key=self.output().aws_secret_key,
            file_format_name=self.file_format_name,
        )
        connection.cursor().execute(query)

    def init_copy(self, connection):
        self.attempted_removal = True
        if self.overwrite:
            marker_table = self.output().marker_table
            query = "DELETE FROM {database}.{schema}.{marker_table} where target_table='{target_table}'".format(
                database=self.database, schema=self.schema, marker_table=marker_table, target_table=self.table,
            )
            connection.cursor().execute(query)

            # TRUNCATE doesn't seem to be causing an implicit commit
            connection.cursor().execute("TRUNCATE TABLE {database}.{schema}.{table}".format(
                database=self.database, schema=self.schema, table=self.table
            ))

    def copy(self, connection):
        query = "COPY INTO {database}.{schema}.{table} from @{database}.{schema}.{table}_stage pattern='{pattern}'".format(
            database=self.database,
            schema=self.schema,
            table=self.table,
            pattern=self.pattern,
        )
        connection.cursor().execute(query)

    def run(self):
        connection = self.output().connect()
        try:
            cursor = connection.cursor()
            self.create_database(connection)
            self.create_schema(connection)
            self.create_table(connection)
            self.create_format(connection)
            self.create_stage(connection)

            self.output().create_warehouse(connection)

            cursor.execute("BEGIN")
            self.init_copy(connection)
            self.copy(connection)
            self.output().touch(connection)
            connection.commit()
        except Exception as exc:
            log.exception("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            raise
        finally:
            self.output().suspend_warehouse(connection)
            connection.close()

    def output(self):
        if self.output_target is None:
            self.output_target = SnowflakeTarget(
                credentials_target=self.input()['credentials'],
                database=self.database,
                schema=self.schema,
                table=self.table,
                role=self.role,
                warehouse=self.warehouse,
                update_id=self.update_id(),
            )

        return self.output_target

    def update_id(self):
        return '{task_name}(date={key})'.format(task_name=self.task_family, key=self.date.isoformat())
