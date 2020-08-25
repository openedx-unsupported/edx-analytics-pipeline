"""
Support for loading data into a Snowflake database.
"""
import json
import logging

import luigi
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector import ProgrammingError

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.s3_util import canonicalize_s3_url
from edx.analytics.tasks.util.url import ExternalURL

log = logging.getLogger(__name__)


def _execute_query(connection, query):
    try:
        connection.cursor().execute(query)
    except ProgrammingError as e:
        # Display the query along with the stack trace.
        import sys
        raise type(e), type(e)(str(e) + "\nQuery: %s\n" % query), sys.exc_info()[2]


def qualified_table_name(database, schema, table):
    """
    Fully qualified table name.
    """
    return "{database}.{schema}.{table}".format(
        database=database,
        schema=schema,
        table=table
    )


class SnowflakeTarget(luigi.Target):
    """
    Target for a resource in Snowflake.
    """

    marker_table = 'table_updates'

    def __init__(self, credentials_target, database, schema, scratch_schema, run_id, table, role, warehouse, update_id):
        with credentials_target.open('r') as credentials_file:
            creds = json.load(credentials_file)
            self.private_key = creds.get('private_key')
            self.passphrase = creds.get('passphrase')
            self.user = creds.get('user')
            self.account = creds.get('account')
            self.aws_key_id = creds.get('aws_key_id')
            self.aws_secret_key = creds.get('aws_secret_key')
            self.sf_database = database
            self.schema = schema
            self.scratch_schema = scratch_schema
            self.run_id = run_id
            self.table = table
            self.role = role
            self.warehouse = warehouse
            self.update_id = update_id

    def connect(self, autocommit=False):
        """
        Connects to the snowflake database.
        """
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
            autocommit=autocommit,
            warehouse=self.warehouse
        )

        # Switch to specified role.
        connection.cursor().execute("USE ROLE {}".format(self.role))
        # Set timezone to UTC
        connection.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")
        # Set timestamp default type to TIMESTAMP_TZ (instead of the Snowflake default of TIMESTAMP_NTZ).
        # Our Snowflake instance-wide standard for timestamps is TIMESTAMP_TZ, as it stores the timezone
        # along with the timestamp.
        connection.cursor().execute("ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_TZ'")

        return connection

    def touch(self, connection):
        """
        Mark this update as complete.
        """

        self.create_marker_table()

        query = """
        INSERT INTO {marker_table} (update_id, target_table)
        VALUES ('{update_id}', '{table}')""".format(
            update_id=str(self.update_id),
            marker_table=qualified_table_name(self.sf_database, self.schema, self.marker_table),
            table=qualified_table_name(self.sf_database, self.schema, self.table)
        )
        _execute_query(connection, query)

    def exists(self, connection=None):
        close_connection = False
        if connection is None:
            # Luigi first checks for task completion by calling the exists() method. We create a new connection here
            # so that we can execute the SELECT query below.
            close_connection = True
            connection = self.connect()

        try:
            if not self.marker_table_exists(connection):
                return False

            cursor = connection.cursor()
            query = """
            SELECT 1
            FROM {marker_table}
            WHERE update_id='{update_id}' AND target_table='{table}'
            """.format(
                marker_table=qualified_table_name(self.sf_database, self.schema, self.marker_table),
                update_id=self.update_id,
                table=qualified_table_name(self.sf_database, self.schema, self.table),
            )
            log.debug(query)
            cursor.execute(query)
            row = cursor.fetchone()
        finally:
            if close_connection:
                connection.close()

        return row is not None

    def marker_table_exists(self, connection):
        """
        Checks whether the `marker_table` exists.
        """
        cursor = connection.cursor()
        try:
            cursor.execute("SHOW TABLES LIKE '{marker_table}' IN SCHEMA {database}.{schema}".format(
                marker_table=self.marker_table,
                database=self.sf_database,
                schema=self.schema,
            ))
            row = cursor.fetchone()
        except ProgrammingError as err:
            if "does not exist" in str(err):
                # If so then the query failed because the database or schema doesn't exist.
                row = None
            else:
                raise

        return row is not None

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        """

        connection = self.connect(autocommit=True)
        query = """
        CREATE TABLE IF NOT EXISTS {marker_table} (
            id            INT AUTOINCREMENT,
            update_id     VARCHAR(4096)  NOT NULL,
            target_table  VARCHAR(128),
            inserted      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()::timestamp_ntz,
            PRIMARY KEY (update_id, id)
        )
        """.format(marker_table=qualified_table_name(self.sf_database, self.schema, self.marker_table))
        _execute_query(connection, query)

    def clear_marker_table(self, connection):
        """
        Delete all markers related to this table update.
        """
        if self.marker_table_exists(connection):
            query = """
            DELETE FROM {marker_table}
            WHERE target_table='{table}'
            """.format(
                marker_table=qualified_table_name(self.sf_database, self.schema, self.marker_table),
                table=qualified_table_name(self.sf_database, self.schema, self.table)
            )
            _execute_query(connection, query)


class SnowflakeLoadDownstreamMixin(OverwriteOutputMixin):
    """
    Parameters for copying a table into Snowflake.
    """

    credentials = luigi.Parameter(description='Path to the external access credentials file.')
    sf_database = luigi.Parameter(description='Name of the Snowflake database to which to write.')
    schema = luigi.Parameter(description='Name of the Snowflake schema to which to write the final loaded table.')
    scratch_schema = luigi.Parameter(description='Name of the Snowflake scratch schema to which to initially load the table before swapping to final destination.')
    run_id = luigi.Parameter(description='Id number to uniquely identify this run of table-copying.')
    warehouse = luigi.Parameter(description='Name of Snowflake virtual warehouse to use.')
    role = luigi.Parameter(description='Snowflake user role used to execute DDL/DML statements')


class SnowflakeLoadTask(SnowflakeLoadDownstreamMixin, luigi.Task):
    """
    A task for copying data into a Snowflake database table.
    """

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
        """
        Defines the task that provides source of data.
        """
        raise NotImplementedError

    @property
    def table(self):
        """
        Provides the name of the database table.
        """
        raise NotImplementedError

    @property
    def columns(self):
        """
        Provides definition of columns. If only writing to existing tables, then columns() need only provide a list of
        names.

        If also needing to create the table, then columns() should define a list of (name, definition) tuples.
        For example, ('first_name', 'VARCHAR(255)').
        """
        raise NotImplementedError

    @property
    def file_format_name(self):
        raise NotImplementedError

    @property
    def pattern(self):
        """
        Files matching this pattern will be used in the COPY operation.
        """
        return ".*"

    @property
    def truncate_columns(self):
        """
        Whether to truncate text strings that exceed the target column length.
        Note that this will be applied on all string columns of the table.
        """
        return "FALSE"

    @property
    def table_description(self):
        """
        Description of table containing various facts, such as import time and excluded fields.
        """
        return ''

    @property
    def qualified_stage_name(self):
        """
        Fully qualified stage name.
        """
        return "{database}.{schema}.{table}_stage".format(
            database=self.sf_database,
            schema=self.schema,
            table=self.table,
        )

    @property
    def qualified_table_name(self):
        """
        Fully qualified table name.
        """
        return qualified_table_name(
            database=self.sf_database,
            schema=self.schema,
            table=self.table,
        )

    @property
    def qualified_scratch_table_name(self):
        """
        Fully qualified scratch table name.
        """
        return "{database}.{scratch_schema}.{table}_{run_id}".format(
            database=self.sf_database,
            scratch_schema=self.scratch_schema,
            table=self.table,
            run_id=self.run_id,
        )

    def create_scratch_table(self, connection):
        coldefs = ','.join(
            '{name} {definition}'.format(name=name, definition=definition) for name, definition in self.columns
        )
        query = "CREATE TABLE {scratch_table} ({coldefs}) COMMENT='{comment}'".format(
            scratch_table=self.qualified_scratch_table_name,
            coldefs=coldefs,
            comment=self.table_description.replace("'", "\\'")
        )
        _execute_query(connection, query)

    def create_format(self, connection):
        """
        Invoke Snowflake's CREATE FILE FORMAT statement to create the named file format which
        configures the loading.

        The resulting file format name should be: {self.sf_database}.{self.schema}.{self.file_format_name}
        """
        raise NotImplementedError

    def create_stage(self, connection):
        stage_url = canonicalize_s3_url(self.input()['insert_source_task'].path)
        query = """
        CREATE OR REPLACE STAGE {stage_name}
            URL = '{stage_url}'
            CREDENTIALS = (AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
            FILE_FORMAT = {database}.{schema}.{file_format_name};
        """.format(
            stage_name=self.qualified_stage_name,
            database=self.sf_database,
            schema=self.schema,
            stage_url=stage_url,
            aws_key_id=self.output().aws_key_id,
            aws_secret_key=self.output().aws_secret_key,
            file_format_name=self.file_format_name,
        )
        _execute_query(connection, query)

    def init_copy(self, connection):
        self.attempted_removal = True
        if self.overwrite:
            # Delete all markers related to this table
            self.output().clear_marker_table(connection)

    def copy(self, connection):
        query = """
        COPY INTO {scratch_table}
        FROM @{stage_name}
        PATTERN='{pattern}'
        TRUNCATECOLUMNS = {truncate_columns}
        """.format(
            scratch_table=self.qualified_scratch_table_name,
            stage_name=self.qualified_stage_name,
            pattern=self.pattern,
            truncate_columns=self.truncate_columns,
        )
        log.debug(query)
        _execute_query(connection, query)

    def swap(self, connection):
        query = """
        ALTER TABLE {scratch_table}
        SWAP WITH {table}
        """.format(
            scratch_table=self.qualified_scratch_table_name,
            table=self.qualified_table_name,
        )
        log.debug(query)
        try:
            _execute_query(connection, query)
        except ProgrammingError as err:
            if "does not exist" in str(err):
                # Since the table did not exist in the target schema, simply move it instead of swapping.
                query = """
                ALTER TABLE {scratch_table}
                RENAME TO {table}
                """.format(
                    scratch_table=self.qualified_scratch_table_name,
                    table=self.qualified_table_name,
                )
                _execute_query(connection, query)
            else:
                raise

    def drop_scratch(self, connection):
        query = """
        DROP TABLE IF EXISTS {scratch_table}
        """.format(
            scratch_table=self.qualified_scratch_table_name
        )
        log.debug(query)
        _execute_query(connection, query)

    def run(self):
        connection = self.output().connect()
        try:
            cursor = connection.cursor()
            self.create_scratch_table(connection)
            self.create_format(connection)
            self.create_stage(connection)
            cursor.execute("BEGIN")
            self.init_copy(connection)
            self.copy(connection)
            self.swap(connection)
            self.drop_scratch(connection)
            self.output().touch(connection)
            connection.commit()
        except Exception as exc:
            log.exception("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            raise
        finally:
            connection.close()

    def output(self):
        if self.output_target is None:
            self.output_target = SnowflakeTarget(
                credentials_target=self.input()['credentials'],
                database=self.sf_database,
                schema=self.schema,
                scratch_schema=self.scratch_schema,
                run_id=self.run_id,
                table=self.table,
                role=self.role,
                warehouse=self.warehouse,
                update_id=self.update_id(),
            )

        return self.output_target

    def update_id(self):
        return '{task_name}(date={key})'.format(task_name=self.task_family, key=self.date.isoformat())


class SnowflakeLoadFromHiveTSVTask(SnowflakeLoadTask):  # pylint: disable=abstract-method
    """
    Abstract Task for loading CSV data from s3 into a table in Snowflake.

    Implementations should define the following properties:

    - self.insert_source_task
    - self.table
    - self.columns
    - self.file_format_name
    """

    @property
    def field_delimiter(self):
        """
        The delimiter in the data to be copied. Default is tab (\t).
        """
        return "\t"

    @property
    def null_marker(self):
        """
        The null sequence in the data to be copied. Default is Hive NULL (\\N).
        """
        return r'\\N'

    def create_format(self, connection):
        query = """
        CREATE OR REPLACE FILE FORMAT {database}.{schema}.{file_format_name}
        TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = '{field_delimiter}'
        FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
        EMPTY_FIELD_AS_NULL = FALSE ESCAPE_UNENCLOSED_FIELD = 'NONE'
        NULL_IF = ('{null_marker}')
        """.format(
            database=self.sf_database,
            schema=self.schema,
            file_format_name=self.file_format_name,
            field_delimiter=self.field_delimiter,
            null_marker=self.null_marker,
        )
        log.debug(query)
        connection.cursor().execute(query)


class SnowflakeLoadJSONTask(SnowflakeLoadTask):  # pylint: disable=abstract-method
    """
    Abstract Task for loading JSON data from s3 into a table in Snowflake.  The resulting table will
    contain a single VARIANT column called raw_json.

    Implementations should define the following properties:

    - self.insert_source_task
    - self.table
    - self.file_format_name
    """

    @property
    def columns(self):
        return [
            ('raw_json', 'VARIANT'),
        ]

    def create_format(self, connection):
        query = """
        CREATE OR REPLACE FILE FORMAT {database}.{schema}.{file_format_name}
        TYPE = 'JSON'
        COMPRESSION = 'AUTO'
        """.format(
            database=self.sf_database,
            schema=self.schema,
            file_format_name=self.file_format_name,
        )
        log.debug(query)
        connection.cursor().execute(query)
