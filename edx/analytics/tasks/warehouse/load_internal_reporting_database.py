"""
Loads a mysql database into the warehouse through the pipeline via Sqoop.
"""
import datetime
import logging
import re

import luigi

from edx.analytics.tasks.common.bigquery_load import BigQueryLoadDownstreamMixin, BigQueryLoadTask, BigQueryTarget
from edx.analytics.tasks.common.mysql_load import get_mysql_query_results
from edx.analytics.tasks.common.sqoop import SqoopImportFromMysql
from edx.analytics.tasks.common.vertica_load import SchemaManagementTask, VerticaCopyTask
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

try:
    from google.cloud.bigquery import SchemaField
    bigquery_available = True  # pylint: disable=invalid-name
except ImportError:
    bigquery_available = False  # pylint: disable=invalid-name


log = logging.getLogger(__name__)


class MysqlToWarehouseTaskMixin(WarehouseMixin):
    """
    Parameters for importing a mysql database into the warehouse.
    """

    db_credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    database = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'database'}
    )


class LoadMysqlToVerticaTableTask(MysqlToWarehouseTaskMixin, VerticaCopyTask):
    """
    Task to import a table from mysql into vertica.
    """

    table_name = luigi.Parameter(
        description='The name of the table.',
    )

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
    )

    def __init__(self, *args, **kwargs):
        super(LoadMysqlToVerticaTableTask, self).__init__(*args, **kwargs)
        self.table_schema = []

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'insert_source': self.insert_source_task,
            }
        return self.required_tasks

    def vertica_compliant_schema(self):
        """Transforms mysql table schema into a vertica compliant schema."""

        if not self.table_schema:
            results = get_mysql_query_results(self.db_credentials, self.database, 'describe {}'.format(self.table_name))
            for result in results:
                field_name = result[0].strip()
                field_type = result[1].strip()
                field_null = result[2].strip()

                types_with_parentheses = ['tinyint', 'smallint', 'int', 'bigint', 'datetime']
                if any(_type in field_type for _type in types_with_parentheses):
                    field_type = field_type.rsplit('(')[0]
                elif field_type == 'longtext':
                    field_type = 'LONG VARCHAR'
                elif field_type == 'longblob':
                    field_type = 'LONG VARBINARY'
                elif field_type == 'double':
                    field_type = 'DOUBLE PRECISION'

                if field_null == "NO":
                    field_type = field_type + " NOT NULL"

                field_name = "\"{}\"".format(field_name)

                self.table_schema.append((field_name, field_type))

        return self.table_schema

    @property
    def copy_delimiter(self):
        """The delimiter in the data to be copied."""
        return "','"

    @property
    def copy_null_sequence(self):
        """The null sequence in the data to be copied."""
        return "'NULL'"

    @property
    def enclosed_by(self):
        return "''''"

    @property
    def insert_source_task(self):
        partition_path_spec = HivePartition('dt', self.date.isoformat()).path_spec
        destination = url_path_join(
            self.warehouse_path,
            "database_import",
            self.database,
            self.table_name,
            partition_path_spec
        ) + '/'
        return SqoopImportFromMysql(
            table_name=self.table_name,
            credentials=self.db_credentials,
            database=self.database,
            destination=destination,
            overwrite=self.overwrite,
            mysql_delimiters=True,
        )

    @property
    def default_columns(self):
        return []

    @property
    def table(self):
        return self.table_name

    @property
    def columns(self):
        return self.vertica_compliant_schema()

    @property
    def auto_primary_key(self):
        return None


class PreImportDatabaseTask(SchemaManagementTask):
    """
    Task needed to run before importing database into warehouse.
    """

    @property
    def queries(self):
        return [
            "DROP SCHEMA IF EXISTS {schema} CASCADE;".format(schema=self.schema_loading),
            "CREATE SCHEMA IF NOT EXISTS {schema}".format(schema=self.schema_loading),
        ]

    @property
    def marker_name(self):
        return 'pre_database_import_{schema}_{date}'.format(
            schema=self.schema,
            date=self.date.strftime('%Y-%m-%d')
        )


class PostImportDatabaseTask(SchemaManagementTask):
    """
    Task needed to run after importing database into warehouse.
    """

    # Override the standard roles here since these tables will be rather raw. We may want to restrict access to a
    # subset of users.
    roles = luigi.ListParameter(
        config_path={'section': 'vertica-export', 'name': 'business_intelligence_team_roles'},
    )

    @property
    def queries(self):
        return [
            "DROP SCHEMA IF EXISTS {schema} CASCADE;".format(schema=self.schema),
            "ALTER SCHEMA {schema_loading} RENAME TO {schema};".format(
                schema_loading=self.schema_loading,
                schema=self.schema
            ),
            "GRANT USAGE ON SCHEMA {schema} TO {roles};".format(schema=self.schema, roles=self.vertica_roles),
            "GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {roles};".format(
                schema=self.schema,
                roles=self.vertica_roles
            ),
        ]

    @property
    def marker_name(self):
        return 'post_database_import_{schema}_{date}'.format(
            schema=self.schema,
            date=self.date.strftime('%Y-%m-%d')
        )


class ImportMysqlToVerticaTask(MysqlToWarehouseTaskMixin, luigi.WrapperTask):
    """Provides entry point for importing a mysql database into Vertica."""

    schema = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'schema'},
        description='The schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
    )
    overwrite = luigi.BoolParameter(
        default=False,
        significant=False,
    )

    exclude = luigi.ListParameter(
        default=(),
    )

    marker_schema = luigi.Parameter(
        description='The marker schema to which to write the marker table.'
    )

    def __init__(self, *args, **kwargs):
        super(ImportMysqlToVerticaTask, self).__init__(*args, **kwargs)
        self.table_list = []
        self.is_complete = False

    def should_exclude_table(self, table_name):
        """Determines whether to exclude a table during the import."""
        if any(re.match(pattern, table_name) for pattern in self.exclude):
            return True
        return False

    def run(self):
        # Add yields of tasks in run() method, to serve as dynamic dependencies.
        # This method should be rerun each time it yields a job.
        if not self.table_list:
            results = get_mysql_query_results(self.db_credentials, self.database, 'show tables')
            self.table_list = [result[0].strip() for result in results]

        pre_import_task = PreImportDatabaseTask(
            date=self.date,
            schema=self.schema,
            credentials=self.credentials,
            marker_schema=self.marker_schema,
            overwrite=self.overwrite,
        )
        yield pre_import_task

        for table_name in self.table_list:
            if not self.should_exclude_table(table_name):
                yield LoadMysqlToVerticaTableTask(
                    credentials=self.credentials,
                    schema=pre_import_task.schema_loading,
                    db_credentials=self.db_credentials,
                    database=self.database,
                    warehouse_path=self.warehouse_path,
                    table_name=table_name,
                    overwrite=self.overwrite,
                    date=self.date,
                    marker_schema=self.marker_schema,
                )

        yield PostImportDatabaseTask(
            date=self.date,
            schema=self.schema,
            credentials=self.credentials,
            marker_schema=self.marker_schema,
            overwrite=self.overwrite,
        )
        self.is_complete = True

    def complete(self):
        return self.is_complete


# Accepted types for standard tables are 'STRING', 'INT64', 'FLOAT64', 'BOOL', 'TIMESTAMP', 'BYTES', 'DATE', 'TIME', 'DATETIME'.
MYSQL_TO_BIGQUERY_TYPE_MAP = {
    # STRING -- BQ assumes input is utf8.
    # Loading will fail if the string data being loaded is not utf8, or contains ASCII 0.
    'char': 'STRING',  # includes 'NCHAR'.
    'varchar': 'STRING',  # includes 'NVARCHAR'.
    'text': 'STRING',
    'tinytext': 'STRING',
    'mediumtext': 'STRING',
    'longtext': 'STRING',
    'enum': 'STRING',
    # BIT is actually output by Sqoop as a Hex number.  Requires conversion on BigQuery side from Hex string to number,
    # by prepending '0x' to the string and safe-casting to INT64.
    'bit': 'STRING',
    # BOOL
    'tinyint(1)': 'BOOL',  # includes "BOOL" and "BOOLEAN".
    # INT64
    'tinyint': 'INT64',
    'smallint': 'INT64',
    'mediumint': 'INT64',
    'int': 'INT64',  # includes 'INTEGER'.
    'bigint': 'INT64',
    'year': 'INT64',
    # FLOAT64
    'decimal': 'FLOAT64',  # includes 'DEC', 'DECIMAL', 'NUMERIC', 'FIXED'.
    'float': 'FLOAT64',
    'double': 'FLOAT64',  # includes 'DOUBLE', 'DOUBLE PRECISION', 'REAL'.
    # BYTES
    # Sqoop outputs these as two character hex-codes separated by a space.  BigQuery reads these in and displays them as
    # the corresponding hex values without the space separation.  This doesn't seem like the desired behavior, but the
    # problem probably lies with Sqoop.  At present, LMS has only two columns that are longblobs, and they are passwords
    # in a table that is excluded.
    'binary': 'BYTES',
    'varbinary': 'BYTES',
    'tinyblob': 'BYTES',
    'blob': 'BYTES',
    'mediumblob': 'BYTES',
    'longblob': 'BYTES',
    # DATE
    'date': 'DATE',
    # TIME
    'time': 'TIME',
    # DATETIME
    'datetime': 'DATETIME',
    # TIMESTAMP -- like DATETIME, but with timezone
    'timestamp': 'TIMESTAMP',
}


class MysqlToBigQueryTaskMixin(MysqlToWarehouseTaskMixin):
    """
    Parameters for importing a mysql database into BigQuery.
    """

    exclude_field = luigi.ListParameter(
        default=(),
        description='List of regular expression patterns for matching "tablename.fieldname" fields that should not be output.',
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
    )
    # Don't use the same source for BigQuery loads as was used for Vertica loads, until their formats match.
    warehouse_subdirectory = luigi.Parameter(
        default='import_mysql_to_bq',
        description='Subdirectory under warehouse_path to store intermediate data.'
    )


class LoadMysqlToBigQueryTableTask(MysqlToBigQueryTaskMixin, BigQueryLoadTask):
    """
    Task to import a table from MySQL into BigQuery.
    """

    table_name = luigi.Parameter(
        description='The name of the table.',
    )

    def __init__(self, *args, **kwargs):
        super(LoadMysqlToBigQueryTableTask, self).__init__(*args, **kwargs)
        # Get schema for BigQuery and also Mysql columns that are excluded from that schema.
        self.table_schema = []
        self.deleted_fields = []

    def should_exclude_field(self, field_name):
        """Determines whether to exclude an individual field during the import, matching against 'table.field'."""
        full_name = "{}.{}".format(self.table_name, field_name)
        if any(re.match(pattern, full_name) for pattern in self.exclude_field):
            return True
        return False

    def get_bigquery_schema(self):
        """Transforms mysql table schema into a vertica compliant schema."""

        if not self.table_schema:
            results = get_mysql_query_results(self.db_credentials, self.database, 'describe {}'.format(self.table_name))
            for result in results:
                field_name = result[0].strip()
                field_type = result[1].strip()
                field_null = result[2].strip()

                # Strip off size information from any type except booleans.
                if field_type != 'tinyint(1)':
                    field_type = field_type.rsplit('(')[0]

                bigquery_type = MYSQL_TO_BIGQUERY_TYPE_MAP.get(field_type)
                mode = 'REQUIRED' if field_null == 'NO' else 'NULLABLE'
                description = ''

                if self.should_exclude_field(field_name):
                    self.deleted_fields.append(field_name)
                else:
                    self.table_schema.append(SchemaField(field_name, bigquery_type, description=description, mode=mode))

        return self.table_schema

    @property
    def field_delimiter(self):
        """The delimiter in the data to be copied."""
        # Select a delimiter string that will not occur in field values.
        return '\x01'

    @property
    def null_marker(self):
        """The null sequence in the data to be copied."""
        # Using "NULL" doesn't work, because there are values in several tables
        # in non-nullable fields that store the string value "NULL" (or "Null" or "null").
        # As with the field delimiter, we must use something here that we don't expect to
        # appear in a field value.  (This was an arbitrary but hopefully still readable choice.)
        # Note that this option only works if "direct" mode is disabled, otherwise it will be ignored
        # and "NULL" will always be output.
        return 'NNULLL'

    @property
    def quote_character(self):
        # BigQuery does not handle escaping of quotes.  It hews to a narrower standard for CSV
        # input, which expects quote characters to be doubled as a way of escaping them.
        # We therefore have to avoid escaping quotes by selecting delimiters and null markers
        # so they won't appear in field values at all.
        return ''

    @property
    def insert_source_task(self):
        # Make sure yet again that columns have been calculated.
        columns = [field.name for field in self.schema]

        partition_path_spec = HivePartition('dt', self.date.isoformat()).path_spec
        destination = url_path_join(
            self.warehouse_path,
            self.warehouse_subdirectory,
            self.database,
            self.table_name,
            partition_path_spec
        ) + '/'
        return SqoopImportFromMysql(
            table_name=self.table_name,
            credentials=self.db_credentials,
            database=self.database,
            destination=destination,
            overwrite=self.overwrite,
            mysql_delimiters=False,
            fields_terminated_by=self.field_delimiter,
            null_string=self.null_marker,
            delimiter_replacement=' ',
            direct=False,
            columns=columns,
        )

    @property
    def table(self):
        return self.table_name

    @property
    def schema(self):
        return self.get_bigquery_schema()

    @property
    def table_description(self):
        optional = ''
        if self.deleted_fields:
            optional = '  Fields not included in the copy: {}'.format(', '.join(self.deleted_fields))
        return "Copy of '{}' table from '{}' MySQL database on {}.{}".format(self.table_name, self.database, self.date.isoformat(), optional)

    @property
    def table_friendly_name(self):
        return '{} from {}'.format(self.table_name, self.database)


class ImportMysqlDatabaseToBigQueryDatasetTask(MysqlToBigQueryTaskMixin, BigQueryLoadDownstreamMixin, luigi.WrapperTask):
    """
    Provides entry point for importing a mysql database into BigQuery as a single dataset.

    It is assumed that there is one database written to each dataset.
    """

    exclude = luigi.ListParameter(
        default=(),
        description='List of regular expressions matching database table names that should not be imported from MySQL to BigQuery.'
    )

    def __init__(self, *args, **kwargs):
        super(ImportMysqlDatabaseToBigQueryDatasetTask, self).__init__(*args, **kwargs)
        self.table_list = []
        self.is_complete = False
        self.required_tasks = None

        # If we are overwriting the database output, then delete the entire marker table.
        # That way, any future activity on it should only consist of inserts, rather than any deletes
        # of existing marker entries.  There are quotas on deletes and upserts on a table, of no more
        # than 96 per day.   This allows us to work around hitting those limits.
        # Note that we have to do this early, before scheduling begins, so that no entries are present
        # when scheduling occurs (so everything gets properly scheduled).
        if self.overwrite:
            # First, create a BigQueryTarget object, so we can connect to BigQuery.  This is only
            # for the purpose of deleting the marker table, so use dummy values.
            credentials_target = ExternalURL(url=self.credentials).output()
            target = BigQueryTarget(
                credentials_target=credentials_target,
                dataset_id=self.dataset_id,
                table="dummy_table",
                update_id="dummy_id",
            )
            # Now ask it to delete the marker table completely.
            target.delete_marker_table()

    def should_exclude_table(self, table_name):
        """Determines whether to exclude a table during the import."""
        if any(re.match(pattern, table_name) for pattern in self.exclude):
            return True
        return False

    def requires(self):
        if not self.table_list:
            results = get_mysql_query_results(self.db_credentials, self.database, 'show tables')
            unfiltered_table_list = [result[0].strip() for result in results]
            self.table_list = [table_name for table_name in unfiltered_table_list if not self.should_exclude_table(table_name)]
        if self.required_tasks is None:
            self.required_tasks = []
            for table_name in self.table_list:
                self.required_tasks.append(
                    LoadMysqlToBigQueryTableTask(
                        db_credentials=self.db_credentials,
                        database=self.database,
                        warehouse_path=self.warehouse_path,
                        warehouse_subdirectory=self.warehouse_subdirectory,
                        table_name=table_name,
                        overwrite=self.overwrite,
                        date=self.date,
                        dataset_id=self.dataset_id,
                        credentials=self.credentials,
                        max_bad_records=self.max_bad_records,
                        skip_clear_marker=self.overwrite,
                        exclude_field=self.exclude_field,
                    )
                )
        return self.required_tasks

    def output(self):
        return [task.output() for task in self.requires()]

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
