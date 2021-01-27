"""
Loads a mysql database into the warehouse through the pipeline via Sqoop.
"""
import datetime
import json
import logging
import re

import luigi

from edx.analytics.tasks.common.mysql_load import get_mysql_query_results
from edx.analytics.tasks.common.snowflake_load import (
    SnowflakeLoadDownstreamMixin, SnowflakeLoadFromHiveTSVTask, SnowflakeTarget
)
from edx.analytics.tasks.common.sqoop import METADATA_FILENAME, SqoopImportFromMysql
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

# define pseudo-table-name to use for storing database-level metadata output.
DUMP_METADATA_OUTPUT = '_metadata_export_database'


class MysqlToWarehouseTaskMixin(WarehouseMixin):
    """
    Parameters for importing a mysql database into the warehouse.
    """

    exclude_field = luigi.ListParameter(
        default=(),
        description='List of regular expression patterns for matching "tablename.fieldname" fields that should not be output.',
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
    )
    db_credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    database = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'database'}
    )

    def should_exclude_field(self, table_name, field_name):
        """Determines whether to exclude an individual field during the import, matching against 'table.field'."""
        full_name = "{}.{}".format(table_name, field_name)
        if any(re.match(pattern, full_name) for pattern in self.exclude_field):
            return True
        return False

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
        """The character used to enclose field values to treat as literal when they contain special characters.  Default is none."""
        # BigQuery does not handle escaping of quotes.  It hews to a narrower standard for CSV
        # input, which expects quote characters to be doubled as a way of escaping them.
        # We therefore have to avoid escaping quotes by selecting delimiters and null markers
        # so they won't appear in field values at all.  We therefore do without any quote character at all.
        return ''


class MysqlToS3TaskMixin(MysqlToWarehouseTaskMixin):
    """
    Parameters for exporting a mysql database to S3.
    """
    warehouse_subdirectory = luigi.Parameter(
        default='import_mysql_to_s3',
        description='Subdirectory under warehouse_path to store intermediate data.'
    )


class MysqlTableExportMixin(MysqlToS3TaskMixin):
    """Methods used by classes that export or load MySQL tables."""

    def mysql_table_schema(self):
        return self.metadata['additional_metadata']['table_schema']

    def get_table_metadata_target(self):
        """Returns target for metadata file from the given dump."""
        # find the .metadata file in the source directory.
        metadata_path = url_path_join(self.s3_location_for_table, METADATA_FILENAME)
        return get_target_from_url(metadata_path)

    @property
    def s3_location_for_table(self):
        partition_path_spec = HivePartition('dt', self.date.isoformat()).path_spec
        destination = url_path_join(
            self.warehouse_path,
            self.warehouse_subdirectory,
            self.database,
            self.table_name,
            partition_path_spec
        ) + '/'
        return destination

    def get_table_metadata(self):
        metadata_target = self.get_table_metadata_target()
        with metadata_target.open('r') as metadata_file:
            metadata = json.load(metadata_file)
        return metadata


class ExportMysqlTableToS3Task(MysqlTableExportMixin, OverwriteOutputMixin, luigi.Task):
    """
    Task to export a table from MySQL to S3.
    """

    table_name = luigi.Parameter(
        description='The name of the table.',
    )

    def __init__(self, *args, **kwargs):
        super(ExportMysqlTableToS3Task, self).__init__(*args, **kwargs)
        self.required_tasks = None
        self.sqoop_export_task = None
        self.deleted_fields = []

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'sqoop_export_task': self.sqoop_export_mysql_table_task
            }
        return self.required_tasks

    def output(self):
        return get_target_from_url(self.s3_location_for_table)

    def complete(self):
        return self.sqoop_export_mysql_table_task.complete()

    def mysql_compliant_schema(self):
        table_schema = []
        results = get_mysql_query_results(self.db_credentials, self.database, 'describe {}'.format(self.table_name))
        for result in results:
            field_name = result[0].strip()
            field_type = result[1].strip()
            field_null = result[2].strip()
            if self.should_exclude_field(self.table_name, field_name):
                self.deleted_fields.append(field_name)
            else:
                table_schema.append((field_name, field_type, field_null))
        return table_schema

    @property
    def sqoop_export_mysql_table_task(self):
        if self.sqoop_export_task is None:
            mysql_schema = self.mysql_compliant_schema()
            column_names = [field_name for (field_name, _field_type, _field_null) in mysql_schema]
            additional_metadata = {
                'table_schema': mysql_schema,
                'deleted_fields': self.deleted_fields,
                'database': self.database,
                'table_name': self.table_name,
                'date': self.date.isoformat(),
            }
            self.sqoop_export_task = SqoopImportFromMysql(
                table_name=self.table_name,
                credentials=self.db_credentials,
                database=self.database,
                destination=self.s3_location_for_table,
                overwrite=self.overwrite,
                mysql_delimiters=False,
                fields_terminated_by=self.field_delimiter,
                null_string=self.null_marker,
                delimiter_replacement=' ',
                direct=False,
                columns=column_names,
                additional_metadata=additional_metadata,
            )

        return self.sqoop_export_task


class MysqlDatabaseExportMixin(MysqlToS3TaskMixin):
    """Parameters and methods used by classes that export or load MySQL databases."""

    include = luigi.ListParameter(
        default=(),
        description='List of regular expression patterns for matching the names of tables that should be output.',
    )

    exclude = luigi.ListParameter(
        default=(),
        description='List of regular expression patterns for matching the names of tables that should not be output.',
    )

    def should_exclude_table(self, table_name):
        """Determines whether to exclude a table during the import."""

        if self.include:
            return not any(re.match(pattern, table_name) for pattern in self.include)
        elif self.exclude:
            return any(re.match(pattern, table_name) for pattern in self.exclude)

        return False

    def get_table_list_for_database(self):
        return self.metadata['table_list']

    def database_metadata_target(self):
        partition_path_spec = HivePartition('dt', self.date.isoformat()).path_spec
        metadata_destination = url_path_join(
            self.warehouse_path,
            self.warehouse_subdirectory,
            self.database,
            DUMP_METADATA_OUTPUT,
            partition_path_spec,
            METADATA_FILENAME,
        )
        return get_target_from_url(metadata_destination)


class ExportMysqlDatabaseToS3Task(MysqlDatabaseExportMixin, luigi.Task):
    """
    Provides entry point for exporting a MySQL database to S3.
    """

    overwrite = luigi.BoolParameter(
        default=False,
        significant=False,
    )

    def __init__(self, *args, **kwargs):
        super(ExportMysqlDatabaseToS3Task, self).__init__(*args, **kwargs)
        self.table_includes_list = []
        self.creation_time = None
        self.required_tasks = None

    def requires(self):
        if self.creation_time is None:
            self.creation_time = datetime.datetime.utcnow().isoformat()

        if self.required_tasks is None:
            self.required_tasks = []
            if not self.table_includes_list:
                results = get_mysql_query_results(self.db_credentials, self.database, 'show tables')
                table_list = [result[0].strip() for result in results]
                self.table_includes_list = [table_name for table_name in table_list if not self.should_exclude_table(table_name)]

            for table_name in self.table_includes_list:
                self.required_tasks.append(
                    ExportMysqlTableToS3Task(
                        date=self.date,
                        database=self.database,
                        db_credentials=self.db_credentials,
                        exclude_field=self.exclude_field,
                        table_name=table_name,
                    )
                )
        return self.required_tasks

    def output(self):
        return self.database_metadata_target()

    def run(self):
        metadata = {
            'table_list': self.table_includes_list,
            'database': self.database,
            'date': self.date.isoformat(),
            'exclude': self.exclude,
            'include': self.include,
            'warehouse_path': self.warehouse_path,
            'warehouse_subdirectory': self.warehouse_subdirectory,
            'creation_time': self.creation_time,
            'completion_time': datetime.datetime.utcnow().isoformat(),
        }
        with self.output().open('w') as metadata_file:
            json.dump(metadata, metadata_file)


# Snowflake reserves these keywords - they cannot be used as column (field) names in a table unless double-quoted.
SNOWFLAKE_RESERVED_KEYWORDS = (
    'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'BETWEEN', 'BY', 'CASE', 'CAST', 'CHECK', 'CLUSTER',
    'COLUMN', 'CONNECT', 'CREATE', 'CROSS', 'CURRENT_DATE', 'CURRENT_ROLE', 'CURRENT_USER', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
    'DELETE', 'DESC', 'DISTINCT', 'DROP', 'ELSE', 'EXCLUSIVE', 'EXISTS', 'FALSE', 'FOR', 'FROM', 'FULL',
    'GRANT', 'GROUP', 'HAVING', 'IDENTIFIED', 'ILIKE', 'IMMEDIATE', 'IN', 'INCREMENT', 'INNER', 'INSERT',
    'INTERSECT', 'INTO', 'IS', 'JOIN', 'LATERAL', 'LEFT', 'LIKE', 'LOCK', 'LONG', 'MAXEXTENTS', 'MINUS',
    'MODIFY', 'NATURAL', 'NOT', 'NULL', 'OF', 'ON', 'OPTION', 'OR', 'ORDER', 'REGEXP', 'RENAME', 'REVOKE',
    'RIGHT', 'RLIKE', 'ROW', 'ROWS', 'SAMPLE', 'SELECT', 'SET', 'SOME', 'START', 'TABLE', 'TABLESAMPLE',
    'THEN', 'TO', 'TRIGGER', 'TRUE', 'UNION', 'UNIQUE', 'UPDATE', 'USING', 'VALUES', 'VIEW', 'WHEN',
    'WHENEVER', 'WHERE', 'WITH'
)


class LoadMysqlTableFromS3ToSnowflakeTask(MysqlTableExportMixin, SnowflakeLoadFromHiveTSVTask):
    """
    Task to import a table from S3 but originally from MySQL into Snowflake.
    """
    table_name = luigi.Parameter(
        description='The name of the table to be loaded.',
    )
    # Some fields are no longer needed when reading from S3.
    exclude_field = None
    db_credentials = None

    def __init__(self, *args, **kwargs):
        """
        Init this task.
        """
        super(LoadMysqlTableFromS3ToSnowflakeTask, self).__init__(*args, **kwargs)
        self.metadata = self.get_table_metadata()

    def get_snowflake_schema(self):
        """
        Transforms MySQL table schema into a Snowflake-compliant schema.
        """
        results = []
        for field_name, field_type, field_null in self.mysql_table_schema():

            # Enclose any Snowflake-reserved keyword field names within double-quotes.
            if field_name.upper() in SNOWFLAKE_RESERVED_KEYWORDS:
                field_name = '"{}"'.format(field_name.upper())

            mysql_types_with_parentheses = ['smallint', 'int', 'bigint', 'varchar']
            if field_type == 'tinyint(1)':
                field_type = 'BOOLEAN'
            elif 'datetime' in field_type:
                field_type = 'TIMESTAMP_TZ'
            elif any(_type in field_type for _type in mysql_types_with_parentheses):
                field_type = field_type.rsplit('(')[0]
            elif field_type == 'longtext':
                field_type = 'VARCHAR'
            elif field_type == 'longblob':
                field_type = 'BINARY'

            if field_null == 'NO':
                field_type += ' NOT NULL'

            results.append((field_name, field_type))

        return results

    @property
    def insert_source_task(self):
        """
        Insert the Sqoop task that imports the source MySQL data into S3.
        """
        return ExternalURL(url=self.s3_location_for_table)

    @property
    def table(self):
        return self.table_name

    @property
    def table_schema(self):
        return self.get_snowflake_schema()

    @property
    def columns(self):
        return self.table_schema

    @property
    def file_format_name(self):
        return 'SQOOP_MYSQL_FORMAT'

    @property
    def pattern(self):
        return '.*part-m.*'

    @property
    def table_description(self):
        optional = ''
        deleted_fields = self.metadata['additional_metadata']['deleted_fields']
        if deleted_fields:
            optional = '  Fields not included in the copy: {}'.format(', '.join(deleted_fields))
        return "Copy of '{}' table from '{}' MySQL database on {}.{}".format(self.table_name, self.database, self.date.isoformat(), optional)


class ImportMysqlDatabaseFromS3ToSnowflakeSchemaTask(MysqlDatabaseExportMixin, SnowflakeLoadDownstreamMixin, luigi.WrapperTask):
    """
    Provides entry point for importing a MySQL database into Snowflake as a single schema.

    Assumes that only one database is written to each schema.
    """
    # Some fields are no longer needed when reading from S3.
    exclude_field = None
    db_credentials = None

    def __init__(self, *args, **kwargs):
        """
        Inits this Luigi task.
        """
        super(ImportMysqlDatabaseFromS3ToSnowflakeSchemaTask, self).__init__(*args, **kwargs)
        metadata_target = self.database_metadata_target()
        with metadata_target.open('r') as metadata_file:
            self.metadata = json.load(metadata_file)

        self.required_tasks = None

    def requires(self):
        """
        Determines the required tasks given the list of tables exported from MySQL.
        """
        if self.required_tasks is None:
            self.required_tasks = []
            for table_name in self.get_table_list_for_database():
                if not self.should_exclude_table(table_name):
                    self.required_tasks.append(
                        LoadMysqlTableFromS3ToSnowflakeTask(
                            sf_database=self.sf_database,
                            schema=self.schema,
                            scratch_schema=self.scratch_schema,
                            run_id=self.run_id,
                            warehouse=self.warehouse,
                            role=self.role,
                            warehouse_path=self.warehouse_path,
                            warehouse_subdirectory=self.warehouse_subdirectory,
                            database=self.database,
                            table_name=table_name,
                            overwrite=self.overwrite,
                            date=self.date,
                            credentials=self.credentials,
                        )
                    )
        return self.required_tasks

    def complete(self):
        """
        Reduces the result of the required tasks into a single complete/not-complete.
        """
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
