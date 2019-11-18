"""
Supports exporting data from Vertica to S3, for loading into other databases.
"""
import datetime
import json
import logging
import re

import luigi

from edx.analytics.tasks.common.sqoop import SqoopImportFromVertica
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

try:
    import vertica_python
    vertica_client_available = True  # pylint: disable=invalid-name
except ImportError:
    log.warn('Unable to import Vertica client libraries')
    # On hadoop slave nodes we don't have Vertica client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    vertica_client_available = False  # pylint: disable=invalid-name


# Define default values to be used by Sqoop when exporting tables from Vertica, whether for loading in
# BigQuery or Snowflake (or some other database).

VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER = u'\x01'

VERTICA_EXPORT_DEFAULT_NULL_MARKER = 'NNULLL'

VERTICA_EXPORT_DEFAULT_DELIMITER_REPLACEMENT = ' '


def get_vertica_results(credentials, query):
    """Run a single query in Vertica and return the results."""
    credentials_target = ExternalURL(url=credentials).output()
    cred = None
    with credentials_target.open('r') as credentials_file:
        cred = json.load(credentials_file)

    # Externalize autocommit and read timeout
    # WE CHANGED DATABASE FROM WAREHOUSE TO DOCKER
    # connection = vertica_python.connect(user=cred.get('username'), password=cred.get('password'), host=cred.get('host'),
    #                                     port=cred.get('port'), database='docker', autocommit=False,
    #                                     read_timeout=None)

    connection = vertica_python.connect(user=cred.get('username'), password=cred.get('password'), host=cred.get('host'),
                                        port=cred.get('port'), database='warehouse', autocommit=False,
                                        read_timeout=None)

    if not vertica_client_available:
        raise ImportError('Vertica client library not available')

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
    finally:
        connection.close()

    return results


def get_vertica_table_schema(credentials, schema_name, table_name):
    """
    Returns the Vertica schema in the format (column name, column type, is_nullable) for the indicated table.

    The column_type value is lowercased, but the size information is not stripped.
    """

    query = "SELECT column_name, data_type, is_nullable FROM columns WHERE table_schema='{schema}' AND " \
            "table_name='{table}' ORDER BY ordinal_position;".format(schema=schema_name, table=table_name)
    results = []
    rows = get_vertica_results(credentials, query)

    for row in rows:
        column_name = row[0]
        field_type = row[1].lower()
        nullable = row[2]

        if field_type.split('(')[0] in ['long varbinary']:
            log.error('Error Vertica jdbc tool is unable to export field type \'%s\'.  This field will be '
                      'excluded from the extract.', field_type)
        else:
            results.append((column_name, field_type, nullable))

    return results


class VerticaTableFromS3Mixin(object):
    """
    Defines Sqoop parameters to be used in the loading of exported data from S3.

    The parameters for null-string and field-terminator should be
    matched by the loaders that load Vertica-exported data from S3.

    The delimiter replacement is only used when dumping, not loading, so it is excluded.
    """
    sqoop_null_string = luigi.Parameter(
        default=VERTICA_EXPORT_DEFAULT_NULL_MARKER,
        description='A string replacement value for any (null) values encountered by Sqoop when exporting from Vertica.'
    )
    sqoop_fields_terminated_by = luigi.Parameter(
        default=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER,
        description='The field delimiter used by Sqoop.'
    )


class VerticaTableToS3Mixin(VerticaTableFromS3Mixin):
    """
    Defines Sqoop parameters to be used in the export of Vertica data to S3.

    The parameters for null-string and field-terminator should be
    matched by the loaders that load Vertica-exported data from S3.

    The delimiter replacement is only used when dumping, not loading, so it is added here.
    """
    sqoop_delimiter_replacement = luigi.Parameter(
        default=VERTICA_EXPORT_DEFAULT_DELIMITER_REPLACEMENT,
        description='The string replacement value for special characters encountered by Sqoop when exporting from '
                    'Vertica.'
    )


class VerticaExportMixin(WarehouseMixin):
    """Information about Vertica that is needed by all classes involved in exporting data and also in loading it elsewhere."""

    vertica_warehouse_name = luigi.Parameter(
        default='warehouse',
        description='The Vertica warehouse that houses the schema being copied.'
    )
    vertica_schema_name = luigi.Parameter(
        description='The Vertica schema being copied. '
    )
    vertica_credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external Vertica access credentials file.',
    )
    intermediate_warehouse_path = luigi.Parameter(
        description='A path specifying the S3 root for storing dumps from Vertica.',
        default=None,
    )

    def __init__(self, *args, **kwargs):
        super(VerticaExportMixin, self).__init__(*args, **kwargs)
        # If no explicit value is passed in, use the default.
        if self.intermediate_warehouse_path is None:
            self.intermediate_warehouse_path = self.default_intermediate_warehouse_path

    @property
    def default_intermediate_warehouse_path(self):
        """Define root URL under which data should be written to or read from in S3."""
        return url_path_join(self.warehouse_path, 'import/vertica/sqoop/')


class VerticaTableExportMixin(VerticaExportMixin):
    """A set of parameters and methods used by classes that dump or load a Vertica table."""

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date.  This parameter is used to isolate intermediate datasets.'
    )
    table_name = luigi.Parameter(
        description='The Vertica table being dumped to S3.'
    )

    def __init__(self, *args, **kwargs):
        super(VerticaTableExportMixin, self).__init__(*args, **kwargs)
        self.vertica_source_table_schema = None

    @property
    def vertica_table_schema(self):
        """The schema of the Vertica table."""
        if self.vertica_source_table_schema is None:
            self.vertica_source_table_schema = get_vertica_table_schema(
                self.vertica_credentials, self.vertica_schema_name, self.table_name
            )
        return self.vertica_source_table_schema

    @property
    def s3_location_for_table(self):
        """
        Returns the URL for the location of S3 data for the given table and schema.

        This logic is shared by classes that dump data to S3 and those that load data from S3, so they agree.
        """
        partition_path_spec = HivePartition('dt', self.date).path_spec
        url = url_path_join(self.intermediate_warehouse_path,
                            self.vertica_warehouse_name,
                            self.vertica_schema_name,
                            self.table_name,
                            partition_path_spec) + '/'
        return url


class VerticaSchemaExportMixin(VerticaExportMixin):
    """A set of parameters and methods used by classes that dump or load a Vertica schema."""

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date.  This parameter is used to isolate intermediate datasets.'
    )
    exclude = luigi.ListParameter(
        default=[],
        description='The Vertica tables that are to be excluded from exporting.'
    )

    def __init__(self, *args, **kwargs):
        super(VerticaSchemaExportMixin, self).__init__(*args, **kwargs)
        self._table_names_list = None

    def should_exclude_table(self, table_name):
        """Determine whether to exclude a table during the import, based on regex matching agains specified patterns."""
        if any(re.match(pattern, table_name) for pattern in self.exclude):  # pylint: disable=not-an-iterable
            return True
        return False

    def get_table_list_for_schema(self):
        """Returns a list of tables in the specified Vertica schema, applying all exclude patterns."""
        # Cache list for tables in schema, to reduce repeated calls to Vertica.
        if not self._table_names_list:
            query = "SELECT table_name FROM all_tables WHERE schema_name='{schema_name}' AND table_type='TABLE' " \
                    "".format(schema_name=self.vertica_schema_name)
            table_list = [row[0] for row in get_vertica_results(self.vertica_credentials, query)]

            self._table_names_list = [table_name for table_name in table_list if not self.should_exclude_table(table_name)]
        return self._table_names_list


class ExportVerticaTableToS3Task(VerticaTableExportMixin, VerticaTableToS3Mixin, OverwriteOutputMixin, luigi.Task):
    """
    Export a table from Vertica to S3 using sqoop.

    An exporter that reads a table from Vertica and persists the data to S3.  In order to use
    LoadVerticaToS3TableTask the caller must know the Vertica schema and table name. The columns are automatically
    discovered at run time.
    """
    def __init__(self, *args, **kwargs):
        super(ExportVerticaTableToS3Task, self).__init__(*args, **kwargs)
        self._required_tasks = None
        self._sqoop_dump_vertica_table_task = None

    def requires(self):
        if self._required_tasks is None:
            self._required_tasks = {
                'credentials': ExternalURL(url=self.vertica_credentials),
                'sqoop_dump_vertica_table_task': self.sqoop_dump_vertica_table_task,
            }
        return self._required_tasks

    def complete(self):
        return self.sqoop_dump_vertica_table_task.complete()

    def output(self):
        return get_target_from_url(self.s3_location_for_table)

    @property
    def sqoop_dump_vertica_table_task(self):
        """The sqoop command that manages the data transfer from the source datasource."""
        # Only calculate this once, since it's used in required() and complete() methods.
        if self._sqoop_dump_vertica_table_task is None:
            # We not only need the list of column names, but we also
            # need the list of columns being copied that are of type
            # timestamptz.  This is necessary because of a bug in the
            # time zone processing of the Vertica JDBC client.  Note
            # timetz fields are automatically converted to UTC.
            column_list = []
            timestamptz_column_list = []
            for field_name, vertica_field_type, _ in self.vertica_table_schema:
                column_list.append(field_name)
                if vertica_field_type.split('(')[0] in ['timestamptz']:
                    timestamptz_column_list.append(field_name)

            if len(column_list) <= 0:
                raise RuntimeError('Error Sqoop copy of {schema}.{table} found no viable columns!'.
                                   format(schema=self.schema_name, table=self.table))

            self._sqoop_dump_vertica_table_task = SqoopImportFromVertica(
                schema_name=self.vertica_schema_name,
                table_name=self.table_name,
                credentials=self.vertica_credentials,
                database=self.vertica_warehouse_name,
                destination=self.s3_location_for_table,
                overwrite=self.overwrite,
                null_string=self.sqoop_null_string,
                fields_terminated_by=self.sqoop_fields_terminated_by,
                delimiter_replacement=self.sqoop_delimiter_replacement,
                columns=column_list,
                timezone_adjusted_column_list=timestamptz_column_list,
            )
        return self._sqoop_dump_vertica_table_task


@workflow_entry_point
class ExportVerticaSchemaToS3Task(VerticaSchemaExportMixin, VerticaTableToS3Mixin, luigi.WrapperTask):
    """
    A task that copies all the tables in a Vertica schema to S3, so other jobs can load from there.

    Reads all tables in a schema and, if they are not listed in the `exclude` parameter, schedules a
    ExportVerticaTableToS3Task task for each table.
    """
    overwrite = luigi.BoolParameter(
        default=False,
        significant=False,
        description='Indicates if the target data sources should be removed prior to generating.'
    )

    def requires(self):
        yield ExternalURL(url=self.vertica_credentials)

        for table_name in self.get_table_list_for_schema():
            yield ExportVerticaTableToS3Task(
                date=self.date,
                overwrite=self.overwrite,
                table_name=table_name,
                intermediate_warehouse_path=self.intermediate_warehouse_path,
                vertica_schema_name=self.vertica_schema_name,
                vertica_warehouse_name=self.vertica_warehouse_name,
                vertica_credentials=self.vertica_credentials,
                sqoop_null_string=self.sqoop_null_string,
                sqoop_fields_terminated_by=self.sqoop_fields_terminated_by,
                sqoop_delimiter_replacement=self.sqoop_delimiter_replacement,
            )
