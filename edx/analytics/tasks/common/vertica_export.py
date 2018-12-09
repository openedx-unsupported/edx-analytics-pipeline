"""
Supports exporting data from Vertica.
"""
import datetime
import json
import logging
import re

import luigi
from google.cloud import bigquery

from edx.analytics.tasks.common.bigquery_load import BigQueryLoadTask
from edx.analytics.tasks.common.sqoop import SqoopImportFromVertica
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import HivePartition
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

try:
    import vertica_python
    from vertica_python.errors import QueryError
    vertica_client_available = True  # pylint: disable=invalid-name
except ImportError:
    log.warn('Unable to import Vertica client libraries')
    # On hadoop slave nodes we don't have Vertica client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    vertica_client_available = False  # pylint: disable=invalid-name


def get_vertica_results(credentials, query):
    """Run a single query in Vertica and return the results."""
    credentials_target = ExternalURL(url=credentials).output()
    cred = None
    with credentials_target.open('r') as credentials_file:
        cred = json.load(credentials_file)

    # Externalize autocommit and read timeout
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


VERTICA_TO_BIGQUERY_FIELD_MAPPING = {
    'boolean': 'bool',
    'integer': 'int64',
    'int': 'int64',
    'bigint': 'int64',
    'varbinary': 'bytes',
    'long varbinary': 'bytes',
    'binary': 'bytes',
    'char': 'string',
    'varchar': 'string',
    'long varchar': 'string',
    'money': 'float64',
    'numeric': 'float64',
    'float': 'float64',
    'date': 'date',
    'time': 'time',
    # Due to an error with the Vertica JDBC client time zone information is stripped prior to extracting.  All
    # timestamptz and timetz values are coerced to UTC (manually in the case of timestamptz).
    'timetz': 'time',
    'timestamptz': 'datetime',
    'timestamp': 'datetime'
}


def get_vertica_table_schema(credentials, schema_name, table_name):
    """
    Returns the Vertica schema in the format (column name, column type, nullable?) for the indicated table.
    """

    query = "select column_name, data_type, is_nullable from columns where table_schema='{schema}' and " \
            "table_name='{table}' order by ordinal_position;".format(schema=schema_name, table=table_name)
    results = []
    rows = get_vertica_results(credentials, query)

    for row in rows:
        column_name = row[0]
        nullable = row[2]
        field_type = row[1]
        field_type = field_type.rsplit('(')[0]

        if field_type.lower() in ['long varbinary']:
            log.error('Error Vertica jdbc tool is unable to export field type \'%s\'.  This field will be '
                      'excluded from the extract.', field_type.lower())
        else:
            results.append((column_name, field_type.lower(), nullable))

    return results


class VerticaTableToS3Task(OverwriteOutputMixin, luigi.Task):
    """
    Export a table from Vertica to S3 using sqoop.

    An exporter that reads a table from Vertica and persists the data to S3.  In order to use
    LoadVerticaToS3TableTask the caller must know the Vertica schema and table name. The columns are automatically
    discovered at run time.
    """
    date = luigi.DateParameter(
        description='A URL location of the data warehouse.'
    )
    intermediate_warehouse_path = luigi.Parameter(
        description='A dictionary specifying the S3-centric configuration.'
    )
    sqoop_null_string = luigi.Parameter(
        default='null',
        description='A string replacement value for any (null) values encountered by Sqoop when exporting from Vertica.'
    )
    sqoop_fields_terminated_by = luigi.Parameter(
        default=',',
        description='The field delimiter used by Sqoop.'
    )
    sqoop_delimiter_replacement = luigi.Parameter(
        default=' ',
        description='The string replacement value for special characters encountered by Sqoop when exporting from '
                    'Vertica.'
    )
    column_list = luigi.ListParameter(
        description='The column names being extracted from this table.'
    )
    warehouse_name = luigi.Parameter(
        description='The Vertica warehouse that houses the schema being copied.'
    )
    schema_name = luigi.Parameter(
        description='The Vertica schema being copied. '
    )
    credentials = luigi.Parameter(
        description='Path to the external Vertica access credentials file.',
    )
    table_name = luigi.Parameter(
        description='The Vertica table being copied.'
    )
    timestamptz_column_list = luigi.ListParameter(
        default=[],
        description='The list of columns being copied that are of type timestamptz.  This is necessary because of a '
                    'bug in the time zone processing of the Vertica JDBC client.  Note timetz fields are automatically'
                    'converted to UTC.'
    )

    def __init__(self, *args, **kwargs):
        super(VerticaTableToS3Task, self).__init__(*args, **kwargs)
        self.required_tasks = None
        self.table_schema = None
        self.vertica_source_table_schema = None

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'insert_source': self.insert_source_task,
            }
        return self.required_tasks

    def complete(self):
        return self.insert_source_task.complete()

    def s3_output_path(self):
        partition_path_spec = HivePartition('dt', self.date).path_spec
        target_url = url_path_join(self.intermediate_warehouse_path,
                                   self.warehouse_name,
                                   self.schema_name,
                                   self.table_name,
                                   partition_path_spec) + '/'
        return target_url

    def output(self):
        return get_target_from_url(self.s3_output_path())

    @property
    def insert_source_task(self):
        """The sqoop command that manages the data transfer from the source datasource."""
        if len(self.column_list) <= 0:
            raise RuntimeError('Error Sqoop copy of {schema}.{table} found no viable columns!'.
                               format(schema=self.schema_name,
                                      table=self.table))

        target_url = self.s3_output_path()

        return SqoopImportFromVertica(
            schema_name=self.schema_name,
            table_name=self.table_name,
            credentials=self.credentials,
            database=self.warehouse_name,
            columns=self.column_list,
            destination=target_url,
            overwrite=self.overwrite,
            null_string=self.sqoop_null_string,
            fields_terminated_by=self.sqoop_fields_terminated_by,
            delimiter_replacement=self.sqoop_delimiter_replacement,
            timezone_adjusted_column_list=self.timestamptz_column_list,
        )


class LoadVerticaTableToBigQuery(BigQueryLoadTask):
    """Copies one table from Vertica through S3/GCP into BigQuery."""

    intermediate_warehouse_path = luigi.Parameter(
        description='The intermediate warehouse path used on S3 and GCP'
    )
    table_name = luigi.Parameter(
        description='The Vertica table being copied.'
    )
    vertica_warehouse_name = luigi.Parameter(
        default='warehouse', description='The Vertica warehouse that houses the schema being copied.'
    )
    vertica_schema_name = luigi.Parameter(
        description='The Vertica schema being copied. '
    )
    vertica_credentials = luigi.Parameter(
        description='Path to the external Vertica access credentials file.',
    )
    exclude = luigi.ListParameter(
        description='The Vertica tables that are to be excluded from exporting.'
    )

    def __init__(self, *args, **kwargs):
        super(LoadVerticaTableToBigQuery, self).__init__(*args, **kwargs)
        self.vertica_source_table_schema = None
        self.bigquery_compliant_schema = None
        self.timestamptz_column_list = None

    @property
    def vertica_table_schema(self):
        """The schema of the Vertica table."""
        if self.vertica_source_table_schema is None:
            self.vertica_source_table_schema = get_vertica_table_schema(self.vertica_credentials,
                                                                        self.vertica_schema_name,
                                                                        self.table_name)
        return self.vertica_source_table_schema

    @property
    def table(self):
        return self.table_name

    @property
    def table_description(self):
        return "Copy of Vertica table {}.{} on {}".format(self.vertica_schema_name,
                                                          self.table_name,
                                                          self.date)

    @property
    def table_friendly_name(self):
        return '{} from {}'.format(self.table_name, self.vertica_schema_name)

    @property
    def field_delimiter(self):
        return '\x01'

    @property
    def null_marker(self):
        return 'NNULLL'

    @property
    def delimiter_replacement(self):
        return ' '

    @property
    def schema(self):
        """The BigQuery compliant schema."""
        if self.bigquery_compliant_schema is None:
            res = []
            timestamp_list = []
            for field_name, vertica_field_type, nullable in self.vertica_table_schema:
                # column_name, data_type, is_nullable
                if vertica_field_type in VERTICA_TO_BIGQUERY_FIELD_MAPPING:
                    res.append(bigquery.SchemaField(field_name,
                                                    VERTICA_TO_BIGQUERY_FIELD_MAPPING[vertica_field_type],
                                                    mode='NULLABLE' if nullable else 'REQUIRED'))
                    if vertica_field_type in ['timestamptz']:
                        timestamp_list.append(field_name)
                else:
                    raise RuntimeError('Error for field {field}: Vertica type {type} does not have a mapping to '
                                       'BigQuery'.format(field=field_name, type=vertica_field_type))
            self.bigquery_compliant_schema = res
            self.timestamptz_column_list = timestamp_list
        return self.bigquery_compliant_schema

    @property
    def insert_source_task(self):
        return VerticaTableToS3Task(
            intermediate_warehouse_path=self.intermediate_warehouse_path,
            date=self.date,
            overwrite=self.overwrite,
            sqoop_null_string=self.null_marker,
            sqoop_fields_terminated_by=self.field_delimiter,
            sqoop_delimiter_replacement=self.delimiter_replacement,
            column_list=[field.name for field in self.schema],
            warehouse_name=self.vertica_warehouse_name,
            schema_name=self.vertica_schema_name,
            credentials=self.vertica_credentials,
            table_name=self.table_name,
            timestamptz_column_list=self.timestamptz_column_list
        )


@workflow_entry_point
class VerticaSchemaToBigQueryTask(luigi.WrapperTask):
    """
    A task that copies all the tables in a Vertica schema to S3.

    Reads all tables in a schema and, if they are not listed in the `exclude` parameter, schedules a
    LoadVerticaToS3TableTask task for each table.
    """
    overwrite = luigi.BoolParameter(
        default=False,
        significant=False,
        description='Indicates if the target data sources should be removed prior to generating.'
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date.  This parameter is used to isolate intermediate datasets.'
    )
    exclude = luigi.ListParameter(
        default=[],
        description='The Vertica tables that are to be excluded from exporting.'
    )
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
    bigquery_dataset = luigi.Parameter(
        default=None,
        description='The target dataset that will hold the Vertica schema.'
    )
    max_bad_records = luigi.IntParameter(
        default=0,
        description="Number of bad records ignored by BigQuery before failing a load job."
    )
    gcp_credentials = luigi.Parameter(
        description='Path to the external GCP/BigQuery access credentials file.',
    )
    s3_warehouse_path = luigi.Parameter(
        config_path={'section': 'hive', 'name': 'warehouse_path'},
        description='The warehouse path to store intermediate data on S3.'
    )

    def __init__(self, *args, **kwargs):
        super(VerticaSchemaToBigQueryTask, self).__init__(*args, **kwargs)

    def should_exclude_table(self, table_name):
        """Determines whether to exclude a table during the import."""
        if any(re.match(pattern, table_name) for pattern in self.exclude):
            return True
        return False

    def requires(self):
        yield ExternalURL(url=self.vertica_credentials)
        yield ExternalURL(url=self.gcp_credentials)

        if self.bigquery_dataset is None:
            self.bigquery_dataset = self.vertica_schema_name

        intermediate_warehouse_path = url_path_join(self.s3_warehouse_path, 'import/vertica/sqoop/')

        query = "SELECT table_name FROM all_tables WHERE schema_name='{schema_name}' AND table_type='TABLE' " \
                "".format(schema_name=self.vertica_schema_name)
        table_list = [row[0] for row in get_vertica_results(self.vertica_credentials, query)]

        for table_name in table_list:
            if not self.should_exclude_table(table_name):

                yield LoadVerticaTableToBigQuery(
                    date=self.date,
                    overwrite=self.overwrite,
                    intermediate_warehouse_path=intermediate_warehouse_path,
                    dataset_id=self.bigquery_dataset,
                    credentials=self.gcp_credentials,
                    max_bad_records=self.max_bad_records,
                    table_name=table_name,
                    vertica_schema_name=self.vertica_schema_name,
                    vertica_warehouse_name=self.vertica_warehouse_name,
                    vertica_credentials=self.vertica_credentials,
                    exclude=self.exclude,
                )
