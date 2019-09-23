"""
Tasks to load a Vertica schema into Snowflake.
"""

import datetime
import logging
import re

import luigi

from edx.analytics.tasks.common.snowflake_load import SnowflakeLoadDownstreamMixin, SnowflakeLoadFromHiveTSVTask
from edx.analytics.tasks.common.vertica_export import get_vertica_results
from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)


class LoadVerticaTableFromS3ToSnowflakeMixin(WarehouseMixin):
    """
    Common parameters for loading a vertica table from S3 to Snowflake.
    """
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


class LoadVerticaTableFromS3ToSnowflake(LoadVerticaTableFromS3ToSnowflakeMixin, SnowflakeLoadFromHiveTSVTask):
    """
    Task to load a vertica table from S3 into Snowflake.
    """

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date.  This parameter is used to isolate intermediate datasets.'
    )
    table_name = luigi.Parameter(
        description='The Vertica table being copied.'
    )

    def snowflake_compliant_schema(self):
        """
        Returns the Vertica schema in the format (column name, column type, nullable?) for the indicated table.
        """

        query = """
        SELECT column_name, data_type FROM columns
        WHERE table_schema='{schema}' AND table_name='{table}'
        ORDER BY ordinal_position
        """.format(
            schema=self.vertica_schema_name,
            table=self.table_name
        )
        results = []
        rows = get_vertica_results(self.vertica_credentials, query)

        for row in rows:
            column_name = row[0]
            field_type = row[1]

            if column_name == 'start':
                column_name = '"{}"'.format(column_name)

            if field_type.lower() in ['long varbinary']:
                log.error('Error Vertica jdbc tool is unable to export field type \'%s\'.  This field will be '
                          'excluded from the extract.', field_type.lower())
                continue
            elif field_type.lower().startswith('long'):
                field_type = field_type.lower().lstrip('long ')
            elif '(40' in field_type.lower():
                field_type = field_type.rsplit('(')[0]

            results.append((column_name, field_type.lower()))

        return results

    @property
    def insert_source_task(self):
        """
        This assumes we have already exported vertica tables to S3 using SqoopImportFromVertica through VerticaSchemaToS3Task
        workflow, so we specify ExternalURL here.
        """
        partition_path_spec = HivePartition('dt', self.date).path_spec
        intermediate_warehouse_path = url_path_join(self.warehouse_path, 'import/vertica/sqoop/')
        url = url_path_join(intermediate_warehouse_path,
                            self.vertica_warehouse_name,
                            self.vertica_schema_name,
                            self.table_name,
                            partition_path_spec) + '/'

        return ExternalURL(url=url)

    @property
    def table(self):
        return self.table_name

    @property
    def file_format_name(self):
        return 'vertica_sqoop_export_format'

    @property
    def columns(self):
        return self.snowflake_compliant_schema()

    @property
    def null_marker(self):
        return "NNULLL"

    @property
    def pattern(self):
        return '.*part-m.*'

    @property
    def field_delimiter(self):
        return r'\x01'


class VerticaSchemaToSnowflakeTask(LoadVerticaTableFromS3ToSnowflakeMixin,
                                   SnowflakeLoadDownstreamMixin,
                                   luigi.WrapperTask):
    """
    A task that copies all the tables in a Vertica schema to S3.

    Reads all tables in a schema and, if they are not listed in the `exclude` parameter, schedules a
    LoadVerticaTableFromS3ToSnowflake task for each table.
    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date.  This parameter is used to isolate intermediate datasets.'
    )
    exclude = luigi.ListParameter(
        default=[],
        description='The Vertica tables that are to be excluded from exporting.'
    )

    def should_exclude_table(self, table_name):
        """
        Determines whether to exclude a table during the import.
        """
        if any(re.match(pattern, table_name) for pattern in self.exclude):  # pylint: disable=not-an-iterable
            return True
        return False

    def requires(self):
        query = "SELECT table_name FROM all_tables WHERE schema_name='{schema_name}' AND table_type='TABLE' " \
                "".format(schema_name=self.vertica_schema_name)
        table_list = [row[0] for row in get_vertica_results(self.vertica_credentials, query)]

        for table_name in table_list:
            if not self.should_exclude_table(table_name):

                yield LoadVerticaTableFromS3ToSnowflake(
                    warehouse_path=self.warehouse_path,
                    date=self.date,
                    overwrite=self.overwrite,
                    credentials=self.credentials,
                    warehouse=self.warehouse,
                    role=self.role,
                    sf_database=self.sf_database,
                    schema=self.schema,
                    table_name=table_name,
                    vertica_schema_name=self.vertica_schema_name,
                    vertica_warehouse_name=self.vertica_warehouse_name,
                    vertica_credentials=self.vertica_credentials,
                )

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
