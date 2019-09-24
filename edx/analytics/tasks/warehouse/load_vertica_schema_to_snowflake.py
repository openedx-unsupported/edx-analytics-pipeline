"""
Tasks to load a Vertica schema into Snowflake.
"""

import logging

import luigi

from edx.analytics.tasks.common.snowflake_load import SnowflakeLoadDownstreamMixin, SnowflakeLoadFromHiveTSVTask
from edx.analytics.tasks.common.vertica_export import LoadVerticaTableFromS3Mixin, VerticaSchemaExportMixin, VerticaTableExportMixin
from edx.analytics.tasks.util.url import ExternalURL


log = logging.getLogger(__name__)


class LoadVerticaTableFromS3ToSnowflakeTask(VerticaTableExportMixin, VerticaTableFromS3Mixin, SnowflakeLoadFromHiveTSVTask):
    """
    Task to load a vertica table from S3 into Snowflake.
    """

    def snowflake_compliant_schema(self):
        """
        Returns the Snowflake schema in the format (column name, column type) for the indicated table.

        Information about "nullable" or required fields is not included.
        """
        for column_name, field_type, _ in self.vertica_table_schema:
            if column_name == 'start':
                column_name = '"{}"'.format(column_name)
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
        return ExternalURL(url=self.s3_location_for_table)

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
        return self.sqoop_null_string

    @property
    def pattern(self):
        return '.*part-m.*'

    @property
    def field_delimiter(self):
        return self.sqoop_fields_terminated_by


class LoadVerticaSchemaFromS3ToSnowflakeTask(VerticaSchemaExportMixin, VerticaTableFromS3Mixin, SnowflakeLoadDownstreamMixin, luigi.WrapperTask):
    """
    A task that loads into Snowflake all the tables in S3 dumped from a Vertica schema.

    Reads all tables in a schema and, if they are not listed in the `exclude` parameter, schedules a
    LoadVerticaTableFromS3ToSnowflake task for each table.
    """

    def requires(self):
        yield ExternalURL(url=self.vertica_credentials)

        for table_name in self.get_table_list_for_schema():
            yield LoadVerticaTableFromS3ToSnowflakeTask(
                date=self.date,
                overwrite=self.overwrite,
                intermediate_warehouse_path=self.intermediate_warehouse_path,
                credentials=self.credentials,
                warehouse=self.warehouse,
                role=self.role,
                sf_database=self.sf_database,
                schema=self.schema,
                table_name=table_name,
                vertica_schema_name=self.vertica_schema_name,
                vertica_warehouse_name=self.vertica_warehouse_name,
                vertica_credentials=self.vertica_credentials,
                sqoop_null_string=self.sqoop_null_string,
                sqoop_fields_terminated_by=self.sqoop_fields_terminated_by,
            )

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
