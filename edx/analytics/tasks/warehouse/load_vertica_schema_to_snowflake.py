"""
Tasks to load a Vertica schema from S3 into Snowflake.
"""

import json
import logging
import re

import luigi

from edx.analytics.tasks.common.snowflake_load import SnowflakeLoadDownstreamMixin, SnowflakeLoadFromHiveTSVTask
from edx.analytics.tasks.common.sqoop import METADATA_FILENAME
from edx.analytics.tasks.common.vertica_export import (
    VerticaSchemaExportMixin, VerticaTableExportMixin, VerticaTableFromS3Mixin
)
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class LoadVerticaTableFromS3ToSnowflakeTask(VerticaTableExportMixin, VerticaTableFromS3Mixin, SnowflakeLoadFromHiveTSVTask):
    """
    Task to load a Vertica table from S3 into Snowflake.
    """

    def snowflake_compliant_schema(self):
        """
        Returns the Snowflake schema in the format (column name, column type) for the indicated table.

        Information about "nullable" or required fields is not included.
        """
        results = []
        for column_name, field_type, _ in self.vertica_table_schema:
            if column_name == 'start' or " " in column_name:
                column_name = '"{}"'.format(column_name)

            if field_type.startswith('long '):
                field_type = field_type.lstrip('long ')
            elif 'numeric(' in field_type:
                # Snowflake only handles numeric precision up to 38. This regex should find either 1 or 2 numbers
                # ex: numeric(52) or numeric(58, 4). First number is precision, second is scale.
                precision_and_scale = re.findall(r'[0-9]+', field_type)
                if int(precision_and_scale[0]) > 38:
                    # If it has scale, try to preserve that
                    if len(precision_and_scale) == 2:
                        field_type = 'numeric(38,{})'.format(precision_and_scale[1])
                    else:
                        field_type = 'numeric(38)'
            elif field_type == 'uuid':
                # Snowflake has no uuid type, but Vertica's is just a 36 character string
                field_type = 'varchar(36)'

            results.append((column_name, field_type))

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
                scratch_schema=self.scratch_schema,
                run_id=self.run_id,
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


# class LoadVerticaTableFromS3WithMetadataToSnowflakeTask(VerticaTableExportMixin, VerticaTableFromS3Mixin, SnowflakeLoadFromHiveTSVTask):
# We don't need VerticaTableFromS3Mixin, because we will get the information from the metadata file.
class LoadVerticaTableFromS3WithMetadataToSnowflakeTask(VerticaTableExportMixin, SnowflakeLoadFromHiveTSVTask):
    """
    Task to load a Vertica table from S3 into Snowflake, but using a metadata file instead of Vertica.
    """
    # Make sure we don't use this at the table level:
    vertica_credentials = None

    def __init__(self, *args, **kwargs):
        super(LoadVerticaTableFromS3WithMetadataToSnowflakeTask, self).__init__(*args, **kwargs)
        metadata_target = self._get_metadata_target()
        with metadata_target.open('r') as metadata_file:
            self.metadata = json.load(metadata_file)

    def _get_metadata_target(self):
        """Returns target for metadata file from the given dump."""
        # find the .metadata file in the source directory.
        metadata_path = url_path_join(self.s3_location_for_table, METADATA_FILENAME)
        return get_target_from_url(metadata_path)

    @property
    def vertica_table_schema(self):
        # Override the default property.
        return self.metadata['additional_metadata']['table_schema']

    def snowflake_compliant_schema(self):
        """
        Returns the Snowflake schema in the format (column name, column type) for the indicated table.

        Information about "nullable" or required fields is not included.
        """
        results = []
        for column_name, field_type, _ in self.vertica_table_schema:
            if column_name == 'start' or " " in column_name:
                column_name = '"{}"'.format(column_name)

            if field_type.startswith('long '):
                field_type = field_type.lstrip('long ')
            elif 'numeric(' in field_type:
                # Snowflake only handles numeric precision up to 38. This regex should find either 1 or 2 numbers
                # ex: numeric(52) or numeric(58, 4). First number is precision, second is scale.
                precision_and_scale = re.findall(r'[0-9]+', field_type)
                if int(precision_and_scale[0]) > 38:
                    # If it has scale, try to preserve that
                    if len(precision_and_scale) == 2:
                        field_type = 'numeric(38,{})'.format(precision_and_scale[1])
                    else:
                        field_type = 'numeric(38)'
            elif field_type == 'uuid':
                # Snowflake has no uuid type, but Vertica's is just a 36 character string
                field_type = 'varchar(36)'

            results.append((column_name, field_type))

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
        # We could check for "source_database_type", and automatically set the format based on that.
        # self.metadata['source_database_type'] == 'vertica'
        return 'vertica_sqoop_export_format'

    @property
    def columns(self):
        return self.snowflake_compliant_schema()

    @property
    def null_marker(self):
        # return self.sqoop_null_string
        return self.metadata['format']['null_string']

    @property
    def pattern(self):
        return '.*part-m.*'

    @property
    def field_delimiter(self):
        # return self.sqoop_fields_terminated_by
        return self.metadata['format']['fields_terminated_by']


class LoadVerticaSchemaFromS3WithMetadataToSnowflakeTask(VerticaSchemaExportMixin, VerticaTableFromS3Mixin, SnowflakeLoadDownstreamMixin, luigi.WrapperTask):
    """
    A task that loads into Snowflake all the tables in S3 dumped from a Vertica schema.

    Reads all tables in a schema and, if they are not listed in the `exclude` parameter, schedules a
    LoadVerticaTableFromS3ToSnowflake task for each table.
    """

    def requires(self):
        # Should no longer need this....
        yield ExternalURL(url=self.vertica_credentials)

        # TODO: instead of getting the table list from Vertica, get it instead from S3 (somehow).
        # unfortunately it's output is not organized by date and then table, but rather by table and then date.
        # So it would take some digging.  Or another metadata file representing the schema-level dump.
        # In which case, the complete method of the to-S3 schema dump would be the table list.
        for table_name in self.get_table_list_for_schema():
            yield LoadVerticaTableFromS3WithMetadataToSnowflakeTask(
                date=self.date,
                overwrite=self.overwrite,
                intermediate_warehouse_path=self.intermediate_warehouse_path,
                credentials=self.credentials,
                warehouse=self.warehouse,
                role=self.role,
                sf_database=self.sf_database,
                schema=self.schema,
                scratch_schema=self.scratch_schema,
                run_id=self.run_id,
                table_name=table_name,
                vertica_schema_name=self.vertica_schema_name,
                vertica_warehouse_name=self.vertica_warehouse_name,
                # vertica_credentials=self.vertica_credentials,
                # sqoop_null_string=self.sqoop_null_string,
                # sqoop_fields_terminated_by=self.sqoop_fields_terminated_by,
            )

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
