"""
Tasks to load a Vertica schema from S3 into BigQuery.
"""

from __future__ import absolute_import

import logging

import luigi
from google.cloud.bigquery import SchemaField

from edx.analytics.tasks.common.bigquery_load import BigQueryLoadTask
from edx.analytics.tasks.common.vertica_export import (
    VerticaSchemaExportMixin, VerticaTableExportMixin, VerticaTableFromS3Mixin
)
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.url import ExternalURL

log = logging.getLogger(__name__)


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


class LoadVerticaTableFromS3ToBigQueryTask(VerticaTableExportMixin, VerticaTableFromS3Mixin, BigQueryLoadTask):
    """Copies one table from S3 through GCP into BigQuery, after having been dumped from Vertica to S3."""

    def __init__(self, *args, **kwargs):
        super(LoadVerticaTableFromS3ToBigQueryTask, self).__init__(*args, **kwargs)
        self._bigquery_compliant_schema = None

    @property
    def table(self):
        return self.table_name

    @property
    def table_description(self):
        return "Copy of Vertica table {}.{} on {}".format(
            self.vertica_schema_name, self.table_name, self.date
        )

    @property
    def table_friendly_name(self):
        return '{} from {}'.format(self.table_name, self.vertica_schema_name)

    @property
    def field_delimiter(self):
        return self.sqoop_fields_terminated_by

    @property
    def null_marker(self):
        return self.sqoop_null_string

    @property
    def schema(self):
        """
        The BigQuery compliant schema.

        Returns a list of SchemaField objects, which contain the name, type and mode of the field.
        """
        if self._bigquery_compliant_schema is None:
            res = []
            for field_name, vertica_field_type, nullable in self.vertica_table_schema:
                # Above is analogous to "column_name, data_type, is_nullable" in Vertica.
                # In BigQuery, strip off all sizes before remapping, because BigQuery doesn't want size information.
                vertica_field_type = vertica_field_type.split('(')[0]
                if vertica_field_type in VERTICA_TO_BIGQUERY_FIELD_MAPPING:
                    res.append(
                        SchemaField(
                            field_name,
                            VERTICA_TO_BIGQUERY_FIELD_MAPPING[vertica_field_type],
                            mode='NULLABLE' if nullable else 'REQUIRED'
                        )
                    )
                else:
                    raise RuntimeError('Error for field {field}: Vertica type {type} does not have a mapping to '
                                       'BigQuery'.format(field=field_name, type=vertica_field_type))
            self._bigquery_compliant_schema = res
        return self._bigquery_compliant_schema

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.s3_location_for_table)


@workflow_entry_point
class LoadVerticaSchemaFromS3ToBigQueryTask(VerticaSchemaExportMixin, luigi.WrapperTask):
    """
    A task that copies all the tables in a Vertica schema to BigQuery from where they were written in S3.

    Reads all tables in a schema and, if they are not listed in the `exclude` parameter, schedules a
    LoadVerticaTableFromS3ToBigQueryTask task for each table.
    """
    overwrite = luigi.BoolParameter(
        default=False,
        significant=False,
        description='Indicates if the target data sources should be removed prior to generating.'
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

    def requires(self):
        yield ExternalURL(url=self.vertica_credentials)

        yield ExternalURL(url=self.gcp_credentials)

        if self.bigquery_dataset is None:
            self.bigquery_dataset = self.vertica_schema_name

        for table_name in self.get_table_list_for_schema():
            yield LoadVerticaTableFromS3ToBigQueryTask(
                date=self.date,
                overwrite=self.overwrite,
                intermediate_warehouse_path=self.intermediate_warehouse_path,
                dataset_id=self.bigquery_dataset,
                credentials=self.gcp_credentials,
                max_bad_records=self.max_bad_records,
                table_name=table_name,
                vertica_schema_name=self.vertica_schema_name,
                vertica_warehouse_name=self.vertica_warehouse_name,
                vertica_credentials=self.vertica_credentials,
            )
