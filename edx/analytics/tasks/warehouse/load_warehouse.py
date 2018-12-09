"""
Workflow to load the warehouse, this serves as a replacement for pentaho loading.
"""
import logging

import luigi

from edx.analytics.tasks.common.vertica_load import SchemaManagementTask
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL
from edx.analytics.tasks.util.vertica_target import CredentialFileVerticaTarget
from edx.analytics.tasks.warehouse.course_catalog import DailyLoadSubjectsToVerticaTask
from edx.analytics.tasks.warehouse.load_internal_reporting_certificates import LoadInternalReportingCertificatesToWarehouse
from edx.analytics.tasks.warehouse.load_internal_reporting_country import LoadInternalReportingCountryToWarehouse
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import LoadInternalReportingCourseCatalogToWarehouse
from edx.analytics.tasks.warehouse.load_internal_reporting_user_activity import LoadInternalReportingUserActivityToWarehouse
from edx.analytics.tasks.warehouse.load_internal_reporting_user_course import LoadUserCourseSummary
from edx.analytics.tasks.warehouse.load_internal_reporting_user import LoadInternalReportingUserToWarehouse


log = logging.getLogger(__name__)


class WarehouseWorkflowMixin(WarehouseMixin):
    """
    Parameters for running warehouse workflow.
    """

    date = luigi.DateParameter()

    n_reduce_tasks = luigi.Parameter()

    # We are not using VerticaCopyTaskMixin as OverwriteOutputMixin changes the complete() method behavior.
    schema = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'schema'},
        description='The schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )

    overwrite = luigi.BooleanParameter(default=False, significant=False)

    # We rename the schema after warehouse loading step. This causes
    # Luigi to think that tasks are not complete as it cannot find the
    # marker table entries. Using a different schema for the marker table
    # solves this issue.
    marker_schema = luigi.Parameter()


class PreLoadWarehouseTask(SchemaManagementTask):
    """
    Task needed to run before loading data into warehouse.
    """
    priority = 100

    @property
    def queries(self):
        return [
            "DROP SCHEMA IF EXISTS {schema} CASCADE;".format(schema=self.schema_last),
            "DROP SCHEMA IF EXISTS {schema} CASCADE;".format(schema=self.schema_loading),
            "CREATE SCHEMA IF NOT EXISTS {schema}".format(schema=self.schema_loading),
        ]

    @property
    def marker_name(self):
        return 'pre_load_' + self.date.strftime('%Y-%m-%d')


class LoadWarehouseTask(WarehouseWorkflowMixin, luigi.WrapperTask):
    """Runs the tasks needed to load warehouse."""

    def requires(self):
        kwargs = {
            'schema': self.schema + '_loading',
            'marker_schema': self.marker_schema,
            'credentials': self.credentials,
            'overwrite': self.overwrite,
            'warehouse_path': self.warehouse_path,
        }

        yield PreLoadWarehouseTask(
            date=self.date,
            schema=self.schema,
            credentials=self.credentials,
            marker_schema=self.marker_schema,
            overwrite=self.overwrite
        )
        yield (
            LoadInternalReportingCertificatesToWarehouse(
                date=self.date,
                **kwargs
            ),
            LoadInternalReportingCountryToWarehouse(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            LoadInternalReportingCourseCatalogToWarehouse(
                date=self.date,
                **kwargs
            ),
            LoadUserCourseSummary(
                date=self.date,
                **kwargs
            ),
            LoadInternalReportingUserActivityToWarehouse(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            LoadInternalReportingUserToWarehouse(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            DailyLoadSubjectsToVerticaTask(
                date=self.date,
                **kwargs
            ),
        )


class PostLoadWarehouseTask(SchemaManagementTask):
    """
    Task needed to run after loading data into warehouse.
    """
    n_reduce_tasks = luigi.Parameter()

    priority = -100

    def requires(self):
        return {
            'source': LoadWarehouseTask(
                date=self.date,
                schema=self.schema,
                credentials=self.credentials,
                marker_schema=self.marker_schema,
                overwrite=self.overwrite,
                n_reduce_tasks=self.n_reduce_tasks
            ),
            'credentials': ExternalURL(self.credentials)
        }

    @property
    def queries(self):
        return [
            "ALTER SCHEMA {schema} RENAME TO {schema_last};".format(schema=self.schema, schema_last=self.schema_last),
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
        return 'post_load_' + self.date.strftime('%Y-%m-%d')

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        tables = [
            'd_user_course_certificate',
            'd_course',
            'd_country',
            'd_user_course',
            'f_user_activity',
            'd_user',
            'd_course_subjects'
        ]
        for table in tables:
            query = "SELECT 1 FROM {schema_loading}.{table} LIMIT 1".format(
                schema_loading=self.schema_loading,
                table=table
            )
            log.debug(query)
            cursor.execute(query)
            row = cursor.fetchone()
            if row is None:
                connection.close()
                raise Exception('Failed to validate table: {table}'.format(table=table))

        connection.close()

        super(PostLoadWarehouseTask, self).run()


class LoadWarehouseWorkflow(WarehouseWorkflowMixin, luigi.WrapperTask):
    """
    Provides entry point for loading data into warehouse.
    """

    def requires(self):
        return PostLoadWarehouseTask(
            n_reduce_tasks=self.n_reduce_tasks,
            date=self.date,
            schema=self.schema,
            credentials=self.credentials,
            marker_schema=self.marker_schema,
            overwrite=self.overwrite
        )
