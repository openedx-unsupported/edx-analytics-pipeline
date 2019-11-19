"""
Workflow to load the warehouse, this serves as a replacement for pentaho loading.
"""
from __future__ import absolute_import

import logging

import luigi

from edx.analytics.tasks.common.vertica_load import SchemaManagementTask
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL
from edx.analytics.tasks.warehouse.load_internal_reporting_certificates import (
    LoadInternalReportingCertificatesToWarehouse
)
from edx.analytics.tasks.warehouse.load_internal_reporting_country import LoadInternalReportingCountryToWarehouse
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import (
    LoadInternalReportingCourseCatalogToWarehouse
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_structure import (
    LoadInternalReportingCourseStructureToWarehouse
)
from edx.analytics.tasks.warehouse.load_internal_reporting_user import LoadInternalReportingUserToWarehouse
from edx.analytics.tasks.warehouse.load_internal_reporting_user_activity import (
    LoadInternalReportingUserActivityToWarehouse
)
from edx.analytics.tasks.warehouse.load_internal_reporting_user_course import LoadUserCourseSummary

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

    overwrite = luigi.BoolParameter(default=False, significant=False)

    # We rename the schema after warehouse loading step. This causes
    # Luigi to think that tasks are not complete as it cannot find the
    # marker table entries. Using a different schema for the marker table
    # solves this issue.
    marker_schema = luigi.Parameter()


class PreLoadWarehouseTask(SchemaManagementTask):
    """
    Task needed to run before loading data into warehouse.
    """

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
        yield PreLoadWarehouseTask(
            date=self.date,
            schema=self.schema,
            credentials=self.credentials,
            marker_schema=self.marker_schema,
            overwrite=self.overwrite
        )

    def __init__(self, *args, **kwargs):
        super(LoadWarehouseTask, self).__init__(*args, **kwargs)
        self.is_complete = False

        kwargs = {
            'schema': self.schema + '_loading',
            'marker_schema': self.marker_schema,
            'credentials': self.credentials,
            'overwrite': self.overwrite,
            'warehouse_path': self.warehouse_path,
        }
        self.tasks_to_run = [
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
            LoadInternalReportingCourseStructureToWarehouse(
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
        ]

    def run(self):
        # This is called again after each yield that provides a dynamic dependency.
        # Because overwrite might be set on the tasks, make sure that each is called only once.
        if self.tasks_to_run:
            yield self.tasks_to_run.pop()

        self.is_complete = True

    def complete(self):
        return self.is_complete


class PostLoadWarehouseTask(WarehouseMixin, SchemaManagementTask):
    """
    Task needed to run after loading data into warehouse.
    """
    n_reduce_tasks = luigi.Parameter()

    def requires(self):
        return {
            'source': LoadWarehouseTask(
                date=self.date,
                schema=self.schema,
                credentials=self.credentials,
                marker_schema=self.marker_schema,
                overwrite=self.overwrite,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
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
            'd_course_subjects',
            'd_course_seat',
            'd_program_course',
            'course_structure'
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

            # run analyze statistics function on each table
            query = "SELECT ANALYZE_STATISTICS('{schema_loading}.{table}');".format(
                schema_loading=self.schema_loading,
                table=table
            )
            log.debug(query)
            cursor.execute(query)
            row = cursor.fetchone()
            if row is None or row[0] != 0:
                # only 0 as response from ANALYZE_STATISTICS means success
                connection.close()
                raise Exception('Failed to run analyze_statistics on : {table}'.format(table=table))

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
            overwrite=self.overwrite,
            warehouse_path=self.warehouse_path,
        )
