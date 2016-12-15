"""
Workflow to load the warehouse, this serves as a replacement for pentaho loading.
"""
import logging
import luigi

from edx.analytics.tasks.load_internal_reporting_certificates import LoadInternalReportingCertificatesToWarehouse
from edx.analytics.tasks.load_internal_reporting_country import LoadInternalReportingCountryToWarehouse
from edx.analytics.tasks.load_internal_reporting_course_catalog import LoadInternalReportingCourseCatalogToWarehouse
from edx.analytics.tasks.load_internal_reporting_course_catalog import LoadInternalReportingProgramCourseToWarehouse
from edx.analytics.tasks.load_internal_reporting_user_activity import LoadInternalReportingUserActivityToWarehouse
from edx.analytics.tasks.load_internal_reporting_user_course import LoadUserCourseSummary
from edx.analytics.tasks.load_internal_reporting_user import LoadInternalReportingUserToWarehouse
from edx.analytics.tasks.course_catalog import DailyLoadSubjectsToVerticaTask
from edx.analytics.tasks.vertica_load import VerticaCopyTaskMixin

from edx.analytics.tasks.util.vertica_target import CredentialFileVerticaTarget
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.url import ExternalURL

log = logging.getLogger(__name__)

try:
    import vertica_python
except ImportError:
    log.warn('Unable to import Vertica client libraries')


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


class SchemaManagementTask(VerticaCopyTaskMixin, luigi.Task):
    """
    Base class for running schema management commands on warehouse
    """

    date = luigi.DateParameter()

    roles = luigi.Parameter(
        is_list=True,
        config_path={'section': 'vertica-export', 'name': 'roles'},
    )

    def __init__(self, *args, **kwargs):
        super(SchemaManagementTask, self).__init__(*args, **kwargs)
        self.schema_last = self.schema + '_last'
        self.schema_loading = self.schema + '_loading'
        self.vertica_roles = ','.join(self.roles)

    @property
    def queries(self):
        """
        Provides queries that are needed to run this task.
        """
        raise NotImplementedError

    @property
    def marker_name(self):
        """
        Although we are not writing any data into a table, we still need a marker
        name for the marker table entry so the task can be checked for completeness.
        This should be defined in a derived class.
        """
        raise NotImplementedError

    def requires(self):
        return {
            'credentials': ExternalURL(self.credentials)
        }

    def init_touch(self, connection):
        """
        Clear the relevant rows from the marker table if we are overwriting.
        """
        # This method is the same as appears on vertica_load#L339.
        # However, we do not call cursor.flush_to_query_ready() here as
        # we are not copying any data after this method.
        if self.overwrite:
            # Clear the appropriate rows from the luigi Vertica marker table
            marker_table = self.output().marker_table
            try:
                query = """
                DELETE FROM {marker_schema}.{marker_table} where target_table='{schema}.{target_table}';
                """.format(
                    schema=self.schema,
                    marker_schema=self.marker_schema,
                    marker_table=marker_table,
                    target_table=self.marker_name
                )
                log.debug(query)
                connection.cursor().execute(query)
            except vertica_python.errors.Error as err:
                is_missing_relation = type(err) is vertica_python.errors.MissingRelation
                is_missing_table = 'Sqlstate: 42V01' in err.args[0]
                is_missing_schema = 'Sqlstate: 3F000' in err.args[0]
                if is_missing_relation or is_missing_table or is_missing_schema:
                    # If so, then our query error failed because the schema or table doesn't exist.
                    pass
                else:
                    raise

    def run(self):
        connection = self.output().connect()

        try:
            self.init_touch(connection)
            for query in self.queries:
                log.debug(query)
                connection.cursor().execute(query)

            self.attempted_removal = True
            self.output().touch(connection)
            connection.commit()
        except Exception as exc:
            log.exception("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            raise
        finally:
            connection.close()

    def output(self):
        """
        Returns a VerticaTarget representing the task ran.
        """
        return CredentialFileVerticaTarget(
            credentials_target=self.input()['credentials'],
            table=self.marker_name,
            schema=self.schema,
            update_id=self.update_id(),
            marker_schema=self.marker_schema
        )

    def update_id(self):
        return str(self)


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
