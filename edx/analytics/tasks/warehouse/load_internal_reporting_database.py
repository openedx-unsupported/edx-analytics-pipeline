"""
Loads a mysql database into the warehouse through the pipeline via Sqoop.
"""
import datetime
import logging
import re

import luigi

from edx.analytics.tasks.sqoop import SqoopImportFromMysql
from edx.analytics.tasks.url import url_path_join, ExternalURL
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.mysql_load import get_mysql_query_results
from edx.analytics.tasks.load_warehouse import SchemaManagementTask

log = logging.getLogger(__name__)


class MysqlToVerticaTaskMixin(WarehouseMixin):
    """
    Parameters for importing a mysql database into the warehouse.
    """

    db_credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    database = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'database'}
    )


class LoadMysqlToVerticaTableTask(MysqlToVerticaTaskMixin, VerticaCopyTask):
    """
    Task to import a table from mysql into vertica.
    """

    table_name = luigi.Parameter(
        description='The name of the table.',
    )

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
    )

    def __init__(self, *args, **kwargs):
        super(LoadMysqlToVerticaTableTask, self).__init__(*args, **kwargs)
        self.table_schema = []

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'insert_source': self.insert_source_task,
            }
        return self.required_tasks

    def vertica_compliant_schema(self):
        """Transforms mysql table schema into a vertica compliant schema."""

        if not self.table_schema:
            results = get_mysql_query_results(self.db_credentials, self.database, 'describe {}'.format(self.table_name))
            for result in results:
                field_name = result[0].strip()
                field_type = result[1].strip()
                field_null = result[2].strip()

                types_with_parentheses = ['tinyint', 'smallint', 'int', 'bigint', 'datetime']
                if any(_type in field_type for _type in types_with_parentheses):
                    field_type = field_type.rsplit('(')[0]
                elif field_type == 'longtext':
                    field_type = 'LONG VARCHAR'
                elif field_type == 'double':
                    field_type = 'DOUBLE PRECISION'

                if field_null == "NO":
                    field_type = field_type + " NOT NULL"

                field_name = "\"{}\"".format(field_name)

                self.table_schema.append((field_name, field_type))

        return self.table_schema

    @property
    def copy_delimiter(self):
        """The delimiter in the data to be copied."""
        return "','"

    @property
    def copy_null_sequence(self):
        """The null sequence in the data to be copied."""
        return "'NULL'"

    @property
    def enclosed_by(self):
        return "''''"

    @property
    def insert_source_task(self):
        partition_path_spec = HivePartition('dt', self.date.isoformat()).path_spec
        destination = url_path_join(
            self.warehouse_path,
            "database_import",
            self.database,
            self.table_name,
            partition_path_spec
        ) + '/'
        return SqoopImportFromMysql(
            table_name=self.table_name,
            credentials=self.db_credentials,
            database=self.database,
            destination=destination,
            overwrite=self.overwrite,
            mysql_delimiters=True,
        )

    @property
    def default_columns(self):
        return []

    @property
    def table(self):
        return self.table_name

    @property
    def columns(self):
        return self.vertica_compliant_schema()

    @property
    def auto_primary_key(self):
        return None


class PreImportDatabaseTask(SchemaManagementTask):
    """
    Task needed to run before importing database into warehouse.
    """
    priority = 100

    @property
    def queries(self):
        return [
            "DROP SCHEMA IF EXISTS {schema} CASCADE;".format(schema=self.schema_loading),
            "CREATE SCHEMA IF NOT EXISTS {schema}".format(schema=self.schema_loading),
        ]

    @property
    def marker_name(self):
        return 'pre_database_import_{schema}_{date}'.format(
            schema=self.schema,
            date=self.date.strftime('%Y-%m-%d')
        )


class PostImportDatabaseTask(SchemaManagementTask):
    """
    Task needed to run after importing database into warehouse.
    """
    priority = -100

    @property
    def queries(self):
        return [
            "DROP SCHEMA IF EXISTS {schema} CASCADE;".format(schema=self.schema),
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
        return 'post_database_import_{schema}_{date}'.format(
            schema=self.schema,
            date=self.date.strftime('%Y-%m-%d')
        )


class ImportMysqlToVerticaTask(MysqlToVerticaTaskMixin, luigi.WrapperTask):
    """Provides entry point for importing a mysql database into Vertica."""

    schema = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'schema'},
        description='The schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
    )
    overwrite = luigi.BooleanParameter(
        default=False,
        significant=False,
    )

    exclude = luigi.Parameter(
        is_list=True,
        default=(),
    )

    marker_schema = luigi.Parameter(
        description='The marker schema to which to write the marker table.'
    )

    def __init__(self, *args, **kwargs):
        super(ImportMysqlToVerticaTask, self).__init__(*args, **kwargs)
        self.table_list = []

    def should_exclude_table(self, table_name):
        """Determines whether to exlude a table during the import."""
        if any(re.match(pattern, table_name) for pattern in self.exclude):
            return True
        return False

    def requires(self):
        if not self.table_list:
            results = get_mysql_query_results(self.db_credentials, self.database, 'show tables')
            self.table_list = [result[0].strip() for result in results]

        pre_import_task = PreImportDatabaseTask(
            date=self.date,
            schema=self.schema,
            credentials=self.credentials,
            marker_schema=self.marker_schema,
            overwrite=self.overwrite
        )
        yield pre_import_task

        for table_name in self.table_list:
            if not self.should_exclude_table(table_name):
                yield LoadMysqlToVerticaTableTask(
                    credentials=self.credentials,
                    schema=pre_import_task.schema_loading,
                    db_credentials=self.db_credentials,
                    database=self.database,
                    warehouse_path=self.warehouse_path,
                    table_name=table_name,
                    overwrite=self.overwrite,
                    date=self.date,
                    marker_schema=self.marker_schema,
                )

        yield PostImportDatabaseTask(
            date=self.date,
            schema=self.schema,
            credentials=self.credentials,
            marker_schema=self.marker_schema,
            overwrite=self.overwrite
        )
