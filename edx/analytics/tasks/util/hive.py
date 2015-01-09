"""Various helper utilities that are commonly used when working with Hive"""

import logging
import textwrap

import luigi
from luigi.configuration import get_config
from luigi.hive import HiveQueryTask, HivePartitionTarget, HiveQueryRunner, HiveTableTarget
from luigi.parameter import Parameter

from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin


log = logging.getLogger(__name__)


def hive_database_name():
    """The name of the database where all metadata is being stored in Hive"""
    return get_config().get('hive', 'database', 'default')


def hive_emr_version():
    return get_config().get('hive', 'emr_version', None)


class WarehouseMixin(object):
    """Task that is aware of the data warehouse."""

    warehouse_path = luigi.Parameter(
        default_from_config={'section': 'hive', 'name': 'warehouse_path'}
    )


class HiveTableTask(WarehouseMixin, OverwriteOutputMixin, HiveQueryTask):

    table_override = luigi.Parameter(default=None)
    columns_override = luigi.Parameter(default=None, is_list=True)

    def query(self):
        # TODO: Figure out how to clean up old data. This just cleans
        # out old metastore info, and doesn't actually remove the table
        # data.

        partition_recover_command = ''
        if self.recover_partitions:
            if hive_emr_version() is not None:
                partition_recover_command = "ALTER TABLE {table} RECOVER PARTITIONS;"
            else:
                partition_recover_command = "MSCK REPAIR TABLE {table};"

        query_format = """
            USE {database_name};
            DROP TABLE IF EXISTS {table};
            CREATE EXTERNAL TABLE {table} (
                {col_spec}
            )
            PARTITIONED BY ({partition_key} STRING)
            {table_format}
            LOCATION '{location}';
        """ + partition_recover_command

        query = query_format.format(
            database_name=hive_database_name(),
            table=self.table,
            col_spec=','.join([' '.join(c) for c in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition_key=self.partition_key,
            recover_partitions=partition_recover_command,
        )

        query = textwrap.dedent(query)

        return query

    @property
    def partition_key(self):
        """Provides name of Hive database table partition."""
        return 'dt'

    @property
    def table(self):
        """Provides name of Hive database table."""
        if self.table_override is not None:
            return self.table_override
        else:
            raise NotImplementedError

    @property
    def table_format(self):
        """Provides format of Hive database table's data."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"

    @property
    def table_location(self):
        """Provides root location of Hive database table's data."""
        return url_path_join(self.warehouse_path, self.table) + '/'

    @property
    def recover_partitions(self):
        return True

    @property
    def columns(self):
        """
        Provides definition of columns in Hive.

        This should define a list of (name, definition) tuples, where
        the definition defines the Hive type to use. For example,
        ('first_name', 'STRING').

        """
        if self.columns_override is not None:
            return self.columns_override
        else:
            raise NotImplementedError

    def output(self):
        return HiveTableTarget(
            self.table, database=hive_database_name()
        )

    def job_runner(self):
        return OverwriteAwareHiveQueryRunner()


class HivePartitionTask(WarehouseMixin, OverwriteOutputMixin, HiveQueryTask):

    def query(self):
        return "ALTER TABLE {table} ADD IF NOT EXISTS PARTITION ({partition.query_spec});".format(
            table=self.hive_table_task.table,
            partition=self.partition,
        )

    @property
    def hive_table_task(self):
        raise NotImplementedError

    def requires(self):
        return self.hive_table_task

    @property
    def partition_value(self):
        raise NotImplementedError

    @property
    def partition(self):
        return HivePartition(self.hive_table_task.partition_key, self.partition_value)

    @property
    def partition_location(self):
        """Provides location of Hive database table's partition data."""
        # Make sure that input path ends with a slash, to indicate a directory.
        # (This is necessary for S3 paths that are output from Hadoop jobs.)
        return url_path_join(self.hive_table_task.table_location, self.partition.path_spec + '/')

    def output(self):
        return HivePartitionTarget(
            self.hive_table_task.table,
            self.partition.as_dict(),
            database=hive_database_name(),
            fail_missing_table=False
        )

    def job_runner(self):
        return OverwriteAwareHiveQueryRunner()


class OverwriteAwareHiveQueryRunner(HiveQueryRunner):
    """A custom hive query runner that logs the query being executed and is aware of the "overwrite" option."""

    def run_job(self, job):
        if hasattr(job, 'remove_output_on_overwrite'):
            job.remove_output_on_overwrite()

        log.debug('Executing hive query: %s', job.query())
        return super(OverwriteAwareHiveQueryRunner, self).run_job(job)


class HivePartition(object):
    """
    Represents a particular partition in Hive.

    Can produce strings in the formats that are used to represent it in queries, file paths etc.
    """

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def as_dict(self):
        """This is the format that luigi uses internally to represent partitions"""
        return {self.key: self.value}

    @property
    def query_spec(self):
        """This format is used when a partition needs to be referred to in a query"""
        return "{key}='{value}'".format(
            key=self.key,
            value=self.value,
        )

    @property
    def path_spec(self):
        """This is the format that should be used when dealing with a partition-specific file path"""
        return "{key}={value}".format(
            key=self.key,
            value=self.value,
        )

    def __str__(self):
        return self.path_spec


class HivePartitionParameter(Parameter):
    """Allows partitions to be specified on the command line."""

    def parse(self, serialized_param):
        parts = serialized_param.split('=')
        return HivePartition(parts[0], parts[1])


class HivePartitionFromQueryTask(HivePartitionTask):  # pylint: disable=abstract-method
    """Creates a hive table from the results of a hive query."""

    def query(self):
        create_table_statements = super(HivePartitionFromQueryTask, self).query()
        full_insert_query = """
            INSERT INTO TABLE {table}
            PARTITION ({partition.query_spec})
            {insert_query}
        """.format(
            table=self.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),  # pylint: disable=no-member
        )

        return create_table_statements + textwrap.dedent(full_insert_query)

    def output(self):
        return get_target_from_url(self.partition_location)

    @property
    def insert_query(self):
        """Hive query to run."""
        raise NotImplementedError


class HivePartitionFromParameterQueryTask(HivePartitionFromQueryTask):  # pylint: disable=abstract-method
    """Creates a hive table from the results of a hive query, given parameters instead of properties."""

    insert_query = luigi.Parameter()
    table = luigi.Parameter()
    columns = luigi.Parameter(is_list=True)

    @property
    def hive_table_task(self):
        return HiveTableTask(
            table_override=self.table,
            columns_override=self.columns,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )


class HiveQueryToMysqlTask(WarehouseMixin, MysqlInsertTask):
    """Populates a MySQL table with the results of a hive query."""

    overwrite = luigi.BooleanParameter(default=True)  # Overwrite the MySQL data?
    hive_overwrite = luigi.BooleanParameter(default=False)

    SQL_TO_HIVE_TYPE = {
        'varchar': 'STRING',
        'datetime': 'TIMESTAMP',
        'date': 'STRING',
        'integer': 'INT',
        'int': 'INT',
        'double': 'DOUBLE',
        'tinyint': 'TINYINT',
        'longtext': 'STRING',
    }

    @property
    def insert_source_task(self):
        return HivePartitionFromParameterQueryTask(
            warehouse_path=self.warehouse_path,
            insert_query=self.query,
            table=self.table,
            columns=self.hive_columns,
            partition_value=self.partition_value,
            overwrite=self.hive_overwrite,
        )

    def requires(self):
        # MysqlInsertTask customizes requires() somewhat, so don't clobber that logic. Instead allow subclasses to
        # extend the requirements with their own.
        requirements = super(HiveQueryToMysqlTask, self).requires()
        requirements['other_tables'] = self.required_table_tasks
        return requirements

    @property
    def table(self):
        raise NotImplementedError

    @property
    def query(self):
        """Hive query to run."""
        raise NotImplementedError

    @property
    def columns(self):
        raise NotImplementedError

    @property
    def partition_value(self):
        """HivePartition object specifying the partition to store the data in."""
        raise NotImplementedError

    @property
    def required_table_tasks(self):
        """List of tasks that generate any tables needed to run the query."""
        return []

    @property
    def hive_columns(self):
        """Convert MySQL column data types to hive data types and return hive column specs as (name, type) tuples."""
        hive_cols = []
        for column in self.columns:
            column_name, sql_type = column
            raw_sql_type = sql_type.split(' ')[0]
            unparam_sql_type = raw_sql_type.split('(')[0]
            hive_type = self.SQL_TO_HIVE_TYPE[unparam_sql_type.lower()]

            hive_cols.append((column_name, hive_type))

        return hive_cols
