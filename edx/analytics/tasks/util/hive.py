"""Various helper utilities that are commonly used when working with Hive"""

import logging
import textwrap

import luigi
from luigi.configuration import get_config
from luigi.contrib.hive import HivePartitionTarget, HiveQueryRunner, HiveQueryTask, HiveTableTarget
from luigi.parameter import BoolParameter, Parameter

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


def hive_database_name():
    """The name of the database where all metadata is being stored in Hive"""
    return get_config().get('hive', 'database', 'default')


def hive_version():
    """
    Returns the version of Hive that is declared in the configuration file. Defaults to 1.0 if it's not specified.

    Returns: A tuple with each index representing a part of the version. For example: version="0.11.0.1" would return
    (0, 11, 0, 1). The 0 indexed integer is the most significant part of the version number.
    """
    version_str = luigi.configuration.get_config().get('hive', 'version', '1.0')
    return tuple([int(x) for x in version_str.split('.')])


def hive_decimal_type(precision, scale):
    """
    Return the appropriate DECIMAL type declaration depending on the Hive version.

    In versions >0.12, the syntax for declaring DECIMAL field types was changed. In Hive >0.12 using the DECIMAL type
    without a precision or scale value defaults to the equivalent of DECIMAL(10, 0). This declaration only supports
    round integers, so any fractional parts of the number are rounded off. In prior versions of Hive they are preserved.
    If we are using an older version of Hive, which does not support the precision and scale arguments, then we should
    use the bare "DECIMAL" declaration. Otherwise, we should include the precision and scale values.

    Args:
        precision: See the Java BigDecimal definition.
        scale: See the Java BigDecimal definition.

    Returns: The string that is used to declare the decimal type. It will either simply be "DECIMAL" or "DECIMAL(p, s)"
    depending on the version of Hive.

    """
    version = hive_version()
    if version[0] == 0 and version[1] < 13:
        return 'DECIMAL'
    else:
        return 'DECIMAL({0},{1})'.format(precision, scale)


class WarehouseMixin(object):
    """Task that is aware of the data warehouse."""

    warehouse_path = luigi.Parameter(
        config_path={'section': 'hive', 'name': 'warehouse_path'},
        description='A URL location of the data warehouse.',
    )

    def hive_partition_path(self, table_name, partition_value, partition_key='dt'):
        """
        Given a table name and partition value return the full URL of the folder for that partition in the warehouse.

        Arguments:
            table_name (str): The name of the hive table.
            partition_value (object): Usually a string specifying the partition in the table. If it is a `date` object
                it will be serialized to a ISO8601 formatted date string. This is a common use case.
            partition_key (str): The partition key. This is usually "dt".
        """
        if hasattr(partition_value, 'isoformat'):
            partition_value = partition_value.isoformat()
        partition = HivePartition(partition_key, partition_value)
        return url_path_join(self.warehouse_path, table_name, partition.path_spec) + '/'


class HiveTableTask(WarehouseMixin, OverwriteOutputMixin, HiveQueryTask):
    """
    Abstract class to import data into a Hive table.

    Currently supports a single partition that represents the version of the table data. This allows us to use a
    consistent location for the table and swap out the data in the tables by simply pointing at different partitions
    within the folder that contain different "versions" of the table data. For example, if a snapshot is taken of an
    RDBMS table, we might store that in a partition with today's date. Any subsequent jobs that need to join against
    that table will continue to use the data snapshot from the beginning of the day (since that is the "live"
    partition). However, the next time a snapshot is taken a new partition is created and loaded and becomes the "live"
    partition that is used in all joins etc.

    Important note: this code currently does *not* clean up old unused partitions, they will just continue to exist
    until they are cleaned up by some external process.
    """

    def query(self):
        # TODO: Figure out how to clean up old data. This just cleans
        # out old metastore info, and doesn't actually remove the table
        # data.

        # Ensure there is exactly one available partition in the table.
        query_format = """
            USE {database_name};
            DROP TABLE IF EXISTS `{table}`;
            CREATE EXTERNAL TABLE `{table}` (
                {col_spec}
            )
            PARTITIONED BY (`{partition.key}` STRING)
            {table_format}
            LOCATION '{location}';
            ALTER TABLE `{table}` ADD PARTITION ({partition.query_spec});
        """

        query = query_format.format(
            database_name=hive_database_name(),
            table=self.table,
            col_spec=','.join(['`{}` {}'.format(name, col_type) for name, col_type in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition=self.partition,
        )

        query = textwrap.dedent(query)

        return query

    @property
    def partition(self):
        """Provides name of Hive database table partition."""
        raise NotImplementedError

    @property
    def partition_location(self):
        """Provides location of Hive database table's partition data."""
        # Make sure that input path ends with a slash, to indicate a directory.
        # (This is necessary for S3 paths that are output from Hadoop jobs.)
        return url_path_join(self.table_location, self.partition.path_spec + '/')

    @property
    def table(self):
        """Provides name of Hive database table."""
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
    def columns(self):
        """
        Provides definition of columns in Hive.

        This should define a list of (name, definition) tuples, where
        the definition defines the Hive type to use. For example,
        ('first_name', 'STRING').

        """
        raise NotImplementedError

    def output(self):
        return HivePartitionTarget(
            self.table, self.partition.as_dict(), database=hive_database_name(), fail_missing_table=False
        )

    def job_runner(self):
        return OverwriteAwareHiveQueryRunner()


# TODO: rename this to HiveTableTask once the old one is removed
class BareHiveTableTask(WarehouseMixin, OverwriteOutputMixin, HiveQueryTask):
    """
    Abstract class that represents the metadata associated with a Hive table.

    Note that all this task does is ensure that the table is created, it does not populate it with any data, simply runs
    the DDL commands to create the table.

    Also note that it will not change the schema of the table if it already exists unless the overwrite parameter is
    set to True.
    """

    def query(self):
        partition_clause = ''
        if self.partition_by:
            partition_clause = 'PARTITIONED BY (`{partition_by}` STRING)'.format(partition_by=self.partition_by)

        if self.overwrite:
            drop_on_overwrite = 'DROP TABLE IF EXISTS `{table}`;'.format(table=self.table)
        else:
            drop_on_overwrite = ''

        query_format = """
            USE {database_name};
            {drop_on_overwrite}
            CREATE EXTERNAL TABLE IF NOT EXISTS `{table}` (
                {col_spec}
            )
            {partition_clause}
            {table_format}
            LOCATION '{location}';
        """

        query = query_format.format(
            database_name=hive_database_name(),
            table=self.table,
            col_spec=','.join(['`{}` {}'.format(name, col_type) for name, col_type in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition_clause=partition_clause,
            drop_on_overwrite=drop_on_overwrite
        )

        query = textwrap.dedent(query)

        return query

    @property
    def partition_by(self):
        """The partitioning key name. Specify None to create a table that is not partitioned."""
        raise NotImplementedError

    @property
    def table(self):
        """Provides name of Hive database table."""
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
    def columns(self):
        """
        Provides definition of columns in Hive.

        This should define a list of (name, definition) tuples, where
        the definition defines the Hive type to use. For example,
        ('first_name', 'STRING').

        """
        raise NotImplementedError

    def output(self):
        return HiveTableTarget(
            self.table, database=hive_database_name()
        )

    def job_runner(self):
        return OverwriteAwareHiveQueryRunner()

    def remove_output_on_overwrite(self):
        # Note that the query takes care of actually removing the old partition.
        if self.overwrite:
            self.attempted_removal = True


class HivePartitionTask(WarehouseMixin, OverwriteOutputMixin, HiveQueryTask):
    """
    Abstract class that represents the metadata associated with a partition in a Hive table.

    Note that all this task does is ensure that the partition is created, it does not populate it with any data, simply
    runs the DDL commands to create the partition.
    """

    partition_value = luigi.Parameter()

    def query(self):
        if self.overwrite:
            drop_on_overwrite = 'ALTER TABLE `{table}` DROP IF EXISTS PARTITION ({partition.query_spec});'.format(
                table=self.hive_table_task.table,
                partition=self.partition
            )
        else:
            drop_on_overwrite = ''

        query_format = """
            USE {database_name};
            {drop_on_overwrite}
            ALTER TABLE `{table}` ADD IF NOT EXISTS PARTITION ({partition.query_spec});
        """

        query = query_format.format(
            database_name=hive_database_name(),
            table=self.hive_table_task.table,
            partition=self.partition,
            drop_on_overwrite=drop_on_overwrite
        )

        return textwrap.dedent(query)

    @property
    def hive_table_task(self):
        """Returns a reference to the task that represents the table that this partition is part of."""
        raise NotImplementedError

    @property
    def data_task(self):
        """Returns a luigi task that is used to insert real data into this partition."""
        return None

    @property
    def partition(self):
        """Returns a HivePartition object that represents the partition."""
        return HivePartition(self.hive_table_task.partition_by, self.partition_value)

    @property
    def partition_location(self):
        """Returns the full URL of the partition. This allows data to be written to the partition by external systems"""
        return url_path_join(self.hive_table_task.table_location, self.partition.path_spec + '/')

    def requires(self):
        if self.data_task is not None:
            yield self.data_task
        yield self.hive_table_task

    def output(self):
        # Ugh.  A change in Luigi 1.0.22 (after our 1.0.17 fork) resulted in a change in ApacheHiveCommandClient.table_exists()
        # behavior, so that it throws an exception when checking for a specific partition when the table doesn't exist.
        # This means that HivePartitionTarget.exists() will fail, where before it succeeded even if the table did not exist.
        # So change fail_missing_table=False here.  There is no reason for it anyway.
        return HivePartitionTarget(
            self.hive_table_task.table, self.partition.as_dict(), database=hive_database_name(), fail_missing_table=False
        )

    def job_runner(self):
        return OverwriteAwareHiveQueryRunner()

    def remove_output_on_overwrite(self):
        # Note that the query takes care of actually removing the old partition.
        if self.overwrite:
            self.attempted_removal = True


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
        return "`{key}`='{value}'".format(
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


class HiveTableFromQueryTask(HiveTableTask):  # pylint: disable=abstract-method
    """Creates a hive table from the results of a hive query."""

    def query(self):
        create_table_statements = super(HiveTableFromQueryTask, self).query()
        full_insert_query = """
            INSERT INTO TABLE `{table}`
            PARTITION ({partition.query_spec})
            {insert_query}
        """.format(
            table=self.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),  # pylint: disable=no-member
        )

        return create_table_statements + textwrap.dedent(full_insert_query)

    def output(self):
        return get_target_from_url(self.partition_location.rstrip('/') + '/')

    @property
    def insert_query(self):
        """Hive query to run."""
        raise NotImplementedError


class OverwriteAwareHiveQueryDataTask(WarehouseMixin, OverwriteOutputMixin, HiveQueryTask):
    """
    A generalized Data task whose output is a hive table populated from a hive query.
    """

    overwrite_target_partition = BoolParameter(
        significant=False,
        description='Overwrite the target partition, deleting any existing data.  This will not impact other '
                    'partitions.  Do not use with incrementally built partitions.',
        default=True
    )

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table.  This insert_query()
        is used as part of the query() function below."""
        raise NotImplementedError

    @property
    def hive_partition_task(self):
        """The HivePartitionTask that needs to be generated."""
        raise NotImplementedError

    @property
    def data_modification_sql_text(self):
        """Returns the appropriate SQL text for the chosen overwrite_target_partition strategy."""
        if self.overwrite_target_partition:
            return "OVERWRITE"
        else:
            return "INTO"

    def query(self):  # pragma: no cover
        full_insert_query = """
                    USE {database_name};
                    INSERT {into_or_overwrite} TABLE {table}
                    PARTITION ({partition.query_spec})
                    {insert_query};
                    """.format(database_name=hive_database_name(),
                               into_or_overwrite=self.data_modification_sql_text,
                               table=self.partition_task.hive_table_task.table,
                               partition=self.partition,
                               insert_query=self.insert_query.strip(),  # pylint: disable=no-member
                               )
        return textwrap.dedent(full_insert_query)

    @property
    def partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        if not hasattr(self, '_partition_task'):
            self._partition_task = self.hive_partition_task
        return self._partition_task

    @property
    def partition(self):  # pragma: no cover
        """A shorthand for the partition information on the upstream partition task."""
        return self.partition_task.partition  # pylint: disable=no-member

    def output(self):  # pragma: no cover
        output_root = url_path_join(self.warehouse_path,
                                    self.partition_task.hive_table_task.table,
                                    self.partition.path_spec + '/')
        return get_target_from_url(output_root, marker=True)

    def on_success(self):  # pragma: no cover
        """Overload the success method to touch the _SUCCESS file.  Any class that uses a separate Marker file from the
        data file will need to override the base on_success() call to create this marker."""
        self.output().touch_marker()

    def run(self):
        self.remove_output_on_overwrite()
        return super(OverwriteAwareHiveQueryDataTask, self).run()

    def requires(self):  # pragma: no cover
        for requirement in super(OverwriteAwareHiveQueryDataTask, self).requires():
            yield requirement
        yield self.partition_task
