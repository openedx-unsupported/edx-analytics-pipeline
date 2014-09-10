"""Various helper utilities that are commonly used when working with Hive"""

import logging
import textwrap

import luigi
from luigi.configuration import get_config
from luigi.hive import HiveQueryTask, HivePartitionTarget, HiveQueryRunner

from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin


log = logging.getLogger(__name__)

# TODO: create a format class that allows records to be written using this format, as well as providing a mechanism for
# storing this string so that it can be passed to Hive to tell it how to interpret the data. Or perhaps a complete "data
# table" abstraction that can be written to from map-reduce and/or hive, created/dropped in hive and transfered into the
# result store.
TABLE_FORMAT_TSV = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"


def hive_database_name():
    """The name of the database where all metadata is being stored in Hive"""
    return get_config().get('hive', 'database', 'default')


class WarehouseDownstreamMixin(object):
    """Defines output_root parameter with appropriate default."""

    warehouse_path = luigi.Parameter(
        default_from_config={'section': 'hive', 'name': 'warehouse_path'}
    )


class ImportIntoHiveTableTask(WarehouseDownstreamMixin, OverwriteOutputMixin, HiveQueryTask):
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
        # Ideally this would be added to the query by the custom runner, however, this is easier.
        drop_on_overwrite = ''
        if self.overwrite:
            drop_on_overwrite = 'DROP TABLE IF EXISTS {0};'.format(self.table_name)

        # TODO: Figure out how to clean up old data. This just cleans
        # out old metastore info, and doesn't actually remove the table
        # data.

        # Ensure there is exactly one available partition in the table.
        query_format = """
            USE {database_name};
            {drop_on_overwrite}
            CREATE EXTERNAL TABLE {table_name} (
                {col_spec}
            )
            PARTITIONED BY ({partition.key} STRING)
            {table_format}
            LOCATION '{location}';
            ALTER TABLE {table_name} ADD PARTITION ({partition.query_spec});
        """

        query = query_format.format(
            database_name=hive_database_name(),
            drop_on_overwrite=drop_on_overwrite,
            table_name=self.table_name,
            col_spec=','.join([' '.join(c) for c in self.columns]),
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
    def table_name(self):
        """Provides name of Hive database table."""
        raise NotImplementedError

    @property
    def table_format(self):
        """Provides format of Hive database table's data."""
        return ''

    @property
    def table_location(self):
        """Provides root location of Hive database table's data."""
        return url_path_join(self.warehouse_path, self.table_name) + '/'

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
            self.table_name, self.partition.as_dict(), database=hive_database_name(), fail_missing_table=False
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


class HiveTableFromQueryTask(ImportIntoHiveTableTask):
    """Creates a hive table from the results of a hive query."""

    def query(self):
        create_table_statements = super(HiveTableFromQueryTask, self).query()
        full_insert_query = """
            INSERT INTO TABLE {table_name}
            PARTITION ({partition.query_spec})
            {insert_query}
        """.format(
            table_name=self.table_name,
            partition=self.partition,
            insert_query=self.insert_query.strip(),
        )

        return create_table_statements + textwrap.dedent(full_insert_query)

    @property
    def insert_query(self):
        """The query to execute to populate the table."""
        raise NotImplementedError

    def output(self):
        return get_target_from_url(self.partition_location)

    @property
    def table_format(self):
        """Provides format of Hive external table data."""
        return TABLE_FORMAT_TSV
