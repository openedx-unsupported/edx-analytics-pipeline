
import logging
import textwrap

import luigi
from luigi.configuration import get_config
from luigi.hive import HiveQueryTask, HivePartitionTarget, HiveQueryRunner

from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin


log = logging.getLogger(__name__)
TABLE_FORMAT_TSV = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"


def hive_database_name():
    return get_config().get('hive', 'database', 'default')


class WarehouseDownstreamMixin(object):
    """Defines output_root parameter with appropriate default."""

    warehouse_path = luigi.Parameter(
       default_from_config={'section': 'hive', 'name': 'warehouse_path'}
    )


class ImportIntoHiveTableTask(WarehouseDownstreamMixin, OverwriteOutputMixin, HiveQueryTask):
    """
    Abstract class to import data into a Hive table.

    Requires four properties and a requires() method to be defined.
    """

    def query(self):
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

    def run_job(self, job):
        if hasattr(job, 'remove_output_on_overwrite'):
            job.remove_output_on_overwrite()

        log.debug('Executing hive query: %s', job.query())
        return super(OverwriteAwareHiveQueryRunner, self).run_job(job)


class HivePartition(object):

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def as_dict(self):
        return {self.key: self.value}

    @property
    def query_spec(self):
        return "{key}='{value}'".format(
            key=self.key,
            value=self.value,
        )

    @property
    def path_spec(self):
        return "{key}={value}".format(
            key=self.key,
            value=self.value,
        )


class HiveTableFromQueryTask(ImportIntoHiveTableTask):

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
        raise NotImplementedError

    def output(self):
        return get_target_from_url(self.partition_location)

    @property
    def table_format(self):
        """Provides format of Hive external table data."""
        return TABLE_FORMAT_TSV
