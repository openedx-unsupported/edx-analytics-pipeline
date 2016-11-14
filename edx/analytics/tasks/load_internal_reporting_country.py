"""
Loads the country table into the warehouse through the pipeline via Hive.
"""

import luigi
import logging

from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, WarehouseMixin, HivePartition
from edx.analytics.tasks.url import url_path_join, ExternalURL
from edx.analytics.tasks.location_per_course import (
    LastCountryOfUserDownstreamMixin,
    LastCountryOfUserPartitionTask,
    InsertToMysqlCourseEnrollByCountryWorkflow,
)

log = logging.getLogger(__name__)


class LoadInternalReportingCountryTableHive(LastCountryOfUserDownstreamMixin, HiveTableFromQueryTask):
    """Loads internal_reporting_d_country Hive table from last_country_of_user Hive table."""

    def requires(self):
        return LastCountryOfUserPartitionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def table(self):
        return 'internal_reporting_d_country'

    @property
    def columns(self):
        return [
            ('country_name', 'STRING'),
            ('user_last_location_country_code', 'STRING')
        ]

    @property
    def insert_query(self):
        return """
            SELECT
              collect_set(lcu.country_name)[0]
            , lcu.country_code as user_last_location_country_code
            FROM last_country_of_user lcu
            GROUP BY lcu.country_code
            """


class ImportCountryWorkflow(LastCountryOfUserDownstreamMixin, luigi.WrapperTask):

    def requires(self):
        yield (
            LoadInternalReportingCountryTableHive(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                pattern=self.pattern,
                interval=self.interval,
                interval_start=self.interval_start,
                interval_end=self.interval_end,
                overwrite_n_days=self.overwrite_n_days,
                geolocation_data=self.geolocation_data,
                overwrite=self.overwrite,
            ),
            InsertToMysqlCourseEnrollByCountryWorkflow(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                pattern=self.pattern,
                interval=self.interval,
                interval_start=self.interval_start,
                interval_end=self.interval_end,
                overwrite_n_days=self.overwrite_n_days,
                geolocation_data=self.geolocation_data,
                overwrite=self.overwrite,
            ),
        )


class LoadInternalReportingCountryToWarehouse(WarehouseMixin, VerticaCopyTask):
    """
    Loads the country table from Hive into the Vertica data warehouse.
    """

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()

    @property
    def table(self):
        return 'd_country'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def auto_primary_key(self):
        """No automatic primary key here; user's id is enough."""
        return None

    @property
    def columns(self):
        return [
            ('country_name', 'VARCHAR(45)'),
            ('user_last_location_country_code', 'VARCHAR(45) NOT NULL')
        ]

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        hive_table = "internal_reporting_d_country"
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)
