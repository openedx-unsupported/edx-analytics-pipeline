"""
Loads the country table into the warehouse through the pipeline via Hive.
"""
import logging

import luigi

from edx.analytics.tasks.insights.location_per_course import (
    InsertToMysqlLastCountryPerCourseTask, LastCountryOfUserDownstreamMixin, LastCountryOfUserPartitionTask
)
from edx.analytics.tasks.util.hive import HivePartition, HiveTableFromQueryTask, WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)


class LoadInternalReportingCountryTableHive(LastCountryOfUserDownstreamMixin, HiveTableFromQueryTask):
    """Loads internal_reporting_d_country Hive table from last_country_of_user_id Hive table."""

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
            FROM last_country_of_user_id lcu
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
            InsertToMysqlLastCountryPerCourseTask(
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
