"""
Loads the user_activity table into the warehouse through the pipeline via Hive.

On the roadmap is to write a task that runs validation queries on the aggregated Hive data pre-load.
"""
from __future__ import absolute_import

import logging

import luigi.date_interval

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.insights.database_imports import ImportAuthUserTask
from edx.analytics.tasks.insights.user_activity import InsertToMysqlCourseActivityTask, UserActivityTableTask
from edx.analytics.tasks.util.hive import (
    BareHiveTableTask, HivePartition, HivePartitionTask, WarehouseMixin, hive_database_name
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.util.vertica_target import CredentialFileVerticaTarget
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin

log = logging.getLogger(__name__)


class LoadInternalReportingUserActivityToWarehouse(WarehouseMixin, VerticaCopyTask):
    """
    Loads the user activity table from Hive into the Vertica data warehouse.

    """
    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter(
        description='Number of reduce tasks',
    )

    @property
    def insert_source_task(self):
        hive_table = "user_activity_by_user"
        # User activity data for each day is stored in a dated directory.
        # We want to be able to load all that data into Vertica in one go, hence we use
        # a wildcard('*') here.
        url = url_path_join(self.warehouse_path, hive_table) + '/dt=*/'
        return ExternalURL(url=url)

    @property
    def table(self):
        return 'f_user_activity'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def auto_primary_key(self):
        """The warehouse schema defines an auto primary key called row_number for this table."""
        return ('row_number', 'AUTO_INCREMENT')

    @property
    def foreign_key_mapping(self):
        """Foreign keys are specified in the warehouse schema."""
        return {}

    @property
    def columns(self):
        return [
            ('user_id', 'INTEGER NOT NULL'),
            ('course_id', 'VARCHAR(256) NOT NULL'),
            ('date', 'DATE'),
            ('activity_type', 'VARCHAR(200)'),
            ('number_of_activities', 'INTEGER')
        ]
