"""Computes active users over the last one year."""

import datetime
import os
import logging
import luigi

from edx.analytics.tasks.common.pathutil import (
    PathSetTask,
    EventLogSelectionMixin,
    EventLogSelectionDownstreamMixin,
)
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition, BareHiveTableTask, HivePartitionTask
from edx.analytics.tasks.util.url import ExternalURL, url_path_join, get_target_from_url
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin

log = logging.getLogger(__name__)


class ActiveUsersDownstreamMixin(
        WarehouseMixin,
        OverwriteOutputMixin,
        MapReduceJobTaskMixin,
        EventLogSelectionDownstreamMixin):
    """Common parameters needed for the workflow."""
    pass


class ActiveUsersTask(ActiveUsersDownstreamMixin, EventLogSelectionMixin, MapReduceJobTask):
    """Task to compute active users."""

    def mapper(self, line):
        value = self.get_event_and_date_string(line)

        if value is None:
            return
        event, _date_string = value

        username = eventlog.get_event_username(event)

        if not username:
            log.error("Encountered event with no username: %s", event)
            self.incr_counter('Active Users last year', 'Discard Event Missing username', 1)
            return

        yield username, 1

    def reducer(self, key, values):
        yield self.interval.date_a.isoformat(), self.interval.date_b.isoformat(), key

    def output(self):
        output_url = self.hive_partition_path('active_users_this_year', self.interval.date_b)
        return get_target_from_url(output_url)

    def run(self):
        self.remove_output_on_overwrite()
        super(ActiveUsersTask, self).run()

class ActiveUsersTableTask(BareHiveTableTask):
    """Hive table that stores the active users over time."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'active_users_this_year'

    @property
    def columns(self):
        return [
            ('window_start_date', 'STRING'),
            ('window_end_date', 'STRING'),
            ('username', 'STRING'),
        ]


class ActiveUsersPartitionTask(WeeklyIntervalMixin, ActiveUsersDownstreamMixin, HivePartitionTask):
    """Creates hive table partition to hold active users data."""

    @property
    def hive_table_task(self):  # pragma: no cover
        return ActiveUsersTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def partition_value(self):
        """ Use a dynamic partition value based on the interval end date. """
        return self.interval.date_b.isoformat()

    @property
    def data_task(self):
        return ActiveUsersTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            interval=self.interval,
            overwrite=self.overwrite,
        )


class LoadInternalReportingActiveUsersToWarehouse(WarehouseMixin, VerticaCopyTask):
    """Loads the active_users_this_year hive table into Vertica warehouse."""

    HIVE_TABLE = 'active_users_this_year'

    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super(LoadInternalReportingActiveUsersToWarehouse, self).__init__(*args, **kwargs)

        path = url_path_join(self.warehouse_path, self.HIVE_TABLE)
        path_targets = PathSetTask([path]).output()
        paths = list(set([os.path.dirname(target.path) for target in path_targets]))
        dates = [path.rsplit('/', 2)[-1] for path in paths]
        latest_date = sorted(dates)[-1]

        self.load_date = datetime.datetime.strptime(latest_date, "dt=%Y-%m-%d").date()

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.load_date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        partition_location = url_path_join(self.warehouse_path, self.HIVE_TABLE, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)

    @property
    def table(self):
        return 'f_active_users_this_year'

    @property
    def auto_primary_key(self):
        return None

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def columns(self):
        return [
            ('window_start_date', 'DATE'),
            ('window_end_date', 'DATE'),
            ('username', 'VARCHAR(45) NOT NULL'),
        ]
