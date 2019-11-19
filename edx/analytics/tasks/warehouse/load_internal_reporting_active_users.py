"""Computes active users over the last one year."""

from __future__ import absolute_import

import datetime
import logging

import isoweek
import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.common.vertica_load import IncrementalVerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import get_target_from_url
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin

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
        event, date_string = value

        username = eventlog.get_event_username(event)

        if not username:
            log.error("Encountered event with no username: %s", event)
            self.incr_counter('Active Users last year', 'Discard Event Missing username', 1)
            return

        date = datetime.date(*[int(x) for x in date_string.split('-')])
        iso_year, iso_weekofyear, _iso_weekday = date.isocalendar()
        week = isoweek.Week(iso_year, iso_weekofyear)
        start_date = week.monday().isoformat()
        end_date = (week.sunday() + datetime.timedelta(1)).isoformat()

        yield (start_date, end_date, username), 1

    def reducer(self, key, _values):
        yield key

    def output(self):
        output_url = self.hive_partition_path('active_users_per_week', self.interval.date_b)
        return get_target_from_url(output_url)

    def run(self):
        self.remove_output_on_overwrite()
        super(ActiveUsersTask, self).run()

    def extra_modules(self):
        import isoweek
        return [isoweek]


class ActiveUsersTableTask(BareHiveTableTask):
    """Hive table that stores the active users over time."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'active_users_per_week'

    @property
    def columns(self):
        return [
            ('start_date', 'STRING'),
            ('end_date', 'STRING'),
            ('username', 'STRING'),
        ]


class ActiveUsersPartitionTask(ActiveUsersDownstreamMixin, HivePartitionTask):
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


class LoadInternalReportingActiveUsersToWarehouse(WeeklyIntervalMixin, ActiveUsersDownstreamMixin, IncrementalVerticaCopyTask):
    """Loads the active_users_this_year hive table into Vertica warehouse."""

    HIVE_TABLE = 'active_users_per_week'

    @property
    def record_filter(self):
        return "start_date='{start_date}' and end_date='{end_date}'".format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat()
        )

    def update_id(self):
        return '{task_name}(start_date={start_date},end_date={end_date})'.format(
            task_name=self.task_family,
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat()
        )

    @property
    def insert_source_task(self):
        return ActiveUsersPartitionTask(
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        ).data_task

    @property
    def table(self):
        return 'f_active_users_per_week'

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
            ('start_date', 'DATE'),
            ('end_date', 'DATE'),
            ('username', 'VARCHAR(45) NOT NULL'),
        ]


class ActiveUsersWorkflow(ActiveUsersDownstreamMixin, VerticaCopyTaskMixin, luigi.WrapperTask):

    date = luigi.DateParameter()
    overwrite_n_weeks = luigi.IntParameter(default=1)
    interval = None

    def requires(self):
        kwargs = {
            'schema': self.schema,
            'credentials': self.credentials,
            'weeks': 1,
            'warehouse_path': self.warehouse_path,
            'n_reduce_tasks': self.n_reduce_tasks,
        }

        yield LoadInternalReportingActiveUsersToWarehouse(
            end_date=self.date,
            overwrite=self.overwrite,
            **kwargs
        )

        weeks_to_overwrite = self.overwrite_n_weeks
        end_date = self.date

        while weeks_to_overwrite > 0:
            end_date = end_date - datetime.timedelta(weeks=1)

            yield LoadInternalReportingActiveUsersToWarehouse(
                end_date=end_date,
                overwrite=True,
                **kwargs
            )
            weeks_to_overwrite -= 1

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
