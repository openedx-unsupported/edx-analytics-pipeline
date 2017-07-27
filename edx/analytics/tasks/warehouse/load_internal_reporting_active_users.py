"""Computes active users over the last one year."""

import datetime
import os
import logging
import luigi
import isoweek

from edx.analytics.tasks.common.pathutil import (
    PathSetTask,
    EventLogSelectionMixin,
    EventLogSelectionDownstreamMixin,
)
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition, BareHiveTableTask, HivePartitionTask
from edx.analytics.tasks.util.url import ExternalURL, url_path_join, get_target_from_url
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin

log = logging.getLogger(__name__)

try:
    import vertica_python
    from vertica_python.errors import QueryError
    vertica_client_available = True  # pylint: disable=invalid-name
except ImportError:
    log.warn('Unable to import Vertica client libraries')
    # On hadoop slave nodes we don't have Vertica client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    vertica_client_available = False  # pylint: disable=invalid-name


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
        iso_year, iso_weekofyear, iso_weekday = date.isocalendar()
        week = isoweek.Week(iso_year, iso_weekofyear)
        start_date = week.monday().isoformat()
        end_date = (week.sunday() + datetime.timedelta(1)).isoformat()

        yield (start_date, end_date, username), 1

    def reducer(self, key, values):
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


class LoadInternalReportingActiveUsersToWarehouse(ActiveUsersDownstreamMixin, VerticaCopyTask):
    """Loads the active_users_this_year hive table into Vertica warehouse."""

    HIVE_TABLE = 'active_users_per_week'

    date = luigi.DateParameter()

    interval = None

    def init_copy(self, connection):
        self.attempted_removal = True
        if self.overwrite:
            # Before changing the current contents table, we have to make sure there
            # are no aggregate projections on it.
            self.drop_aggregate_projections(connection)

            # Use "DELETE" instead of TRUNCATE since TRUNCATE forces an implicit commit before it executes which would
            # commit the currently open transaction before continuing with the copy.
            query = "DELETE FROM {schema}.{table} where end_date='{date}'".format(
                schema=self.schema,
                table=self.table,
                date=self.date.isoformat())
            log.debug(query)
            connection.cursor().execute(query)

        # vertica-python and its maintainers intentionally avoid supporting open
        # transactions like we do when self.overwrite=True (DELETE a bunch of rows
        # and then COPY some), per https://github.com/uber/vertica-python/issues/56.
        # The DELETE commands in this method will cause the connection to see some
        # messages that will prevent it from trying to copy any data (if the cursor
        # successfully executes the DELETEs), so we flush the message buffer.
        connection.cursor().flush_to_query_ready()

    def init_touch(self, connection):
        if self.overwrite:
            # Clear the appropriate rows from the luigi Vertica marker table
            marker_table = self.output().marker_table  # side-effect: sets self.output_target if it's None
            marker_schema = self.output().marker_schema
            try:
                query = "DELETE FROM {marker_schema}.{marker_table} where update_id='{update_id}';".format(
                    marker_schema=marker_schema,
                    marker_table=marker_table,
                    update_id=self.update_id(),
                )
                log.debug(query)
                connection.cursor().execute(query)
            except vertica_python.errors.Error as err:
                if (type(err) is vertica_python.errors.MissingRelation) or ('Sqlstate: 42V01' in err.args[0]):
                    # If so, then our query error failed because the table doesn't exist.
                    pass
                else:
                    raise
        connection.cursor().flush_to_query_ready()

    def update_id(self):
        return '{task_name}(date={date})'.format(
            task_name=self.task_family,
            date=self.date.isoformat()
        )

    @property
    def insert_source_task(self):
        return ActiveUsersPartitionTask(
            end_date=self.date,
            weeks=1,
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
        yield LoadInternalReportingActiveUsersToWarehouse(
            schema=self.schema,
            credentials=self.credentials,
            date=self.date,
            warehouse_path=self.warehouse_path,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite,
        )

        weeks_to_overwrite = self.overwrite_n_weeks
        end_date = self.date

        while weeks_to_overwrite > 0:
            end_date = end_date - datetime.timedelta(weeks=1)

            yield LoadInternalReportingActiveUsersToWarehouse(
                schema=self.schema,
                credentials=self.credentials,
                date=end_date,
                warehouse_path=self.warehouse_path,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=True,
            )
            weeks_to_overwrite -= 1

    def complete(self):
        # OverwriteOutputMixin changes the complete() method behavior, so we override it.
        return all(r.complete() for r in luigi.task.flatten(self.requires()))
