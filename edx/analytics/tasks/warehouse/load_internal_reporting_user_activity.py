"""
Loads the user_activity table into the warehouse through the pipeline via Hive.

On the roadmap is to write a task that runs validation queries on the aggregated Hive data pre-load.
"""
import datetime
import os
import logging
import luigi
import luigi.date_interval

from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin, IncrementalVerticaCopyTask
from edx.analytics.tasks.insights.database_imports import ImportAuthUserTask
from edx.analytics.tasks.insights.user_activity import CourseActivityWeeklyTask, UserActivityTableTask, UserActivityPartitionTask
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, WarehouseMixin, HivePartition, BareHiveTableTask, HivePartitionTask, hive_database_name
from edx.analytics.tasks.util.url import ExternalURL, url_path_join, get_target_from_url
from edx.analytics.tasks.util.vertica_target import CredentialFileVerticaTarget
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin

log = logging.getLogger(__name__)


class InternalReportingUserActivityTableTask(BareHiveTableTask):

    @property
    def table(self):
        return 'internal_reporting_user_activity'

    @property
    def partition_by(self):
        return 'dt'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('date', 'STRING'),
            ('activity_type', 'STRING'),
            ('number_of_activities', 'INT'),
        ]


class InternalReportingUserActivityPartitionTask(HivePartitionTask):

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()

    def query(self):
        query = """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec})
        SELECT
            au.id,
            uad.course_id,
            uad.`date`,
            uad.category,
            uad.count
        FROM auth_user au
        JOIN user_activity_daily uad
            ON au.username = uad.username
        WHERE
            uad.`date` = "{date}";
        """.format(
            database_name=hive_database_name(),
            table=self.hive_table_task.table,
            partition=self.partition,
            date=self.date.isoformat(),
        )

        return query

    def requires(self):
        yield (
            ImportAuthUserTask(overwrite=False, destination=self.warehouse_path),
            InternalReportingUserActivityTableTask(
                warehouse_path=self.warehouse_path,
            ),
            UserActivityPartitionTask(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite, #change to self.overwrite
            )
        )

    @property
    def partition_value(self):
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return InternalReportingUserActivityTableTask(
            warehouse_path=self.warehouse_path,
        )

    def remove_output_on_overwrite(self):
        super(HivePartitionTask, self).remove_output_on_overwrite()

    def output(self):
        return get_target_from_url(self.hive_partition_path(self.hive_table_task.table, self.date.isoformat()))


class IncrementalLoadInternalReportingUserActivityToWarehouse(WarehouseMixin, IncrementalVerticaCopyTask):

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()

    @property
    def record_filter(self):
        return "date='{date}'".format(
            date=self.date.isoformat()
        )

    def update_id(self):
        return '{task_name}(date={date})'.format(
            task_name=self.task_family,
            date=self.date.isoformat(),
        )

    @property
    def insert_source_task(self):
        return InternalReportingUserActivityPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return 'f_user_activity_test'

    @property
    def auto_primary_key(self):
        return ('row_number', 'AUTO_INCREMENT')

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def columns(self):
        return [
            ('user_id', 'INTEGER NOT NULL'),
            ('course_id', 'VARCHAR(256) NOT NULL'),
            ('date', 'DATE'),
            ('activity_type', 'VARCHAR(200)'),
            ('number_of_activities', 'INTEGER')
        ]


class LoadInternalReportingUserActivityWorkflow(WarehouseMixin, luigi.WrapperTask):

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()
    overwrite_n_days = luigi.IntParameter(
        significant=False,
    )
    overwrite = luigi.BooleanParameter(
        default=False,
        significant=False
    )

    def requires(self):
        overwrite_from_date = self.date - datetime.timedelta(days=self.overwrite_n_days)
        interval = luigi.date_interval.Custom(overwrite_from_date, self.date)

        for date in interval:
            yield IncrementalLoadInternalReportingUserActivityToWarehouse(
                warehouse_path=self.warehouse_path,
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
            )


class LoadInternalReportingUserActivityToWarehouse(WarehouseMixin, VerticaCopyTask):
    """
    Loads the user activity table from Hive into the Vertica data warehouse.

    """
    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter(
        description='Number of reduce tasks',
    )

    def __init__(self, *args, **kwargs):
        super(LoadInternalReportingUserActivityToWarehouse, self).__init__(*args, **kwargs)

        path = url_path_join(self.warehouse_path, 'internal_reporting_user_activity')
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
        hive_table = "internal_reporting_user_activity"
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)

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


class BuildInternalReportingUserActivityCombinedView(VerticaCopyTaskMixin, WarehouseMixin, luigi.Task):
    """luigi task to build the combined view on top of the history and production tables for user activity."""
    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()
    history_schema = luigi.Parameter(default='history')

    def requires(self):
        return {'insert_source': LoadInternalReportingUserActivityToWarehouse(
            n_reduce_tasks=self.n_reduce_tasks,
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
            schema=self.schema,
            credentials=self.credentials),
            'credentials': ExternalURL(self.credentials)}

    @property
    def view(self):
        """The "table name" is the name of the view we build over the table we insert here and the history table."""
        return "f_user_activity_combined"

    def update_id(self):
        """All that matters is whether we've built the view before, and the parameter information doesn't matter."""
        return "user_activity_view_built"

    def run(self):
        """Construct the view on top of the historical and new user activity tables."""
        connection = self.output().connect()
        try:
            cursor = connection.cursor()

            # We mark this task as complete first, since the view creation does an implicit commit.
            self.output().touch(connection)

            # Creating the view commits the transaction as well.
            build_view_query = """CREATE VIEW {schema}.{view} AS SELECT * FROM (
                        SELECT * FROM {schema}.f_user_activity
                        UNION
                        SELECT * FROM {history}.f_user_activity
                    ) AS u""".format(schema=self.schema, view=self.view, history=self.history_schema)
            log.debug(build_view_query)
            cursor.execute(build_view_query)
            log.debug("Committed transaction.")
        except Exception as exc:
            log.debug("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            raise
        finally:
            connection.close()

    def output(self):
        """
        Returns a Vertica target noting that the update occurred.
        """
        return CredentialFileVerticaTarget(
            credentials_target=self.input()['credentials'],
            table=self.view,
            schema=self.schema,
            update_id=self.update_id()
        )

    def complete(self):
        """
        OverwriteOutputMixin redefines the complete method so that tasks are re-run, which is great for the Vertica
        loading tasks where we would delete and then re-start, but detrimental here, as the existence of the view does
        not depend on the data inside the table, only on the table's existence.  We override this method again to
        revert to the standard luigi complete() method, because we can't meaningfully re-run this task given that
        CREATE VIEW IF NOT EXISTS and DROP VIEW IF EXISTS are not supported in Vertica.
        """
        return self.output().exists()


class InternalReportingUserActivityWorkflow(VerticaCopyTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """Wrapper to provide a single entry point for the user activity table construction and view construction."""
    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()
    history_schema = luigi.Parameter(default='history')

    def requires(self):
        """
        We require the view to be built and the data to be loaded.

        The existence check for the output of the view-building task is merely an attempt to select from the view
        (and thus makes no guarantee about the freshness of the data in the table), so we may need to re-run the
        warehouse loading task even if the view-building task has been built, and thus we require the warehouse load
        task here too.
        """
        return [
            LoadInternalReportingUserActivityToWarehouse(
                n_reduce_tasks=self.n_reduce_tasks,
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
                schema=self.schema,
                credentials=self.credentials
            ),
            BuildInternalReportingUserActivityCombinedView(
                n_reduce_tasks=self.n_reduce_tasks,
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
                schema=self.schema,
                credentials=self.credentials,
                history_schema=self.history_schema
            )
        ]


class UserActivityWorkflow(WeeklyIntervalMixin, luigi.WrapperTask):

    n_reduce_tasks = luigi.Parameter()
    credentials = luigi.Parameter()

    def requires(self):
        yield CourseActivityWeeklyTask(
            end_date=self.end_date,
            weeks=self.weeks,
            n_reduce_tasks=self.n_reduce_tasks,
            credentials=self.credentials,
        )
        yield AggregateInternalReportingUserActivityTableHive(
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
        )
