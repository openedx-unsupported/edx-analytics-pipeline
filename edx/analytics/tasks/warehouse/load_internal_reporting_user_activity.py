"""
Loads the user_activity table into the warehouse through the pipeline via Hive.

On the roadmap is to write a task that runs validation queries on the aggregated Hive data pre-load.
"""
import datetime
import os
import logging
import luigi
from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import ExternalURL, url_path_join
from edx.analytics.tasks.user_activity import UserActivityTableTask
from edx.analytics.tasks.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.database_imports import ImportAuthUserTask
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, WarehouseMixin, HivePartition
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin
from edx.analytics.tasks.util.vertica_target import CredentialFileVerticaTarget
from edx.analytics.tasks.user_activity import CourseActivityWeeklyTask

log = logging.getLogger(__name__)


class AggregateInternalReportingUserActivityTableHive(HiveTableFromQueryTask):
    """Aggregate the user activity table in Hive."""
    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()

    def requires(self):
        """
        This task reads from auth_user and user_activity_daily, so require that they be
        loaded into Hive (via MySQL loads into Hive or via the pipeline as needed).
        """
        return [ImportAuthUserTask(overwrite=False, destination=self.warehouse_path),
                UserActivityTableTask(interval=self.interval, warehouse_path=self.warehouse_path,
                                      n_reduce_tasks=self.n_reduce_tasks)]

    @property
    def table(self):
        return 'internal_reporting_user_activity'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('date', 'STRING'),
            ('activity_type', 'STRING'),
            ('number_of_activities', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
              au.id
            , uad.course_id
            , uad.date
            , uad.category
            , uad.count
            FROM auth_user au
            JOIN user_activity_daily uad ON au.username = uad.username
            """


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
