"""
Loads the user_activity table into the warehouse through the pipeline via Hive.

On the roadmap is to write a task that runs validation queries on the aggregated Hive data pre-load.
"""
import datetime
import logging

import luigi.date_interval

from edx.analytics.tasks.common.spark import SparkJobTask
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.insights.database_imports import ImportAuthUserTask
from edx.analytics.tasks.insights.user_activity import InsertToMysqlCourseActivityTask, UserActivityTableTask, \
    UserActivityTaskSpark
from edx.analytics.tasks.util.hive import (
    BareHiveTableTask, HivePartition, HivePartitionTask, WarehouseMixin, hive_database_name
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join
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


class InternalReportingUserActivityPartitionTaskSpark(WarehouseMixin, SparkJobTask):
    """Spark alternative of InternalReportingUserActivityPartitionTask"""

    date = luigi.DateParameter()
    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        significant=False,
    )

    def run(self):
        self.remove_output_on_overwrite()
        super(InternalReportingUserActivityPartitionTaskSpark, self).run()

    def requires(self):
        required_tasks = [
            ImportAuthUserTask(overwrite=False, destination=self.warehouse_path)
        ]
        if self.overwrite_n_days > 0:
            overwrite_from_date = self.date - datetime.timedelta(days=self.overwrite_n_days)
            overwrite_interval = luigi.date_interval.Custom(overwrite_from_date, self.date)
            required_tasks.append(
                UserActivityTaskSpark(
                    interval=overwrite_interval,
                    warehouse_path=self.warehouse_path,
                    output_root=self._get_user_activity_hive_table_path(),
                    overwrite=True,
                )
            )
        yield required_tasks

    def _get_auth_user_hive_table_path(self):
        import_date = datetime.datetime.utcnow().date() # we only need to join import date's data with user activity
        return url_path_join(
            self.warehouse_path,
            'auth_user',
            'dt={}'.format(import_date.isoformat())
        )

    def _get_auth_user_table_schema(self):
        from pyspark.sql.types import StructType, StringType
        schema = StructType().add("id", StringType(), True) \
            .add("username", StringType(), True) \
            .add("last_login", StringType(), True) \
            .add("date_joined", StringType(), True) \
            .add("is_active", StringType(), True) \
            .add("is_superuser", StringType(), True) \
            .add("is_staff", StringType(), True) \
            .add("email", StringType(), True) \
            .add("dt", StringType(), True)
        return schema

    def _get_user_activity_hive_table_path(self, *args):
        return url_path_join(
            self.warehouse_path,
            'user_activity'
        )

    def _get_user_activity_table_schema(self):
        from pyspark.sql.types import StructType, StringType
        schema = StructType().add("course_id", StringType(), True) \
            .add("username", StringType(), True) \
            .add("date", StringType(), True) \
            .add("category", StringType(), True) \
            .add("count", StringType(), True) \
            .add("dt", StringType(), True)
        return schema

    def spark_job(self, *args):
        auth_user_df = self._spark.read.csv(
            self._get_auth_user_hive_table_path(),
            schema=self._get_auth_user_table_schema(),
            sep='\x01',
            nullValue='\N'
        )
        user_activity_df = self._spark.read.csv(
            self._get_user_activity_hive_table_path(*args),
            sep='\t',
            schema=self._get_user_activity_table_schema()
        )
        # self._sql_context.registerDataFrameAsTable(auth_user_df, 'auth_user')
        # self._sql_context.registerDataFrameAsTable(user_activity_df, 'user_activity')
        # query = """
        #             SELECT
        #                 au.id,
        #                 ua.course_id,
        #                 ua.date,
        #                 ua.category,
        #                 ua.count
        #             FROM auth_user au
        #             JOIN user_activity ua
        #                 ON au.username = ua.username
        #         """
        # result = self._sql_context.sql(query)
        # result.coalesce(2).write.csv(self.output().path, mode='overwrite', sep='\t')
        from pyspark.sql.functions import broadcast
        auth_df = broadcast(auth_user_df)
        user_activity_df.join(
            auth_df,
            auth_df.username == user_activity_df.username
        ).select(
            auth_df['id'],
            user_activity_df['course_id'],
            user_activity_df['date'],
            user_activity_df['category'],
            user_activity_df['count'],
        ).coalesce(1).write.csv(self.output().path, mode='overwrite', sep='\t')

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.warehouse_path,
                'internal_reporting_user_activity',
                'dt={}'.format(self.date.isoformat())
            )
        )


class InternalReportingUserActivityPartitionTask(HivePartitionTask):
    """Aggregate the user activity table in Hive."""

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()
    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        significant=False,
    )

    def query(self):
        query = """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec})
        SELECT
            au.id,
            ua.course_id,
            ua.`date`,
            ua.category,
            ua.count
        FROM auth_user au
        JOIN user_activity ua
            ON au.username = ua.username;
        """.format(
            database_name=hive_database_name(),
            table=self.hive_table_task.table,
            partition=self.partition,
        )

        return query

    def requires(self):
        yield (
            ImportAuthUserTask(overwrite=False, destination=self.warehouse_path),
            self.hive_table_task,
            UserActivityTableTask(
                warehouse_path=self.warehouse_path,
                date=self.date,
                overwrite_n_days=self.overwrite_n_days,
            )
        )

    @property
    def partition_value(self):
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return InternalReportingUserActivityTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    def remove_output_on_overwrite(self):
        # HivePartitionTask overrides the behaviour of this method, such that
        # it does not actually remove the HDFS data. Here we want to make sure that
        # data is removed from HDFS as well so we default to OverwriteOutputMixin's implementation.
        OverwriteOutputMixin.remove_output_on_overwrite(self)

    def output(self):
        # HivePartitionTask returns HivePartitionTarget as output which does not implement remove().
        # We override output() here so that it can be deleted when overwrite is specified.
        return get_target_from_url(self.hive_partition_path(self.hive_table_task.table, self.date.isoformat()))


class LoadInternalReportingUserActivityToWarehouse(WarehouseMixin, VerticaCopyTask):
    """
    Loads the user activity table from Hive into the Vertica data warehouse.

    """
    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter(
        description='Number of reduce tasks',
    )

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

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


class UserActivityWorkflow(WeeklyIntervalMixin, WarehouseMixin, luigi.WrapperTask):

    overwrite_hive = luigi.BoolParameter(
        default=False,
        description='Whether or not to overwrite Hive data.',
        significant=False
    )

    overwrite_mysql = luigi.BoolParameter(
        default=False,
        description='Whether or not to overwrite existing outputs in Mysql.',
        significant=False
    )

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        description='This parameter is used by UserActivityTask which will overwrite user-activity counts '
                    'for the most recent n days. Default is pulled from user-activity.overwrite_n_days.',
        significant=False,
    )

    n_reduce_tasks = luigi.Parameter()

    def requires(self):
        yield InternalReportingUserActivityPartitionTask(
            date=self.end_date,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
            overwrite=self.overwrite_hive,
        )
        yield InsertToMysqlCourseActivityTask(
            end_date=self.end_date,
            weeks=self.weeks,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
            overwrite_hive=self.overwrite_hive,
            overwrite_mysql=self.overwrite_mysql,
        )
