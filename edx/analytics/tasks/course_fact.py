
import logging
import textwrap

import luigi
from luigi.hive import HiveQueryTask, ExternalHiveTask

from edx.analytics.tasks.database_imports import ImportAuthUserProfileTask, ImportIntoHiveTableTask
from edx.analytics.tasks.url import get_target_from_url, ExternalURL, url_path_join
from edx.analytics.tasks.util.hive import hive_database_name
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)


class WarehouseDownstreamMixin(object):
    """
    Defines parameters for the data warehouse

    Parameters:
        warehouse_path:  location to write query results.
    """
    warehouse_path = luigi.Parameter(
        default_from_config={'section': 'hive', 'name': 'warehouse_path'}
    )


class CourseFactTask(
        WarehouseDownstreamMixin,
        OverwriteOutputMixin,
        HiveQueryTask):

    interval = luigi.DateIntervalParameter()

    def query(self):
        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                date STRING,
                course_id STRING,
                gender STRING,
                age INT,
                level_of_education INT,
                users_enrolled INT,
                users_enrolled_verified INT,
                active_prev_week INT,
                attempted_problem_prev_week INT,
                played_video_prev_week INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE {table_name}
            SELECT
                uce.date,
                uce.course_id,
                pr.gender,
                (year(uce.date) - pr.year_of_birth) AS age,
                pr.level_of_education,
                COUNT(uce.user_id),
                COUNT(uce_ver.user_id),
                SUM(act.active) AS active_prev_week,
                SUM(act.attempted_problem) AS attempted_problem_prev_week,
                SUM(act.played_video) AS played_video_prev_week
            FROM user_course_enrollment_history uce
            LEFT OUTER JOIN auth_userprofile pr
                ON uce.user_id = pr.user_id
            LEFT OUTER JOIN user_course_enrollment_history uce_ver
                ON  uce.date = uce_ver.date
                AND uce.course_id = uce_ver.course_id
                AND uce.user_id = uce_ver.user_id
                AND uce_ver.mode = 'verified'
            LEFT OUTER JOIN user_course_activity_weekly_history act
                ON  act.date = uce.date
                AND act.course_id = uce.course_id
                AND act.user_id = uce.user_id
            WHERE
                "{start_date}" <= uce.date AND uce.date < "{end_date}"
            GROUP BY uce.date, uce.course_id, pr.gender, (year(uce.date) - pr.year_of_birth), pr.level_of_education;
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            location=self.output().path,
            table_name=self.table_name,
            start_date=self.interval.date_a.strftime('%Y-%m-%d'),
            end_date=self.interval.date_b.strftime('%Y-%m-%d'),
        )

        log.debug('Executing hive query: %s', query)
        return query

    @property
    def table_name(self):
        return 'course_fact'

    def init_local(self):
        super(CourseFactTask, self).init_local()
        self.remove_output_on_overwrite()

    def requires(self):
        yield (
            ExternalHiveTask(table='auth_userprofile', database=hive_database_name()),
            ExternalHiveTask(table='user_course_enrollment_history', database=hive_database_name()),
            ExternalHiveTask(table='user_course_activity_weekly_history', database=hive_database_name()),
        )

    def output(self):
        return get_target_from_url(url_path_join(self.warehouse_path, self.table_name + '/'))


class CourseFactWorkflow(CourseFactTask):

    def requires(self):
        # Note that import parameters not included are 'destination', 'num_mappers', 'verbose',
        # and 'date' -- we will use the default values for those.
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
        }
        yield ImportAuthUserProfileTask(**kwargs_for_db_import)
        yield UserCourseEnrollmentHistoryHiveTableTask()
        yield WeeklyUserActivityHistoryHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )


class UserCourseEnrollmentHistoryHiveTableTask(WarehouseDownstreamMixin, ImportIntoHiveTableTask):

    @property
    def table_name(self):
        return 'user_course_enrollment_history'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('user_id', 'INT'),
            ('mode', 'STRING'),
        ]

    @property
    def table_location(self):
        return url_path_join(self.warehouse_path, self.table_name + '/')

    @property
    def table_format(self):
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"

    @property
    def partition_date(self):
        return '2014-08-21'

    def requires(self):
        return ExternalURL(self.table_location)


class UserActivityHistoryHiveTableTask(WarehouseDownstreamMixin, ImportIntoHiveTableTask):

    @property
    def table_name(self):
        return 'user_course_activity_history'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('user_id', 'INT'),
            ('active', 'INT'),
            ('attempted_problem', 'INT'),
            ('played_video', 'INT'),
        ]

    @property
    def table_location(self):
        return url_path_join(self.warehouse_path, self.table_name + '/')

    @property
    def table_format(self):
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"

    @property
    def partition_date(self):
        return '2014-08-21'

    def requires(self):
        return ExternalURL(self.table_location)


class WeeklyUserActivityHistoryHiveTableTask(
        WarehouseDownstreamMixin,
        OverwriteOutputMixin,
        HiveQueryTask):

    def query(self):
        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                date STRING,
                course_id STRING,
                user_id INT,
                active INT,
                attempted_problem INT,
                played_video INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE {table_name}
            SELECT
                date,
                course_id,
                user_id,
                MAX(active) OVER (last_week),
                MAX(attempted_problem) OVER (last_week),
                MAX(played_video) OVER (last_week)
            FROM user_course_activity_history
            WINDOW last_week AS (PARTITION BY course_id, user_id ORDER BY date ROWS 7 PRECEDING);
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            location=self.output().path,
            table_name=self.table_name,
        )

        log.debug('Executing hive query: %s', query)
        return query

    @property
    def table_name(self):
        return 'user_course_activity_weekly_history'

    def init_local(self):
        super(WeeklyUserActivityHistoryHiveTableTask, self).init_local()
        self.remove_output_on_overwrite()

    def requires(self):
        yield UserActivityHistoryHiveTableTask()

    def output(self):
        return get_target_from_url(url_path_join(self.warehouse_path, self.table_name + '/'))
