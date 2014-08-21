
import logging
import textwrap

import luigi
from luigi.hive import HiveQueryTask, ExternalHiveTask

from edx.analytics.tasks.database_imports import ImportAuthUserProfileTask, ImportIntoHiveTableTask
from edx.analytics.tasks.url import get_target_from_url, ExternalURL
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

    def query(self):
        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                date STRING,
                course_id STRING,
                country_code STRING,
                gender STRING,
                age INT,
                level_of_education INT,
                users_enrolled INT,
                users_enrolled_verified INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE {table_name}
            SELECT
                uce.date,
                uce.course_id,
                pr.gender,
                (year(uce.date) - pr.year_of_birth),
                pr.level_of_education,
                count(uce.user_id),
                count(uce_ver.user_id)
            FROM user_course_enrollment_history uce
            LEFT OUTER JOIN auth_userprofile pr ON sce.user_id = pr.user_id
            LEFT OUTER JOIN user_course_enrollment_history uce_ver ON uce.date = uce_ver.date AND uce.course_id = uce_ver.course_id AND uce.user_id = uce_ver.user_id AND uce_ver.mode = 'verified'
            WHERE
                "{start_date}" <= uce.date AND uce.date < "{end_date}"
            GROUP BY 0, 1, 2, 3, 4;
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
        )

    def output(self):
        return get_target_from_url(url_path_join(warehouse_path, self.table_name + '/'))


class CourseFactWorkflow(CourseFactTask):

    def requires(self):
        # Note that import parameters not included are 'destination', 'num_mappers', 'verbose',
        # and 'date' -- we will use the default values for those.
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
        }
        yield ImportAuthUserProfileTask(**kwargs_for_db_import)
        yield UserCourseEnrollmentHistoryHiveTableTask()


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
