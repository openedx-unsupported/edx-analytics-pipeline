"""
Determine the number of users in each country are enrolled in each course.
"""
import logging
import textwrap

import luigi
from luigi.hive import HiveQueryTask, ExternalHiveTask

from edx.analytics.tasks.database_imports import ImportStudentCourseEnrollmentTask, ImportAuthUserTask
from edx.analytics.tasks.database_imports import ImportIntoHiveTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.util.geolocation import (
    GeolocationMixin, GeolocationDownstreamMixin, UNKNOWN_COUNTRY, UNKNOWN_CODE,
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, hive_database_name
from edx.analytics.tasks.decorators import workflow_entry_point

log = logging.getLogger(__name__)


class LastCountryOfUserMixin(
        WarehouseMixin,
        MapReduceJobTaskMixin,
        EventLogSelectionDownstreamMixin,
        GeolocationDownstreamMixin,
        OverwriteOutputMixin):
    """
    Defines parameters for LastCountryOfUser task and downstream tasks that require it.

    """
    pass


class LastCountryOfUser(LastCountryOfUserMixin, EventLogSelectionMixin, GeolocationMixin, MapReduceJobTask):
    """
    Identifies the country of the last IP address associated with each user.

    Uses :py:class:`LastCountryOfUserMixin` to define parameters, :py:class:`EventLogSelectionMixin`
    to define required input log files, and :py:class:`GeolocationMixin` to provide geolocation setup.

    """

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.warehouse_path,
                'last_country_of_user',
                'dt={0}/'.format(self.interval.date_b.strftime('%Y-%m-%d'))  # pylint: disable=no-member
            )
        )

    def init_local(self):
        super(LastCountryOfUser, self).init_local()
        self.remove_output_on_overwrite()

    def mapper(self, line):
        # Get events prefiltered by interval:
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        username = event.get('username')
        if not username:
            return
        username = username.strip()

        # Get timestamp instead of date string, so we get the latest ip
        # address for events on the same day.
        # TODO: simplify the round-trip conversion, so that we don't have to wait for the slow parse.
        # The parse provides error checking, allowing bad dates to be skipped.
        # But we may now have enough confidence in the data that this is rare (if ever).
        # Or we could implement a faster check, e.g. reject any that are "greater" than now.
        timestamp_as_datetime = eventlog.get_event_time(event)
        if timestamp_as_datetime is None:
            return
        timestamp = eventlog.datetime_to_timestamp(timestamp_as_datetime)

        ip_address = event.get('ip')
        if not ip_address:
            log.warning("No ip_address found for user '%s' on '%s'.", username, timestamp)
            return

        yield username, (timestamp, ip_address)

    def reducer(self, key, values):
        """Outputs country for last ip address associated with a user."""

        # DON'T presort input values (by timestamp).  The data potentially takes up too
        # much memory.  Scan the input values instead.

        # We assume the timestamp values (strings) are in ISO
        # representation, so that they can be compared as strings.
        username = key
        last_ip = None
        last_timestamp = ""
        for timestamp, ip_address in values:
            if timestamp > last_timestamp:
                last_ip = ip_address
                last_timestamp = timestamp

        if not last_ip:
            return

        # This ip address might not provide a country name.
        try:
            country = self.geoip.country_name_by_addr(last_ip)
            code = self.geoip.country_code_by_addr(last_ip)
        except Exception:
            log.exception("Encountered exception getting country:  user '%s', last_ip '%s' on '%s'.",
                          username, last_ip, last_timestamp)
            country = UNKNOWN_COUNTRY
            code = UNKNOWN_CODE

        if country is None or len(country.strip()) <= 0:
            log.error("No country found for user '%s', last_ip '%s' on '%s'.", username, last_ip, last_timestamp)
            # TODO: try earlier IP addresses, if we find this happens much.
            country = UNKNOWN_COUNTRY

        if code is None or len(code.strip()) <= 0:
            log.error("No code found for user '%s', last_ip '%s', country '%s' on '%s'.",
                      username, last_ip, country, last_timestamp)
            # TODO: try earlier IP addresses, if we find this happens much.
            code = UNKNOWN_CODE

        # Add the username for debugging purposes.  (Not needed for counts.)
        yield (country.encode('utf8'), code.encode('utf8')), username.encode('utf8')


class ImportLastCountryOfUserToHiveTask(LastCountryOfUserMixin, ImportIntoHiveTableTask):
    """
    Creates a Hive Table that points to Hadoop output of LastCountryOfUser task.

    """

    @property
    def table_name(self):
        return 'last_country_of_user'

    @property
    def columns(self):
        return [
            ('country_name', 'STRING'),
            ('country_code', 'STRING'),
            ('username', 'STRING'),
        ]

    @property
    def table_location(self):
        return url_path_join(self.warehouse_path, 'last_country_of_user')

    @property
    def table_format(self):
        """Provides name of Hive database table."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"

    @property
    def partition_date(self):
        # Because this data comes from EventLogSelectionDownstreamMixin,
        # we will use the end of the interval used to calculate
        # the country information.
        return self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def requires(self):
        return LastCountryOfUser(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )


class ExternalLastCountryOfUserToHiveTask(ImportLastCountryOfUserToHiveTask):

    interval = None
    date = luigi.DateParameter()

    @property
    def partition_date(self):
        return self.date.isoformat()  # pylint: disable=no-member

    def partition_spec(self):
        return "{key}={value}".format(
            key=self.partition.keys()[0],
            value=self.partition.values()[0],
        )

    def requires(self):
        yield ExternalURL(
            url=url_path_join(self.warehouse_path, 'last_country_of_user', self.partition_spec()) + '/'
        )


class InsertToMysqlLastCountryOfUserTask(LastCountryOfUserMixin, MysqlInsertTask):
    """
    Copy the last_country_of_user table from Hive into MySQL.
    """
    @property
    def table(self):
        return "last_country_of_user"

    @property
    def columns(self):
        return [
            ('country_name', 'VARCHAR(255)'),
            ('country_code', 'VARCHAR(10)'),
            ('username', 'VARCHAR(255)'),
        ]

    @property
    def insert_source_task(self):
        return LastCountryOfUser(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )


class QueryLastCountryPerCourseMixin(object):
    """
    Defines parameters for QueryLastCountryPerCourseTask

    """
    course_country_output = luigi.Parameter(
        config_path={'section': 'query-country-per-course', 'name': 'course_country_output'},
        description='Location to write query results.',
    )


class QueryLastCountryPerCourseTask(
        QueryLastCountryPerCourseMixin,
        OverwriteOutputMixin,
        HiveQueryTask):
    """Defines task to perform join in Hive to find course enrollment per-country counts."""

    def query(self):

        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                date STRING,
                course_id STRING,
                country_code STRING,
                count INT,
                cumulative_count INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE course_enrollment_location_current
            SELECT
                sce.dt,
                sce.course_id,
                uc.country_code,
                sum(if(sce.is_active, 1, 0)),
                count(sce.user_id)
            FROM student_courseenrollment sce
            LEFT OUTER JOIN auth_user au on sce.user_id = au.id
            LEFT OUTER JOIN last_country_of_user uc on au.username = uc.username
            GROUP BY sce.dt, sce.course_id, uc.country_code;
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            location=self.output().path,
            table_name='course_enrollment_location_current',
        )

        log.debug('Executing hive query: %s', query)
        return query

    def init_local(self):
        super(QueryLastCountryPerCourseTask, self).init_local()
        self.remove_output_on_overwrite()

    def output(self):
        return get_target_from_url(self.course_country_output + "/")

    def requires(self):
        yield (
            ExternalHiveTask(table='student_courseenrollment', database=hive_database_name()),
            ExternalHiveTask(table='auth_user', database=hive_database_name()),
            ExternalHiveTask(table='last_country_of_user', database=hive_database_name()),
        )


class QueryLastCountryPerCourseWorkflow(LastCountryOfUserMixin, QueryLastCountryPerCourseTask):

    """Defines dependencies for performing join in Hive to find course enrollment per-country counts."""
    def requires(self):
        # Note that import parameters not included are 'destination', 'num_mappers', 'verbose',
        # and 'date' -- we will use the default values for those.
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
        }
        yield (
            ImportLastCountryOfUserToHiveTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                geolocation_data=self.geolocation_data,
                overwrite=self.overwrite,
            ),
            InsertToMysqlLastCountryOfUserTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                geolocation_data=self.geolocation_data,
                overwrite=self.overwrite,
            ),
            # We can't make explicit dependencies on this yet, until we
            # solve the multiple-credentials problem, as well as the split-kwargs
            # problem.
            ImportStudentCourseEnrollmentTask(**kwargs_for_db_import),
            ImportAuthUserTask(**kwargs_for_db_import),
        )


class InsertToMysqlCourseEnrollByCountryTaskBase(MysqlInsertTask):  # pylint: disable=abstract-method
    """
    Define course_enrollment_location_current table.
    """
    @property
    def table(self):
        return "course_enrollment_location_current"

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('country_code', 'VARCHAR(10) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
            ('cumulative_count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            # Note that the order here is extremely important. The API query pattern needs to filter first by course and
            # then by date.
            ('course_id', 'date'),
        ]


class InsertToMysqlCourseEnrollByCountryTask(InsertToMysqlCourseEnrollByCountryTaskBase):
    """
    Write to course_enrollment_location_current table from specified source, as a standalone task.
    """
    insert_source = luigi.Parameter()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.insert_source)


@workflow_entry_point
class InsertToMysqlCourseEnrollByCountryWorkflow(
        QueryLastCountryPerCourseMixin,
        LastCountryOfUserMixin,
        InsertToMysqlCourseEnrollByCountryTaskBase):
    """
    Write to course_enrollment_location_current table from CountUserActivityPerInterval.
    """

    @property
    def insert_source_task(self):
        return QueryLastCountryPerCourseWorkflow(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
            course_country_output=self.course_country_output,
        )
