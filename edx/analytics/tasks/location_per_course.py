"""
Determine the number of users in each country are enrolled in each course.
"""
import datetime
import logging

import luigi
from luigi.hive import ExternalHiveTask

from edx.analytics.tasks.database_imports import ImportStudentCourseEnrollmentTask, ImportAuthUserTask
from edx.analytics.tasks.util.hive import (
    HiveTableTask, HiveTableFromQueryTask, WarehouseMixin, HivePartition
)
from edx.analytics.tasks.enrollments import CourseEnrollmentTable
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import ExternalURL, get_target_from_url
from edx.analytics.tasks.user_location import BaseGeolocation, GeolocationMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import hive_database_name

log = logging.getLogger(__name__)


class LastCountryOfUserMixin(
        MapReduceJobTaskMixin,
        EventLogSelectionDownstreamMixin,
        GeolocationMixin,
        OverwriteOutputMixin):
    """
    Defines parameters for LastCountryOfUser task and downstream tasks that require it.

    Just inherits parameters from :py:class:`MapReduceJobTaskMixin, :py:class:`EventLogSelectionDownstreamMixin`,
    :py:class:`GeolocationMixin` and :py:class:`OverwriteOutputMixin` classes.

    """
    pass


class LastCountryOfUser(LastCountryOfUserMixin, EventLogSelectionMixin, BaseGeolocation, MapReduceJobTask):
    """
    Identifies the country of the last IP address associated with each user.

    Uses :py:class:`LastCountryOfUserMixin` to define parameters, :py:class:`EventLogSelectionMixin`
    to define required input log files, and :py:class:`BaseGeolocation` to provide geolocation setup.

    """

    output_root = luigi.Parameter()

    def requires_local(self):
        return ExternalURL(self.geolocation_data)

    def geolocation_data_target(self):
        return self.input_local()

    def output(self):
        return get_target_from_url(self.output_root)

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


class ImportLastCountryOfUserToHiveTask(LastCountryOfUserMixin, HiveTableTask):
    """
    Creates a Hive Table that points to Hadoop output of LastCountryOfUser task.

    Parameters are defined by :py:class:`LastCountryOfUserMixin`.
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
    def partition(self):
        # Because this data comes from EventLogSelectionDownstreamMixin,
        # we will use the end of the interval used to calculate
        # the country information.
        return HivePartition('dt', self.interval.date_b.strftime('%Y-%m-%d'))  # pylint: disable=no-member

    def requires(self):
        return LastCountryOfUser(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
            output_root=self.partition_location,
        )


class QueryLastCountryPerCourseTask(HiveTableFromQueryTask):
    """Defines task to perform join in Hive to find course enrollment per-country counts."""

    import_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    @property
    def insert_query(self):
        return """
            SELECT
                sce.date,
                sce.course_id,
                uc.country_code,
                count(sce.user_id)
            FROM course_enrollment sce
            LEFT OUTER JOIN auth_user au ON sce.user_id = au.id
            LEFT OUTER JOIN last_country_of_user uc ON au.username = uc.username
            WHERE sce.at_end = 1
            GROUP BY sce.date, sce.course_id, uc.country_code;
        """

    @property
    def partition(self):
        return HivePartition('dt', self.import_date.isoformat())  # pylint: disable=no-member

    @property
    def table_name(self):
        return 'course_enrollment_location_current'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('country_code', 'STRING'),
            ('count', 'INT'),
        ]

    def requires(self):
        yield (
            ExternalHiveTask(table='course_enrollment', database=hive_database_name()),
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
            'import_date': self.import_date,
        }
        yield (
            ImportLastCountryOfUserToHiveTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                geolocation_data=self.geolocation_data,
                overwrite=self.overwrite
            ),
            CourseEnrollmentTable(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                overwrite=self.overwrite,
                warehouse_path=self.warehouse_path
            ),
            ImportAuthUserTask(**kwargs_for_db_import),
        )


class InsertToMysqlCourseEnrollByCountryTask(MysqlInsertTask):
    """
    Define course_enrollment_location_current table.
    """

    overwrite = luigi.BooleanParameter(default=True)

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
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('date', 'course_id'),
        ]

    @property
    def insert_source_task(self):
        raise NotImplementedError


class InsertToMysqlCourseEnrollByCountryWorkflow(
        LastCountryOfUserMixin,
        WarehouseMixin,
        InsertToMysqlCourseEnrollByCountryTask):
    """
    Write to course_enrollment_location_current table from CountUserActivityPerInterval.
    """

    import_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

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
            warehouse_path=self.warehouse_path,
            import_date=self.import_date,
        )
