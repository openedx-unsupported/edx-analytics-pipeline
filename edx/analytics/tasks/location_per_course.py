"""
Determine the number of users in each country are enrolled in each course.
"""
import datetime
import logging
import tempfile

import luigi
from luigi.hive import ExternalHiveTask
import pygeoip

from edx.analytics.tasks.database_imports import ImportStudentCourseEnrollmentTask, ImportAuthUserTask
from edx.analytics.tasks.util.hive import (
    HiveTableTask, HiveQueryToMysqlTask, HiveTableFromQueryTask, WarehouseMixin, HivePartition
)
from edx.analytics.tasks.enrollments import CourseEnrollmentTable
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import ExternalURL, get_target_from_url
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import hive_database_name

log = logging.getLogger(__name__)


class LastCountryOfUserMixin(
        MapReduceJobTaskMixin,
        EventLogSelectionDownstreamMixin,
        OverwriteOutputMixin):
    """
    Defines parameters for LastCountryOfUser task and downstream tasks that require it.

    Just inherits parameters from :py:class:`MapReduceJobTaskMixin, :py:class:`EventLogSelectionDownstreamMixin`,
    :py:class:`GeolocationMixin` and :py:class:`OverwriteOutputMixin` classes.

    """
    geolocation_data = luigi.Parameter(
        default_from_config={'section': 'geolocation', 'name': 'geolocation_data'}
    )


class LastCountryOfUser(LastCountryOfUserMixin, EventLogSelectionMixin, BaseGeolocation, MapReduceJobTask):
    """
    Identifies the country of the last IP address associated with each user.

    Uses :py:class:`LastCountryOfUserMixin` to define parameters, :py:class:`EventLogSelectionMixin`
    to define required input log files, and :py:class:`BaseGeolocation` to provide geolocation setup.

    """

    output_root = luigi.Parameter()
    geoip = None

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

    def init_reducer(self):
        # Copy the remote version of the geolocation data file to a local file.
        # This is required by the GeoIP call, which assumes that the data file is located
        # on a local file system.
        self.temporary_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_data')
        with self.geolocation_data_target().open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self.temporary_data_file.write(transfer_buffer)
                else:
                    break
        self.temporary_data_file.seek(0)

        self.geoip = pygeoip.GeoIP(self.temporary_data_file.name, pygeoip.STANDARD)

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
        yield (country, code), username

    def final_reducer(self):
        """Clean up after the reducer is done."""
        del self.geoip
        self.temporary_data_file.close()

        return tuple()

    def extra_modules(self):
        """Pygeoip is required by all tasks that load this file."""
        return [pygeoip]


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
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

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


class EnrollmentByLocationTask(LastCountryOfUserMixin, HiveQueryToMysqlTask):
    """Defines task to perform join in Hive to find course enrollment per-country counts."""

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
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def table_name(self):
        return 'course_enrollment_location_current'

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
    def required_tables(self):
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
                warehouse_path=self.warehouse_path
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


class UsersPerCountryReport(LastCountryOfUserMixin, HiveQueryToMysqlTask):
    """
    Calculates TSV report containing number of users per country.

    Parameters:
        report: Location of the resulting report. The output format is a
            tsv file with country and count.
    """

    @property
    def query(self):
        return """
            SELECT
                (COUNT(lc.username) / t.total_users) AS percent,
                COUNT(lc.username) AS count,
                lc.country_name AS country,
                lc.country_code AS code
            FROM last_country_of_user lc
            JOIN (
                    SELECT
                        dt,
                        COUNT(username) AS total_users
                    FROM last_country_of_user
                    GROUP BY dt
                 ) t
            GROUP BY
                lc.dt, lc.country_name, lc.country_code, t.total_users
            ORDER BY
                percent DESC;
        """

    @property
    def table_name(self):
        return 'country_user_summary'

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def columns(self):
        return [
            ('percent', 'DOUBLE NOT NULL'),
            ('count', 'INT NOT NULL'),
            ('country', 'VARCHAR(255) NOT NULL'),
            ('code', 'VARCHAR(255) NOT NULL')
        ]

    @property
    def required_tables(self):
        return ImportLastCountryOfUserToHiveTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
            warehouse_path=self.warehouse_path
        )