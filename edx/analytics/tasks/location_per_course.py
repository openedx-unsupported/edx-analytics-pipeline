"""
Determine the number of users in each country are enrolled in each course.
"""
import datetime
import logging
import textwrap
from collections import defaultdict

import luigi
from luigi.hive import HiveQueryTask, ExternalHiveTask
from luigi.parameter import DateIntervalParameter

from edx.analytics.tasks.database_imports import ImportStudentCourseEnrollmentTask, ImportAuthUserTask
from edx.analytics.tasks.database_imports import ImportIntoHiveTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.pathutil import PathSelectionByDateIntervalTask, EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join, UncheckedExternalURL
from edx.analytics.tasks.util.geolocation import (
    GeolocationMixin, GeolocationDownstreamMixin, UNKNOWN_COUNTRY, UNKNOWN_CODE,
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import (
    WarehouseMixin, hive_database_name, BareHiveTableTask, HivePartitionTask,
    HiveQueryToMysqlTask, HivePartition,
)
from edx.analytics.tasks.decorators import workflow_entry_point

log = logging.getLogger(__name__)


class LastDailyAddressOfUserTask(
        WarehouseMixin,
        OverwriteOutputMixin,
        EventLogSelectionMixin,
        MultiOutputMapReduceJobTask):
    """
    Task to extract IP address information from eventlogs over a given interval.
    This would produce a different output file for each day within the interval
    containing that day's last IP address for each user only.
    """

    # FILEPATH_PATTERN should match the output files defined by output_path_for_key().
    FILEPATH_PATTERN = '.*?last_ip_of_user_(?P<date>\\d{4}-\\d{2}-\\d{2})'

    # We use warehouse_path to generate the output path, so we make this a non-param.
    output_root = None

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = eventlog.get_event_username(event)
        if not username:
            return

        # Get timestamp instead of date string, so we get the latest ip
        # address for events on the same day.
        timestamp = eventlog.get_event_time_string(event)
        if not timestamp:
            return

        ip_address = event.get('ip')
        if not ip_address:
            log.warning("No ip_address found for user '%s' on '%s'.", username, timestamp)
            return

        # Get the course_id from context, if it happens to be present.
        # It's okay if it isn't.  Not sure if there are particular
        # types of course interaction we care about, but we might want
        # to only collect the course_id off of explicit events, and
        # ignore implicit events as not being "real" interactions with
        # course content.  Or maybe we add a flag indicating explicit
        # vs. implicit, so that this can be better teased apart.
        # (E.g.  we use the latest explicit event for a course, but if
        # there are none, then use the latest implicit event for the
        # course, and if there are none, then use the latest overall
        # event.
        course_id = eventlog.get_course_id(event)

        # TODO: should we also get user_id?

        # For multi-output, we will generate a single file for each key value.
        # When looking at location for user in a course, we don't want to have
        # an output file per course per date, so just use date as the key,
        # and have a single file representing all events on the date.
        yield date_string, (timestamp, ip_address, course_id, username)

    def multi_output_reducer(self, _date_string, values, output_file):
        # All values are for a given date, but we want to find the last ip_address
        # for each user (and eventually in each course).
        last_ip = defaultdict()
        last_timestamp = defaultdict()
        for value in values:
            (timestamp, ip_address, course_id, username) = value

            # We are storing different IP addresses depending on the username
            # *and* the course.  This anticipates a future requirement to provide
            # different countries depending on which course.
            last_key = (username, course_id)

            last_time = last_timestamp[last_key] if last_key in last_timestamp else ""
            if timestamp > last_time:
                last_ip[last_key] = ip_address
                last_timestamp[last_key] = timestamp

        # Now output the resulting "last" values for each key.
        for last_key, ip_address in last_ip.iteritems():
            timestamp = last_timestamp[last_key]
            username, course_id = last_key
            value = [timestamp, ip_address, username, course_id]
            output_file.write('\t'.join([unicode(field).encode('utf8') for field in value]))
            output_file.write('\n')

    def output_path_for_key(self, key):
        date_string = key
        return url_path_join(
            self.hive_partition_path('last_ip_of_user', date_string),
            'last_ip_of_user_{date}'.format(
                date=date_string,
            ),
        )

    def downstream_input_tasks(self):
        """
        MultiOutputMapReduceJobTask returns marker as output.
        This method returns the external tasks which can then be used as input in other jobs.
        Note that this method does not verify the existence of the underlying urls. It assumes that
        there is an output file for every date within the interval. Any MapReduce job
        which uses this as input would fail if there is missing data for any date within the interval.
        """

        tasks = []
        for date in self.interval:
            url = self.output_path_for_key(date.isoformat())
            tasks.append(UncheckedExternalURL(url))

        return tasks

    def run(self):
        self.remove_output_on_overwrite()
        super(LastDailyAddressOfUserTask, self).run()

        # This makes sure that a output file exists for each date in the interval
        # as downstream tasks require that they exist.
        for date in self.interval:
            url = self.output_path_for_key(date.isoformat())
            target = get_target_from_url(url)
            if not target.exists():
                target.open("w").close()  # touch the file


class LastCountryOfUserDownstreamMixin(
        WarehouseMixin,
        OverwriteOutputMixin,
        MapReduceJobTaskMixin,
        EventLogSelectionDownstreamMixin,
        GeolocationDownstreamMixin):

    """
    Defines parameters for LastCountryOfUserDataTask task and downstream tasks that require it.

    """

    # Make the interval be optional:
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to extract ip addresses for. '
        'If not specified, `interval_start` and `interval_end` are used to construct the `interval`.',
    )

    # Define optional parameters, to be used if 'interval' is not defined.
    interval_start = luigi.DateParameter(
        config_path={'section': 'location-per-course', 'name': 'interval_start'},
        significant=False,
        description='The start date to extract ip addresses for.  Ignored if `interval` is provided.',
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='The end date to extract ip addresses for.  Ignored if `interval` is provided. '
        'Default is today, UTC.',
    )

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'location-per-course', 'name': 'overwrite_n_days'},
        significant=False,
        description='This parameter is used by LastCountryOfUserDataTask which will overwrite ip address per user'
                    ' for the most recent n days.'
    )

    def __init__(self, *args, **kwargs):
        super(LastCountryOfUserDownstreamMixin, self).__init__(*args, **kwargs)

        if not self.interval:
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)


class LastCountryOfUserDataTask(LastCountryOfUserDownstreamMixin, GeolocationMixin, MapReduceJobTask):
    """
    Identifies the country of the last IP address associated with each user.

    Uses :py:class:`LastCountryOfUserDownstreamMixin` to define parameters, :py:class:`EventLogSelectionMixin`
    to define required input log files, and :py:class:`GeolocationMixin` to provide geolocation setup.

    """
    # This is a special Luigi override that instructs the output to be written directly to output,
    # rather than being written to a temp directory that is later renamed.  Renaming in S3 is actually
    # a copy-and-delete, which can be expensive for large datasets.
    enable_direct_output = True

    # Calculate requirements once.
    cached_local_requirements = None
    cached_hadoop_requirements = None

    def __init__(self, *args, **kwargs):
        super(LastCountryOfUserDataTask, self).__init__(*args, **kwargs)

        self.overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)

    def requires_local(self):
        if not self.cached_local_requirements:
            requirements = super(LastCountryOfUserDataTask, self).requires_local()
            # Default is an empty list, but assume that any real data added is done
            # so as a dict.
            if not requirements:
                requirements = {}

            if self.overwrite_n_days > 0:
                overwrite_interval = DateIntervalParameter().parse('{}-{}'.format(
                    self.overwrite_from_date,
                    self.interval.date_b
                ))

                requirements['user_addresses_task'] = LastDailyAddressOfUserTask(
                    interval=overwrite_interval,
                    source=self.source,
                    pattern=self.pattern,
                    warehouse_path=self.warehouse_path,
                    mapreduce_engine=self.mapreduce_engine,
                    n_reduce_tasks=self.n_reduce_tasks,
                    overwrite=True,
                )
            self.cached_local_requirements = requirements

        return self.cached_local_requirements

    def requires_hadoop(self):
        # We want to pass in the historical data as well as the output of LastDailyAddressOfUserTask to the hadoop job.
        # LastDailyAddressOfUserTask returns the marker as output, so we need custom logic to pass the output
        # of LastDailyAddressOfUserTask as actual hadoop input to this job.
        if not self.cached_hadoop_requirements:
            path_selection_interval = DateIntervalParameter().parse('{}-{}'.format(
                self.interval.date_a,
                self.overwrite_from_date,
            ))

            last_ip_of_user_root = url_path_join(self.warehouse_path, 'last_ip_of_user')
            path_selection_task = PathSelectionByDateIntervalTask(
                source=[last_ip_of_user_root],
                pattern=[LastDailyAddressOfUserTask.FILEPATH_PATTERN],
                interval=path_selection_interval,
                expand_interval=datetime.timedelta(0),
                date_pattern='%Y-%m-%d',
            )

            requirements = {
                'path_selection_task': path_selection_task,
            }

            if self.overwrite_n_days > 0:
                requirements['downstream_input_tasks'] = self.requires_local()['user_addresses_task'].downstream_input_tasks()

            self.cached_hadoop_requirements = requirements

        return self.cached_hadoop_requirements

    def output(self):
        return get_target_from_url(self.hive_partition_path('last_country_of_user', self.interval.date_b))  # pylint: disable=no-member

    def complete(self):
        return get_target_from_url(url_path_join(self.output().path, '_SUCCESS')).exists()

    def run(self):
        output_target = self.output()
        if not self.complete() and output_target.exists():
            output_target.remove()
        super(LastCountryOfUserDataTask, self).run()

    def init_local(self):
        # TODO: this was not defined in the enrollment code.  Is it really needed here, or is it handled
        # now in the run() method?  (If not in the enrollment code, where did it come from?  From the previous
        # geolocation code, I'm guessing, but confirm.)
        super(LastCountryOfUserDataTask, self).init_local()
        self.remove_output_on_overwrite()

    def mapper(self, line):
        (
            timestamp,
            ip_address,
            username,
            _course_id,
        ) = line.split('\t')

        # Output all events for a username, regardless of course (for now).
        # When including course_id, it should be included in the value, not the key.
        # That way we can provide an appropriate default value for the user for
        # their latest ip_address in any (or in no) course.
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

        debug_message = u"user '{}' on '{}'".format(username.decode('utf8'), last_timestamp)
        country = self.get_country_name(last_ip, debug_message)
        code = self.get_country_code(last_ip, debug_message)

        # Add the username for debugging purposes.  (Not needed for counts.)
        yield (country.encode('utf8'), code.encode('utf8')), username


class LastCountryOfUserTableTask(BareHiveTableTask):
    """The hive table for this engagement data."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'last_country_of_user'

    @property
    def columns(self):
        return [
            ('country_name', 'STRING'),
            ('country_code', 'STRING'),
            ('username', 'STRING'),
        ]


class LastCountryOfUserPartitionTask(LastCountryOfUserDownstreamMixin, HivePartitionTask):
    """The hive table partition for this engagement data."""

    @property
    def partition_value(self):
        """Use a dynamic partition value based on the date parameter."""
        return self.interval.date_b.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return LastCountryOfUserTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return LastCountryOfUserDataTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )


class ExternalLastCountryOfUserToHiveTask(LastCountryOfUserPartitionTask):
    """Load last_country_of_user without regenerating the underlying data."""

    interval = None
    date = luigi.DateParameter()

    @property
    def partition_value(self):
        return self.date.isoformat()  # pylint: disable=no-member

    def partition_spec(self):
        return "{key}={value}".format(
            key=self.partition.keys()[0],
            value=self.partition.values()[0],
        )

    def data_task(self):
        yield ExternalURL(self.hive_partition_path('last_country_of_user', self.partition_value))


class InsertToMysqlLastCountryOfUserTask(LastCountryOfUserDownstreamMixin, MysqlInsertTask):
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
        return LastCountryOfUserDataTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )


class InsertToMysqlLastCountryPerCourseTask(
        LastCountryOfUserDownstreamMixin,
        HiveQueryToMysqlTask):
    """Defines task to perform join in Hive to find course enrollment per-country counts."""

    @property
    def table(self):
        return 'course_enrollment_location_current'

    @property
    def query(self):
        return """
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
        """

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('country_code', 'VARCHAR(10)'),
            ('count', 'INT(11) NOT NULL'),
            ('cumulative_count', 'INT(11) NOT NULL'),
        ]

    @property
    def required_table_tasks(self):
        # TODO: should DB imports be overridden this way?
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
        }
        yield (
            LastCountryOfUserPartitionTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                pattern=self.pattern,
                interval=self.interval,
                interval_start=self.interval_start,
                interval_end=self.interval_end,
                overwrite_n_days=self.overwrite_n_days,
                geolocation_data=self.geolocation_data,
                overwrite=self.overwrite,
            ),
            # TODO: FIGURE OUT WHAT THE FOLLOWING COMMENT MEANS....
            # We can't make explicit dependencies on this yet, until we
            # solve the multiple-credentials problem, as well as the split-kwargs
            # problem.
            ImportStudentCourseEnrollmentTask(**kwargs_for_db_import),
            ImportAuthUserTask(**kwargs_for_db_import),
        )

    @property
    def indexes(self):
        return [
            ('course_id',),
            # Note that the order here is extremely important. The API query pattern needs to filter first by course and
            # then by date.
            ('course_id', 'date'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member


# TODO: can we change the name of this entry point?
@workflow_entry_point
class InsertToMysqlCourseEnrollByCountryWorkflow(
        LastCountryOfUserDownstreamMixin,
        luigi.WrapperTask):
    """
    Write last-country information to Mysql.

    Includes LastCountryOfUser and LastCountryPerCourse.
    """

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'pattern': self.pattern,
            'interval': self.interval,
            'interval_start': self.interval_start,
            'interval_end': self.interval_end,
            'overwrite_n_days': self.overwrite_n_days,
            'geolocation_data': self.geolocation_data,
            'overwrite': self.overwrite,
        }
        
        yield (
            InsertToMysqlLastCountryOfUserTask(**kwargs),
            InsertToMysqlLastCountryPerCourseTask(**kwargs),
        )
