"""
Determine the number of users in each country are enrolled in each course.
"""
import datetime
import logging
import textwrap
from collections import defaultdict

import luigi
from luigi.contrib.hive import HiveQueryTask
from luigi.parameter import MissingParameterException

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.pathutil import (
    EventLogSelectionDownstreamMixin, EventLogSelectionMixin, PathSelectionByDateIntervalTask
)
from edx.analytics.tasks.insights.database_imports import ImportStudentCourseEnrollmentTask
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.geolocation import GeolocationDownstreamMixin, GeolocationMixin
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin, hive_database_name
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import ExternalURL, UncheckedExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class LastIpAddressRecord(Record):
    """
    Store information about last IP address observed for a given user in a given course.

    Values are not written to a database, so string lengths are not specified.
    """
    timestamp = StringField(description='Timestamp of last event by user in a course.')
    ip_address = StringField(description='IP address recorded on last event by user in a course.')
    user_id = IntegerField(description='User ID recorded on last event by user in a course.')
    course_id = StringField(description='Course ID recorded on last event by user in a course.')


class LastDailyIpAddressOfUserTask(
        WarehouseMixin,
        OverwriteOutputMixin,
        EventLogSelectionMixin,
        MultiOutputMapReduceJobTask):
    """
    Task to extract IP address information from eventlogs over a given interval.

    This would produce a different output file for each day within the interval
    containing that day's last IP address for each user only.  If there are no
    events on a given day, an empty output file is produced for that day.
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

        user_id = event.get('context', {}).get('user_id')
        if not user_id:
            return

        try:
            user_id = int(user_id)
        except ValueError:
            self.incr_counter('User Location', 'Discard event with malformed user_id', 1)
            return

        # Get timestamp instead of date string, so we get the latest ip
        # address for events on the same day.
        timestamp = eventlog.get_event_time_string(event)
        if not timestamp:
            return

        ip_address = event.get('ip')
        if not ip_address:
            log.warning("No ip_address found for user '%s' on '%s'.", user_id, timestamp)
            return

        # Get the course_id from context, if it happens to be present.
        # It's okay if it isn't.

        # (Not sure if there are particular types of course
        # interaction we care about, but we might want to only collect
        # the course_id off of explicit events, and ignore implicit
        # events as not being "real" interactions with course content.
        # Or maybe we add a flag indicating explicit vs. implicit, so
        # that this can be better teased apart.  For example, we could
        # use the latest explicit event for a course, but if there are
        # none, then use the latest implicit event for the course, and
        # if there are none, then use the latest overall event.)
        course_id = eventlog.get_course_id(event)

        # For multi-output, we will generate a single file for each key value.
        # When looking at location for user in a course, we don't want to have
        # an output file per course per date, so just use date as the key,
        # and have a single file representing all events on the date.
        yield date_string, (timestamp, ip_address, course_id, user_id)

    def multi_output_reducer(self, _date_string, values, output_file):
        # All values are for a given date, but we want to find the last ip_address
        # for each user (and eventually in each course).
        last_ip = defaultdict()
        last_timestamp = defaultdict()
        for value in values:
            (timestamp, ip_address, course_id, user_id) = value

            # We are storing different IP addresses depending on the user_id
            # *and* the course.  This anticipates a future requirement to provide
            # different countries depending on which course.
            last_key = (user_id, course_id)

            last_time = last_timestamp.get(last_key, '')
            if timestamp > last_time:
                last_ip[last_key] = ip_address
                last_timestamp[last_key] = timestamp

        # Now output the resulting "last" values for each key.
        for last_key, ip_address in last_ip.iteritems():
            timestamp = last_timestamp[last_key]
            user_id, course_id = last_key
            value = [timestamp, ip_address, user_id, course_id]
            record = LastIpAddressRecord(*value)
            output_file.write(record.to_separated_values())
            output_file.write('\n')

    def output_path_for_key(self, key):
        date_string = key
        return url_path_join(
            self.hive_partition_path('last_ip_of_user_id', date_string),
            'last_ip_of_user_{date}'.format(date=date_string),
        )

    def downstream_input_tasks(self):
        """
        Provide a list of tasks that a downstream task would use as input.

        This is necessary because a MultiOutputMapReduceJobTask returns a marker as output.
        Note that this method does not verify the existence of the underlying urls. It assumes that
        there is an output file for every date within the interval. Any MapReduce job
        which uses this as input would fail if there is missing data for any date within the interval,
        so this task will create empty output files for dates with no data.
        """
        tasks = []
        for date in self.interval:
            url = self.output_path_for_key(date.isoformat())
            tasks.append(UncheckedExternalURL(url))

        return tasks

    def run(self):
        self.remove_output_on_overwrite()
        super(LastDailyIpAddressOfUserTask, self).run()

        # This makes sure that a output file exists for each date in the interval
        # as downstream tasks require that they exist (as provided by downstream_input_tasks()).
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
    Defines parameters for LastCountryOfUser task and downstream tasks that require it.

    """

    # Make the interval be optional:
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to extract ip addresses for. '
        'If not specified, `interval_start` and `interval_end` are used to construct the `interval`.',
    )

    # Define optional parameters, to be used if 'interval' is not defined.
    interval_start = luigi.DateParameter(
        default=None,
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
        description='This parameter is used by LastCountryOfUser which will overwrite ip address per user'
                    ' for the most recent n days.'
    )

    def __init__(self, *args, **kwargs):
        super(LastCountryOfUserDownstreamMixin, self).__init__(*args, **kwargs)

        if not self.interval:
            if self.interval_start is None:
                raise MissingParameterException("Either 'interval' or 'interval_start' parameter is required to be specified.")
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)


class LastCountryOfUser(LastCountryOfUserDownstreamMixin, GeolocationMixin, MapReduceJobTask):
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
        super(LastCountryOfUser, self).__init__(*args, **kwargs)

        self.overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)

    def requires_local(self):
        if not self.cached_local_requirements:
            requirements = super(LastCountryOfUser, self).requires_local()
            # Default is an empty list, but assume that any real data added is done
            # so as a dict.
            if not requirements:
                requirements = {}

            if self.overwrite_n_days > 0:
                # This calculates IP addresses for user and course for a recent period of time,
                # which allows for late-arriving events to be eventually included (as well as the
                # latest day).  Because LastDailyIpAddressOfUserTask returns a marker file as output,
                # we need to include the calls to the actual task here to have them included as
                # dependencies but *not* be included as Hadoop input to this job.  We will use
                # the downstream_input_tasks() method on the LastDailyIpAddressOfUserTask task to
                # actually get pointers to inputs.
                overwrite_interval = luigi.date_interval.Custom(self.overwrite_from_date, self.interval.date_b)
                requirements['user_addresses_task'] = LastDailyIpAddressOfUserTask(
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
        # This defines the data that is treated as input to the Hadoop job.

        if not self.cached_hadoop_requirements:
            # We want to pass in the historical data as well as the overwritten output
            # of LastDailyIpAddressOfUserTask to the hadoop job.
            # So go find whatever is there in the historical date range.
            # This allows us in future to collapse historical data into fewer files,
            # if we felt that was worth the effort.  For example, a month's worth
            # of daily files could be cooked down into a single file representing the
            # last IP address for users per course in that month.  This code wouldn't
            # care.
            path_selection_interval = luigi.date_interval.Custom(self.interval.date_a, self.overwrite_from_date)
            last_ip_of_user_root = url_path_join(self.warehouse_path, 'last_ip_of_user_id')
            path_selection_task = PathSelectionByDateIntervalTask(
                source=[last_ip_of_user_root],
                pattern=[LastDailyIpAddressOfUserTask.FILEPATH_PATTERN],
                interval=path_selection_interval,
                expand_interval=datetime.timedelta(0),
                date_pattern='%Y-%m-%d',
            )

            requirements = {
                'path_selection_task': path_selection_task,
            }

            if self.overwrite_n_days > 0:
                # LastDailyIpAddressOfUserTask returns the marker as output,
                # so we need custom logic to pass the output of
                # LastDailyIpAddressOfUserTask as actual hadoop input to this job.
                downstream_input_tasks = self.requires_local()['user_addresses_task'].downstream_input_tasks()
                requirements['downstream_input_tasks'] = downstream_input_tasks

            self.cached_hadoop_requirements = requirements

        return self.cached_hadoop_requirements

    def output_url(self):
        """Return URL for output."""
        return self.hive_partition_path('last_country_of_user_id', self.interval.date_b)  # pylint: disable=no-member

    def output(self):
        return get_target_from_url(self.output_url())

    def complete(self):
        if self.overwrite and not self.attempted_removal:
            return False
        else:
            return get_target_from_url(url_path_join(self.output_url(), '_SUCCESS')).exists()

    def run(self):
        self.remove_output_on_overwrite()
        output_target = self.output()
        # This is different from remove_output_on_overwrite()
        # in that it also removes the target directory if
        # the success marker file is missing.
        if not self.complete() and output_target.exists():
            output_target.remove()
        super(LastCountryOfUser, self).run()

    def mapper(self, line):
        record = LastIpAddressRecord.from_tsv(line)

        # Output all events for a user_id, regardless of course (for now).
        # (When including course_id, it should be included in the value, not the key.
        # That way we can provide an appropriate default value for the user for
        # their latest ip_address in any (or in no) course.)
        yield record.user_id, (record.timestamp, record.ip_address)

    def reducer(self, key, values):
        """Outputs country for last ip address associated with a user."""

        # DON'T presort input values (by timestamp).  The data potentially takes up too
        # much memory.  Scan the input values instead.

        # We assume the timestamp values (strings) are in ISO
        # representation, so that they can be compared as strings.
        user_id = key
        last_ip = None
        last_timestamp = ""
        for timestamp, ip_address in values:
            if timestamp > last_timestamp:
                last_ip = ip_address
                last_timestamp = timestamp

        if not last_ip:
            return

        debug_message = u"user '{}' on '{}'".format(user_id, last_timestamp)
        country = self.get_country_name(last_ip, debug_message)
        code = self.get_country_code(last_ip, debug_message)

        # Add the user_id for debugging purposes.  (Not needed for counts.)
        yield (country.encode('utf8'), code.encode('utf8')), user_id


class LastCountryOfUserRecord(Record):
    """For a given user_id, stores information about last country."""
    country_name = StringField(length=255, description="Name of last country.")
    country_code = StringField(length=10, description="Code for last country.")
    user_id = IntegerField(description="User ID of user with country information.")


class LastCountryOfUserTableTask(BareHiveTableTask):
    """The hive table for last-country-of-user data."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'last_country_of_user_id'

    @property
    def columns(self):
        return LastCountryOfUserRecord.get_hive_schema()


class LastCountryOfUserPartitionTask(LastCountryOfUserDownstreamMixin, HivePartitionTask):
    """
    Creates a Hive Table that points to Hadoop output of LastCountryOfUser task.

    """

    @property
    def partition_value(self):
        # Because this data comes from EventLogSelectionDownstreamMixin,
        # we will use the end of the interval used to calculate
        # the country information.
        return self.interval.date_b.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return LastCountryOfUserTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return LastCountryOfUser(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )


class ExternalLastCountryOfUserToHiveTask(LastCountryOfUserPartitionTask):

    interval = None
    interval_start = None
    interval_end = None
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        self.interval_start = kwargs.get('date')
        self.interval_end = kwargs.get('date')
        super(ExternalLastCountryOfUserToHiveTask, self).__init__(*args, **kwargs)

    @property
    def partition_value(self):
        return self.date.isoformat()

    @property
    def data_task(self):
        url = super(ExternalLastCountryOfUserToHiveTask, self).data_task.output_url()
        return ExternalURL(url.rstrip('/') + '/')


class LastCountryPerCourseRecord(Record):
    """For a given course, stores aggregates about last country."""
    date = DateField(nullable=False, description="Date of course enrollment data.")
    course_id = StringField(length=255, nullable=False, description="Course ID for course/last-country pair being counted.")
    country_code = StringField(length=10, nullable=True, description="Code for country in course/last-country pair being counted.")
    count = IntegerField(nullable=False, description="Number enrolled in course whose current last country code matches.")
    cumulative_count = IntegerField(nullable=False, description="Number ever enrolled in course whose current last-country code matches.")


class QueryLastCountryPerCourseTask(
        LastCountryOfUserDownstreamMixin,
        HiveQueryTask):
    """Defines task to perform join in Hive to find course enrollment per-country counts."""

    # TODO: note that this doesn't have partitions.  At some point this can be refactored
    # to use PartitionTask and TableTask.
    def query(self):

        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                `date` STRING,
                course_id STRING,
                country_code STRING,
                count INT,
                cumulative_count INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE {table_name}
            SELECT
                sce.dt,
                sce.course_id,
                uc.country_code,
                sum(if(sce.is_active, 1, 0)),
                count(sce.user_id)
            FROM student_courseenrollment sce
            LEFT OUTER JOIN last_country_of_user_id uc on sce.user_id = uc.user_id
            GROUP BY sce.dt, sce.course_id, uc.country_code;
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            location=self.table_location,
            table_name=self.table,
        )
        log.debug('Executing hive query: %s', query)
        return query

    def run(self):
        self.remove_output_on_overwrite()
        super(QueryLastCountryPerCourseTask, self).run()

    @property
    def table(self):
        """Provides name of Hive database table."""
        return 'course_enrollment_location_current'

    @property
    def table_location(self):
        """Provides root location of Hive database table's data."""
        return url_path_join(self.warehouse_path, self.table) + '/'

    def output(self):
        return get_target_from_url(self.table_location)

    def requires(self):
        # Note that import parameters not included are 'destination', 'num_mappers', 'verbose',
        # and 'date' -- we will use the default values for those.
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
        }
        yield (
            LastCountryOfUserPartitionTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
                interval=self.interval,
                interval_start=self.interval_start,
                interval_end=self.interval_end,
                overwrite_n_days=self.overwrite_n_days,
                geolocation_data=self.geolocation_data,
                overwrite=self.overwrite,
            ),
            ImportStudentCourseEnrollmentTask(**kwargs_for_db_import),
        )


@workflow_entry_point
class InsertToMysqlLastCountryPerCourseTask(
        LastCountryOfUserDownstreamMixin,
        MysqlInsertTask):  # pylint: disable=abstract-method
    """
    Define course_enrollment_location_current table.
    """
    @property
    def table(self):
        return "course_enrollment_location_current"

    @property
    def columns(self):
        return LastCountryPerCourseRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
            # Note that the order here is extremely important. The API query pattern needs to filter first by course and
            # then by date.
            ('course_id', 'date'),
        ]

    @property
    def insert_source_task(self):
        return QueryLastCountryPerCourseTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )
