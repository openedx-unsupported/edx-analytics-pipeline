"""Compute metrics related to user enrollments in courses"""

import logging
import textwrap
import datetime

import luigi


from edx.analytics.tasks.database_imports import ImportAuthUserProfileTask, ImportIntoHiveTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin, MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog, opaque_key_util
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask


log = logging.getLogger(__name__)
DEACTIVATED = 'edx.course.enrollment.deactivated'
ACTIVATED = 'edx.course.enrollment.activated'
MODE_CHANGED = 'edx.course.enrollment.mode_changed'


class CourseEnrollmentTask(EventLogSelectionMixin, MapReduceJobTask):
    """Produce a data set that shows which days each user was enrolled in each course."""

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            return

        if event_type not in (DEACTIVATED, ACTIVATED, MODE_CHANGED):
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            log.error("encountered event with bad timestamp: %s", event)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        course_id = event_data.get('course_id')
        if course_id is None or not opaque_key_util.is_valid_course_id(course_id):
            log.error("encountered explicit enrollment event with invalid course_id: %s", event)
            return

        user_id = event_data.get('user_id')
        if user_id is None:
            log.error("encountered explicit enrollment event with no user_id: %s", event)
            return

        mode = event_data.get('mode')
        if mode is None:
            log.error("encountered explicit enrollment event with no mode: %s", event)
            return

        yield (course_id, user_id), (timestamp, event_type, mode)

    def reducer(self, key, values):
        """Emit records for each day the user was enrolled in the course."""
        course_id, user_id = key

        event_stream_processor = DaysEnrolledForEvents(course_id, user_id, self.interval, values)
        for day_enrolled_record in event_stream_processor.days_enrolled():
            yield day_enrolled_record

    def output(self):
        return get_target_from_url(self.output_root)


class EnrollmentEvent(object):
    """The critical information necessary to process the event in the event stream."""

    def __init__(self, timestamp, event_type, mode):
        self.timestamp = timestamp
        self.datestamp = eventlog.timestamp_to_datestamp(timestamp)
        self.event_type = event_type
        self.mode = mode


class DaysEnrolledForEvents(object):
    """
    Determine which days a user was enrolled in a course given a stream of enrollment events.

    Produces a record for each date in which the user was enrolled in the course. Note that the user need not have been
    enrolled in the course for the entire day. These records will have the following format:

        datestamp (str): The date the user was enrolled in the course during.
        course_id (str): Identifies the course the user was enrolled in.
        user_id (int): Identifies the user that was enrolled in the course.
        enrolled_at_end (int): 1 if the user was still enrolled in the course at the end of the day.
        change_since_last_day (int): 1 if the user has changed to the enrolled state, -1 if the user has changed
            to the unenrolled state and 0 if the user's enrollment state hasn't changed.

    If the first event in the stream for a user in a course is an unenrollment event, that would indicate that the user
    was enrolled in the course before that moment in time. It is unknown, however, when the user enrolled in the course,
    so we conservatively omit records for the time before that unenrollment event even though it is likely they were
    enrolled in the course for some unknown amount of time before then. Enrollment counts for dates before the
    unenrollment event will be less than the actual value.

    If the last event for a user is an enrollment event, that would indicate that the user was still enrolled in the
    course at the end of the interval, so records are produced from that last enrollment event all the way to the end of
    the interval. If we miss an unenrollment event after this point, it will result in enrollment counts that are
    actually higher than the actual value.

    Both of the above paragraphs describe edge cases that account for the majority of the error that can be observed in
    the results of this analysis.

    Ranges of dates where the user is continuously enrolled will be represented as contiguous records with the first
    record indicating the change (new enrollment), and the last record indicating the unenrollment. It will look
    something like this::

        datestamp,enrolled_at_end,change_since_last_day
        2014-01-01,1,1
        2014-01-02,1,0
        2014-01-03,1,0
        2014-01-04,0,-1

    The above activity indicates that the user enrolled in the course on 2014-01-01 and unenrolled from the course on
    2014-01-04.

    If a user enrolls and unenrolls from a course on the same day, a record will appear that looks like this::

        datestamp,enrolled_at_end,change_since_last_day
        2014-01-01,0,0

    Args:
        course_id (str): Identifies the course the user was enrolled in.
        user_id (int): Identifies the user that was enrolled in the course.
        interval (luigi.date_interval.DateInterval): The interval of time in which these enrollment events took place.
        events (iterable): The enrollment events as produced by the map tasks. This is expected to be an iterable
            structure whose elements are tuples consisting of a timestamp and an event type.

    """

    ENROLLED = 1
    UNENROLLED = 0
    MODE_UNKNOWN = 'unknown'

    def __init__(self, course_id, user_id, interval, events):
        self.course_id = course_id
        self.user_id = user_id
        self.interval = interval

        self.sorted_events = sorted(events)
        # After sorting, we can discard time information since we only care about date transitions.
        self.sorted_events = [
            EnrollmentEvent(timestamp, event_type, mode) for timestamp, event_type, mode in self.sorted_events
        ]
        # Since each event looks ahead to see the time of the next event, insert a dummy event at then end that
        # indicates the end of the requested interval. If the user's last event is an enrollment activation event then
        # they are assumed to be enrolled up until the end of the requested interval. Note that the mapper ensures that
        # no events on or after date_b are included in the analyzed data set.
        self.sorted_events.append(EnrollmentEvent(self.interval.date_b.isoformat(), None, None))  # pylint: disable=no-member

        self.first_event = self.sorted_events[0]

        # track the previous state in order to easily detect state changes between days.
        if self.first_event.event_type == DEACTIVATED:
            # First event was an unenrollment event, assume the user was enrolled before that moment in time.
            log.warning('First event is an unenrollment for user %d in course %s on %s',
                        self.user_id, self.course_id, self.first_event.datestamp)
        elif self.first_event.event_type == MODE_CHANGED:
            log.warning('First event is a mode change for user %d in course %s on %s',
                        self.user_id, self.course_id, self.first_event.datestamp)

        # Before we start processing events, we can assume that their current state is the same as it has been for all
        # time before the first event.
        self.state = self.previous_state = self.UNENROLLED
        self.mode = self.MODE_UNKNOWN

    def days_enrolled(self):
        """
        A record is yielded for each day during which the user was enrolled in the course.

        Yields:
            tuple: An enrollment record for each day during which the user was enrolled in the course.

        """
        # The last element of the list is a placeholder indicating the end of the interval. Don't process it.
        for index in range(len(self.sorted_events) - 1):
            self.event = self.sorted_events[index]
            self.next_event = self.sorted_events[index + 1]

            self.change_state()

            if self.event.datestamp != self.next_event.datestamp:
                change_since_last_day = self.state - self.previous_state

                if self.state == self.ENROLLED:
                    # There may be a very wide gap between this event and the next event. If the user is currently
                    # enrolled, we can assume they continue to be enrolled at least until the next day we see an event.
                    # Emit records for each of those intermediary days. Since the end of the interval is represented by
                    # a dummy event at the end of the list of events, it will be represented by self.next_event when
                    # processing the last real event in the stream. This allows the records to be produced up to the end
                    # of the interval if the last known state was "ENROLLED".
                    for datestamp in self.all_dates_between(self.event.datestamp, self.next_event.datestamp):
                        yield self.enrollment_record(
                            datestamp,
                            self.ENROLLED,
                            change_since_last_day if datestamp == self.event.datestamp else 0,
                            self.mode
                        )
                else:
                    # This indicates that the user was enrolled at some point on this day, but was not enrolled as of
                    # 23:59:59.999999.
                    yield self.enrollment_record(
                        self.event.datestamp,
                        self.UNENROLLED,
                        change_since_last_day,
                        self.mode
                    )

                self.previous_state = self.state

    def all_dates_between(self, start_date_str, end_date_str):
        """
        All dates from the start date up to the end date.

        Yields:
            str: ISO 8601 datestamp for each date from the first date (inclusive) up to the end date (exclusive).

        """
        current_date = self.parse_date_string(start_date_str)
        end_date = self.parse_date_string(end_date_str)

        while current_date < end_date:
            yield current_date.isoformat()
            current_date += datetime.timedelta(days=1)

    def parse_date_string(self, date_str):
        """Efficiently parse an ISO 8601 date stamp into a datetime.date() object."""
        date_parts = [int(p) for p in date_str.split('-')[:3]]
        return datetime.date(*date_parts)

    def enrollment_record(self, datestamp, enrolled_at_end, change_since_last_day, mode_at_end):
        """A complete enrollment record."""
        return (datestamp, self.course_id, self.user_id, enrolled_at_end, change_since_last_day, mode_at_end)

    def change_state(self):
        """Change state when appropriate.

        Note that in spite of our best efforts some events might be lost, causing invalid state transitions.
        """
        self.mode = self.event.mode

        if self.state == self.ENROLLED and self.event.event_type == DEACTIVATED:
            self.state = self.UNENROLLED
        elif self.state == self.UNENROLLED and self.event.event_type == ACTIVATED:
            self.state = self.ENROLLED
        elif self.event.event_type == MODE_CHANGED:
            pass
        else:
            log.warning(
                'No state change for %s event. User %d is already in the requested state for course %s on %s.',
                self.event.event_type, self.user_id, self.course_id, self.event.datestamp
            )


class CourseEnrollmentTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the CourseEnrollmentTable task."""
    pass


class CourseEnrollmentTable(CourseEnrollmentTableDownstreamMixin, ImportIntoHiveTableTask):
    """Hive table that stores the set of users enrolled in each course over time."""

    @property
    def table_name(self):
        return 'course_enrollment'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('user_id', 'INT'),
            ('at_end', 'TINYINT'),
            ('change', 'TINYINT'),
            ('mode', 'STRING'),
        ]

    @property
    def table_location(self):
        return url_path_join(self.warehouse_path, self.table_name)

    @property
    def table_format(self):
        """Provides name of Hive database table."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"

    @property
    def partition_date(self):
        return self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def requires(self):
        return CourseEnrollmentTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )


class EnrollmentCourseBlacklistTable(WarehouseMixin, ImportIntoHiveTableTask):
    """The set of courses to exclude from enrollment metrics due to incomplete input data."""

    blacklist_date = luigi.Parameter(
        default_from_config={'section': 'enrollments', 'name': 'blacklist_date'}
    )

    @property
    def table_name(self):
        return 'course_enrollment_blacklist'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
        ]

    @property
    def table_location(self):
        return url_path_join(self.warehouse_path, self.table_name)

    @property
    def partition_date(self):
        return self.blacklist_date

    @property
    def table_format(self):
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"


class EnrollmentDemographicTask(CourseEnrollmentTableDownstreamMixin, ImportIntoHiveTableTask):
    """Base class for demographic breakdowns of enrollments"""

    def query(self):
        create_table_statements = super(EnrollmentDemographicTask, self).query()
        query_format = textwrap.dedent("""
            INSERT OVERWRITE TABLE {table_name}
            PARTITION (dt='{partition_date}')
            SELECT e.*
            FROM
            (
                {insert_query}
            ) e
            LEFT OUTER JOIN course_enrollment_blacklist b ON (e.course_id = b.course_id)
            WHERE b.course_id IS NULL;
        """)

        insert_query_statements = query_format.format(
            table_name=self.table_name,
            partition_date=self.partition_date,
            insert_query=textwrap.dedent(self.insert_query)
        )

        query = create_table_statements + insert_query_statements
        log.debug('Executing hive query: %s', query)
        return query

    @property
    def insert_query(self):
        """Query the data to insert into the table."""
        raise NotImplementedError

    @property
    def table_name(self):
        raise NotImplementedError

    @property
    def columns(self):
        raise NotImplementedError

    @property
    def table_location(self):
        return url_path_join(self.warehouse_path, self.table_name)

    @property
    def table_format(self):
        """Provides format of Hive external table data."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"

    @property
    def partition_date(self):
        return self.interval.date_b.isoformat()  # pylint: disable=no-member

    def output(self):
        # partition_location is a property depending on table_location and partitions
        return get_target_from_url(self.partition_location)

    def requires(self):
        yield (
            CourseEnrollmentTable(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
            ),
            ImportAuthUserProfileTask(),
            EnrollmentCourseBlacklistTable(
                warehouse_path=self.warehouse_path
            )
        )


class EnrollmentByGenderTask(EnrollmentDemographicTask):
    """Breakdown of enrollments by gender as reported by the user"""

    @property
    def insert_query(self):
        return """
            SELECT
                ce.date,
                ce.course_id,
                IF(p.gender != '', p.gender, NULL),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
            WHERE ce.at_end = 1
            GROUP BY
                ce.date,
                ce.course_id,
                IF(p.gender != '', p.gender, NULL)
            """

    @property
    def table_name(self):
        return 'course_enrollment_gender_daily'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('gender', 'STRING'),
            ('count', 'INT'),
        ]


class ImportEnrollmentByGenderIntoMysql(CourseEnrollmentTableDownstreamMixin, MysqlInsertTask):
    """Load gender breakdowns into MySQL"""

    overwrite = luigi.BooleanParameter(default=True)

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('gender', 'VARCHAR(6)'),
            ('count', 'INTEGER'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('date', 'course_id'),
        ]

    @property
    def table(self):
        return 'course_enrollment_gender_daily'

    @property
    def insert_source_task(self):
        return EnrollmentByGenderTask(
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class EnrollmentByBirthYearTask(EnrollmentDemographicTask):
    """Breakdown of enrollments by age as reported by the user"""

    @property
    def insert_query(self):
        return """
            SELECT
                ce.date,
                ce.course_id,
                p.year_of_birth,
                COUNT(ce.user_id)
            FROM course_enrollment ce
            LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
            WHERE ce.at_end = 1
            GROUP BY
                ce.date,
                ce.course_id,
                p.year_of_birth
            """

    @property
    def table_name(self):
        return 'course_enrollment_birth_year_daily'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('birth_year', 'INT'),
            ('count', 'INT'),
        ]


class ImportEnrollmentByBirthYearIntoMysql(CourseEnrollmentTableDownstreamMixin, MysqlInsertTask):
    """Load age breakdowns into MySQL"""

    overwrite = luigi.BooleanParameter(default=True)

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('birth_year', 'INTEGER'),
            ('count', 'INTEGER'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('date', 'course_id'),
        ]

    @property
    def table(self):
        return 'course_enrollment_birth_year_daily'

    @property
    def insert_source_task(self):
        return EnrollmentByBirthYearTask(
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class EducationLevelCodeMappingTask(luigi.Task):
    """A static table that maps the education level codes found in auth_userprofile to canonical codes."""

    output_root = luigi.Parameter()

    MAPPING = {
        'none': 'none',
        'other': 'other',
        'el': 'primary',
        'jhs': 'junior_secondary',
        'hs': 'secondary',
        'a': 'associates',
        'b': 'bachelors',
        'm': 'masters',
        'p': 'doctorate',
        'p_se': 'doctorate',
        'p_oth': 'doctorate'
    }

    def run(self):
        with self.output().open('w') as output_file:
            for item in self.MAPPING.iteritems():
                output_file.write('\t'.join(item))
                output_file.write('\n')

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'mapping.tsv'))


class EducationLevelCodeMappingTableTask(WarehouseMixin, ImportIntoHiveTableTask):
    """A hive table for the code mapping."""

    @property
    def table_name(self):
        return 'education_level'

    @property
    def columns(self):
        return [
            ('auth_userprofile_code', 'STRING'),
            ('education_level_code', 'STRING')
        ]

    @property
    def table_location(self):
        return url_path_join(self.warehouse_path, self.table_name)

    @property
    def partition_date(self):
        return 'default'

    @property
    def table_format(self):
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'"

    def requires(self):
        yield EducationLevelCodeMappingTask(output_root=self.partition_location)


class EnrollmentByEducationLevelTask(EnrollmentDemographicTask):
    """Breakdown of enrollments by education level as reported by the user"""

    @property
    def insert_query(self):
        return """
            SELECT
                ce.date,
                ce.course_id,
                el.education_level_code,
                COUNT(ce.user_id)
            FROM course_enrollment ce
            LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
            LEFT OUTER JOIN education_level el ON el.auth_userprofile_code = p.level_of_education
            WHERE ce.at_end = 1
            GROUP BY
                ce.date,
                ce.course_id,
                el.education_level_code
            """

    @property
    def table_name(self):
        return 'course_enrollment_education_level_daily'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('education_level', 'STRING'),
            ('count', 'INT'),
        ]

    def requires(self):
        for req in luigi.task.flatten(super(EnrollmentByEducationLevelTask, self).requires()):
            yield req

        yield EducationLevelCodeMappingTableTask(
            warehouse_path=self.warehouse_path
        )


class ImportEnrollmentByEducationLevelIntoMysql(CourseEnrollmentTableDownstreamMixin, MysqlInsertTask):
    """Load education level breakdowns into MySQL"""

    overwrite = luigi.BooleanParameter(default=True)

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('education_level', 'VARCHAR(16)'),
            ('count', 'INTEGER'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('date', 'course_id'),
        ]

    @property
    def table(self):
        return 'course_enrollment_education_level_daily'

    @property
    def insert_source_task(self):
        return EnrollmentByEducationLevelTask(
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class EnrollmentByModeTask(EnrollmentDemographicTask):
    """Breakdown of enrollments by age as reported by the user"""

    @property
    def insert_query(self):
        return """
            SELECT
                ce.date,
                ce.course_id,
                ce.mode,
                COUNT(ce.user_id)
            FROM course_enrollment ce
            WHERE ce.at_end = 1
            GROUP BY
                ce.date,
                ce.course_id,
                ce.mode
            """

    @property
    def table_name(self):
        return 'course_enrollment_mode_daily'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('mode', 'STRING'),
            ('count', 'INT'),
        ]


class ImportEnrollmentByModeIntoMysql(CourseEnrollmentTableDownstreamMixin, MysqlInsertTask):
    """Load age breakdowns into MySQL"""

    overwrite = luigi.BooleanParameter(default=True)

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('mode', 'VARCHAR(255) NOT NULL'),
            ('count', 'INTEGER'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('date', 'course_id'),
        ]

    @property
    def table(self):
        return 'course_enrollment_mode_daily'

    @property
    def insert_source_task(self):
        return EnrollmentByModeTask(
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class ImportDemographicsIntoMysql(CourseEnrollmentTableDownstreamMixin, luigi.WrapperTask):
    """Import all demographic breakdowns of enrollment into MySQL"""

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            ImportEnrollmentByGenderIntoMysql(**kwargs),
            ImportEnrollmentByBirthYearIntoMysql(**kwargs),
            ImportEnrollmentByEducationLevelIntoMysql(**kwargs)
        )
