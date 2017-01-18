"""Compute metrics related to user enrollments in courses"""

import logging
import datetime

import luigi
import luigi.task
from luigi.parameter import DateIntervalParameter

from edx.analytics.tasks.database_imports import ImportAuthUserProfileTask
from edx.analytics.tasks.load_internal_reporting_course_catalog import (
    CoursePartitionTask,
    LoadInternalReportingCourseCatalogMixin,
)
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin, MapReduceJobTask, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import (
    PathSelectionByDateIntervalTask,
    EventLogSelectionDownstreamMixin,
    EventLogSelectionMixin,
)
from edx.analytics.tasks.url import get_target_from_url, url_path_join, ExternalURL, UncheckedExternalURL
from edx.analytics.tasks.util import eventlog, opaque_key_util
from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveQueryToMysqlTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, BooleanField, DateTimeField

log = logging.getLogger(__name__)
DEACTIVATED = 'edx.course.enrollment.deactivated'
ACTIVATED = 'edx.course.enrollment.activated'
MODE_CHANGED = 'edx.course.enrollment.mode_changed'
ENROLLED = 1
UNENROLLED = 0


class CourseEnrollmentEventsTask(
        OverwriteOutputMixin,
        WarehouseMixin,
        EventLogSelectionMixin,
        MultiOutputMapReduceJobTask):
    """
    Task to extract enrollment events from eventlogs over a given interval.
    This would produce a different output file for each day within the interval
    containing that day's enrollment events only.
    """

    # FILEPATH_PATTERN should match the output files defined by output_path_for_key().
    FILEPATH_PATTERN = '.*?course_enrollment_events_(?P<date>\\d{4}-\\d{2}-\\d{2})'

    # We use warehouse_path to generate the output path, so we make this a non-param.
    output_root = None

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

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

        course_id = opaque_key_util.normalize_course_id(event_data.get('course_id'))
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

        yield date_string, (course_id, user_id, timestamp, event_type, mode)

    def multi_output_reducer(self, _date_string, values, output_file):
        for value in values:
            output_file.write('\t'.join([str(field) for field in value]))
            output_file.write('\n')

    def output_path_for_key(self, key):
        date_string = key
        return url_path_join(
            self.hive_partition_path('course_enrollment_events', date_string),
            'course_enrollment_events_{date}'.format(
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
        super(CourseEnrollmentEventsTask, self).run()

        # This makes sure that a output file exists for each date in the interval
        # as downstream tasks require that they exist.
        for date in self.interval:
            url = self.output_path_for_key(date.isoformat())
            target = get_target_from_url(url)
            if not target.exists():
                target.open("w").close()  # touch the file


class CourseEnrollmentDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the CourseEnrollmentTask task."""

    # Make the interval be optional:
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to extract enrollments events for. '
        'If not specified, `interval_start` and `interval_end` are used to construct the `interval`.',
    )

    # Define optional parameters, to be used if 'interval' is not defined.
    interval_start = luigi.DateParameter(
        config_path={'section': 'enrollments', 'name': 'interval_start'},
        significant=False,
        description='The start date to extract enrollments events for.  Ignored if `interval` is provided.',
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='The end date to extract enrollments events for.  Ignored if `interval` is provided. '
        'Default is today, UTC.',
    )

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'enrollments', 'name': 'overwrite_n_days'},
        significant=False,
        description='This parameter is used by CourseEnrollmentTask which will overwrite course enrollment '
                    ' events for the most recent n days.'
    )

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentDownstreamMixin, self).__init__(*args, **kwargs)

        if not self.interval:
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)


class CourseEnrollmentTask(CourseEnrollmentDownstreamMixin, MapReduceJobTask):
    """Produce a data set that shows which days each user was enrolled in each course."""

    output_root = luigi.Parameter()

    enable_direct_output = True

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentTask, self).__init__(*args, **kwargs)

        self.overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)
        self.course_enrollment_events_root = url_path_join(self.warehouse_path, 'course_enrollment_events')

    def requires_local(self):
        if self.overwrite_n_days == 0:
            return []

        overwrite_interval = DateIntervalParameter().parse('{}-{}'.format(
            self.overwrite_from_date,
            self.interval.date_b
        ))

        return CourseEnrollmentEventsTask(
            interval=overwrite_interval,
            source=self.source,
            pattern=self.pattern,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=True,
        )

    def requires_hadoop(self):
        # We want to pass in the historical data as well as the output of CourseEnrollmentEventsTask to the hadoop job.
        # CourseEnrollmentEventsTask returns the marker as output, so we need custom logic to pass the output
        # of CourseEnrollmentEventsTask as actual hadoop input to this job.

        path_selection_interval = DateIntervalParameter().parse('{}-{}'.format(
            self.interval.date_a,
            self.overwrite_from_date,
        ))

        path_selection_task = PathSelectionByDateIntervalTask(
            source=[self.course_enrollment_events_root],
            interval=path_selection_interval,
            pattern=[CourseEnrollmentEventsTask.FILEPATH_PATTERN],
            expand_interval=datetime.timedelta(0),
            date_pattern='%Y-%m-%d',
        )

        requirements = {
            'path_selection_task': path_selection_task,
        }

        if self.overwrite_n_days > 0:
            requirements['downstream_input_tasks'] = self.requires_local().downstream_input_tasks()

        return requirements

    def mapper(self, line):
        (
            course_id,
            user_id,
            timestamp,
            event_type,
            mode
        ) = line.split('\t')
        yield ((course_id, user_id), (timestamp, event_type, mode))

    def reducer(self, key, values):
        """Emit records for each day the user was enrolled in the course."""
        course_id, user_id = key

        event_stream_processor = DaysEnrolledForEvents(course_id, user_id, self.interval, values)
        for day_enrolled_record in event_stream_processor.days_enrolled():
            yield day_enrolled_record

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        output_target = self.output()
        if not self.complete() and output_target.exists():
            output_target.remove()

        super(CourseEnrollmentTask, self).run()


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

    Produces a record for each date from the date the user enrolled in the course for the first time to the end of the
    interval. Note that the user need not have been enrolled in the course for the entire day. These records will have
    the following format:

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
        2014-01-05,0,0

    The above activity indicates that the user enrolled in the course on 2014-01-01 and unenrolled from the course on
    2014-01-04. Records are created for every date after the date when they first enrolled.

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
        self.state = self.previous_state = UNENROLLED
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

                # There may be a very wide gap between this event and the next event. If the user is currently
                # enrolled, we can assume they continue to be enrolled at least until the next day we see an event.
                # Emit records for each of those intermediary days. Since the end of the interval is represented by
                # a dummy event at the end of the list of events, it will be represented by self.next_event when
                # processing the last real event in the stream. This allows the records to be produced up to the end
                # of the interval if the last known state was "ENROLLED".
                for datestamp in self.all_dates_between(self.event.datestamp, self.next_event.datestamp):
                    yield self.enrollment_record(
                        datestamp,
                        self.state,
                        change_since_last_day if datestamp == self.event.datestamp else 0,
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

        if self.state == ENROLLED and self.event.event_type == DEACTIVATED:
            self.state = UNENROLLED
        elif self.state == UNENROLLED and self.event.event_type == ACTIVATED:
            self.state = ENROLLED
        elif self.event.event_type == MODE_CHANGED:
            pass
        else:
            log.warning(
                'No state change for %s event. User %d is already in the requested state for course %s on %s.',
                self.event.event_type, self.user_id, self.course_id, self.event.datestamp
            )


class CourseEnrollmentTableTask(CourseEnrollmentDownstreamMixin, HiveTableTask):
    """Hive table that stores the set of users enrolled in each course over time."""

    @property
    def table(self):
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
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return CourseEnrollmentTask(
            mapreduce_engine=self.mapreduce_engine,
            warehouse_path=self.warehouse_path,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
            overwrite_n_days=self.overwrite_n_days,
        )


class ExternalCourseEnrollmentTableTask(CourseEnrollmentTableTask):

    def requires(self):
        yield ExternalURL(
            url=url_path_join(self.warehouse_path, 'course_enrollment', self.partition.path_spec) + '/'
        )


class EnrollmentSummaryRecord(Record):
    """Summarizes a user's enrollment history for a particular course."""

    course_id = StringField(length=255, nullable=False, description='Course the learner enrolled in.')
    user_id = IntegerField(nullable=False, description='The user\'s numeric identifier.')
    current_enrollment_mode = StringField(
        length=100,
        nullable=False,
        description='The last mode seen on an activation or mode change event.'
    )
    current_enrollment_is_active = BooleanField(
        nullable=False,
        description='True if the user is currently enrolled as of the end of the interval.'
    )
    first_enrollment_mode = StringField(length=100, nullable=True, description='The mode the user first enrolled with.')
    first_enrollment_time = DateTimeField(nullable=True, description='The time of the user\'s first enrollment.')
    last_unenrollment_time = DateTimeField(nullable=True, description='The time of the user\'s last unenrollment.')
    first_verified_enrollment_time = DateTimeField(
        nullable=True,
        description='The time the user first switched to the verified track.'
    )
    first_credit_enrollment_time = DateTimeField(
        nullable=True,
        description='The time the user first switched to the credit track.'
    )
    end_time = DateTimeField(nullable=False, description='The end of the interval that was analyzed.')


class CourseEnrollmentSummaryTask(CourseEnrollmentTask):
    """Produce a data set that captures critical details about a user's enrollment history in each course."""

    def reducer(self, key, values):
        """Emit one record per user course enrollment, summarizing their enrollment activity."""
        course_id, user_id = key

        sorted_events = sorted(values)
        sorted_events = [
            EnrollmentEvent(timestamp, event_type, mode) for timestamp, event_type, mode in sorted_events
        ]
        first_enroll_event = None
        last_unenroll_event = None
        first_event_by_mode = {}
        most_recent_mode = None
        state = UNENROLLED

        for event in sorted_events:
            is_enrolled_mode_change = (state == ENROLLED and event.event_type == MODE_CHANGED)
            is_enrolled_deactivate = (state == ENROLLED and event.event_type == DEACTIVATED)
            is_unenrolled_activate = (state == UNENROLLED and event.event_type == ACTIVATED)

            if is_enrolled_deactivate:
                state = UNENROLLED
                last_unenroll_event = event
                # If we see more than one deactivate in a row, we only consider the first one as the last unenrollment.
                if event.mode != most_recent_mode:
                    self.incr_counter('Enrollment State', 'Deactivation Mode Changed', 1)
            elif is_unenrolled_activate or is_enrolled_mode_change:
                if event.event_type == ACTIVATED:
                    # If we see multiple activation events in a row, consider the first one to be the first enrollment.
                    state = ENROLLED
                    if first_enroll_event is None:
                        first_enroll_event = event

                if event.event_type == MODE_CHANGED:
                    if event.mode == most_recent_mode:
                        self.incr_counter('Enrollment State', 'Redundant Mode Change', 1)

                # The most recent mode is computed from the activation and mode changes. If we see a different mode
                # on the deactivation event, it is ignored. It's unclear in many of these cases which event to trust,
                # so fairly arbitrary decisions have been made.
                most_recent_mode = event.mode
                if event.mode not in first_event_by_mode:
                    first_event_by_mode[event.mode] = event
            else:
                # increment counters for invalid events
                if state == ENROLLED and event.event_type == ACTIVATED:
                    if event.mode == most_recent_mode:
                        self.incr_counter('Enrollment State', 'Enrolled Activation', 1)
                    else:
                        self.incr_counter('Enrollment State', 'Enrolled Activation Mode Changed', 1)
                elif state == UNENROLLED:
                    if event.event_type == DEACTIVATED:
                        self.incr_counter('Enrollment State', 'Unenrolled Deactivation', 1)
                    elif event.event_type == MODE_CHANGED:
                        self.incr_counter('Enrollment State', 'Unenrolled Mode Change', 1)

        if first_enroll_event is None:
            # The user only has deactivate and mode change events... that's odd, just throw away the record.
            self.incr_counter('Enrollment State', 'Missing Enrollment Event', 1)
            return

        record = EnrollmentSummaryRecord(
            course_id=course_id,
            user_id=int(user_id),
            current_enrollment_mode=most_recent_mode,
            current_enrollment_is_active=(state == ENROLLED),
            first_enrollment_mode=first_enroll_event.mode,
            first_enrollment_time=self.format_timestamp(first_enroll_event),
            last_unenrollment_time=self.format_timestamp(last_unenroll_event),
            first_verified_enrollment_time=self.format_timestamp(first_event_by_mode.get('verified')),
            first_credit_enrollment_time=self.format_timestamp(first_event_by_mode.get('credit')),
            end_time=DateTimeField().deserialize_from_string(self.interval.date_b.isoformat())
        )
        yield record.to_string_tuple()

    @staticmethod
    def format_timestamp(event):
        """Given an event, return a datetime object for its timestamp."""
        if event is None or event.timestamp is None:
            return None
        return DateTimeField().deserialize_from_string(event.timestamp)


class CourseEnrollmentSummaryTableTask(CourseEnrollmentDownstreamMixin, HiveTableTask):
    """Hive table that stores the set of users enrolled in each course over time."""

    @property
    def table(self):
        return 'course_enrollment_summary'

    @property
    def columns(self):
        return EnrollmentSummaryRecord.get_hive_schema()

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return CourseEnrollmentSummaryTask(
            mapreduce_engine=self.mapreduce_engine,
            warehouse_path=self.warehouse_path,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
            overwrite_n_days=self.overwrite_n_days,
        )


class EnrollmentTask(CourseEnrollmentDownstreamMixin, HiveQueryToMysqlTask):
    """Base class for breakdowns of enrollments"""

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

    @property
    def required_table_tasks(self):
        yield (
            CourseEnrollmentTableTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
                overwrite_n_days=self.overwrite_n_days,
            ),
            ImportAuthUserProfileTask()
        )

    @property
    def query_date(self):
        """We want to store demographics breakdown from the enrollment numbers of most recent day only."""
        query_date = self.interval.date_b - datetime.timedelta(days=1)
        return query_date.isoformat()


class EnrollmentByGenderTask(EnrollmentTask):
    """
    Breakdown of enrollments by gender as reported by the user.
    Note that we do not filter by the query_date here as insights displays breakdown by gender accross multiple days.
    """

    @property
    def query(self):
        return """
            SELECT
                ce.date,
                ce.course_id,
                IF(p.gender != '', p.gender, NULL),
                SUM(ce.at_end),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
            GROUP BY
                ce.date,
                ce.course_id,
                IF(p.gender != '', p.gender, NULL)
        """

    @property
    def table(self):
        return 'course_enrollment_gender_daily'

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('gender', 'VARCHAR(6)'),
            ('count', 'INTEGER'),
            ('cumulative_count', 'INTEGER')
        ]


class EnrollmentByBirthYearTask(EnrollmentTask):
    """Breakdown of enrollments by age as reported by the user"""

    @property
    def query(self):
        query = """
            SELECT
                ce.date,
                ce.course_id,
                p.year_of_birth,
                SUM(ce.at_end),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
            WHERE ce.date = '{date}'
            GROUP BY
                ce.date,
                ce.course_id,
                p.year_of_birth
        """.format(date=self.query_date)
        return query

    @property
    def table(self):
        return 'course_enrollment_birth_year_daily'

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('birth_year', 'INTEGER'),
            ('count', 'INTEGER'),
            ('cumulative_count', 'INTEGER')
        ]


class EnrollmentByEducationLevelTask(EnrollmentTask):
    """Breakdown of enrollments by education level as reported by the user"""

    @property
    def query(self):
        query = """
            SELECT
                ce.date,
                ce.course_id,
                CASE p.level_of_education
                    WHEN 'el'    THEN 'primary'
                    WHEN 'jhs'   THEN 'junior_secondary'
                    WHEN 'hs'    THEN 'secondary'
                    WHEN 'a'     THEN 'associates'
                    WHEN 'b'     THEN 'bachelors'
                    WHEN 'm'     THEN 'masters'
                    WHEN 'p'     THEN 'doctorate'
                    WHEN 'p_se'  THEN 'doctorate'
                    WHEN 'p_oth' THEN 'doctorate'
                    WHEN 'none'  THEN 'none'
                    WHEN 'other' THEN 'other'
                    ELSE NULL
                END,
                SUM(ce.at_end),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
            WHERE ce.date = '{date}'
            GROUP BY
                ce.date,
                ce.course_id,
                CASE p.level_of_education
                    WHEN 'el'    THEN 'primary'
                    WHEN 'jhs'   THEN 'junior_secondary'
                    WHEN 'hs'    THEN 'secondary'
                    WHEN 'a'     THEN 'associates'
                    WHEN 'b'     THEN 'bachelors'
                    WHEN 'm'     THEN 'masters'
                    WHEN 'p'     THEN 'doctorate'
                    WHEN 'p_se'  THEN 'doctorate'
                    WHEN 'p_oth' THEN 'doctorate'
                    WHEN 'none'  THEN 'none'
                    WHEN 'other' THEN 'other'
                    ELSE NULL
                END
        """.format(date=self.query_date)
        return query

    @property
    def table(self):
        return 'course_enrollment_education_level_daily'

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('education_level', 'VARCHAR(16)'),
            ('count', 'INTEGER'),
            ('cumulative_count', 'INTEGER')
        ]


class EnrollmentByModeTask(EnrollmentTask):
    """Breakdown of enrollments by mode"""

    @property
    def query(self):
        query = """
            SELECT
                ce.date,
                ce.course_id,
                ce.mode,
                SUM(ce.at_end),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            GROUP BY
                ce.date,
                ce.course_id,
                ce.mode
        """.format(date=self.query_date)
        return query

    @property
    def table(self):
        return 'course_enrollment_mode_daily'

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('mode', 'VARCHAR(255) NOT NULL'),
            ('count', 'INTEGER'),
            ('cumulative_count', 'INTEGER')
        ]


class EnrollmentDailyTask(EnrollmentTask):
    """A history of the number of students enrolled in each course at the end of each day"""

    @property
    def query(self):
        query = """
            SELECT
                ce.course_id,
                ce.date,
                SUM(ce.at_end),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            GROUP BY
                ce.course_id,
                ce.date
        """.format(date=self.query_date)
        return query

    @property
    def table(self):
        return 'course_enrollment_daily'

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('date', 'DATE NOT NULL'),
            ('count', 'INTEGER'),
            ('cumulative_count', 'INTEGER')
        ]


class CourseSummaryEnrollmentDownstreamMixin(CourseEnrollmentDownstreamMixin, LoadInternalReportingCourseCatalogMixin):
    """Combines course enrollment and catalog parameters."""

    enable_course_catalog = luigi.BooleanParameter(
        config_path={'section': 'course-summary-enrollment', 'name': 'enable_course_catalog'},
        default=False,
        description="Enables course catalog data jobs."
    )


class CourseSummaryEnrollmentRecord(Record):
    """Recent enrollment summary and metadata for a course."""
    course_id = StringField(nullable=False, length=255, description='A unique identifier of the course')
    catalog_course_title = StringField(nullable=True, length=255, normalize_whitespace=True,
                                       description='The name of the course')
    catalog_course = StringField(nullable=True, length=255, description='Course identifier without run')
    start_time = DateTimeField(nullable=True, description='The date and time that the course begins')
    end_time = DateTimeField(nullable=True, description='The date and time that the course ends')
    pacing_type = StringField(nullable=True, length=255, description='The type of pacing for this course')
    availability = StringField(nullable=True, length=255, description='Availability status of the course')
    enrollment_mode = StringField(length=100, nullable=False, description='Enrollment mode for the enrollment counts')
    count = IntegerField(nullable=True, description='The count of currently enrolled learners')
    count_change_7_days = IntegerField(nullable=True,
                                       description='Difference in enrollment counts over the past 7 days')
    cumulative_count = IntegerField(nullable=True, description='The cumulative total of all users ever enrolled')


class ImportCourseSummaryEnrollmentsIntoMysql(CourseSummaryEnrollmentDownstreamMixin,
                                              HiveQueryToMysqlTask):
    """Creates the course summary enrollment sql table."""

    @property
    def indexes(self):
        return [('course_id',)]

    @property
    def query(self):
        end_date = self.interval.date_b - datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=7)

        query = """
            SELECT
                enrollment_end.course_id,
                course.catalog_course_title,
                course.catalog_course,
                course.start_time,
                course.end_time,
                course.pacing_type,
                course.availability,
                enrollment_end.mode,
                enrollment_end.count,
                (enrollment_end.count - COALESCE(enrollment_start.count, 0)) AS count_change_7_days,
                enrollment_end.cumulative_count
            FROM course_enrollment_mode_daily enrollment_end
            LEFT OUTER JOIN course_enrollment_mode_daily enrollment_start
                ON enrollment_start.course_id = enrollment_end.course_id
                AND enrollment_start.mode = enrollment_end.mode
                AND enrollment_start.date = '{start_date}'
            LEFT OUTER JOIN course_catalog course
                ON course.course_id = enrollment_end.course_id
            WHERE enrollment_end.date = '{end_date}'
        """.format(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        return query

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def table(self):
        return 'course_meta_summary_enrollment'

    @property
    def columns(self):
        return CourseSummaryEnrollmentRecord.get_sql_schema()

    @property
    def required_table_tasks(self):
        # Note this is not exactly correct, as what we really need is
        # for enrollment-by-mode data to be in Hive.  If we are rerunning
        # and enrollment-by-mode data has already been calculated, this
        # requirement will be met but no data actually exists in Hive.
        # So when the EnrollmentTask-based tasks is refactored to pull out
        # the loading into Hive, this dependency should change to reflect that.
        yield EnrollmentByModeTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
        )

        # We do not propagate the local overwrite value here.
        # The local value comes from the default for HiveQueryToMysqlTask,
        # which is True (so this table is always overwritten in Mysql,
        # rather than being incrementally updated).  We don't really want
        # the course's partition data to always be updated.
        catalog_tasks = [
            CoursePartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                api_root_url=self.api_root_url,
                api_page_size=self.api_page_size,
            ),
        ]

        if self.enable_course_catalog:
            # create the hive tables and populate them with data
            yield catalog_tasks
        else:
            # the Open edX installation isn't running a catalog service, so just create empty hive tables without
            # loading any data into them
            yield [task.hive_table_task for task in catalog_tasks]


@workflow_entry_point
class ImportEnrollmentsIntoMysql(CourseSummaryEnrollmentDownstreamMixin,
                                 luigi.WrapperTask):
    """Import all breakdowns of enrollment into MySQL"""

    def requires(self):
        enrollment_kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
            'overwrite_n_days': self.overwrite_n_days,
        }

        # We do not override overwrite here for now.
        # We want course_summary to use its own default value.
        course_summary_kwargs = dict({
            'date': self.date,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
            'enable_course_catalog': self.enable_course_catalog,
        }, **enrollment_kwargs)

        yield (
            CourseEnrollmentSummaryTableTask(**enrollment_kwargs),
            EnrollmentByGenderTask(**enrollment_kwargs),
            EnrollmentByBirthYearTask(**enrollment_kwargs),
            EnrollmentByEducationLevelTask(**enrollment_kwargs),
            EnrollmentDailyTask(**enrollment_kwargs),
            ImportCourseSummaryEnrollmentsIntoMysql(**course_summary_kwargs),
        )
