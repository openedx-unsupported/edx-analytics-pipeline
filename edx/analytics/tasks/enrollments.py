"""Compute metrics related to user enrollments in courses"""

import logging
import datetime

import luigi
import luigi.task
import luigi.date_interval

from edx.analytics.tasks.database_imports import ImportAuthUserProfileTask
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin, MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join, ExternalURL
from edx.analytics.tasks.util import eventlog, opaque_key_util
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveQueryToMysqlTask, HivePartitionTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin


log = logging.getLogger(__name__)
DEACTIVATED = 'edx.course.enrollment.deactivated'
ACTIVATED = 'edx.course.enrollment.activated'
MODE_CHANGED = 'edx.course.enrollment.mode_changed'


class CourseEnrollmentTask(EventLogSelectionMixin, MapReduceJobTask):
    """Produce a data set that shows which days each user was enrolled in each course."""

    # TODO: Do we want this turned on for incremental?
    # enable_direct_output = True

    date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    # TODO: This is duplicated in DownstreamMixin.
    interval_start = luigi.DateParameter(
        config_path={'section': 'enrollments', 'name': 'interval_start'},
        significant=False,
    )

    def requires(self):
        # yield CanonicalizationTask(
        #     date=self.date,
        #     overwrite=self.overwrite,
        #     warehouse_path=self.warehouse_path,
        # )
        if self.date > self.interval_start:
            yield CourseEnrollmentTask(
                date=(self.date - datetime.timedelta(days=1)),
                overwrite=self.overwrite,
                warehouse_path=self.warehouse_path,
                n_reduce_tasks=self.n_reduce_tasks,
            )

    def mapper(self, line):
        event = eventlog.parse_json_event(line)
        if event is None:
            try:
                datestamp, course_id, user_id, enrolled_at_end, change_since_last_day, mode_at_end = line.split('\t')
                yield (course_id, int(user_id)), ("state", int(enrolled_at_end), mode_at_end)
                return
            except ValueError:
                return

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

        yield (course_id, user_id), ("event", timestamp, event_type, mode)

    def reducer(self, key, values):
        """Emit records for each day the user was enrolled in the course."""
        course_id, user_id = key

        event_stream_processor = DaysEnrolledForEvents(course_id, user_id, self.date, values)
        for day_enrolled_record in event_stream_processor.enrollments():
            yield day_enrolled_record

    def output(self):
        return get_target_from_url(url_path_join(self.warehouse_path, 'course_enrollment', 'dt=' + self.date.isoformat()) + '/')


class EnrollmentEvent(object):
    """The critical information necessary to process the event in the event stream."""

    def __init__(self, timestamp, event_type, mode):
        self.timestamp = timestamp
        self.datestamp = eventlog.timestamp_to_datestamp(timestamp)
        self.event_type = event_type
        self.mode = mode


class EnrollmentState(object):

    def __init__(self, enrolled_at_end, mode_at_end):
        self.enrolled_at_end = enrolled_at_end
        self.mode_at_end = mode_at_end


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

    ENROLLED = 1
    UNENROLLED = 0
    MODE_UNKNOWN = 'unknown'

    def __init__(self, course_id, user_id, date, state_and_events):
        self.course_id = course_id
        self.user_id = user_id
        self.date = date

        self.sorted_events = sorted(state_and_events)
        previous_state = None
        for event in self.sorted_events:
            if event[0] == 'state':
                previous_state = EnrollmentState(
                    enrolled_at_end=event[1],
                    mode_at_end=event[2],
                )

        self.sorted_events = [
            EnrollmentEvent(event[1], event[2], event[3]) for event in self.sorted_events
            if event[0] == 'event'
        ]

        if previous_state is None:
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
            self.previous_state = EnrollmentState(0, self.MODE_UNKNOWN)
            self.state = self.UNENROLLED
            self.mode = self.MODE_UNKNOWN
        else:
            self.previous_state = previous_state
            self.state = self.previous_state.enrolled_at_end
            self.mode = previous_state.mode_at_end

    def enrollments(self):
        for event in self.sorted_events:
            self.mode = event.mode

            if self.state == self.ENROLLED and event.event_type == DEACTIVATED:
                self.state = self.UNENROLLED
            elif self.state == self.UNENROLLED and event.event_type == ACTIVATED:
                self.state = self.ENROLLED
            elif event.event_type == MODE_CHANGED:
                pass
            else:
                log.warning(
                    'No state change for %s event. User %d is already in the requested state for course %s on %s.',
                    event.event_type, self.user_id, self.course_id, event.datestamp
                )

        if self.state == self.ENROLLED and self.previous_state.enrolled_at_end == 0:
            change = 1
        elif self.state == self.UNENROLLED and self.previous_state.enrolled_at_end == 1:
            change = -1
        else:
            change = 0

        yield (
            self.date.isoformat(), self.course_id, self.user_id, self.state, change, self.mode
        )


class CourseEnrollmentTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the CourseEnrollmentTableTask task."""

    # Make the interval be optional:
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to export logs for. '
        'If not specified, `interval_start` and `interval_end` are used to construct the `interval`.',
    )

    # Define optional parameters, to be used if 'interval' is not defined.
    interval_start = luigi.DateParameter(
        config_path={'section': 'enrollments', 'name': 'interval_start'},
        significant=False,
        description='The start date to export logs for.  Ignored if `interval` is provided.',
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='The end date to export logs for.  Ignored if `interval` is provided. '
        'Default is today, UTC.',
    )

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentTableDownstreamMixin, self).__init__(*args, **kwargs)

        if not self.interval:
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)


class CourseEnrollmentTableTask(HiveTableTask):
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


class CourseEnrollmentPartitionTask(CourseEnrollmentTableDownstreamMixin, HivePartitionTask):

    @property
    def hive_table_task(self):
        return CourseEnrollmentTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def partition_value(self):
        return self.date.isoformat()

    def requires(self):
        yield self.hive_table_task
        yield CourseEnrollmentTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            date=self.interval_end,
            interval_start=self.interval_start,
            overwrite=self.overwrite,
        )


class EnrollmentTask(CourseEnrollmentTableDownstreamMixin, HiveQueryToMysqlTask):
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
    def partition_value(self):
        return self.interval.date_b.isoformat()

    @property
    def required_table_tasks(self):
        yield (
            CourseEnrollmentPartitionTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
                overwrite=self.hive_overwrite,
            ),
            ImportAuthUserProfileTask()
        )


class EnrollmentByGenderTask(EnrollmentTask):
    """Breakdown of enrollments by gender as reported by the user"""

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
        return """
            SELECT
                ce.date,
                ce.course_id,
                p.year_of_birth,
                SUM(ce.at_end),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
            GROUP BY
                ce.date,
                ce.course_id,
                p.year_of_birth
        """

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
        return """
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
        """

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
        return """
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
        """

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
        return """
            SELECT
                ce.course_id,
                ce.date,
                SUM(ce.at_end),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            GROUP BY
                ce.course_id,
                ce.date
        """

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


@workflow_entry_point
class ImportEnrollmentsIntoMysql(CourseEnrollmentTableDownstreamMixin, luigi.WrapperTask):
    """Import all breakdowns of enrollment into MySQL"""

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            EnrollmentByGenderTask(**kwargs),
            EnrollmentByBirthYearTask(**kwargs),
            EnrollmentByEducationLevelTask(**kwargs),
            EnrollmentByModeTask(**kwargs),
            EnrollmentDailyTask(**kwargs),
        )
