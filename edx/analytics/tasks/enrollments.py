"""Compute metrics related to user enrollments in courses"""

import logging
import datetime

import luigi
import luigi.task
import luigi.date_interval
from luigi.hive import HiveQueryTask

from edx.analytics.tasks.database_imports import ImportAuthUserProfileTask
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin, MapReduceJobTask
from edx.analytics.tasks.mysql_load import MysqlInsertTask, IncrementalMysqlInsertTask
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join, ExternalURL
from edx.analytics.tasks.util import eventlog, opaque_key_util
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import WarehouseMixin, HiveQueryToMysqlTask, HivePartitionTask, \
    BareHiveTableTask, hive_database_name, OverwriteAwareHiveQueryRunner
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)
DEACTIVATED = 'edx.course.enrollment.deactivated'
ACTIVATED = 'edx.course.enrollment.activated'
MODE_CHANGED = 'edx.course.enrollment.mode_changed'


class CourseEnrollmentTask(EventLogSelectionMixin, OverwriteOutputMixin, WarehouseMixin, MapReduceJobTask):
    """Produce a data set that shows which days each user was enrolled in each course."""

    enable_direct_output = True

    date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    interval = None

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentTask, self).__init__(*args, **kwargs)

        self.interval = luigi.date_interval.Date.from_date(self.date)

    # TODO: This is duplicated in DownstreamMixin.
    interval_start = luigi.DateParameter(
        config_path={'section': 'enrollments', 'name': 'interval_start'},
        significant=False,
    )

    def requires(self):
        yield super(CourseEnrollmentTask, self).requires()

        if self.date > self.interval_start:
            yesterday = self.date - datetime.timedelta(days=1)
            yield ExternalURL(
                url_path_join(self.warehouse_path, 'course_enrollment', 'dt=' + yesterday.isoformat()) + '/'
            )

    def mapper(self, line):
        maybe_tuple = self.get_event_and_date_string(line)
        if maybe_tuple is None:
            try:
                datestamp, course_id, user_id, enrolled_at_end, change_since_last_day, mode_at_end = line.split('\t')
                yield (course_id, int(user_id)), ("state", int(enrolled_at_end), mode_at_end)
                return
            except ValueError:
                return
        else:
            event, _date_string = maybe_tuple

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
        if course_id is None:
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

    @property
    def output_root(self):
        return url_path_join(self.warehouse_path, 'course_enrollment', 'dt=' + self.date.isoformat()) + '/'

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        output_target = self.output()
        if not self.complete() and output_target.exists():
            output_target.remove()

        self.remove_output_on_overwrite()

        super(CourseEnrollmentTask, self).run()


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

    date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    interval = None

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentTableDownstreamMixin, self).__init__(*args, **kwargs)

        self.interval = luigi.date_interval.Date.from_date(self.date)


class CourseEnrollmentTableTask(BareHiveTableTask):
    """Hive table that stores the set of users enrolled in each course over time."""

    @property
    def table(self):
        return 'course_enrollment'

    @property
    def partition_by(self):
        return 'dt'

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

    @property
    def data_task(self):
        return CourseEnrollmentTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            date=self.date,
        )


class ExternalCourseEnrollmentTableTask(CourseEnrollmentTableTask):

    def requires(self):
        yield ExternalURL(
            url=url_path_join(self.warehouse_path, 'course_enrollment', self.partition.path_spec) + '/'
        )


class EnrollmentByModeTableTask(BareHiveTableTask):

    @property
    def table(self):
        return 'course_enrollment_mode_daily'

    @property
    def partition_by(self):
        return 'dt'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('mode', 'STRING'),
            ('count', 'INT'),
            ('cumulative_count', 'INT')
        ]


class EnrollmentByModePartitionTask(CourseEnrollmentTableDownstreamMixin, HivePartitionTask):

    partition_value = None

    def __init__(self, *args, **kwargs):
        super(EnrollmentByModePartitionTask, self).__init__(*args, **kwargs)
        self.partition_value = self.date.isoformat()

    def query(self):
        return """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec})
        SELECT
            ce.date,
            ce.course_id,
            ce.mode,
            SUM(ce.at_end),
            COUNT(ce.user_id)
        FROM course_enrollment ce
        WHERE ce.date = '{date}'
        GROUP BY
            ce.date,
            ce.course_id,
            ce.mode
        """.format(
            date=self.date,
            database_name=hive_database_name(),
            partition=self.partition,
            table=self.hive_table_task.table,
        )

    @property
    def hive_table_task(self):
        return EnrollmentByModeTableTask(
            warehouse_path=self.warehouse_path
        )

    def requires(self):
        yield (
            self.hive_table_task,
            CourseEnrollmentPartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path
            ),
            ImportAuthUserProfileTask()
        )

    def complete(self):
        metadata_exists = super(EnrollmentByModePartitionTask, self).complete()
        output_data_exists = get_target_from_url(self.partition_location).exists()
        return output_data_exists and metadata_exists

    def job_runner(self):
        return OverwriteAwareHiveQueryRunner()

    def remove_output_on_overwrite(self):
        # Note that the query takes care of actually removing the old partition.
        if self.overwrite:
            self.attempted_removal = True


class EnrollmentByModePartitionDataTask(CourseEnrollmentTableDownstreamMixin, OverwriteOutputMixin, luigi.WrapperTask):

    def requires(self):
        return EnrollmentByModePartitionTask(
            warehouse_path=self.warehouse_path,
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite
        )

    def output(self):
        return get_target_from_url(self.requires().partition_location)

    def complete(self):
        return self.output().exists()


class EnrollmentByModeMysqlInsertTask(CourseEnrollmentTableDownstreamMixin, IncrementalMysqlInsertTask):

    @property
    def table(self):
        return "course_enrollment_mode_daily"

    @property
    def record_filter(self):
        return "date='{date}'".format(date=self.date.isoformat())  # pylint: disable=no-member

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('mode', 'VARCHAR(255) NOT NULL'),
            ('count', 'INTEGER'),
            ('cumulative_count', 'INTEGER')
        ]

    @property
    def indexes(self):
        return [
            ('course_id', 'date'),
            ('date',)
        ]

    @property
    def insert_source_task(self):
        return EnrollmentByModePartitionDataTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite
        )


# class EnrollmentByGenderTask(EnrollmentTask):
#     """Breakdown of enrollments by gender as reported by the user"""
#
#     @property
#     def query(self):
#         return """
#             SELECT
#                 ce.date,
#                 ce.course_id,
#                 IF(p.gender != '', p.gender, NULL),
#                 SUM(ce.at_end),
#                 COUNT(ce.user_id)
#             FROM course_enrollment ce
#             LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
#             WHERE ce.date = '{date}'
#             GROUP BY
#                 ce.date,
#                 ce.course_id,
#                 IF(p.gender != '', p.gender, NULL)
#         """.format(
#             date=self.date
#         )
#
#     @property
#     def table(self):
#         return 'course_enrollment_gender_daily'
#
#     @property
#     def columns(self):
#         return [
#             ('date', 'DATE NOT NULL'),
#             ('course_id', 'VARCHAR(255) NOT NULL'),
#             ('gender', 'VARCHAR(6)'),
#             ('count', 'INTEGER'),
#             ('cumulative_count', 'INTEGER')
#         ]
#
#
# class EnrollmentByBirthYearTask(EnrollmentTask):
#     """Breakdown of enrollments by age as reported by the user"""
#
#     @property
#     def query(self):
#         return """
#             SELECT
#                 ce.date,
#                 ce.course_id,
#                 p.year_of_birth,
#                 SUM(ce.at_end),
#                 COUNT(ce.user_id)
#             FROM course_enrollment ce
#             LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
#             WHERE ce.date = '{date}'
#             GROUP BY
#                 ce.date,
#                 ce.course_id,
#                 p.year_of_birth
#         """.format(
#             date=self.date
#         )
#
#     @property
#     def table(self):
#         return 'course_enrollment_birth_year_daily'
#
#     @property
#     def columns(self):
#         return [
#             ('date', 'DATE NOT NULL'),
#             ('course_id', 'VARCHAR(255) NOT NULL'),
#             ('birth_year', 'INTEGER'),
#             ('count', 'INTEGER'),
#             ('cumulative_count', 'INTEGER')
#         ]
#
#
# class EnrollmentByEducationLevelTask(EnrollmentTask):
#     """Breakdown of enrollments by education level as reported by the user"""
#
#     @property
#     def query(self):
#         return """
#             SELECT
#                 ce.date,
#                 ce.course_id,
#                 CASE p.level_of_education
#                     WHEN 'el'    THEN 'primary'
#                     WHEN 'jhs'   THEN 'junior_secondary'
#                     WHEN 'hs'    THEN 'secondary'
#                     WHEN 'a'     THEN 'associates'
#                     WHEN 'b'     THEN 'bachelors'
#                     WHEN 'm'     THEN 'masters'
#                     WHEN 'p'     THEN 'doctorate'
#                     WHEN 'p_se'  THEN 'doctorate'
#                     WHEN 'p_oth' THEN 'doctorate'
#                     WHEN 'none'  THEN 'none'
#                     WHEN 'other' THEN 'other'
#                     ELSE NULL
#                 END,
#                 SUM(ce.at_end),
#                 COUNT(ce.user_id)
#             FROM course_enrollment ce
#             LEFT OUTER JOIN auth_userprofile p ON p.user_id = ce.user_id
#             WHERE ce.date = '{date}'
#             GROUP BY
#                 ce.date,
#                 ce.course_id,
#                 CASE p.level_of_education
#                     WHEN 'el'    THEN 'primary'
#                     WHEN 'jhs'   THEN 'junior_secondary'
#                     WHEN 'hs'    THEN 'secondary'
#                     WHEN 'a'     THEN 'associates'
#                     WHEN 'b'     THEN 'bachelors'
#                     WHEN 'm'     THEN 'masters'
#                     WHEN 'p'     THEN 'doctorate'
#                     WHEN 'p_se'  THEN 'doctorate'
#                     WHEN 'p_oth' THEN 'doctorate'
#                     WHEN 'none'  THEN 'none'
#                     WHEN 'other' THEN 'other'
#                     ELSE NULL
#                 END
#         """.format(
#             date=self.date
#         )
#
#     @property
#     def table(self):
#         return 'course_enrollment_education_level_daily'
#
#     @property
#     def columns(self):
#         return [
#             ('date', 'DATE NOT NULL'),
#             ('course_id', 'VARCHAR(255) NOT NULL'),
#             ('education_level', 'VARCHAR(16)'),
#             ('count', 'INTEGER'),
#             ('cumulative_count', 'INTEGER')
#         ]


@workflow_entry_point
class ImportEnrollmentsIntoMysql(CourseEnrollmentTableDownstreamMixin, luigi.WrapperTask):
    """Import all breakdowns of enrollment into MySQL"""

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            EnrollmentByModeMysqlInsertTask(**kwargs),
            # EnrollmentByGenderTask(**kwargs),
            # EnrollmentByBirthYearTask(**kwargs),
            # EnrollmentByEducationLevelTask(**kwargs),
        )
