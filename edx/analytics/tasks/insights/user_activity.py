"""Categorize activity of users."""

import datetime
import logging

import luigi
import luigi.date_interval

from edx.analytics.tasks.calendar_task import CalendarTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url
import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveQueryToMysqlTask
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin
from edx.analytics.tasks.decorators import workflow_entry_point

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class UserActivityTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    Categorize activity of users.

    Analyze the history of user actions and categorize their activity. Note that categories are not mutually exclusive.
    A single event may belong to multiple categories. For example, we define a generic "ACTIVE" category that refers
    to any event that has a course_id associated with it, but is not an enrollment event. Other events, such as a
    video play event, will also belong to other categories.

    The output from this job is a table that represents the number of events seen for each user in each course in each
    category on each day.

    """

    output_root = luigi.Parameter(
        description='String path to store the output in.',
    )

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        for label in self.get_predicate_labels(event):
            yield self._encode_tuple((course_id, username, date_string, label)), 1

    def get_predicate_labels(self, event):
        """Creates labels by applying hardcoded predicates to a single event."""
        # We only want the explicit event, not the implicit form.
        event_type = event.get('event_type')
        event_source = event.get('event_source')

        # Ignore all background task events, since they don't count as a form of activity.
        if event_source == 'task':
            return []

        # Ignore all enrollment events, since they don't count as a form of activity.
        if event_type.startswith('edx.course.enrollment.'):
            return []

        labels = [ACTIVE_LABEL]

        if event_source == 'server':
            if event_type == 'problem_check':
                labels.append(PROBLEM_LABEL)

            if event_type.startswith('edx.forum.') and event_type.endswith('.created'):
                labels.append(POST_FORUM_LABEL)

        if event_source in ('browser', 'mobile'):
            if event_type == 'play_video':
                labels.append(PLAY_VIDEO_LABEL)

        return labels

    def _encode_tuple(self, values):
        """
        Convert values into a tuple containing encoded strings.

        Parameters:
            Values is a list or tuple.

        This enforces a standard encoding for the parts of the
        key. Without this a part of the key might appear differently
        in the key string when it is coerced to a string by luigi. For
        example, if the same key value appears in two different
        records, one as a str() type and the other a unicode() then
        without this change they would appear as u'Foo' and 'Foo' in
        the final key string. Although python doesn't care about this
        difference, hadoop does, and will bucket the values
        separately. Which is not what we want.
        """
        # TODO: refactor this into a utility function and update jobs
        # to always UTF8 encode mapper keys.
        if len(values) > 1:
            return tuple([value.encode('utf8') for value in values])
        else:
            return values[0].encode('utf8')

    def reducer(self, key, values):
        """Cumulate number of events per key."""
        num_events = sum(values)
        if num_events > 0:
            yield key, num_events

    def output(self):
        return get_target_from_url(self.output_root)


class UserActivityDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the UserActivityTableTask task."""
    pass


class UserActivityTableTask(UserActivityDownstreamMixin, HiveTableTask):
    """Hive table that stores the set of users active in each course over time."""

    @property
    def table(self):
        return 'user_activity_daily'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('date', 'STRING'),
            ('category', 'STRING'),
            ('count', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return UserActivityTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )


class CourseActivityTask(UserActivityDownstreamMixin, HiveQueryToMysqlTask):
    """Base class for activity queries, captures common dependencies and parameters."""

    @property
    def query(self):
        return self.activity_query.format(
            interval_start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
            interval_end=self.interval.date_b.isoformat(),  # pylint: disable=no-member
        )

    @property
    def activity_query(self):
        """Defines the query to be made, using "{interval_start}" and "{interval_end}"."""
        raise NotImplementedError

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            UserActivityTableTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
            ),
            CalendarTableTask(
                warehouse_path=self.warehouse_path,
                overwrite=self.hive_overwrite,
            )
        )


@workflow_entry_point
class CourseActivityWeeklyTask(WeeklyIntervalMixin, CourseActivityTask):
    """
    Number of users performing each category of activity each ISO week.

    Note that this was the original activity metric, so it is stored in the original table that is simply named
    "course_activity" even though it should probably be named "course_activity_weekly". Also the schema does not match
    the other activity tables for the same reason.

    All references to weeks in here refer to ISO weeks. Note that ISO weeks may belong to different ISO years than the
    Gregorian calendar year.

    If, for example, you wanted to analyze all data in the past week, you could run the job on Monday and pass in 1 to
    the "weeks" parameter. This will not analyze data for the week that contains the current day (since it is not
    complete). It will only compute data for the previous week.

    TODO: update table name and schema to be consistent with other tables.

    """

    @property
    def table(self):
        return 'course_activity'

    @property
    def activity_query(self):
        # Note that hive timestamp format is "yyyy-mm-dd HH:MM:SS.ffff" so we have to snap all of our dates to midnight
        return """
            SELECT
                act.course_id as course_id,
                CONCAT(cal.iso_week_start, ' 00:00:00') as interval_start,
                CONCAT(cal.iso_week_end, ' 00:00:00') as interval_end,
                act.category as label,
                COUNT(DISTINCT username) as count
            FROM user_activity_daily act
            JOIN calendar cal ON act.date = cal.date
            WHERE "{interval_start}" <= cal.date AND cal.date < "{interval_end}"
            GROUP BY
                act.course_id,
                cal.iso_week_start,
                cal.iso_week_end,
                act.category;
        """

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('interval_start', 'DATETIME NOT NULL'),
            ('interval_end', 'DATETIME NOT NULL'),
            ('label', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id', 'label'),
            ('interval_end',)
        ]


class CourseActivityDailyTask(CourseActivityTask):
    """Number of users performing each category of activity each calendar day."""

    @property
    def table(self):
        return 'course_activity_daily'

    @property
    def activity_query(self):
        return """
            SELECT
                act.date,
                act.course_id as course_id,
                act.category as label,
                COUNT(DISTINCT username) as count
            FROM user_activity_daily act
            WHERE "{interval_start}" <= act.date AND act.date < "{interval_end}"
            GROUP BY
                act.course_id,
                act.date,
                act.category;
        """

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('label', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id', 'label'),
            ('date',)
        ]


class CourseActivityMonthlyTask(CourseActivityTask):
    """
    Number of users performing each category of activity each calendar month.

    Note that the month containing the end_date is not included in the analysis.

    If, for example, you wanted to analyze all data in the past month, you could run the job on the first day of the
    following month pass in 1 to the "months" parameter. This will not analyze data for the month that contains the
    current day (since it is not complete). It will only compute data for the previous month.

    """

    end_date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='A date within the month that will be the upper bound of the closed-open interval. '
        'Default is today, UTC.',
    )
    months = luigi.IntParameter(
        default=6,
        description='The number of months to include in the analysis, counting back from the month that contains '
        'the end_date.',
    )

    @property
    def interval(self):
        """Given the parameters, compute the first and last date of the interval."""
        from dateutil.relativedelta import relativedelta

        # We don't actually care about the particular day of the month in this computation since we are fixing both the
        # start and end dates to the first day of the month, so we can perform simple arithmetic with the numeric month
        # and only have to worry about adjusting the year. Note that bankers perform this arithmetic differently so it
        # is spelled out here explicitly even though their are third party libraries that contain this computation.

        if self.months == 0:
            raise ValueError('Number of months to process must be greater than 0')

        ending_date = self.end_date.replace(day=1)  # pylint: disable=no-member
        starting_date = ending_date - relativedelta(months=self.months)

        return luigi.date_interval.Custom(starting_date, ending_date)

    @property
    def table(self):
        return 'course_activity_monthly'

    @property
    def activity_query(self):
        return """
            SELECT
                act.course_id as course_id,
                cal.year,
                cal.month,
                act.category as label,
                COUNT(DISTINCT username) as count
            FROM user_activity_daily act
            JOIN calendar cal ON act.date = cal.date
            WHERE "{interval_start}" <= cal.date AND cal.date < "{interval_end}"
            GROUP BY
                act.course_id,
                cal.year,
                cal.month,
                act.category;
        """

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('year', 'INT(11) NOT NULL'),
            ('month', 'INT(11) NOT NULL'),
            ('label', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id', 'label'),
            ('year', 'month')
        ]
