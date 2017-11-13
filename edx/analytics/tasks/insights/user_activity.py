"""Categorize activity of users."""

import datetime
import logging

import luigi
import luigi.date_interval

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.insights.calendar_task import CalendarTableTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveQueryToMysqlTask, BareHiveTableTask, HivePartitionTask, HiveQueryTask, hive_database_name
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin
from edx.analytics.tasks.util.url import get_target_from_url
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.common.mysql_load import IncrementalMysqlInsertTask, MysqlInsertTask

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class UserActivityTask(OverwriteOutputMixin, EventLogSelectionMixin, MapReduceJobTask):
    """
    Categorize activity of users.

    Analyze the history of user actions and categorize their activity. Note that categories are not mutually exclusive.
    A single event may belong to multiple categories. For example, we define a generic "ACTIVE" category that refers
    to any event that has a course_id associated with it, but is not an enrollment event. Other events, such as a
    video play event, will also belong to other categories.

    The output from this job is a table that represents the number of events seen for each user in each course in each
    category on each day.

    """

    date = luigi.DateParameter()
    output_root = luigi.Parameter(
        description='String path to store the output in.',
    )
    interval = None

    def __init__(self, *args, **kwargs):
        super(UserActivityTask, self).__init__(*args, **kwargs)

        self.interval = luigi.date_interval.Date.from_date(self.date)

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

    def run(self):
        self.remove_output_on_overwrite()
        return super(UserActivityTask, self).run()


class UserActivityTableTask(BareHiveTableTask):

    @property
    def table(self):
        return 'user_activity_daily'

    @property
    def partition_by(self):
        return 'dt'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('date', 'STRING'),
            ('category', 'STRING'),
            ('count', 'INT'),
        ]


class UserActivityPartitionTask(MapReduceJobTaskMixin, HivePartitionTask):

    date = luigi.DateParameter()

    @property
    def partition_value(self):
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return UserActivityTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return UserActivityTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite=self.overwrite
        )


class UserActivityDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the UserActivityTableTask task."""
    pass


class UserActivityIntervalTask(UserActivityDownstreamMixin, luigi.WrapperTask):

    overwrite_n_days = luigi.IntParameter(
        significant=False,
        default=0,
    )

    def requires(self):
        overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)
        for date in reversed([d for d in self.interval]):
            should_overwrite = date >= overwrite_from_date
            yield UserActivityPartitionTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=should_overwrite,
            )


class CourseActivityTableTask(BareHiveTableTask):

    @property
    def table(self):
        return 'course_activity_test'

    @property
    def partition_by(self):
        return 'dt'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('interval_start', 'TIMESTAMP'),
            ('interval_end', 'TIMESTAMP'),
            ('label', 'STRING'),
            ('count', 'INT'),
        ]


class CourseActivityPartitionTask(UserActivityDownstreamMixin, HivePartitionTask):

    def query(self):
        query = """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec}) {if_not_exists}
        SELECT
            act.course_id as course_id,
            CONCAT(cal.iso_week_start, ' 00:00:00') as interval_start,
            CONCAT(cal.iso_week_end, ' 00:00:00') as interval_end,
            act.category as label,
            COUNT(DISTINCT username) as count
        FROM user_activity_daily act
        JOIN calendar cal
            ON act.`date` = cal.`date`
        WHERE
            "{interval_start}" <= cal.`date` AND cal.`date` < "{interval_end}"
        GROUP BY
            act.course_id,
            cal.iso_week_start,
            cal.iso_week_end,
            act.category;
        """.format(
            database_name=hive_database_name(),
            table=self.hive_table_task.table,
            partition=self.partition,
            if_not_exists='' if self.overwrite else 'IF NOT EXISTS',
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat(),
        )

        return query

    @property
    def partition_value(self):
        return self.interval.date_b.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return CourseActivityTableTask(
            warehouse_path=self.warehouse_path,
        )

    def requires(self):
        yield (
            CourseActivityTableTask(
                warehouse_path=self.warehouse_path,
            ),
            UserActivityIntervalTask(
                interval=self.interval,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite_n_days=0,
            ),
            CalendarTableTask(
                warehouse_path=self.warehouse_path,
            )
        )

    def remove_output_on_overwrite(self):
        super(HivePartitionTask, self).remove_output_on_overwrite()

    def output(self):
        return get_target_from_url(self.hive_partition_path(self.hive_table_task.table, self.interval.date_b.isoformat()))


class InsertToMysqlCourseActivityTask(UserActivityDownstreamMixin, IncrementalMysqlInsertTask):

    @property
    def record_filter(self):
        return "interval_start='{interval_start}' AND interval_end='{interval_end}'".format(
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat()
        )


    def update_id(self):
        return '{task_name}(interval_start={interval_start},interval_end={interval_end})'.format(
            task_name=self.task_family,
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat()
        )

    @property
    def table(self):
        return "course_activity_test"

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

    @property
    def insert_source_task(self):
        return CourseActivityPartitionTask(
            warehouse_path=self.warehouse_path,
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=True,
        )


class CourseActivityWorkflow(WeeklyIntervalMixin, UserActivityDownstreamMixin, luigi.WrapperTask):

    def requires(self):
        yield InsertToMysqlCourseActivityTask(
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            #credentials=self.credentials,
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
            JOIN calendar cal ON act.`date` = cal.`date`
            WHERE "{interval_start}" <= cal.`date` AND cal.`date` < "{interval_end}"
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
                act.`date`,
                act.course_id as course_id,
                act.category as label,
                COUNT(DISTINCT username) as count
            FROM user_activity_daily act
            WHERE "{interval_start}" <= act.`date` AND act.`date` < "{interval_end}"
            GROUP BY
                act.course_id,
                act.`date`,
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
            ('`date`',)
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
            JOIN calendar cal ON act.`date` = cal.`date`
            WHERE "{interval_start}" <= cal.`date` AND cal.`date` < "{interval_end}"
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
