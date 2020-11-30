"""Categorize activity of users."""

import datetime
import logging
from collections import Counter

import luigi
import luigi.date_interval

import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.common.mapreduce import MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.insights.calendar_task import CalendarTableTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin, hive_database_name
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateTimeField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class UserActivityTask(OverwriteOutputMixin, WarehouseMixin, EventLogSelectionMixin, MultiOutputMapReduceJobTask):
    """
    Categorize activity of users.

    Analyze the history of user actions and categorize their activity. Note that categories are not mutually exclusive.
    A single event may belong to multiple categories. For example, we define a generic "ACTIVE" category that refers
    to any event that has a course_id associated with it, but is not an enrollment event. Other events, such as a
    video play event, will also belong to other categories.

    The output from this job is a table that represents the number of events seen for each user in each course in each
    category on each day.

    """

    output_root = None

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        user_id = event.get('context', {}).get('user_id')
        if not user_id:
            self.incr_counter('UserActivity', 'Discard Missing User ID', 1)
            log.error("User-Activity: event without user_id in context: %s", event)
            return

        # Course user activity URLs have changed recently with the introduction of micro-frontends (MFEs).
        # This code attempts to handle those URL changes with minimal diffences in the number of events processed/used.
        #
        # Attempt to extract the course_id. The get_course_id() method will first look for an explicit course ID in the
        # event context. If that explicit course ID does not exist, the code will then look in the event URL to attempt
        # to parse out a course ID, using both an old-style URL pattern and a new-style micro-frontend courseware URL pattern.
        course_id = eventlog.get_course_id(event, from_url=True)
        if not course_id:
            # If a course_id has not been extracted successfully, ignore this event.
            self.incr_counter('UserActivity', 'Discard Missing Course ID', 1)
            return

        for label in self.get_predicate_labels(event):
            yield date_string, self._encode_tuple((str(user_id), course_id, date_string, label))

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

    def multi_output_reducer(self, _date_string, values, output_file):
        counter = Counter(values)

        for key, num_events in counter.iteritems():
            user_id, course_id, date_string, label = key
            value = (user_id, course_id, date_string, label, num_events)
            output_file.write('\t'.join([str(field) for field in value]))
            output_file.write('\n')

    def output_path_for_key(self, key):
        date_string = key
        return url_path_join(
            self.hive_partition_path('user_activity_by_user', date_string),
            'user_activity_{date}'.format(
                date=date_string,
            )
        )

    def run(self):
        # Remove the marker file.
        self.remove_output_on_overwrite()
        # Also remove actual output files in case of overwrite.
        if self.overwrite:
            for date in self.interval:
                url = self.output_path_for_key(date.isoformat())
                target = get_target_from_url(url)
                if target.exists():
                    target.remove()

        return super(UserActivityTask, self).run()


class UserActivityDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the UserActivityTableTask task."""

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        description='This parameter is used by UserActivityTask which will overwrite user-activity counts '
                    'for the most recent n days. Default is pulled from user-activity.overwrite_n_days.',
        significant=False,
    )


class UserActivityTableTask(UserActivityDownstreamMixin, BareHiveTableTask):
    """
    The hive table for storing user activity data. This task also adds partition metadata info to the Hive metastore.
    """

    date = luigi.DateParameter()
    interval = None

    def requires(self):
        # Overwrite n days of user activity data before recovering partitions.
        if self.overwrite_n_days > 0:
            overwrite_from_date = self.date - datetime.timedelta(days=self.overwrite_n_days)
            overwrite_interval = luigi.date_interval.Custom(overwrite_from_date, self.date)

            yield UserActivityTask(
                interval=overwrite_interval,
                warehouse_path=self.warehouse_path,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=True,
            )

    def query(self):
        query = super(UserActivityTableTask, self).query()
        # Append a metastore check command with the repair option.
        # This will add metadata about partitions that exist in HDFS.
        return query + "MSCK REPAIR TABLE {table};".format(table=self.table)

    @property
    def table(self):
        return 'user_activity_by_user'

    @property
    def partition_by(self):
        return 'dt'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('date', 'STRING'),
            ('category', 'STRING'),
            ('count', 'INT'),
        ]


class CourseActivityRecord(Record):
    """Represents count of users performing each category of activity each ISO week."""
    course_id = StringField(length=255, nullable=False, description='The course the learner is enrolled in.')
    interval_start = DateTimeField(nullable=False, description='Start time of ISO week.')
    interval_end = DateTimeField(nullable=False, description='End time of ISO week.')
    label = StringField(length=255, nullable=False, description='The name of activity user performed in the interval.')
    count = IntegerField(description='Total count of activities performed between the interval.')


class CourseActivityTableTask(BareHiveTableTask):

    @property
    def table(self):
        return 'course_activity'

    @property
    def partition_by(self):
        return 'dt'

    @property
    def columns(self):
        return CourseActivityRecord.get_hive_schema()


class CourseActivityPartitionTask(WeeklyIntervalMixin, UserActivityDownstreamMixin, HivePartitionTask):
    """
    Number of users performing each category of activity each ISO week.

    All references to weeks in here refer to ISO weeks. Note that ISO weeks may belong to different ISO years than the
    Gregorian calendar year.

    If, for example, you wanted to analyze all data in the past week, you could run the job on Monday and pass in 1 to
    the "weeks" parameter. This will not analyze data for the week that contains the current day (since it is not
    complete). It will only compute data for the previous week.
    """

    def query(self):
        query = """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec})
        SELECT
            act.course_id as course_id,
            CONCAT(cal.iso_week_start, ' 00:00:00') as interval_start,
            CONCAT(cal.iso_week_end, ' 00:00:00') as interval_end,
            act.category as label,
            COUNT(DISTINCT user_id) as count
        FROM user_activity_by_user act
        JOIN calendar cal
            ON act.`date` = cal.`date` AND act.dt >= "{interval_start}" AND act.dt < "{interval_end}"
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
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat(),
        )

        return query

    @property
    def partition_value(self):
        return self.end_date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return CourseActivityTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):
        yield (
            self.hive_table_task,
            UserActivityTableTask(
                warehouse_path=self.warehouse_path,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
                overwrite_n_days=self.overwrite_n_days,
                date=self.end_date
            ),
            CalendarTableTask(
                warehouse_path=self.warehouse_path,
            )
        )

    def remove_output_on_overwrite(self):
        # HivePartitionTask overrides the behaviour of this method, such that
        # it does not actually remove the HDFS data. Here we want to make sure that
        # data is removed from HDFS as well so we default to OverwriteOutputMixin's implementation.
        OverwriteOutputMixin.remove_output_on_overwrite(self)

    def output(self):
        # HivePartitionTask returns HivePartitionTarget as output which does not implement remove() and
        # open().
        # We override output() here so that it can be deleted when overwrite is specified, and also
        # be read as input by other tasks.
        return get_target_from_url(self.hive_partition_path(self.hive_table_task.table, self.end_date.isoformat()))


@workflow_entry_point
class InsertToMysqlCourseActivityTask(WeeklyIntervalMixin, UserActivityDownstreamMixin, MysqlInsertTask):
    """
    Creates/populates the `course_activity` Result store table.
    """

    overwrite_hive = luigi.BoolParameter(
        default=False,
        description='Overwrite the hive data used as source for this task. Users should set this to True '
                    'when using a persistent Hive metastore.',
        significant=False
    )

    overwrite_mysql = luigi.BoolParameter(
        default=False,
        description='Overwrite the table if set to True. Allow users to override this behavior if they '
                    'want.',
        significant=False
    )

    overwrite = None

    def __init__(self, *args, **kwargs):
        super(InsertToMysqlCourseActivityTask, self).__init__(*args, **kwargs)
        self.overwrite = self.overwrite_mysql

    @property
    def table(self):
        return "course_activity"

    @property
    def columns(self):
        return CourseActivityRecord.get_sql_schema()

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
            end_date=self.end_date,
            weeks=self.weeks,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite_hive,
            overwrite_n_days=self.overwrite_n_days,
        )
