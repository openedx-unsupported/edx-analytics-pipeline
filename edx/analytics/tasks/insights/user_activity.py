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
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join
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


class UserActivityDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the UserActivityTableTask task."""
    pass


class UserActivityTableTask(UserActivityDownstreamMixin, BareHiveTableTask):

    overwrite_n_days = luigi.IntParameter()
    date = luigi.DateParameter()
    interval = None

    def requires(self):
        overwrite_from_date = self.date - datetime.timedelta(days=self.overwrite_n_days)
        interval = luigi.date_interval.Custom(overwrite_from_date, self.date)

        for date in interval:
            partition = HivePartition(self.partition_by, date.isoformat())
            partition_location = url_path_join(self.table_location, partition.path_spec + '/')

            yield UserActivityTask(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                output_root=partition_location,
                overwrite=True
            )

    def query(self):
        query = super(UserActivityTableTask, self).query()
        return query + "MSCK REPAIR TABLE {table};".format(table=self.table)

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


class CourseActivityTableTask(BareHiveTableTask):

    @property
    def table(self):
        # TODO: revert to course_activity
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

    overwrite_n_days = luigi.IntParameter()
    date = luigi.DateParameter()

    def query(self):
        query = """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec})
        SELECT
            act.course_id as course_id,
            CONCAT(cal.iso_week_start, ' 00:00:00') as interval_start,
            CONCAT(cal.iso_week_end, ' 00:00:00') as interval_end,
            act.category as label,
            COUNT(DISTINCT username) as count
        FROM user_activity_daily act
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
        return self.date.isoformat()  # pylint: disable=no-member

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
            UserActivityTableTask(
                warehouse_path=self.warehouse_path,
                overwrite_n_days=self.overwrite_n_days,
                date=self.date
            ),
            CalendarTableTask(
                warehouse_path=self.warehouse_path,
            )
        )

    def remove_output_on_overwrite(self):
        super(HivePartitionTask, self).remove_output_on_overwrite()

    def output(self):
        return get_target_from_url(self.hive_partition_path(self.hive_table_task.table, self.interval.date_b.isoformat()))


class InsertToMysqlCourseActivityTask(WeeklyIntervalMixin, UserActivityDownstreamMixin, MysqlInsertTask):

    overwrite_n_days = luigi.IntParameter()

    @property
    def table(self):
        # TODO: revert to course_activity
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
            date=self.end_date,
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite,
            overwrite_n_days=self.overwrite_n_days,
        )
