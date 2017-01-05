"""Categorize activity of users."""

import datetime
import logging
import textwrap
from collections import defaultdict

import luigi
import luigi.date_interval
from luigi.parameter import DateIntervalParameter

from edx.analytics.tasks.calendar_task import CalendarTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin, PathSelectionByDateIntervalTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join, UncheckedExternalURL
import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveQueryToMysqlTask, BareHiveTableTask, HivePartitionTask, HiveQueryTask, hive_database_name
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.mysql_load import MysqlInsertTask, IncrementalMysqlInsertTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class UserActivityDataTask(EventLogSelectionMixin, OverwriteOutputMixin, MapReduceJobTask):

    # FILEPATH_PATTERN should match the output files defined by output_path_for_key().
    FILEPATH_PATTERN = '.*?user_activity_(?P<date>\\d{4}-\\d{2}-\\d{2})'

    date = luigi.DateParameter()
    output_root = luigi.Parameter()
    interval = None

    def __init__(self, *args, **kwargs):
        super(UserActivityDataTask, self).__init__(*args, **kwargs)

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
        return super(UserActivityDataTask, self).run()


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

class UserActivityDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the UserActivityTableTask task."""
    pass


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
        return UserActivityDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite=self.overwrite
        )


class UserActivityIntervalTask(MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin, WarehouseMixin, luigi.WrapperTask):

    overwrite_n_days = luigi.IntParameter(
        significant=False,
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
        return 'course_activity'

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


class CourseActivityDataTask(MapReduceJobTaskMixin, WarehouseMixin, OverwriteOutputMixin,EventLogSelectionDownstreamMixin, HiveQueryTask):

    overwrite_n_days = luigi.IntParameter(
        significant=False,
    )

    table = luigi.Parameter()

    def query(self):
        query = """
        USE {database_name};
        DROP TABLE IF EXISTS {table_name};
        CREATE EXTERNAL TABLE {table_name} (
            course_id STRING,
            interval_start TIMESTAMP,
            interval_end TIMESTAMP,
            label STRING,
            count INT
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        LOCATION '{location}';

        INSERT OVERWRITE TABLE {table_name}
        SELECT
            act.course_id as course_id,
            CONCAT(cal.iso_week_start, ' 00:00:00') as interval_start,
            CONCAT(cal.iso_week_end, ' 00:00:00') as interval_end,
            act.category as label,
            COUNT(DISTINCT username) as count
        FROM user_activity_daily act
        JOIN calendar cal
            ON act.date = cal.date
        WHERE 
            "{interval_start}" <= cal.date AND cal.date < "{interval_end}"
        GROUP BY
            act.course_id,
            cal.iso_week_start,
            cal.iso_week_end,
            act.category;
        """.format(
            database_name=hive_database_name(),
            table_name=self.table,
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat(),
            location=self.hive_partition_path(self.table, self.interval.date_b.isoformat()),
        )
        return query

    def run(self):
        self.remove_output_on_overwrite()
        super(CourseActivityDataTask, self).run()

    def output(self):
        return get_target_from_url(self.hive_partition_path(self.table, self.interval.date_b.isoformat()))

    def requires(self):
        yield (
            CourseActivityTableTask(
                warehouse_path=self.warehouse_path,
            ),
            UserActivityIntervalTask(
                interval=self.interval,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite_n_days=self.overwrite_n_days,
            ),
            CalendarTableTask(
                warehouse_path=self.warehouse_path,
            )
        )

class CourseActivityPartitionTask(WeeklyIntervalMixin, MapReduceJobTaskMixin, HivePartitionTask):

    @property
    def partition_value(self):
        return self.interval.date_b.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return CourseActivityTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return CourseActivityDataTask(
            end_date=self.end_date,
            weeks=self.weeks,
            table=self.hive_table_task.table,
        )

class InsertToMysqlCourseActivityTask(UserActivityDownstreamMixin, MysqlInsertTask):

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        significant=False,
    )

    @property
    def table(self):
        return "course_activity"

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
        return CourseActivityDataTask(
            interval=self.interval,
            table='course_activity',
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
            overwrite_n_days=self.overwrite_n_days,
        )


class CourseActivityMysqlTask(WeeklyIntervalMixin, UserActivityDownstreamMixin, IncrementalMysqlInsertTask)

    @property
    def table(self):
        return "course_activity"

    @property
    def record_filter(self):
        return "interval_start='{start_date}' and interval_end='{end_date}'".format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_a.isoformat()
        )  # pylint: disable=no-member

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
        return CourseActivityDataTask(
            interval=self.interval,
            table='course_activity',
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
            overwrite_n_days=self.overwrite_n_days,
        )
