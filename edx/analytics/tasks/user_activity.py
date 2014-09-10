"""Group events by institution and export them for research purposes"""

from collections import defaultdict
import logging

import luigi
from luigi.hive import ExternalHiveTask

from edx.analytics.tasks.calendar import ImportCalendarToHiveTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.util.hive import (
    ImportIntoHiveTableTask,
    HiveTableFromQueryTask,
    WarehouseDownstreamMixin,
    HivePartition,
    TABLE_FORMAT_TSV,
    hive_database_name,
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.url import get_target_from_url, ExternalURL
import edx.analytics.tasks.util.eventlog as eventlog

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class CategorizeUserActivityTask(EventLogSelectionMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    Categorize events from logs over a selected interval of time.

    Parameters:
        The following are defined in EventLogSelectionMixin:

        source: A URL to a path that contains log files that contain the events.
        interval: The range of dates to export logs for.
        pattern: A regex with a named capture group for the date that approximates the date that the events within were
            emitted. Note that the search interval is expanded, so events don't have to be in exactly the right file
            in order for them to be processed.
    """

    output_root = luigi.Parameter()

    def mapper(self, line):
        """Default mapper implementation, that always outputs the log line, but with a configurable key."""
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        course_id = self.get_course_id(event)
        if not course_id:
            return

        predicate_labels = self.extract_predicate_labels(event)
        key = self._encode_tuple((course_id, username, date_string))

        for label in predicate_labels:
            yield key, label

    def get_course_id(self, event):
        """Gets course_id from event's data."""
        # TODO: move into eventlog

        # Get the event data:
        event_context = event.get('context')
        if event_context is None:
            # Assume it's old, and not worth logging...
            return None

        # Get the course_id from the data, and validate.
        course_id = event_context.get('course_id', '')
        if not course_id:
            return None

        if not eventlog.is_valid_course_id(course_id):
            log.error("encountered event with bogus course_id: %s", event)
            return None

        return course_id

    def extract_predicate_labels(self, event):
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

            if event_type.endswith("threads/create"):
                labels.append(POST_FORUM_LABEL)

        if event_source == 'browser':
            if event_type == 'play_video':
                labels.append(PLAY_VIDEO_LABEL)

        return labels

    def _encode_tuple(self, values):
        """
        Convert values into a tuple containing encoded strings.

        Parameters:
            Values is a list or tuple.

        This enforces a standard encoding for the parts of the key. Without this a part of the key might appear
        differently in the key string when it is coerced to a string by luigi. For example, if the same key value
        appears in two different records, one as a str() type and the other a unicode() then without this change they
        would appear as u'Foo' and 'Foo' in the final key string. Although python doesn't care about this difference,
        hadoop does, and will bucket the values separately. Which is not what we want.
        """
        # TODO: refactor this into a utility function and update jobs
        # to always UTF8 encode mapper keys.
        if len(values) > 1:
            return tuple([value.encode('utf8') for value in values])
        else:
            return values[0].encode('utf8')

    def reducer(self, key, values):
        """Outputs labels and usernames for a given course and interval."""
        course_id, username, date_string = key
        category_counts = defaultdict(int)
        for category in values:
            category_counts[category] += 1
        for category, count in category_counts.iteritems():
            yield date_string, course_id, username, category, count

    def output(self):
        return get_target_from_url(self.output_root)


class UserActivityDailyTask(ImportIntoHiveTableTask):
    """
    Creates a Hive Table that points to Hadoop output of CategorizeUserActivityTask task.
    """

    interval = luigi.DateIntervalParameter()

    @property
    def table_name(self):
        return 'user_activity_daily'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('category', 'STRING'),
            ('count', 'INT')
        ]

    @property
    def table_format(self):
        """Provides structure of Hive external table data."""
        return TABLE_FORMAT_TSV

    @property
    def partition(self):
        return HivePartition('interval', str(self.interval))

    def requires(self):
        return ExternalURL(self.partition_location)


class UserActivityDailyWorkflow(
    EventLogSelectionDownstreamMixin,
    MapReduceJobTaskMixin,
    UserActivityDailyTask
):
    """Populate the daily user activity data using a map reduce job"""

    def requires(self):
        return CategorizeUserActivityTask(
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite=self.overwrite,
        )


class UserActivityWeeklyTask(HiveTableFromQueryTask):
    """Calculate unique user counts per week"""

    interval = luigi.Parameter()

    @property
    def insert_query(self):
        return """
            SELECT
                act.course_id as course_id,
                cal.iso_week_start as interval_start,
                cal.iso_week_end as interval_end,
                act.category as category,
                COUNT(DISTINCT username) as num_users
            FROM calendar cal
            LEFT OUTER JOIN user_activity_daily act ON act.date = cal.date
            WHERE category IS NOT NULL
            GROUP BY cal.iso_week_start, cal.iso_week_end, act.course_id, act.category;
        """

    @property
    def table_name(self):
        return 'user_activity_weekly'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('interval_start', 'STRING'),
            ('interval_end', 'STRING'),
            ('label', 'STRING'),
            ('count', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('interval', str(self.interval))

    def requires(self):
        yield (
            ExternalHiveTask(table='calendar', database=hive_database_name()),
            ExternalHiveTask(table='user_activity_daily', database=hive_database_name()),
        )


class UserActivityWeeklyWorkflow(
    EventLogSelectionDownstreamMixin,
    MapReduceJobTaskMixin,
    UserActivityWeeklyTask
):
    """Populate the tables required to compute weekly activity"""

    def requires(self):
        yield (
            ImportCalendarToHiveTask(
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            ),
            UserActivityDailyWorkflow(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            ),
        )


class UserActivityMysqlInsertTask(MysqlInsertTask):
    """Define course_activity table."""
    @property
    def table(self):
        return 'course_activity'

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
        raise NotImplementedError


class UserActivityWorkflow(
        EventLogSelectionDownstreamMixin,
        MapReduceJobTaskMixin,
        WarehouseDownstreamMixin,
        OverwriteOutputMixin,
        UserActivityMysqlInsertTask):
    """Write to course_activity table to mysql."""

    @property
    def insert_source_task(self):
        return UserActivityWeeklyWorkflow(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def init_copy(self, connection):
        if self.overwrite:
            # pylint: disable=no-member
            query = "DELETE FROM {table} WHERE interval_start >= {start} AND interval_start < {end}".format(
                    table=self.table,
                    start=self.interval.date_a.isoformat(),
                    end=self.interval.date_b.isoformat()
            )
            log.info('Removing stale data with query: %s', query)
            connection.cursor().execute(query)
