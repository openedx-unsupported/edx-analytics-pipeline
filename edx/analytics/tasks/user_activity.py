"""Group events by institution and export them for research purposes"""

import datetime
import logging

import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import url_path_join, get_target_from_url, ExternalURL
import edx.analytics.tasks.util.eventlog as eventlog

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class UserActivityBaseTaskDownstreamMixin(object):
    """Defines output_root parameter with appropriate default."""

    output_root = luigi.Parameter(
        default_from_config={'section': 'user-activity', 'name': 'output_root'}
    )


class UserActivityBaseTask(EventLogSelectionMixin, MapReduceJobTask, UserActivityBaseTaskDownstreamMixin):
    """
    Base class to extract events from logs over a selected interval of time.

    This class provides a parameterized mapper.  Derived classes must
    override the predicates applied to events, and the definition of
    what is a key and what a value from the mapper.  Derived classes
    also define the reducer that consumes the mapper output.

    Parameters:
        output_root: Directory to store the output in.

        The following are defined in EventLogSelectionMixin:

        source: A URL to a path that contains log files that contain the events.
        interval: The range of dates to export logs for.
        pattern: A regex with a named capture group for the date that approximates the date that the events within were
            emitted. Note that the search interval is expanded, so events don't have to be in exactly the right file
            in order for them to be processed.
    """

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

    def get_predicate_labels(self, event):
        """Override this with calculation of one or more possible labels for the current event."""
        raise NotImplementedError

    def get_mapper_key(self, _course_id, _username, _date_string):
        """Return a tuple representing the key to use for a log entry, or None if skipping."""
        raise NotImplementedError

    def get_mapper_value(self, _course_id, _username, _date_string, label):
        """Return a tuple representing the value to use for a log entry."""
        raise NotImplementedError

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

        predicate_labels = self.get_predicate_labels(event)
        key = self.get_mapper_key(course_id, username, date_string)

        for label in predicate_labels:
            output_value = self.get_mapper_value(course_id, username, date_string, label)
            yield self._encode_tuple(key), output_value


def extract_predicate_labels(event):
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


class UserActivityPerIntervalTask(UserActivityBaseTask):
    """Make a basic task to gather activity per user for a single time interval."""

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.output_root,
                'user-activity-per-interval-{interval}.tsv/'.format(interval=self.interval),
            )
        )

    def get_predicate_labels(self, event):
        return extract_predicate_labels(event)

    def get_mapper_key(self, course_id, username, date_string):
        # This is much faster than strptime
        current_date = datetime.date(*[int(a) for a in date_string.split('-')])

        # The interval is open, so the last day included in it will be one day less than the end date.
        interval_end = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member

        week_from_end = (interval_end - current_date).days / 7

        interval_end_date = self.interval.date_b - datetime.timedelta(weeks=week_from_end)  # pylint: disable=no-member
        interval_start_date = interval_end_date - datetime.timedelta(weeks=1)

        # For daily output, do reduction on all of these.
        return (course_id, username, '-'.join([d.isoformat() for d in (interval_start_date, interval_end_date)]))

    def get_mapper_value(self, _course_id, _username, _date_string, label):
        return label

    def reducer(self, key, values):
        """Outputs labels and usernames for a given course and interval."""
        course_id, username, interval_string = key
        interval_nums = interval_string.split('-')
        interval_start = '-'.join(interval_nums[0:3])
        interval_end = '-'.join(interval_nums[3:6])

        # Dedupe the output values.
        output_values = []
        for value in values:
            if value not in output_values:
                yield course_id, interval_start, interval_end, value, username
                output_values.append(value)


class CountLastElementMixin(object):
    """Replaces the last element in a tuple with the count of values."""
    def mapper(self, line):
        """Counts number of values of last element."""
        values = line.split('\t')
        yield tuple(values[0:-1]), 1

    def reducer(self, key, values):
        """Sums values for the given key. """
        count = sum(int(v) for v in values)
        yield key, count


class CountUserActivityPerIntervalTask(
        CountLastElementMixin,
        MapReduceJobTask,
        UserActivityBaseTaskDownstreamMixin,
        EventLogSelectionDownstreamMixin):
    """Counts the number of users for each course/interval/label combination."""

    def requires(self):
        return UserActivityPerIntervalTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.output_root,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
        )

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.output_root,
                'count-user-activity-per-interval-{interval}.tsv/'.format(interval=self.interval),
            )
        )


class InsertToMysqlCourseActivityTableMixin(MysqlInsertTask):
    """
    Define course_activity table.
    """
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

    def init_copy(self, connection):
        connection.cursor().execute("DELETE FROM " + self.table)


class InsertToMysqlCourseActivityTable(InsertToMysqlCourseActivityTableMixin):
    """
    Write to course_activity table from specified source.
    """
    insert_source = luigi.Parameter()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.insert_source)


class CountUserActivityPerIntervalTaskWorkflow(
        InsertToMysqlCourseActivityTableMixin,
        UserActivityBaseTaskDownstreamMixin,
        MapReduceJobTaskMixin,
        EventLogSelectionDownstreamMixin):
    """
    Write to course_activity table from CountUserActivityPerInterval.
    """

    @property
    def insert_source_task(self):
        return CountUserActivityPerIntervalTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.output_root,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
        )
