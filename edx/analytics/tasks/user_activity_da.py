"""Group events by institution and export them for research purposes"""

import datetime
import logging
import json
import luigi

from collections import defaultdict
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import url_path_join, get_target_from_url, ExternalURL
import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class DirectAccessEventCountTask(EventLogSelectionMixin, MapReduceJobTask):
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
    name = luigi.Parameter()
    dest = luigi.Parameter()

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

        if not opaque_key_util.is_valid_course_id(course_id):
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
        try:
            value = self.get_event_and_date_string(line)
        except TypeError:
            return

        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username or not username.startswith("anon__"):
            return

        evt_time = event.get('time')
        if not evt_time:
            return

        labels = extract_predicate_labels(event)

        for label in labels:
            if label == PLAY_VIDEO_LABEL:
                evt_evt = json.loads(event.get('event', "{}"))
#                log.info(evt_evt.get('id'))
                yield username, (label, evt_evt.get('id'))
            elif label == PROBLEM_LABEL:
#                log.info(event.get('event', "{}"))
                evt_evt = event.get('event', {})
                yield username, (label, evt_evt.get('problem_id'))
            elif label == POST_FORUM_LABEL:
                yield username, (label, "posted")

    def reducer(self, key, values):
        values_set = set(values)

        data = {
            PLAY_VIDEO_LABEL: 0,
            PROBLEM_LABEL: 0,
            POST_FORUM_LABEL: 0,
        }
        for value in values_set:
            data[value[0]] += 1

        for label in data:
            yield key, label, data[label]

    def output(self):
        output_name = u'direct_access_event_count_{name}/'.format(name=self.name)
        log.info(output_name)
        return get_target_from_url(url_path_join(self.dest, output_name))


class DirectAccessEventCountHist(EventLogSelectionMixin, MapReduceJobTask):
    name = luigi.Parameter()
    dest = luigi.Parameter()

    def requires(self):
        results = {
            'users': DirectAccessEventCountTask(
                mapreduce_engine=self.mapreduce_engine,
                interval=self.interval,
                name=self.name,
                dest=self.dest,
            ),
        }
        return results

    def output(self):
        output_name = u'direct_access_event_histogram_{name}/'.format(name=self.name)
        log.info(output_name)
        return get_target_from_url(url_path_join(self.dest, output_name))

    def mapper(self, line):
        username, label, count = line.split('\t')
        yield (label, count), username

    def reducer(self, key, values):
        yield key, len(list(values))


class DirectAccessLifetimeTask(EventLogSelectionMixin, MapReduceJobTask):
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
    name = luigi.Parameter()
    dest = luigi.Parameter()

    def mapper(self, line):
        """Default mapper implementation, that always outputs the log line, but with a configurable key."""
        try:
            value = self.get_event_and_date_string(line)
        except TypeError:
            return

        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username or not username.startswith("anon__"):
            return

        evt_time = event.get('time')
        if not evt_time:
            return

        yield username, eventlog.get_event_time(event)

    def reducer(self, key, values):
        values_list = list(values)
        timediff = max(values_list) - min(values_list)
        yield key, int(timediff.total_seconds() / 60)

    def output(self):
        output_name = u'direct_access_session_lifetime_{name}/'.format(name=self.name)
        log.info(output_name)
        return get_target_from_url(url_path_join(self.dest, output_name))


class DirectAccessLifetimeHist(EventLogSelectionMixin, MapReduceJobTask):
    name = luigi.Parameter()
    dest = luigi.Parameter()

    def requires(self):
        results = {
            'users': DirectAccessLifetimeTask(
                mapreduce_engine=self.mapreduce_engine,
                interval=self.interval,
                name=self.name,
                dest=self.dest,
            ),
        }
        return results

    def mapper(self, line):
        username, minutes = line.split('\t')
        yield minutes, username

    def reducer(self, key, values):
        yield key, len(list(values))

    def output(self):
        output_name = u'direct_access_session_lifetime_hist_{name}/'.format(name=self.name)
        log.info(output_name)
        return get_target_from_url(url_path_join(self.dest, output_name))


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

    labels = []

    if event_source == 'server':
        if event_type == 'problem_check':
            labels.append(PROBLEM_LABEL)

        if event_type.endswith("threads/create"):
            labels.append(POST_FORUM_LABEL)

    if event_source == 'browser':
        if event_type == 'play_video':
            labels.append(PLAY_VIDEO_LABEL)

    return labels
