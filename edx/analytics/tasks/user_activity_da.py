"""Group events by institution and export them for research purposes"""

import logging
import json
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import url_path_join, get_target_from_url
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
        if not username or not username.startswith("anon__"):  # filter out non-direct-access user
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


class CourseFilterEventCountTask(EventLogSelectionMixin, MapReduceJobTask):
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
    course_ids_allowed = {
        "DB/Constraints/SelfPaced",
        "DB/Indexes/SelfPaced",
        "DB/JSON/SelfPaced",
        "DB/OLAP/SelfPaced",
        "DB/RA/SelfPaced",
        "DB/RD/SelfPaced",
        "DB/RDB/SelfPaced",
        "DB/Recursion/SelfPaced",
        "DB/SQL/SelfPaced",
        "DB/UML/SelfPaced",
        "DB/Views/SelfPaced",
        "DB/XML/SelfPaced",
        "DB/XPath/SelfPaced",
        "DB/XSLT/SelfPaced",
        "Education/OpenKnowledge/Fall2014",
        "Engineering/CS144/Introduction_to_Computer_Networking",
        "Engineering/Networking/Winter2014",
        "JaneU/test2/JaneTest2",
        "SampleUniversity/102/Fall2013",
        "StanfordOnline/JS101/2014",
        "StanfordOnline/OpenEdX/Demo",
    }

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
        if not username or username.startswith("anon__"):  # filter out direct access
            return

        if self.get_course_id(event) not in self.course_ids_allowed:
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
        output_name = u'course_filter_event_count_{name}/'.format(name=self.name)
        log.info(output_name)
        return get_target_from_url(url_path_join(self.dest, output_name))


class CourseFilterEventCountHist(EventLogSelectionMixin, MapReduceJobTask):
    name = luigi.Parameter()
    dest = luigi.Parameter()

    def requires(self):
        results = {
            'users': CourseFilterEventCountTask(
                mapreduce_engine=self.mapreduce_engine,
                interval=self.interval,
                name=self.name,
                dest=self.dest,
            ),
        }
        return results

    def output(self):
        output_name = u'course_filter_event_histogram_{name}/'.format(name=self.name)
        log.info(output_name)
        return get_target_from_url(url_path_join(self.dest, output_name))

    def mapper(self, line):
        username, label, count = line.split('\t')
        yield (label, count), username

    def reducer(self, key, values):
        yield key, len(list(values))


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
