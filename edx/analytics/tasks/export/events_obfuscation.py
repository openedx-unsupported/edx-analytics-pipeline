"""Obfuscate course event files by removing/stubbing user information."""

import gzip
import logging
import os
import re
from collections import defaultdict, namedtuple

import cjson
import luigi.date_interval

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.common.mapreduce import MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.file_util import read_config_file
from edx.analytics.tasks.util.geolocation import GeolocationMixin
from edx.analytics.tasks.util.obfuscate_util import (
    IMPLICIT_EVENT_TYPE_PATTERNS, ObfuscatorDownstreamMixin, ObfuscatorMixin
)
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)

ExplicitEventType = namedtuple("ExplicitEventType", ["event_source", "event_type"])

REDACTED_USERNAME = 'REDACTED_USERNAME'


class ObfuscateCourseEventsTask(ObfuscatorMixin, GeolocationMixin, MultiOutputMapReduceJobTask):
    """
    Task to obfuscate events for a particular course.

    Uses the output produced by EventExportByCourseTask as source for this task.
    """

    course = luigi.Parameter(default=None)
    explicit_event_whitelist = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'explicit_event_whitelist'}
    )
    dump_root = luigi.Parameter(default=None)

    def requires(self):
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        event_files_url = url_path_join(self.dump_root, filename_safe_course_id, 'events')
        return PathSetTask([event_files_url], ['*'])

    def requires_local(self):
        results = super(ObfuscateCourseEventsTask, self).requires_local()

        if os.path.basename(self.explicit_event_whitelist) != self.explicit_event_whitelist:
            results['explicit_events'] = ExternalURL(url=self.explicit_event_whitelist)
        return results

    def init_local(self):
        super(ObfuscateCourseEventsTask, self).init_local()

        with read_config_file(self.explicit_event_whitelist) as explicit_events_file:
            self.explicit_events = self.parse_explicit_events_file(explicit_events_file)

    def parse_explicit_events_file(self, explicit_events_file):
        """Parse explicit_events_file and load in memory."""

        events = set()
        for line in explicit_events_file:
            if not line.strip():
                continue
            if line.startswith('#'):
                continue
            _, event_source, event_type = line.rstrip('\n').split("\t")
            events.add(ExplicitEventType(event_source, event_type))

        return events

    def mapper(self, line):
        event = eventlog.parse_json_event(line)
        date_string = event['time'].split("T")[0]

        filtered_event = self._filter_event(event)
        if filtered_event is None:
            return

        yield date_string.encode('utf-8'), line.rstrip('\r\n')

    def multi_output_reducer(self, _key, values, output_file):
        with gzip.GzipFile(mode='wb', fileobj=output_file) as outfile:
            try:
                for value in values:
                    obfuscated_event_line = self.obfuscate_event_line(value)
                    outfile.write(obfuscated_event_line)
                    outfile.write('\n')
                    # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
                    # Do not remove it.
                    self.incr_counter('Obfuscated Event Exports', 'Raw Bytes Written', len(value) + 1)
            finally:
                outfile.close()

    def output_path_for_key(self, key):
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)

        return url_path_join(
            self.output_root,
            filename_safe_course_id,
            'events',
            '{course}-events-{date}.log.gz'.format(
                course=filename_safe_course_id,
                date=key,
            )
        )

    def obfuscate_event_line(self, line):
        """Parse an event line, obfuscate it, and convert back to a line."""
        input_event = eventlog.parse_json_event(line)
        obfuscated_event = self._obfuscate_event(input_event)
        return eventlog.encode_json(obfuscated_event).strip()

    def _filter_event(self, event):
        """Filter event using different event filtering criteria."""
        if event is None:
            return None

        event_type = event.get('event_type')
        event_source = event.get('event_source')
        if event_type is None or event_source is None:
            return None

        # Filter out synthetic events.  These are identifiable by the
        # 'synthesized' metadata being present in the event.
        if 'synthesized' in event:
            return None

        if event_source == 'server' and event_type.startswith('/'):
            return self._filter_implicit_event(event)
        elif ExplicitEventType(event_source, event_type) in self.explicit_events:
            return event

        return None

    def _filter_implicit_event(self, event):
        """Filter implicit event using the whitelist patterns."""

        event_type = event.get('event_type')

        match = opaque_key_util.COURSE_REGEX.match(event_type)
        if match:
            course_id_string = match.group('course_id')
            event_type = event_type.replace(course_id_string, '(course_id)')

        for included_event_type in IMPLICIT_EVENT_TYPE_PATTERNS:
            match = re.match(included_event_type, event_type)
            if match:
                return event

        return None

    def _get_user_id_as_int(self, user_id):
        """Convert possible str value of user_id to int or None."""
        if user_id is not None and not isinstance(user_id, int):
            if len(user_id) == 0:
                user_id = None
            else:
                user_id = int(user_id)
        return user_id

    def _get_user_info_for_user_id(self, user_id):
        """Return user_info entry for a user_id, or None if not found."""
        try:
            user_info = self.user_by_id[user_id]
            return user_info
        except KeyError:
            log.error("Unable to find user_id %s in the user_by_id map", user_id)
            return None

    def _get_user_info_for_username(self, username):
        """Return user_info entry for a username, or None if not found."""
        try:
            user_info = self.user_by_username[username]
            return user_info
        except KeyError:
            log.error("Unable to find username: %s in the user_by_username map", username)
            return None

    def _remap_username(self, username, user_info):
        """Return remapped version of username, or None if not remapped."""
        info = self._get_user_info_for_username(username)
        if info is not None:
            for key, value in info.iteritems():
                user_info[key].add(value)
            if 'user_id' in info:
                return self.generate_obfuscated_username_from_user_id(info['user_id'])

        return None

    def _get_log_string_for_event(self, event):
        """Create a string to use when logging errors, to provide some context."""
        event_type = event.get('event_type')
        if isinstance(event_type, str):
            event_type = event_type.decode('utf8')
        event_source = event.get('event_source')
        debug_str = u" [source='{}' type='{}']".format(event_source, event_type)
        return debug_str

    def _remap_user_info_in_event(self, event, event_data):
        """
        Harvest user info from event, and remap those values (in place) where appropriate.

        Returns a dict of iterables, with key values of 'username', 'user_id', and 'name'.

        """
        # Find user info, and
        debug_str = self._get_log_string_for_event(event)

        # Create a user_info structure to collect relevant user information to look
        # for elsewhere in the event.  We need to return a dictionary of iterables,
        # but since we will potentially be adding the same values repeatedly from
        # different parts of the event, a set will make sure these are deduped.
        user_info = defaultdict(set)

        # Note that eventlog.get_event_username() does a strip on the username and checks for zero-len,
        # so we don't have to do so here.
        username = eventlog.get_event_username(event)
        if username is not None:
            username = username.decode('utf8')
            remapped_username = self._remap_username(username, user_info)
            if remapped_username is not None:
                event['username'] = remapped_username
            else:
                log.error("Redacting unrecognized username for '%s' field: '%s' %s", 'username', username, debug_str)
                event['username'] = REDACTED_USERNAME

        # Get the user_id from context, either as an int or None, and remap.
        user_id = self._get_user_id_as_int(event.get('context', {}).get('user_id'))
        if user_id is not None:
            user_info['user_id'].add(user_id)
            info = self._get_user_info_for_user_id(user_id)
            if info is not None:
                for key, value in info.iteritems():
                    user_info[key].add(value)
                if username is not None and 'username' in info and username != info['username']:
                    log.error(
                        u"user_id ('%s'=>'%s') does not match username ('%s') %s",
                        user_id, info['username'], username, debug_str,
                    )
            event['context']['user_id'] = self.remap_id(user_id)

        # Clean username from context.
        if 'context' in event:
            # Remap value of username in context, if it is present.  (Removed in more recent events.)
            if 'username' in event['context'] and len(event['context']['username'].strip()) > 0:
                context_username = event['context']['username'].strip().decode('utf8')
                remapped_username = self._remap_username(context_username, user_info)
                if remapped_username is not None:
                    event['context']['username'] = remapped_username
                else:
                    log.error("Redacting unrecognized username for '%s' field: '%s' %s", 'context.username',
                              context_username, debug_str)
                    event['context']['username'] = REDACTED_USERNAME

        # Look into the event payload.
        if event_data:
            # Get the user_id from payload and remap.
            event_user_id = self._get_user_id_as_int(event_data.get('user_id'))
            if event_user_id is not None:
                user_info['user_id'].add(event_user_id)
                info = self._get_user_info_for_user_id(event_user_id)
                if info is not None:
                    for key, value in info.iteritems():
                        user_info[key].add(value)
                event_data['user_id'] = self.remap_id(event_user_id)

            # Remap values of usernames in payload, if present.  Usernames may appear with different key values.
            # TODO: confirm that these values are usernames, not user_id values. (User_id values will fail remapping.)
            for username_key in ['username', 'instructor', 'student', 'user']:
                if username_key in event_data and len(event_data[username_key].strip()) > 0:
                    event_username = event_data[username_key].strip().decode('utf8')
                    remapped_username = self._remap_username(event_username, user_info)
                    if remapped_username is not None:
                        event_data[username_key] = remapped_username
                    else:
                        log.error("Redacting unrecognized username for 'event.%s' field: '%s' %s",
                                  username_key, event_username, debug_str)
                        event_data[username_key] = REDACTED_USERNAME

        # Finally return the fully-constructed dict.
        return user_info

    def _obfuscate_event(self, event):
        """Obfuscate event by removing/stubbing user information."""

        # Create a string to use when logging errors, to provide some context.
        debug_str = self._get_log_string_for_event(event)

        # Remap the user information stored in the event, and collect for later searching.
        event_data = eventlog.get_event_data(event)
        user_info = self._remap_user_info_in_event(event, event_data)

        # Clean or remove values from context.
        if 'context' in event:
            # These aren't present in current events, but are generated historically by some implicit events.
            event['context'].pop('host', None)
            event['context'].pop('ip', None)
            # Not sure how to clean this, so removing.
            event['context'].pop('path', None)
            # Clean out some of the more obvious values in (mobile) client:
            if 'client' in event['context']:
                event['context']['client'].pop('device', None)
                event['context']['client'].pop('ip', None)

        # Do remaining cleanup on the entire event payload (assuming user-based remapping has been done).
        if event_data:
            # Remove possibly sensitive payload fields (or fields we don't know how to clean).
            for key in [
                    'certificate_id', 'certificate_url', 'source_url', 'fileName', 'GET', 'POST',
                    'requesting_student_id', 'report_url', 'url', 'url_name']:
                event_data.pop(key, None)

            for key in ['answer', 'saved_response']:
                if key in event_data and 'file_upload_key' in event_data[key]:
                    event_data[key].pop('file_upload_key')

            # Clean up remaining event payload recursively.
            updated_event_data = self.obfuscator.obfuscate_structure(event_data, u"event", user_info)
            if updated_event_data is not None:
                if self.obfuscator.is_logging_enabled():
                    log.info(u"Obfuscated payload: %s", debug_str)
                event_data = updated_event_data

            # Re-encode payload as a json string if it originally was one.
            # (This test works because we throw away string values that didn't parse as JSON.)
            if isinstance(event.get('event'), basestring):
                event['event'] = cjson.encode(event_data)
            else:
                event['event'] = event_data

        ip_address = event.get('ip')
        # Skip over IPv6-format ip_address values for now.
        if ip_address and ':' not in ip_address:
            country_code = self.get_country_code(ip_address)
            event.update({'augmented': {'country_code': country_code}})

        # Delete base properties other than username.
        for key in ['host', 'ip', 'page', 'referer']:
            event.pop(key, None)

        return event

    def extra_modules(self):
        import numpy
        import pygeoip
        return [numpy, pygeoip]


class EventObfuscationTask(ObfuscatorDownstreamMixin, MapReduceJobTaskMixin, luigi.WrapperTask):
    """Wrapper task for course events obfuscation."""

    course = luigi.ListParameter()
    dump_root = luigi.Parameter()
    output_root = luigi.Parameter()
    explicit_event_whitelist = luigi.Parameter(
        config_path={'section': 'obfuscation', 'name': 'explicit_event_whitelist'}
    )

    def requires(self):
        for course in self.course:   # pylint: disable=not-an-iterable
            # We already have course events dumped separately, so each ObfuscateCourseEventsTask
            # would have a different source.
            kwargs = {
                'course': course,
                'dump_root': self.dump_root,
                'output_root': self.output_root,
                'explicit_event_whitelist': self.explicit_event_whitelist,
                'n_reduce_tasks': self.n_reduce_tasks,
                'entities': self.entities,
                'log_context': self.log_context,
                'auth_user_path': self.auth_user_path,
                'auth_userprofile_path': self.auth_userprofile_path,
            }
            yield ObfuscateCourseEventsTask(**kwargs)
