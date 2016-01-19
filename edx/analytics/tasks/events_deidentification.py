"""Deidentify course event files by removing/stubbing user information."""

import logging
import re
import luigi
import luigi.date_interval
import gzip
import csv
import cjson
import os
import sys
from collections import namedtuple

from edx.analytics.tasks.pathutil import PathSetTask, EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.url import ExternalURL, url_path_join
from edx.analytics.tasks.util.id_codec import UserIdRemapperMixin

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
import edx.analytics.tasks.util.csv_util
import edx.analytics.tasks.util.deid_util as deid_util
from edx.analytics.tasks.util import eventlog

log = logging.getLogger(__name__)

ExplicitEventType = namedtuple("ExplicitEventType", ["event_source", "event_type"])


class DeidentifyCourseEventsTask(UserIdRemapperMixin, MultiOutputMapReduceJobTask):
    """
    Task to deidentify events for a particular course.
    Uses the output produced by EventExportByCourseTask as source for this task.
    """

    course = luigi.Parameter(default=None)
    explicit_event_whitelist = luigi.Parameter(
        config_path={'section': 'events-deidentification', 'name': 'explicit_event_whitelist'}
    )
    dump_root = luigi.Parameter(default=None)

    def requires(self):
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        event_files_url = url_path_join(self.dump_root, filename_safe_course_id, 'events')
        return PathSetTask([event_files_url], ['*'])

    def requires_local(self):
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(self.course)
        results = {
            'auth_user': PathSetTask([url_path_join(self.dump_root, filename_safe_course_id, 'state')], ['*-auth_user-*'])
        }
        if os.path.basename(self.explicit_event_whitelist) != self.explicit_event_whitelist:
            results['explicit_events'] = ExternalURL(url=self.explicit_event_whitelist)

        return results

    def init_local(self):
        super(DeidentifyCourseEventsTask, self).init_local()

        self.explicit_events = []
        if self.input_local().get('explicit_events') is not None:
            with self.input_local()['explicit_events'].open('r') as explicit_events_file:
                self.explicit_events = self.parse_explicit_events_file(explicit_events_file)
        else:
            default_filepath = os.path.join(sys.prefix, 'share', 'edx.analytics.tasks', self.explicit_event_whitelist)
            with open(default_filepath, 'r') as explicit_events_file:
                self.explicit_events = self.parse_explicit_events_file(explicit_events_file)

        # read the latest auth_user for this course in memory, needed to remap usernames
        latest_auth_user_target = sorted(self.input_local()['auth_user'], key=lambda target: target.path)[-1]

        self.username_map = {}

        with latest_auth_user_target.open('r') as auth_user_file:
            reader = csv.reader(auth_user_file, dialect='mysqlexport')
            next(reader, None)  # skip header
            for row in reader:
                user_id = row[0]
                username = row[1]
                self.username_map[username] = user_id

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

        filtered_event = self.filter_event(event)

        if filtered_event is None:
            return

        deidentified_event = self.deidentify_event(filtered_event)
        if deidentified_event is None:
            return

        yield date_string.encode('utf-8'), cjson.encode(deidentified_event)

    def multi_output_reducer(self, _key, values, output_file):
        with gzip.GzipFile(mode='wb', fileobj=output_file) as outfile:
            try:
                for value in values:
                    outfile.write(value.strip())
                    outfile.write('\n')
                    # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
                    # Do not remove it.
                    self.incr_counter('Deidentified Event Exports', 'Raw Bytes Written', len(value) + 1)
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

    def filter_event(self, event):
        """Filter event using different event filtering criteria."""
        if event is None:
            return None

        event_type = event.get('event_type')

        event_source = event.get('event_source')

        if event_type is None or event_source is None:
            return None

        if event_source == 'server' and event_type.startswith('/'):
            return self.filter_implicit_event(event)
        elif ExplicitEventType(event_source, event_type) in self.explicit_events:
            return event

        return None

    def filter_implicit_event(self, event):
        """Filter implicit event using the whitelist patterns."""

        event_type = event.get('event_type')

        match = opaque_key_util.COURSE_REGEX.match(event_type)
        if match:
            course_id_string = match.group('course_id')
            event_type = event_type.replace(course_id_string, '(course_id)')

        for included_event_type in deid_util.IMPLICIT_EVENT_TYPE_PATTERNS:
            match = re.match(included_event_type, event_type)
            if match:
                return event

        return None

    def deidentify_event(self, event):
        """Deidentify event by removing/stubbing user information."""

        if 'context' in event and 'user_id' in event['context'] and event['context']['user_id']:  # found '' as user_id
            event['context']['user_id'] = self.remap_id(event['context']['user_id'])

        event.pop('host', None)  # delete host
        event.pop('ip', None)  # delete ip

        if event['username']:
            try:
                event['username'] = self.generate_deid_username_from_user_id(self.username_map[event['username']])
            except KeyError:
                log.error("Unable to find username: %s in the username_map", event['username'])
                return None

        event_data = eventlog.get_event_data(event)

        if event_data:
            if 'user_id' in event_data and event_data['user_id']:
                event_data['user_id'] = self.remap_id(event_data['user_id'])

            if 'username' in event_data:
                # How to DRY ?
                try:
                    event_data['username'] = self.generate_deid_username_from_user_id(self.username_map[event_data['username']])
                except KeyError:
                    log.error("Unable to find username: %s in the username_map", event_data['username'])
                    return None

            for key in ['certificate_id', 'certificate_url', 'source_url', 'fileName', 'GET', 'POST']:
                event_data.pop(key, None)

            for key in ['answer', 'saved_response']:
                if key in event_data and 'file_upload_key' in event_data[key]:
                    event_data[key].pop('file_upload_key')

            # TODO: clean ?

            if isinstance(event.get('event'), basestring):  # re-encode as a json string if originally
                event['event'] = cjson.encode(event_data)
            else:
                event['event'] = event_data

        return event

    def extra_modules(self):
        import numpy
        return [numpy]

