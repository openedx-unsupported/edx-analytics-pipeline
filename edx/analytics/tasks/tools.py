
import datetime
import logging
import re
import gzip

import cjson
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join, ExternalURL
from edx.analytics.tasks.util import eventlog, opaque_key_util


log = logging.getLogger(__name__)


class SimpleGrep(MapReduceJobTask):

    input_path = luigi.Parameter()
    output_root = luigi.Parameter()
    search = luigi.Parameter(is_list=True)

    def requires(self):
        return ExternalURL(self.input_path)

    def mapper(self, line):
        for p in self.search:
            if re.search(p, line):
                yield (line,)
                break

    def output(self):
        return get_target_from_url(self.output_root)


class Grep(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()
    search = luigi.Parameter(is_list=True)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        for p in self.search:
            if re.search(p, line):
                yield (line,)
                break

    def output(self):
        return get_target_from_url(self.output_root)


class FindUsers(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        payload_str = event.get('event')
        if not payload_str:
            return

        try:
            payload = cjson.decode(payload_str)
        except:
            return

        try:
            get_dict = payload['GET']
        except:
            return

        if 'password' not in get_dict:
            return

        emails = get_dict.get('email')
        if not emails:
            return

        try:
            for email in emails:
                yield (email, 1)
        except:
            yield(emails, 1)

    def reducer(self, email, values):
        yield (email,)

    def output(self):
        return get_target_from_url(self.output_root)


class UserHistoryTask(EventLogSelectionMixin, MultiOutputMapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        username = event['username']
        timestamp = eventlog.get_event_time_string(event)
        if not timestamp:
            return

        m = re.match(r'[A-Za-z0-9.-_]+', username)
        if not m:
            log.debug('Username "%s" failed validation', username.encode('ascii'))
            self.incr_counter('edx-analytics-pipeline', 'Username Validation Failed', 1)
            return

        yield username, (timestamp, line.strip())

    def output_path_for_key(self, key):
        return url_path_join(
            self.output_root,
            key + '.json'
        )

    def multi_output_reducer(self, key, values, output_file):
        total_size = 0
        user_history = []
        for timestamp, event_string in values:
            user_history.append((timestamp, event_string))
            total_size += len(timestamp) + len(event_string)
            if total_size > 1e9:
                self.incr_counter('edx-analytics-pipeline', 'User History Exceeded 1GB', 1)
                output_file.write('Too much history for this user\n')
                return

        for timestamp, event_string in sorted(user_history):
            output_file.write(event_string + '\n')


class EventCounter(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        key = self.get_grouping_key(line, event, date_string)
        if key is None:
            return
        encoded_key = []
        for part in key:
            if isinstance(part, basestring):
                encoded_key.append(part.encode('utf8'))
            else:
                encoded_key.append(part)

        yield tuple(encoded_key), (1, len(line))

    def get_grouping_key(self, line, event, date_string):
        raise NotImplementedError

    def reducer(self, key, values):
        num_events = 0
        total_size = 0
        for count, length in values:
            num_events += count
            total_size += length
        yield key, (num_events, total_size)

    # combiner = reducer

    def output(self):
        return get_target_from_url(self.output_root)


class EventsPerCourseModule(EventCounter):

    def get_grouping_key(self, line, event, date_string):
        course_id = eventlog.get_course_id(event)
        event_data = eventlog.get_event_data(event)
        event_type = event.get('event_type')
        event_source = event.get('event_source', '')

        if event_type is None or event_data is None:
            return None

        module_id = None
        if event_type.endswith('_video') and 'id' in event_data:
            module_id = event_data['id']
        elif event_type == 'problem_check' and event_source == 'server' and 'problem_id' in event_data:
            module_id = event_data['problem_id']

        if module_id is None or course_id is None:
            return None

        return course_id, module_id


class EventsPerCourse(EventCounter):

    def get_grouping_key(self, line, event, date_string):
        return eventlog.get_course_id(event)


class EventsPerUserCourse(EventCounter):

    def get_grouping_key(self, line, event, date_string):
        course_id = eventlog.get_course_id(event)
        username = event.get('username')
        if course_id is None or username is None:
            return None

        return course_id, username


class EventsPerUser(EventCounter):

    def get_grouping_key(self, line, event, date_string):
        username = event.get('username')
        if username is None:
            return None

        return username


class EventsPerCourseEventType(EventCounter):

    def get_grouping_key(self, line, event, date_string):
        course_id = eventlog.get_course_id(event)
        event_type = event.get('event_type')
        if course_id is None or event_type is None:
            return None

        return course_id, event_type


class ProblemCheckEventCount(EventCounter):

    def get_grouping_key(self, line, event, date_string):
        event_type = event.get('event_type')
        event_source = event.get('event_source', '')

        if event_type is None:
            return None

        if event_type == 'problem_check' and event_source == 'server':
            return (date_string,)

        return None


class GatherEventsByCourse(EventLogSelectionMixin, MultiOutputMapReduceJobTask):

    output_root = luigi.Parameter()
    course_id = luigi.Parameter(is_list=True)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        course_id = eventlog.get_course_id(event)
        if not course_id or course_id not in self.course_id:
            return

        yield (course_id.encode('utf8'), line.strip())

    def multi_output_reducer(self, course_id, lines, output_file):
        outfile = gzip.GzipFile(mode='wb', fileobj=output_file)
        try:
            for line in lines:
                outfile.write(line.strip())
                outfile.write('\n')
                # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
                # Do not remove it.
                self.incr_counter('Event Export', 'Raw Bytes Written', len(line) + 1)
        finally:
            outfile.close()

    def output_path_for_key(self, course_id):
        filename = "{course_id}-events.gz".format(
            course_id=opaque_key_util.get_filename_safe_course_id(course_id, '-')
        )
        return url_path_join(self.output_root, filename)


class EventsPerWeek(EventCounter):

    def get_grouping_key(self, line, event, date_string):
        split_date = date_string.split('-')
        if len(split_date) != 3:
            return None
        try:
            dt = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
        except TypeError:
            return None

        iso_year, iso_week_number, iso_weekday = dt.isocalendar()
        return (str(iso_year), str(iso_week_number))


class EventsPerDay(EventCounter):

    def get_grouping_key(self, _line, _event, date_string):
        return (date_string,)
