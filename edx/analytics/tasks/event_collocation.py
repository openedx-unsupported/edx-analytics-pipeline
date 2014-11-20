"""Group events by institution and export them for research purposes"""

import datetime
import logging
import re

import luigi

from edx.analytics.tasks.user_activity import UserActivityBaseTask, UserActivityBaseTaskDownstreamMixin
from edx.analytics.tasks.url import url_path_join, get_target_from_url, ExternalURL
import edx.analytics.tasks.util.eventlog as eventlog

log = logging.getLogger(__name__)


class UserCollocationTask(UserActivityBaseTask):
    """Make a basic task to gather action collocations per user for a single time interval."""
    max_time = luigi.IntParameter(default=300)

    def get_mapper_key(self, course_id, user_id, date_string):
        # For now, break up processing into individual days.
        # This arbitrarily breaks context across midnight, but
        # it will be easier.
        return self._encode_tuple([course_id, user_id, date_string])

    def get_event_name_and_id(self, event):
        event_type = event.get('event_type')
        event_source = event.get('event_source')

        if event_source == 'server':
            if event_type == 'problem_check':
                event_dict = eventlog.get_event_data(event)
                if event_dict:
                    event_id = event_dict.get('problem_id')
                    return event_type, event_id

            # Make an attempt to harvest some forum activity.  For this,
            # harvest the information from the implicit event.
            # Begins with /courses/<course>/discussion/,
            # but may have a .../forum/i4x-<course> and a bunch of other things.
            # For now, just lump these all together.  Note that "/reply" is probably
            # more useful than "threads/create".

            elif '/discussion/' in event_type:
                # Just grab whatever follows the 'discussion', and simplify the event-type for now. 
                event_id = event_type.split('/discussion/')[1]
                return "discussion", event_id

            elif '/wiki/' in event_type:
                # Just grab whatever follows the 'wiki', and simplify the event-type for now. 
                event_id = event_type.split('/wiki/')[1]
                return "wiki", event_id

        if event_source == 'browser':
            if event_type == 'play_video':
                # event: {"id":"i4x-ANUx-ANU-ASTRO3x-video-e75b6863d9af45839b7536d21fd6e75a","currentTime":0,"code":"oi7DqsnFB1k"}
                # page: https://courses.edx.org/courses/ANUx/ANU-ASTRO3x/4T2014/courseware/fc2c2ebe4e3c4636aef36a0b302a50cd/c649ed78ccaf4fbfbd7c7f5b99cd2fdd/
                event_dict = eventlog.get_event_data(event)
                if event_dict:
                    event_id = event_dict.get('id')
                    return event_type, event_id

            elif event_type == 'book':
                event_dict = eventlog.get_event_data(event)
                if event_dict:
                    event_id = event_dict.get('chapter')
                    return event_type, event_id

        return event_type, None

    def get_mapper_value(self, event):
        event_name, event_id = self.get_event_name_and_id(event)
        if event_id is not None:
            timestamp = eventlog.get_event_time_string(event)
            if timestamp is None:
                log.error("encountered event with bad timestamp: %s", event)
                return None
            return (timestamp, event_name, event_id)
        return None

    def get_user_id(self, event):
        """Gets user_id from event's data."""
        # TODO: move into eventlog

        # Get the event data:
        event_context = event.get('context')
        if event_context is None:
            return None

        # Get the course_id from the data, and validate.
        user_id = event_context.get('user_id', '')
        if not user_id:
            return None

        return user_id

    def mapper(self, line):
        """Default mapper implementation, that always outputs the log line, but with a configurable key."""
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        user_id = self.get_user_id(event)
        if not user_id:
            return

        course_id = self.get_course_id(event)
        if not course_id:
            return

        key = self.get_mapper_key(course_id, user_id, date_string)
        value = self.get_mapper_value(event)
        if value:
            yield key, value

    def reducer(self, key, values):
        """Outputs labels and user_ids for a given course and interval."""
        course_id, user_id, date_string = key

        event_stream_processor = CollocatedEventProcessor(course_id, user_id, date_string, values, self.max_time)
        for collocation in event_stream_processor.get_collocations():
            yield self._encode_tuple(collocation)

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.output_root,
                'user-collocation-per-interval/'
            )
        )

                
class CollocatableEvent(object):
    """The critical information necessary to process the event in the event stream."""

    def __init__(self, timestamp, event_name, event_id):
        self.timestamp = timestamp
        self.datestamp = eventlog.timestamp_to_datestamp(timestamp)
        self.event_name = event_name
        self.event_id = event_id
        self.timestamp_object = self._get_datetime_from_timestamp(self.timestamp)

    def _get_datetime_from_timestamp(self, timestamp):
        date_parts = [int(d) for d in re.split(r'[:\-\.T]', timestamp)]
        return datetime.datetime(*date_parts)


class CollocatedEventProcessor(object):

    def __init__(self, course_id, user_id, date_string, events, max_time):
        self.course_id = course_id
        self.user_id = user_id
        self.date_string = date_string
        self.max_time = max_time

        self.sorted_events = sorted(events)
        self.sorted_events = [
            CollocatableEvent(timestamp, event_name, event_id) for timestamp, event_name, event_id in self.sorted_events
        ]

    def get_collocations(self):
        for source_index in range(len(self.sorted_events) - 1):
            for dest_index in range(source_index + 1, len(self.sorted_events) - 1):
                source_value = self.sorted_events[source_index]
                dest_value = self.sorted_events[dest_index]
                time_diff = (dest_value.timestamp_object - source_value.timestamp_object).seconds
                num_events = dest_index - source_index
                if not self.max_time or time_diff <= self.max_time:
                    yield self.collocation_record(
                        source_value.timestamp, dest_value.timestamp, time_diff,
                        num_events, source_value.event_name, source_value.event_id, 
                        dest_value.event_name, dest_value.event_id)
                else:
                    break

    def collocation_record(self, first_timestamp, second_timestamp, time_diff,
                           num_events, first_event_name, first_event_id, second_event_name, second_event_id):
        """A complete collocation record."""
        return (self.date_string, self.course_id, self.user_id, first_timestamp, second_timestamp, time_diff,
                num_events, first_event_name, first_event_id, second_event_name, second_event_id)


# class CourseUserCollocationTable(CourseEnrollmentTableDownstreamMixin, ImportIntoHiveTableTask):
#     """Hive table that stores the set of users enrolled in each course over time."""

#     @property
#     def table_name(self):
#         return 'course_user_collocation'

#     @property
#     def columns(self):
#         return [
#             ('date', 'STRING'),
#             ('course_id', 'STRING'),
#             ('user_id', 'INT'),
#             ('at_end', 'TINYINT'),
#             ('change', 'TINYINT'),
#         ]

#     @property
#     def table_location(self):
#         return url_path_join(self.warehouse_path, self.table_name)

#     @property
#     def table_format(self):
#         """Provides name of Hive database table."""
#         return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"

#     @property
#     def partition_date(self):
#         return self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

#     def requires(self):
#         return CourseEnrollmentTask(
#             mapreduce_engine=self.mapreduce_engine,
#             n_reduce_tasks=self.n_reduce_tasks,
#             source=self.source,
#             interval=self.interval,
#             pattern=self.pattern,
#             output_root=self.partition_location,
#         )
