"""Group events by institution and export them for research purposes"""

import datetime
import logging
import re
import textwrap

import luigi
from luigi.hive import HiveQueryTask

from edx.analytics.tasks.database_imports import ImportIntoHiveTableTask
from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin
from edx.analytics.tasks.user_activity import UserActivityBaseTask, UserActivityBaseTaskDownstreamMixin
from edx.analytics.tasks.url import url_path_join, get_target_from_url, ExternalURL
import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import hive_database_name

log = logging.getLogger(__name__)


class UserCollocationTaskMixin(UserActivityBaseTaskDownstreamMixin):
    """
    Define parameters for user-collocation task.

    Parameters:
        max_time:  maximum number of seconds duration between events.
    """
    max_time = luigi.IntParameter(default=300)


class UserCollocationTask(UserCollocationTaskMixin, UserActivityBaseTask):
    """Make a basic task to gather action collocations per user for a single time interval."""

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
                # The "id", the "code", and the "page" may all provide relevant identifiers.
                # event: {"id":"i4x-ANUx-ANU-ASTRO3x-video-e75b6863d9af45839b7536d21fd6e75a","currentTime":0,"code":"oi7DqsnFB1k"}
                # page: https://courses.edx.org/courses/ANUx/ANU-ASTRO3x/4T2014/courseware/fc2c2ebe4e3c4636aef36a0b302a50cd/c649ed78ccaf4fbfbd7c7f5b99cd2fdd/
                # For now, just use the "id".
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

        # event_stream_processor = CollocatedEventProcessor(course_id, user_id, date_string, values, self.max_time)
        event_stream_processor = UniqueCollocatedEventProcessor(course_id, user_id, date_string, values, self.max_time)

        for collocation in event_stream_processor.get_collocations():
            yield self._encode_tuple(collocation)

    def output(self):
        target = get_target_from_url(
            url_path_join(
                self.output_root,
                'user-collocation-per-interval',
            )
        )
        return target

                
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


class UniqueCollocatedEventProcessor(object):
    """Identifies and deduplicates collocations for a user in a course."""
    def __init__(self, course_id, user_id, date_string, events, max_time):
        self.course_id = course_id
        self.user_id = user_id
        self.date_string = date_string
        self.max_time = max_time

        self.sorted_events = sorted(events)
        self.sorted_events = [
            CollocatableEvent(timestamp, event_name, event_id) for timestamp, event_name, event_id in self.sorted_events
        ]

    def _get_collocation_key(self, source_value, dest_value):
        return (source_value.event_name, source_value.event_id, dest_value.event_name, dest_value.event_id)

    def get_collocations(self):
        collocations = {}
        for source_index in range(len(self.sorted_events) - 1):
            for dest_index in range(source_index + 1, len(self.sorted_events) - 1):
                source_value = self.sorted_events[source_index]
                dest_value = self.sorted_events[dest_index]
                time_diff = (dest_value.timestamp_object - source_value.timestamp_object).seconds
                num_events = dest_index - source_index
                if not self.max_time or time_diff <= self.max_time:
                    key = self._get_collocation_key(source_value, dest_value)
                    # minimize the diffs:
                    if key in collocations:
                        curr_time_diff, curr_num_events = collocations[key]
                        time_diff = curr_time_diff if curr_time_diff < time_diff else time_diff
                        num_events = curr_num_events if curr_num_events < num_events else num_events
                    collocations[key] = (time_diff, num_events)

                else:
                    break
        # Once done with collecting unique collocations per user (and some stats about them),
        # output the results.
        for key, value in collocations.iteritems():
            yield self.collocation_record(key, value)

    def collocation_record(self, key, value):
        """A complete collocation record."""
        first_event_name, first_event_id, second_event_name, second_event_id = key
        time_diff, num_events = value
        return (self.course_id, self.user_id, first_event_name, first_event_id, second_event_name, second_event_id,
                time_diff, num_events)


class UserCollocationOutputForHiveTask(
        UserCollocationTaskMixin,
        EventLogSelectionDownstreamMixin,
        MultiOutputMapReduceJobTask):
    """
    A thin task to partition collocation data by date.

    By doing so, the data can be easily extended by running other date intervals
    and populating other partitions.
    """
    intermediate_output = luigi.Parameter()

    def mapper(self, line):
        """Groups by first element (date_string)."""
        values = line.split('\t')
        yield values[0], tuple(values[1:])

    def multi_output_reducer(self, _key, values, output_file):
        """Returns an iterable of strings that are written out to the appropriate output file for this key."""
        for value in values:
            output_file.write(value)
            output_file.write('\n')

    def output_path_for_key(self, key):
        return get_target_from_url(
            url_path_join(
                self.output_root,
                'dt={partition_date}'.format(partition_date=key),
            )
        )

    def requires(self):
        # Pass the intermediate_output as the value for output_root.
        return UserCollocationTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.intermediate_output,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            max_time=self.max_time,
        )


class CourseUserCollocationTable(
        UserCollocationTaskMixin,
        MapReduceJobTaskMixin,
        EventLogSelectionDownstreamMixin,
        ImportIntoHiveTableTask):
    """Creates a Hive Table that points to Hadoop output of UserCollationTask."""

    @property
    def table_name(self):
        return 'course_user_collocations'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('user_id', 'INT'),
            ('first_event_name', 'STRING'),
            ('first_event_id', 'STRING'),
            ('second_event_name', 'STRING'),
            ('second_event_id', 'STRING'),
            ('time_diff', 'INT'),
            ('num_events', 'INT'),
        ]

    @property
    def table_location(self):
        # return url_path_join(self.warehouse_path, self.table_name)
        return url_path_join(self.output_root, self.table_name)

    @property
    def table_format(self):
        """Provides format of Hive database table."""
        return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"

    @property
    def partition_date(self):
        # This gets called and added to automatically add just this partition,
        # when in fact we want to add all partitions.
        return self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def requires(self):
        # just put all data into a single table.
        # return UserCollocationOutputForHiveTask(
        return UserCollocationTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
            max_time=self.max_time,
        )

class QueryCourseUserCollocationMixin(object):
    """
    Defines parameters for QueryCourseUserCollocationTask.

    Parameters:
        course_collocation_output:  location to write query results.
    """
    course_collocation_output = luigi.Parameter()


class QueryCourseUserCollocationTask(
        QueryCourseUserCollocationMixin,
        OverwriteOutputMixin,
        HiveQueryTask):
    """Defines task to perform join in Hive to find collocation counts across users."""

    def query(self):

        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                course_id STRING,
                first_event_name STRING,
                first_event_id STRING,
                second_event_name STRING,
                second_event_id STRING,
                avg_time_diff DOUBLE,
                avg_num_events DOUBLE,
                count INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE {table_name}
            SELECT
                cuc.course_id,
                cuc.first_event_name,
                cuc.first_event_id,
                cuc.second_event_name,
                cuc.second_event_id,
                avg(cuc.time_diff),
                avg(cuc.num_events),
                count(cuc.user_id)
            FROM course_user_collocations cuc
            GROUP BY cuc.course_id, cuc.first_event_name, cuc.first_event_id, cuc.second_event_name, cuc.second_event_id;
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            location=self.output().path,
            table_name='course_collocation',
        )

        log.debug('Executing hive query: %s', query)
        return query

    def init_local(self):
        super(QueryCourseUserCollocationTask, self).init_local()
        self.remove_output_on_overwrite()

    def output(self):
        return get_target_from_url(self.course_collocation_output + "/")

    def requires(self):
        return ExternalHiveTask(table='course_user_collocations', database=hive_database_name())


class QueryCourseUserCollocationWorkflow(
        UserCollocationTaskMixin,
        MapReduceJobTaskMixin,
        EventLogSelectionDownstreamMixin,
        QueryCourseUserCollocationTask):
    """Defines dependencies for performing join in Hive to find course enrollment per-country counts."""
    def requires(self):
        return CourseUserCollocationTable(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            overwrite=self.overwrite,
            output_root=self.output_root,
        )
