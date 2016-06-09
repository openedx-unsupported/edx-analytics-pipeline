"""
Load events for internal reporting purposes.
Combine segment events and tracking log events.
Define common (wide) representation for all events to share, sparsely.
Need to define a Record, that will also provide mapping of types.
"""

import logging
import os
import pytz

import ciso8601
import luigi
from luigi.configuration import get_config
import luigi.task
import ua_parser
import user_agents

from edx.analytics.tasks.mapreduce import MultiOutputMapReduceJobTask, MapReduceJobTaskMixin
# from edx.analytics.tasks.module_engagement import OverwriteFromDateMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.segment_event_type_dist import SegmentEventLogSelectionMixin
from edx.analytics.tasks.url import ExternalURL, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask,
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import SparseRecord, StringField, DateField, IntegerField, FloatField

log = logging.getLogger(__name__)

VERSION = '0.1.0'


class EventRecord(SparseRecord):
    """Represents an event, either a tracking log event or segment event."""

    # Metadata:
    version = StringField(length=20, nullable=False, description='blah.')
    input_file = StringField(length=255, nullable=True, description='blah.')
    # hash_id = StringField(length=255, nullable=False, description='blah.')

    # Globals:
    project = StringField(length=255, nullable=False, description='blah.')
    event_type = StringField(length=255, nullable=False, description='The type of event.  Example: video_play.')
    event_source = StringField(length=255, nullable=False, description='blah.')
    event_category = StringField(length=255, nullable=True, description='blah.')

    # TODO: decide what type 'timestamp' should be.
    # Also make entries required (not nullable), once we have confidence.
    timestamp = StringField(length=255, nullable=True, description='Timestamp when event was emitted.')
    received_at = StringField(length=255, nullable=True, description='Timestamp when event was received.')
    # TODO: figure out why these have errors, and then make DateField.
    date = StringField(length=255, nullable=False, description='The learner interacted with the entity on this date.')

    # Common (but optional) values:
    # accept_language: how to parse?
    # 'agent' gets parsed into the following:
    agent_type = StringField(length=20, nullable=True, description='')
    agent_device_name = StringField(length=100, nullable=True, description='')
    agent_os = StringField(length=100, nullable=True, description='')
    agent_browser = StringField(length=100, nullable=True, description='')
    agent_touch_capable = IntegerField(nullable=True, description='')  # Should be Boolean

    host = StringField(length=80, nullable=True, description='')
    # TODO: geolocate ip to find country or more specific information?
    ip = StringField(length=20, nullable=True, description='')
    # name: not really used yet?
    page = StringField(length=1024, nullable=True, description='')
    referer = StringField(length=255, nullable=True, description='')
    session = StringField(length=255, nullable=True, description='')
    username = StringField(length=30, nullable=True, description='Learner\'s username.')

    # Common (but optional) context values:
    # We exclude course_user_tags, as it's a set of key-value pairs that affords no stable naming scheme.
    # TODO:  decide how to deal with redundant data.  Shouldn't be specifying "context_" here,
    # since that doesn't generalize to segment data at all.
    context_course_id = StringField(length=255, nullable=True, description='Id of course.')
    context_org_id = StringField(length=255, nullable=True, description='Id of organization, as used in course_id.')
    context_path = StringField(length=1024, nullable=True, description='')
    context_user_id = StringField(length=255, nullable=True, description='')
    context_module_display_name = StringField(length=255, nullable=True, description='')
    context_module_usage_key = StringField(length=255, nullable=True, description='')
    context_module_original_usage_key = StringField(length=255, nullable=True, description='')
    context_module_original_usage_version = StringField(length=255, nullable=True, description='')

    # Per-event values:
    # entity_type = StringField(length=10, nullable=True, description='Category of entity that the learner interacted'
    # ' with. Example: "video".')
    # entity_id = StringField(length=255, nullable=True, description='A unique identifier for the entity within the'
    # ' course that the learner interacted with.')

    attempts = StringField(length=255, nullable=True, description='')  # use int
    # case_sensitive = Bool (textbook)
    category_id = StringField(length=255, nullable=True, description='')
    category_name = StringField(length=255, nullable=True, description='')
    certificate_id = StringField(length=255, nullable=True, description='')
    chapter = StringField(length=255, nullable=True, description='')  # pdf
    chapter_title = StringField(length=255, nullable=True, description='')
    child_id = StringField(length=255, nullable=True, description='')
    choice = StringField(length=255, nullable=True, description='')  # poll
    code = StringField(length=255, nullable=True, description='')  # video
    cohort_id = StringField(length=255, nullable=True, description='')  # int:  cohort
    cohort_name = StringField(length=255, nullable=True, description='')
    commentable_id = StringField(length=255, nullable=True, description='')  # forums
    corrected_text = StringField(length=255, nullable=True, description='')  # forum search
    course_id = StringField(length=255, nullable=True, description='')  # enrollment, certs
    current_time = StringField(length=255, nullable=True, description='')  # float/int/str:  video
    currenttime = StringField(length=255, nullable=True, description='')  # float/int/str:  video
    direction = StringField(length=255, nullable=True, description='')  # pdf
    discussion_id = StringField(length=255, nullable=True, description='')  # discussion.id forum
    displayed_in = StringField(length=255, nullable=True, description='')  # googlecomponent
    duration = StringField(length=255, nullable=True, description='')  # int: videobumper
    enrollment_mode = StringField(length=255, nullable=True, description='')  # certs
    field = StringField(length=255, nullable=True, description='')  # team
    generation_mode = StringField(length=255, nullable=True, description='')  # cert
    grade = StringField(length=255, nullable=True, description='')  # float/int:  problem_check
    group_id = StringField(length=255, nullable=True, description='')  # int:  forum
    group_name = StringField(length=255, nullable=True, description='')  # user_to_partition
    id = StringField(length=255, nullable=True, description='')  # video, forum
    instructor = StringField(length=255, nullable=True, description='')
    location = StringField(length=255, nullable=True, description='')  # library
    max_count = StringField(length=255, nullable=True, description='')  # int:  library
    max_grade = StringField(length=255, nullable=True, description='')  # int:  problem_check
    mode = StringField(length=255, nullable=True, description='')  # enrollment
    module_id = StringField(length=255, nullable=True, description='')  # hint
    name = StringField(length=255, nullable=True, description='')  # pdf
    old = StringField(length=255, nullable=True, description='')  # int: seq, str: book, team, settings
    new = StringField(length=255, nullable=True, description='')  # int: seq, str: book, team, settings
    old_speed = StringField(length=255, nullable=True, description='')  # video
    new_speed = StringField(length=255, nullable=True, description='')  # video
    new_time = StringField(length=255, nullable=True, description='')  # float/int:  video
    num_attempts = StringField(length=255, nullable=True, description='')  # int:  problem_builder
    page = StringField(length=255, nullable=True, description='')  # int/str:  forum, pdf
    previous_cohort_id = StringField(length=255, nullable=True, description='')  # int:  cohort
    previous_cohort_name = StringField(length=255, nullable=True, description='')  # cohort
    previous_count = StringField(length=255, nullable=True, description='')  # int:  lib
    problem_id = StringField(length=255, nullable=True, description='')  # capa
    problem_part_id = StringField(length=255, nullable=True, description='')  # hint
    problem = StringField(length=255, nullable=True, description='')  # show/reset/rescore
    query = StringField(length=255, nullable=True, description='')  # forum, pdf
    question_type = StringField(length=255, nullable=True, description='')  # hint
    response_id = StringField(length=255, nullable=True, description='')  # response.id:  forum
    search_text = StringField(length=255, nullable=True, description='')  # team
    seek_type = StringField(length=255, nullable=True, description='')  # video
    social_network = StringField(length=255, nullable=True, description='')  # certificate
    status = StringField(length=255, nullable=True, description='')  # status
    student = StringField(length=255, nullable=True, description='')  # reset/delete/rescore
    success = StringField(length=255, nullable=True, description='')  # problem_check
    team_id = StringField(length=255, nullable=True, description='')  # team, forum
    thread_type = StringField(length=255, nullable=True, description='')  # forum
    title = StringField(length=255, nullable=True, description='')  # forum
    topic_id = StringField(length=255, nullable=True, description='')  # team
    total_results = StringField(length=255, nullable=True, description='')  # int: forum
    truncated = StringField(length=255, nullable=True, description='')  # bool:  forum
    type = StringField(length=255, nullable=True, description='')  # video, book
    url_name = StringField(length=255, nullable=True, description='')  # poll/survey
    url = StringField(length=255, nullable=True, description='')  # forum, googlecomponent
    user_id = StringField(length=255, nullable=True, description='')  # int: enrollment, cohort, etc.
    event_username = StringField(length=255, nullable=True, description='')  # add/remove forum
    # Stuff from segment:
    channel = StringField(length=255, nullable=True, description='')
    anonymous_id = StringField(length=255, nullable=True, description='')


class EventRecordDownstreamMixin(WarehouseMixin, MapReduceJobTaskMixin):  # , OverwriteFromDateMixin):

    events_list_file_path = luigi.Parameter(default=None)


class EventRecordDataDownstreamMixin(EventRecordDownstreamMixin):

    """Common parameters and base classes used to pass parameters through the event record workflow."""

    # Required parameter
    date = luigi.DateParameter(
        description='Upper bound date for the end of the interval to analyze. Data produced before 00:00 on this'
                    ' date will be analyzed. This workflow is intended to run nightly and this parameter is intended'
                    ' to be set to "today\'s" date, so that all of yesterday\'s data is included and none of today\'s.'
    )

    # Override superclass to disable this parameter
    interval = None
    output_root = luigi.Parameter()


class BaseEventRecordDataTask(EventRecordDataDownstreamMixin, MultiOutputMapReduceJobTask):
    """Base class for loading EventRecords from different sources."""

    # Create a DateField object to help with converting date_string
    # values for assignment to DateField objects.
    date_field_for_converting = DateField()

    def __init__(self, *args, **kwargs):
        super(BaseEventRecordDataTask, self).__init__(*args, **kwargs)

        self.interval = luigi.date_interval.Date.from_date(self.date)

    # TODO: maintain support for info about events.  We may need something similar to identify events
    # that should -- or should not -- be included in the event dump.

    def requires_local(self):
        return ExternalURL(url=self.events_list_file_path)

    def init_local(self):
        super(BaseEventRecordDataTask, self).init_local()
        if self.events_list_file_path is None:
            self.known_events = {}
        else:
            self.known_events = self.parse_events_list_file()

    def parse_events_list_file(self):
        """Read and parse the known events list file and populate it in a dictionary."""
        parsed_events = {}
        with self.input_local().open() as f_in:
            lines = f_in.readlines()
            for line in lines:
                if not line.startswith('#') and len(line.split("\t")) is 3:
                    parts = line.rstrip('\n').split("\t")
                    parsed_events[(parts[1], parts[2])] = parts[0]
        return parsed_events

    def get_map_input_file(self):
        """Returns path to input file from which event is being read, if available."""
        # TODO: decide if this is useful information.  (Share across all logs.  Add to a common base class?)
        try:
            # Hadoop sets an environment variable with the full URL of the input file. This url will be something like:
            # s3://bucket/root/host1/tracking.log.gz. In this example, assume self.source is "s3://bucket/root".
            return os.environ['map_input_file']
        except KeyError:
            log.warn('map_input_file not defined in os.environ, unable to determine input file path')
            return None

    def multi_output_reducer(self, _key, values, output_file):
        """
        Write values to the appropriate file as determined by the key.
        """
        for value in values:
            # Assume that the value is a dict containing the relevant sparse data,
            # either raw or encoded in a json string.
            # Either that, or we could ship the original event as a json string,
            # or ship the resulting sparse record as a tuple.
            # It should be a pretty arbitrary decision, since it all needs
            # to be done, and it's just a question where to do it.
            # For now, keep this simple, and assume it's tupled already.
            output_file.write(value)
            output_file.write('\n')
            # WARNING: This line ensures that Hadoop knows that our process is not sitting in an infinite loop.
            # Do not remove it.
            self.incr_counter('Event Record Exports', 'Raw Bytes Written', len(value) + 1)

    def output_path_for_key(self, key):
        """
        Output based on date and something else.  What else?  Type?

        Mix them together by date, but identify with different files for each project/environment.

        Output is in the form {warehouse_path}/event_records/dt={CCYY-MM-DD}/{project}.tsv
        """
        # If we're only running now with a specific date, then there
        # is no reason to sort by date_received.
        _date_received, project = key

        # return url_path_join(
        #     self.output_root,
        #     'event_records',
        #     'dt={date}'.format(date=date_received),
        #     '{project}.tsv'.format(project=project),
        # )
        return url_path_join(
            self.output_root,
            '{project}.tsv'.format(project=project),
        )

    def extra_modules(self):
        return [pytz, ua_parser, user_agents]

    def normalize_time(self, event_time):
        """
        Convert time string to ISO-8601 format in UTC timezone.

        Returns None if string representation cannot be parsed.
        """
        datetime = ciso8601.parse_datetime(event_time)
        if datetime:
            return datetime.astimezone(pytz.utc).isoformat()
        else:
            return None

    def convert_date(self, date_string):
        """Converts date from string format to date object, for use by DateField."""
        if date_string:
            try:
                # TODO: for now, return as a string.
                # When actually supporting DateField, then switch back to date.
                # ciso8601.parse_datetime(ts).astimezone(pytz.utc).date().isoformat()
                return self.date_field_for_converting.deserialize_from_string(date_string).isoformat()
            except ValueError:
                self.incr_counter('Event Record Exports', 'Cannot convert to date', 1)
                # Don't bother to make sure we return a good value
                # within the interval, so we can find the output for
                # debugging.  Should not be necessary, as this is only
                # used for the column value, not the partitioning.
                return "BAD: {}".format(date_string)
                # return self.lower_bound_date_string
        else:
            self.incr_counter('Event Record Exports', 'Missing date', 1)
            return date_string

    def _canonicalize_user_agent(self, agent):
        """
        There is a lot of variety in the user agent field that is hard for humans to parse, so we canonicalize
        the user agent to extract the information we're looking for.
        Args:
            agent: an agent string.
        Returns:
            a dictionary of information about the user agent.
        """
        agent_dict = {}

        try:
            user_agent = user_agents.parse(agent)
        except:  # If the user agent can't be parsed, just drop the agent data on the floor since it's of no use to us.
            return agent_dict

        device_type = ''  # It is possible that the user agent isn't any of the below
        if user_agent.is_mobile:
            device_type = "mobile"
        elif user_agent.is_tablet:
            device_type = "tablet"
        elif user_agent.is_pc:
            device_type = "desktop"
        elif user_agent.is_bot:
            device_type = "bot"

        if device_type:
            agent_dict['type'] = device_type
            agent_dict['device_name'] = user_agent.device.family
            agent_dict['os'] = user_agent.os.family
            agent_dict['browser'] = user_agent.browser.family
            agent_dict['touch_capable'] = user_agent.is_touch_capable

        return agent_dict

    def add_agent_info(self, event_dict, agent):
        if agent:
            agent_dict = self._canonicalize_user_agent(agent)
            for key in agent_dict.keys():
                new_key = "agent_{}".format(key)
                event_dict[new_key] = agent_dict[key]

    def _add_event_info_recurse(self, event_dict, event_mapping, obj, label):
        if obj is None:
            pass
        elif isinstance(obj, dict):
            for key in obj.keys():
                new_value = obj.get(key)
                new_label = u"{}.{}".format(label, key)
                self._add_event_info_recurse(event_dict, event_mapping, new_value, new_label)
        elif isinstance(obj, list):
            # We will not output any values that are stored in lists.
            pass
        else:
            # We assume it's a single object, and look it up now.
            if label in event_mapping:
                event_record_key, event_record_field = event_mapping[label]
                if isinstance(event_record_field, StringField):
                    event_dict[event_record_key] = unicode(obj)
                elif isinstance(event_record_field, IntegerField):
                    try:
                        event_dict[event_record_key] = int(obj)
                    except ValueError:
                        log.error('Unable to cast value to int for %s: %r', label, obj)
                elif isinstance(event_record_field, FloatField):
                    try:
                        event_dict[event_record_key] = float(obj)
                    except ValueError:
                        log.error('Unable to cast value to float for %s: %r', label, obj)
                else:
                    event_dict[event_record_key] = obj

    def add_event_info(self, event_dict, event_mapping, event):
        self._add_event_info_recurse(event_dict, event_mapping, event, 'root')


class TrackingEventRecordDataTask(EventLogSelectionMixin, BaseEventRecordDataTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    # Override superclass to disable this parameter
    interval = None
    event_mapping = None
    PROJECT_NAME = 'tracking_prod'

    def get_event_emission_time(self, event):
        return super(TrackingEventRecordDataTask, self).get_event_time(event)

    def get_event_arrival_time(self, event):
        try:
            return event['context']['received_at']
        except KeyError:
            return self.get_event_emission_time(event)

    def get_event_time(self, event):
        # Some events may emitted and stored for quite some time before actually being entered into the tracking logs.
        # The primary cause of this is mobile devices that go offline for a significant period of time. They will store
        # events locally and then when connectivity is restored transmit them to the server. We log the time that they
        # were received by the server and use that to batch them into exports since it is much simpler than trying to
        # inject them into past exports.
        return self.get_event_arrival_time(event)

    def get_event_mapping(self):
        """Return dictionary of event attributes to the output keys they map to."""
        if self.event_mapping is None:
            self.event_mapping = {}
            fields = EventRecord.get_fields()
            field_keys = fields.keys()
            for field_key in field_keys:
                # Most common is to map first-level entries in event data directly.
                # Skip values that are explicitly set:
                if field_key in ['version', 'input_file', 'project', 'event_type', 'event_source', 'context_course_id', 'username']:
                    source_key = None
                # Skip values that are explicitly calculated rather than copied:
                if field_key.startswith('agent_') or field_key in ['event_category', 'timestamp', 'received_at', 'date']:
                    source_key = None
                # Map values that are top-level:
                elif field_key in ['host', 'ip', 'page', 'referer', 'session']:
                    source_key = "root.{}".format(field_key)
                elif field_key.startswith('context_module_'):
                    source_key = "root.context.module.{}".format(field_key[15:])
                elif field_key.startswith('context_'):
                    source_key = "root.context.{}".format(field_key[8:])
                else:
                    source_key = "root.event.{}".format(field_key)
                if source_key is not None:
                    self.event_mapping[source_key] = (field_key, fields[field_key])

        return self.event_mapping

    def mapper(self, line):
        event, date_received = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        # Ignore events that begin with a slash (i.e. implicit events).
        if event_type.startswith('/'):
            return

        username = event.get('username', '').strip()
        # if not username:
        #   return

        course_id = eventlog.get_course_id(event)
        # if not course_id:
        #   return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')
        if event_source is None:
            return

        if (event_source, event_type) in self.known_events:
            event_category = self.known_events[(event_source, event_type)]
        else:
            event_category = 'unknown'

        project_name = self.PROJECT_NAME

        event_dict = {
            'version': VERSION,
            'input_file': self.get_map_input_file(),
            'project': project_name,
            'event_type': event_type,
            'event_source': event_source,
            'event_category': event_category,

            'timestamp': self.get_event_emission_time(event),
            'received_at': self.get_event_arrival_time(event),
            'date': self.convert_date(date_received),

            'context_course_id': course_id,
            'username': username,
            # etc.
        }
        self.add_agent_info(event_dict, event.get('agent'))
        event_mapping = self.get_event_mapping()
        self.add_event_info(event_dict, event_mapping, event)

        record = EventRecord(**event_dict)

        key = (date_received, project_name)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


class SegmentEventRecordDataTask(SegmentEventLogSelectionMixin, BaseEventRecordDataTask):
    """Task to compute event_type and event_source values being encountered on each day in a given time interval."""

    # Override superclass to disable this parameter
    interval = None

    # Project information, pulled from config file.
    project_names = {}
    config = None

    event_mapping = None

    def _get_project_name(self, project_id):
        if project_id not in self.project_names:
            if self.config is None:
                self.config = get_config()
            section_name = 'segment:' + project_id
            project_name = self.config.get(section_name, 'project_name', None)
            self.project_names[project_id] = project_name
        return self.project_names[project_id]

    def _get_time_from_segment_event(self, event, key):
        try:
            event_time = event[key]
            event_time = self.normalize_time(event_time)
            if event_time is None:
                log.error("Unparseable %s time from event: %r", key, event)
                self.incr_counter('Event', 'Unparseable {} Time Field'.format(key), 1)
            return event_time
        except KeyError:
            log.error("Missing %s time from event: %r", key, event)
            self.incr_counter('Event', 'Missing {} Time Field'.format(key), 1)
            return None
        except TypeError:
            log.error("Bad type for %s time in event: %r", key, event)
            self.incr_counter('Event', 'Bad type for {} Time Field'.format(key), 1)
            return None
        except ValueError:
            log.error("Bad value for %s time in event: %r", key, event)
            self.incr_counter('Event', 'Bad value for {} Time Field'.format(key), 1)
            return None
        except UnicodeEncodeError:
            log.error("Bad encoding for %s time in event: %r", key, event)
            self.incr_counter('Event', 'Bad encoding for {} Time Field'.format(key), 1)
            return None

    def get_event_arrival_time(self, event):
        return self._get_time_from_segment_event(event, 'receivedAt')

    def get_event_emission_time(self, event):
        return self._get_time_from_segment_event(event, 'sentAt')

    def get_event_time(self, event):
        """
        Returns time information from event if present, else returns None.

        Overrides base class implementation to get correct timestamp
        used by get_event_and_date_string(line).

        """
        # TODO: clarify which value should be used.
        # "originalTimestamp" is almost "sentAt".  "timestamp" is
        # almost "receivedAt".  Order is (probably)
        # "originalTimestamp" < "sentAt" < "timestamp" < "receivedAt".
        return self.get_event_arrival_time(event)

    def get_event_mapping(self):
        """Return dictionary of event attributes to the output keys they map to."""
        if self.event_mapping is None:
            self.event_mapping = {}
            fields = EventRecord.get_fields()
            field_keys = fields.keys()
            for field_key in field_keys:
                # Most common is to map first-level entries in event data directly.
                # Skip values that are explicitly set:
                if field_key in ['version', 'input_file', 'project', 'event_type', 'event_source']:
                    pass
                # Skip values that are explicitly calculated rather than copied:
                elif field_key.startswith('agent_') or field_key in ['event_category', 'timestamp', 'received_at', 'date']:
                    pass
                # Map values that are top-level:
                elif field_key in ['channel']:
                    source_key = "root.{}".format(field_key)
                    self.event_mapping[source_key] = (field_key, fields[field_key])
                elif field_key in ['anonymous_id']:
                    source_key = "root.context.anonymousId"
                    self.event_mapping[source_key] = (field_key, fields[field_key])
                    source_key = "root.anonymousId"
                    self.event_mapping[source_key] = (field_key, fields[field_key])
                elif field_key in ['locale', 'ip']:
                    source_key = "root.context.{}".format(field_key)
                    self.event_mapping[source_key] = (field_key, fields[field_key])
                elif field_key in ['path', 'referrer']:
                    source_key = "root.properties.{}".format(field_key)
                    self.event_mapping[source_key] = (field_key, fields[field_key])
                else:
                    pass

        return self.event_mapping

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_received = value
        self.incr_counter('Segment_Event_Dist', 'Inputs with Dates', 1)

        segment_type = event.get('type')
        self.incr_counter('Segment_Event_Dist', 'Type {}'.format(segment_type), 1)

        channel = event.get('channel')
        self.incr_counter('Segment_Event_Dist', 'Channel {}'.format(channel), 1)

        if segment_type == 'track':
            event_type = event.get('event')

            if event_type is None or date_received is None:
                # Ignore if any of the keys is None
                self.incr_counter('Segment_Event_Dist', 'Tracking with missing type', 1)
                return

            if event_type.startswith('/'):
                # Ignore events that begin with a slash.  How many?
                self.incr_counter('Segment_Event_Dist', 'Tracking with implicit type', 1)
                return

            # Not all 'track' events have event_source information.  In particular, edx.bi.XX events.
            # Their 'properties' lack any 'context', having only label and category.

            event_category = event.get('properties', {}).get('category')
            if channel == 'server':
                event_source = event.get('properties', {}).get('context', {}).get('event_source')
                if event_source is None:
                    event_source = 'track-server'
                elif (event_source, event_type) in self.known_events:
                    event_category = self.known_events[(event_source, event_type)]
                self.incr_counter('Segment_Event_Dist', 'Tracking server', 1)
            else:
                # expect that channel is 'client'.
                event_source = channel
                self.incr_counter('Segment_Event_Dist', 'Tracking non-server', 1)

        else:
            # 'page' or 'identify'
            event_category = segment_type
            event_type = segment_type
            event_source = channel

        self.incr_counter('Segment_Event_Dist', 'Output From Mapper', 1)

        project_id = event.get('projectId')
        project_name = self._get_project_name(project_id) or project_id

        event_dict = {
            'version': VERSION,
            'project': project_name,
            'event_type': event_type,
            'event_source': event_source,
            'event_category': event_category,

            'timestamp': self.get_event_emission_time(event),
            'received_at': self.get_event_arrival_time(event),
            'date': self.convert_date(date_received),

            # 'course_id': course_id,
            # 'username': username,
            # etc.
        }
        self.add_agent_info(event_dict, event.get('context', {}).get('userAgent'))
        event_mapping = self.get_event_mapping()
        self.add_event_info(event_dict, event_mapping, event)

        record = EventRecord(**event_dict)
        key = (date_received, project_name)

        # Convert to form for output by reducer here,
        # so that reducer doesn't do any conversion.
        # yield key, record.to_string_tuple()
        yield key, record.to_separated_values()


class GeneralEventRecordDataTask(EventRecordDataDownstreamMixin, luigi.WrapperTask):
    """Runs all Event Record tasks for a given time interval."""
    # Override superclass to disable this parameter
    # TODO: check if this is redundant, if it's already in the mixin.
    interval = None

    def requires(self):
        kwargs = {
            'output_root': self.output_root,
            'events_list_file_path': self.events_list_file_path,
            'n_reduce_tasks': self.n_reduce_tasks,
            'date': self.date,
            # 'warehouse_path': self.warehouse_path,
        }
        yield (
            TrackingEventRecordDataTask(**kwargs),
            SegmentEventRecordDataTask(**kwargs),
        )


class EventRecordTableTask(BareHiveTableTask):
    """The hive table for event_record data."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'event_records'

    @property
    def columns(self):
        return EventRecord.get_hive_schema()


class EventRecordPartitionTask(EventRecordDownstreamMixin, HivePartitionTask):
    """The hive table partition for this engagement data."""

    # Required parameter
    date = luigi.DateParameter()
    interval = None

    @property
    def partition_value(self):
        """Use a dynamic partition value based on the date parameter."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return EventRecordTableTask(
            warehouse_path=self.warehouse_path,
            # overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return GeneralEventRecordDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            # overwrite=self.overwrite,
            events_list_file_path=self.events_list_file_path,
        )


class EventRecordIntervalTask(EventRecordDownstreamMixin,
                              OverwriteOutputMixin, luigi.WrapperTask):
    """Compute engagement information over a range of dates and insert the results into Hive and Vertica and whatever else."""

    interval = luigi.DateIntervalParameter(
        description='The range of received dates for which to create event records.',
    )

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            # should_overwrite = date >= self.overwrite_from_date
            yield EventRecordPartitionTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                # overwrite=should_overwrite,
                # overwrite_from_date=self.overwrite_from_date,
                events_list_file_path=self.events_list_file_path,
            )
            # yield LoadEventRecordToVerticaTask(
            #     date=date,
            #     n_reduce_tasks=self.n_reduce_tasks,
            #     warehouse_path=self.warehouse_path,
            #     overwrite=should_overwrite,
            #     overwrite_from_date=self.overwrite_from_date,
            # )

    def output(self):
        return [task.output() for task in self.requires()]

    def get_raw_data_tasks(self):
        """
        A generator that iterates through all tasks used to generate the data in each partition in the interval.

        This can be used by downstream map reduce jobs to read all of the raw data.
        """
        for task in self.requires():
            if isinstance(task, EventRecordPartitionTask):
                yield task.data_task
