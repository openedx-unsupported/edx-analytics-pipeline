"""Take canonicalized event logs and do further processing to prepare for loading into the HP Vertica data warehouse."""

import datetime
import gzip
from hashlib import md5
import logging
import os

import ciso8601   # for fast date parsing
import cjson
import ua_parser
import user_agents
from string import strip
import luigi
import luigi.date_interval

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.canonicalization import CanonicalizationTask


log = logging.getLogger(__name__)


class CleanForVerticaTask(EventLogSelectionMixin, WarehouseMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    Clean event logs up for Vertica ingestion.  This includes removal of empty events
    and optionally removal of implicit events.

    May later include reorganizing event data to make flex loading easier.

    Writes output in a set of gzipped files so that the task can be parallelized.

    Once the job finishes, it writes a _SUCCESS file for each date in the interval. If such a file is present on
    startup, those days are not processed again unless the overwrite flag is set.
    """

    interval = None
    output_root = None
    date = luigi.DateParameter()
    remove_implicit = luigi.BooleanParameter()

    VERSION = 2  # Version 1 was after the canonicalization
    OUTPUT_BUCKETS = 100
    MAX_KEY_LENGTH = 256

    n_reduce_tasks = OUTPUT_BUCKETS

    def __init__(self, *args, **kwargs):
        super(CleanForVerticaTask, self).__init__(*args, **kwargs)

        self.interval = luigi.date_interval.Date.from_date(self.date)
        self.output_root = url_path_join(self.warehouse_path, 'events-vertica', 'dt=' + self.date.isoformat()) + '/'
        self.current_time = datetime.datetime.utcnow().isoformat()

    def requires(self):
        """We require the data to already be canonicalized before running this task."""
        return CanonicalizationTask(
            date=self.date,
            overwrite=self.overwrite,
            warehouse_path=self.warehouse_path,
        )

    def event_from_line(self, line):
        """
        Convert a line to an event.

        Because this task requires a canonicalization task, the event will parse and have an event type.
        """
        event = eventlog.parse_json_event(line)
        return event

    def add_metadata(self, event, line):
        """
        Add a metadata dictionary to the event, with the following fields:
         - version
         - last_modified (when the event was processed by this task)
         - id (hash of the raw event format, i.e. the line)
         - original_file -- the source file where we found this event
        """
        if event is None:
            return None

        metadata = event.setdefault('metadata', {})
        metadata.setdefault('version', self.VERSION)
        metadata['last_modified'] = self.current_time
        if 'id' not in metadata:
            metadata['id'] = self.compute_hash(line)

        map_input_file = os.environ['map_input_file']
        metadata['original_file'] = map_input_file

        return event

    def remove_implicit_events(self, event):
        """
        Args:
            event: an event dictionary, or None if something has gone wrong earlier

        Returns:
            the event, if it is an explicit event, or None if it is an implicit event.  This event is called
            only if the remove_implicit parameter is True.
        """
        if event.get('event_type')[0] == '/':  # This is the marker for an implicit event
            return None
        else:
            return event

    def truncate_keys(self, event):
        """
        The JSON parser Vertica uses to ingest into flex tables can't handle key names larger than 256 columns,
        which will sometimes happen, so we truncate those keys.

        Args:
            event: an event dictionary, or None if something has gone wrong earlier

        Returns:
            the event, with all of the keys shortened to be at most 256 characters.
        """
        if event is None:
            return None

        new_event = {}
        for key in event:
            if len(key) > self.MAX_KEY_LENGTH:
                shortened_key = key[:self.MAX_KEY_LENGTH]
                new_event[shortened_key] = self.truncate_keys(event[key]) if type(event[key]) == dict else event[key]
            else:
                new_event[key] = self.truncate_keys(event[key]) if type(event[key]) == dict else event[key]

        return new_event

    def canonicalize_user_agent(self, event):
        """
        There is a lot of variety in the user agent field that is hard for humans to parse, so we canonicalize
        the user agent to extract the information we're looking for.

        Args:
            event: an event dictionary, or None if something has gone wrong earlier

        Returns:
            the event, with the 'agent' value replaced by a dictionary of information about the user agent.
        """
        if event is None:
            return None

        agent_dict = {}
        if event.get('agent') is None:
            event['agent'] = {}
            return event

        agent = event['agent']

        try:
            user_agent = user_agents.parse(agent)

            device_type = ''  # It is possible that the user agent isn't any of the below
            if user_agent.is_mobile:
                device_type = "mobile"
            elif user_agent.is_tablet:
                device_type = "tablet"
            elif user_agent.is_pc:
                device_type = "desktop"

            agent_dict['type'] = device_type
            agent_dict['device_name'] = user_agent.device.family
            agent_dict['os'] = user_agent.os.family
            agent_dict['browser'] = user_agent.browser.family
            agent_dict['touch_capable'] = user_agent.is_touch_capable

            if device_type:  # i.e. if we recognized the user agent as a real device
                event['agent'] = agent_dict
            else:
                event['agent'] = {}
            return event
        except:  # If the user agent can't be parsed, just drop the agent data on the floor since it's of no use to us.
            event['agent'] = {}
            return event

    def mapper(self, line):
        """
        Args:
            line: an event, which is already a proper, canonicalized json.

        Returns:
            (date, bucket), cleaned_for_vertica_event
        """
        event = self.event_from_line(line)
        if self.remove_implicit:
            event = self.remove_implicit_events(event)
        event = self.truncate_keys(event)
        # Currently not ready to be wired up because the table schema in events_to_warehouse also needs to be changed.
        event = self.canonicalize_user_agent(event)
        event = self.add_metadata(event, line)

        if event is None:
            return

        canonical_event_as_string = cjson.encode(event)

        yield self.get_map_output_key(event), canonical_event_as_string

    def reducer(self, _key, values):
        for value in values:
            yield (value,)

    def compute_hash(self, line):
        """
        Compute a hash of an event line.

        Returns:
            The hexdigest of the line, a hexadecimal string.
        """
        hasher = md5()
        hasher.update(line)
        return hasher.hexdigest()

    def get_map_output_key(self, event):
        """
        Generate the grouping key for an event.

        This will be a deterministically generated integer that evenly distributes the events into buckets.

        Returns:
            bucket
        """
        # pick a random bucket using the first 3 digits of the event hash
        string_of_hex_digits = event['metadata']['id'][:3]
        number = int(string_of_hex_digits, 16)
        bucket = number % self.OUTPUT_BUCKETS

        return bucket

    def output(self):
        return get_target_from_url(self.output_root, success_marked=True)

    def run(self):
        self.remove_output_on_overwrite()
        super(CleanForVerticaTask, self).run()

    def extra_modules(self):
        """We need the user agent parser user_agents for the mapper function."""
        return [ua_parser, user_agents]

    def jobconfs(self):
        jcs = super(CleanForVerticaTask, self).jobconfs()
        # Downstream luigi methods won't be able to open a folder of gzipped files from hdfs/s3 properly,
        # so unfortunately we can't put in compression here without doing something to formatting
        # jcs.extend([
        #     'mapred.output.compress=true',
        #     'mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec'
        # ])
        return jcs
