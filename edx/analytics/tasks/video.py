
from collections import namedtuple
import hashlib
import datetime
import logging
import math
import json

import ciso8601
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition, HiveTableTask, HiveQueryToMysqlTask
from edx.analytics.tasks.mysql_load import MysqlInsertTask

log = logging.getLogger(__name__)


VIDEO_PLAYED = 'play_video'
VIDEO_PAUSED = 'pause_video'
VIDEO_POSITION_CHANGED = 'seek_video'
VIDEO_STOPPED = 'stop_video'
VIDEO_EVENT_TYPES = frozenset([
    VIDEO_PLAYED,
    VIDEO_PAUSED,
    VIDEO_POSITION_CHANGED,
    VIDEO_STOPPED
])
VIDEO_SESSION_END_INDICATORS = frozenset([
    'seq_next',
    'seq_prev',
    'seq_goto',
    'page_close',
    '/jsi18n/',
    '/logout',
])


VideoSession = namedtuple('VideoSession', [
    'session_id', 'start_timestamp', 'encoded_module_id', 'start_offset'])


class UserVideoSessionTask(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        username = event.get('username')
        if username is None:
            log.error("encountered event with no username: %s", event)
            return

        if len(username) == 0:
            return

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            log.error("encountered event with bad timestamp: %s", event)
            return

        encoded_module_id = None
        old_time = None
        current_time = None
        if event_type in VIDEO_EVENT_TYPES:
            event_data = eventlog.get_event_data(event)
            encoded_module_id = event_data.get('id')
            if event_type == VIDEO_POSITION_CHANGED:
                old_time = event_data.get('old_time')
                current_time = event_data.get('new_time')
            else:
                current_time = event_data.get('currentTime')

        yield (username, (timestamp, event_type, encoded_module_id, old_time, current_time))

    def reducer(self, username, events):
        sorted_events = sorted(events)
        session = None
        for event in sorted_events:
            timestamp, event_type, encoded_module_id, old_time, current_time = event
            parsed_timestamp = ciso8601.parse_datetime(timestamp)
            if current_time:
                current_time = float(current_time)

            # yield [username] + list(event)

            def start_session():
                m = hashlib.md5()
                m.update(username)
                m.update(encoded_module_id)
                m.update(timestamp)

                return VideoSession(
                    session_id=m.hexdigest(),
                    start_timestamp=parsed_timestamp,
                    encoded_module_id=encoded_module_id,
                    start_offset=current_time
                )

            def end_session(end_time):
                if end_time is None or session.start_offset is None:
                    log.error(
                        'Invalid session ending, %s, %s, %f, %f',
                        username, str(event), end_time, session.start_offset
                    )
                    return None

                if (end_time - session.start_offset) < 0.5:
                    return None
                else:
                    return (
                        username,
                        session.session_id,
                        session.encoded_module_id,
                        session.start_timestamp.isoformat(),
                        session.start_offset,
                        end_time,
                    )

            if event_type == VIDEO_PLAYED:
                if session:
                    time_diff = parsed_timestamp - session.start_timestamp
                    if time_diff < datetime.timedelta(seconds=0.5):
                        # log.warn(
                        #     'Play video events detected %f seconds apart, second one is ignored.',
                        #     time_diff.total_seconds()
                        # )
                        continue
                    else:
                        record = end_session(current_time)
                        if record:
                            yield record

                session = start_session()

            elif event_type == VIDEO_PAUSED or event_type == VIDEO_STOPPED:
                if session:
                    record = end_session(current_time)
                    if record:
                        yield record
                    session = None

            elif event_type == VIDEO_POSITION_CHANGED:
                if session:
                    record = end_session(old_time)
                    if record:
                        yield record
                session = start_session()

            elif event_type in VIDEO_SESSION_END_INDICATORS:
                if session:
                    session_length = (parsed_timestamp - session.start_timestamp).total_seconds()
                    session_end = session.start_offset + session_length
                    record = end_session(session_end)
                    if record:
                        yield record
                    session = None

        if session:
            session_length = (parsed_timestamp - session.start_timestamp).total_seconds()
            session_end = session.start_offset + session_length
            record = end_session(session_end)
            if record:
                yield record
            session = None

    def output(self):
        return get_target_from_url(self.output_root)


class VideoTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    pass


class UserVideoSessionTableTask(VideoTableDownstreamMixin, HiveTableTask):

    @property
    def table(self):
        return 'user_video_session'

    @property
    def columns(self):
        return [
            ('username', 'STRING'),
            ('video_session_id', 'STRING'),
            ('encoded_module_id', 'STRING'),
            ('start_timestamp', 'STRING'),
            ('start_offset', 'FLOAT'),
            ('end_offset', 'FLOAT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return UserVideoSessionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )


class VideoUsageTask(EventLogSelectionDownstreamMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def requires(self):
        return UserVideoSessionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.output_root.replace('video_usage', 'user_video_session'),
        )

    def mapper(self, line):
        username, session_id, encoded_module_id, start_timestamp_str, start_offset, end_offset = line.split('\t')
        yield (encoded_module_id, (username, start_offset, end_offset))

    def reducer_backup(self, encoded_module_id, sessions):
        usage_map = {}

        for session in sessions:
            username, start_offset, end_offset = session
            for second in xrange(int(math.floor(float(start_offset))), int(math.ceil(float(end_offset))), 1):
                stats = usage_map.setdefault(second, {})
                users = stats.setdefault('users', set())
                users.add(username)
                stats['views'] = stats.get('views', 0) + 1

        csv_data = []
        for second in usage_map.keys():
            stats = usage_map[second]
            csv_data.append(','.join([str(x) for x in (second, len(stats['users']), stats['views'])]))

        blob_data = '|'.join(csv_data)
        log.warn(str(len(blob_data)))
        yield encoded_module_id, blob_data

    def reducer(self, encoded_module_id, sessions):
        usage_map = {}

        for session in sessions:
            username, start_offset, end_offset = session
            for second in xrange(int(math.floor(float(start_offset))), int(math.ceil(float(end_offset))), 1):
                stats = usage_map.setdefault(second, {})
                users = stats.setdefault('users', set())
                users.add(username)
                stats['views'] = stats.get('views', 0) + 1

        for second in usage_map.keys():
            stats = usage_map[second]
            yield encoded_module_id, second, len(stats['users']), stats['views']
            del usage_map[second]

    def output(self):
        return get_target_from_url(self.output_root)


class VideoUsageTableTask(VideoTableDownstreamMixin, HiveTableTask):

    @property
    def table(self):
        return 'video_usage'

    @property
    def columns(self):
        return [
            ('module_id', 'STRING'),
            ('segment', 'INTEGER'),
            ('num_users', 'INTEGER'),
            ('num_views', 'INTEGER'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return UserVideoSessionTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )


class InsertToMysqlVideoUsageTask(VideoTableDownstreamMixin, MysqlInsertTask):

    overwrite = luigi.BooleanParameter(default=True)

    @property
    def table(self):
        return "video_usage"

    @property
    def columns(self):
        return [
            ('module_id', 'VARCHAR(255)'),
            ('segment', 'INTEGER'),
            ('num_users', 'INTEGER'),
            ('num_views', 'INTEGER'),
        ]

    @property
    def insert_source_task(self):
        return VideoUsageTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )
