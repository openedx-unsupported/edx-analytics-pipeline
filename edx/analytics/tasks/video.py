
from collections import namedtuple
import hashlib
import datetime
import logging

import ciso8601
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition, HiveTableTask, HiveQueryToMysqlTask

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
                if not end_time or not session.start_offset:
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


class UserVideoSessionTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    pass


class UserVideoSessionTableTask(UserVideoSessionTableDownstreamMixin, HiveTableTask):

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


class NaturalNumbersTask(luigi.Task):

    output_root = luigi.Parameter()
    maximum = luigi.Parameter(default=10000)

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'data.tsv'))

    def run(self):
        with self.output().open('w') as output_file:
            for i in xrange(self.maximum):
                output_file.write(unicode(i).encode('utf8') + '\n')


class NaturalNumbersTableTask(HiveTableTask):

    maximum = luigi.Parameter(default=10000)

    @property
    def table(self):
        return 'natural_numbers'

    @property
    def columns(self):
        return [
            ('num', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('m', str(self.maximum))

    def requires(self):
        return NaturalNumbersTask(
            output_root=self.partition_location,
            maximum=self.maximum,
        )


class VideoHeatmapTableTask(UserVideoSessionTableDownstreamMixin, HiveQueryToMysqlTask):

    @property
    def indexes(self):
        return [
            ('module_id',),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            UserVideoSessionTableTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
            ),
            NaturalNumbersTableTask(
                warehouse_path=self.warehouse_path,
            )
        )

    @property
    def query(self):
        return """
            SELECT
                s.encoded_module_id as module_id,
                second.num as segment,
                COUNT(DISTINCT s.username) as num_users,
                COUNT(1) as num_views
            FROM user_video_session s
            CROSS JOIN natural_numbers second
            WHERE
                FLOOR(s.start_offset) <= second.num AND second.num <= CEIL(s.end_offset)
            GROUP BY
                s.encoded_module_id,
                second.num
        """

    @property
    def table(self):
        return 'video_heatmap'

    @property
    def columns(self):
        return [
            ('module_id', 'VARCHAR(255) NOT NULL'),
            ('segment', 'INTEGER'),
            ('num_users', 'INTEGER'),
            ('num_views', 'INTEGER'),
        ]
