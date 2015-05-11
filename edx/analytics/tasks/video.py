from collections import namedtuple
import logging
import math
import urllib
import json
import re

import ciso8601
import luigi
from luigi import configuration

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, ExternalURL
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition, HiveTableTask, HiveQueryToMysqlTask

log = logging.getLogger(__name__)


VIDEO_PLAYED = 'play_video'
VIDEO_PAUSED = 'pause_video'
VIDEO_SEEK = 'seek_video'
VIDEO_STOPPED = 'stop_video'
VIDEO_EVENT_TYPES = frozenset([
    VIDEO_PLAYED,
    VIDEO_PAUSED,
    VIDEO_SEEK,
    VIDEO_STOPPED,
])
VIDEO_VIEWING_UNKNOWN_DURATION = -1
VIDEO_VIEWING_SECONDS_PER_SEGMENT = 5
VIDEO_VIEWING_MINIMUM_LENGTH = 0.25  # seconds

VideoViewing = namedtuple('VideoViewing', [
    'start_timestamp', 'course_id', 'encoded_module_id', 'start_offset', 'video_duration'])


class UserVideoViewingTask(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def init_local(self):
        super(UserVideoViewingTask, self).init_local()
        self.api_key = configuration.get_config().get('google', 'api_key')

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

        course_id = eventlog.get_course_id(event)

        encoded_module_id = None
        current_time = None
        old_time = None
        youtube_id = None
        if event_type in VIDEO_EVENT_TYPES:
            event_data = eventlog.get_event_data(event)
            encoded_module_id = event_data.get('id')
            current_time = event_data.get('currentTime')
            code = event_data.get('code')
            if code in ('html5', 'mobile') or course_id is None:
                return
            if event_type == VIDEO_PLAYED:
                youtube_id = code
                if current_time is None:
                    log.warn('Play video without valid currentTime: {0}'.format(line))
                    return
            elif event_type == VIDEO_PAUSED:
                if current_time is None:
                    current_time = 0
            elif event_type == VIDEO_SEEK:
                current_time = event_data.get('new_time')
                old_time = event_data.get('old_time')
                if current_time is None or old_time is None:
                    log.warn('Seek event without valid old and new times: {0}'.format(line))
                    return
        else:
            return

        if youtube_id is not None:
            youtube_id = youtube_id.encode('utf8')

        yield (
            (username.encode('utf8'), course_id.encode('utf8'), encoded_module_id.encode('utf8')),
            (timestamp, event_type, current_time, old_time, youtube_id)
        )

    def reducer(self, key, events):
        username, course_id, encoded_module_id = key

        sorted_events = sorted(events)
        video_durations = {}

        last_event = None
        viewing = None
        for event in sorted_events:
            timestamp, event_type, current_time, old_time, youtube_id = event
            parsed_timestamp = ciso8601.parse_datetime(timestamp)
            if current_time is not None:
                current_time = float(current_time)

            def start_viewing():
                video_duration = VIDEO_VIEWING_UNKNOWN_DURATION
                if youtube_id:
                    video_duration = video_durations.get(youtube_id)
                    if not video_duration:
                        video_duration = self.get_video_duration(youtube_id)
                        video_durations[youtube_id] = video_duration

                if last_event is not None and last_event[1] == VIDEO_SEEK:
                    start_offset = last_event[2]
                else:
                    start_offset = current_time

                return VideoViewing(
                    start_timestamp=parsed_timestamp,
                    course_id=course_id,
                    encoded_module_id=encoded_module_id,
                    start_offset=start_offset,
                    video_duration=video_duration
                )

            def end_viewing(end_time):
                if viewing.video_duration == VIDEO_VIEWING_UNKNOWN_DURATION:
                    # log.error('Unknown video duration at end of viewing.\nViewing Start: %r\nEvent: %r',
                    #           viewing, event)
                    return None

                if end_time > viewing.video_duration:
                    log.error('End time of viewing past end of video.\nViewing Start: %r\nEvent: %r\nKey:%r',
                              viewing, event, key)
                    return None

                if (end_time - viewing.start_offset) < VIDEO_VIEWING_MINIMUM_LENGTH:
                    # log.error('Viewing too short and discarded.\nViewing Start: %r\nEvent: %r\nKey:%r',
                    #           viewing, event, key)
                    return None

                if end_time < viewing.start_offset:
                    log.error('End time is before the start time.\nViewing Start: %r\nEvent: %r\nKey:%r',
                              viewing, event, key)
                    return None

                return (
                    username,
                    viewing.course_id,
                    viewing.encoded_module_id,
                    viewing.video_duration,
                    viewing.start_timestamp.isoformat(),
                    viewing.start_offset,
                    end_time,
                    event_type,
                )

            if event_type == VIDEO_PLAYED:
                viewing = start_viewing()
            elif viewing:
                viewing_end_time = None
                if event_type in (VIDEO_PAUSED, VIDEO_STOPPED):
                    # play -> pause
                    # play -> stop
                    viewing_end_time = current_time

                elif event_type == VIDEO_SEEK:
                    # play -> seek
                    viewing_end_time = old_time

                else:
                    log.error('Unexpected event in viewing.\nViewing Start: %r\nEvent: %r\nKey:%r', viewing, event, key)

                if viewing_end_time is not None:
                    record = end_viewing(viewing_end_time)
                    if record:
                        yield record
                viewing = None
            else:
                # this is a non-play event outside of a viewing
                pass

            last_event = event

    def output(self):
        return get_target_from_url(self.output_root)

    def get_video_duration(self, youtube_id):
        duration = VIDEO_VIEWING_UNKNOWN_DURATION
        video_file = None
        try:
            video_file = urllib.urlopen("https://www.googleapis.com/youtube/v3/videos?id={0}&part=contentDetails&key={1}".format(youtube_id, self.api_key))
            content = json.load(video_file)
            items = content['items']
            if len(items) > 0:
                duration_str = items[0]['contentDetails']['duration']
                matcher = re.match(r'PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?', duration_str)
                if not matcher:
                    log.error('Unable to parse duration: %s', duration)
                else:
                    duration_secs = int(matcher.group('hours') or 0) * 3600
                    duration_secs += int(matcher.group('minutes') or 0) * 60
                    duration_secs += int(matcher.group('seconds') or 0)
                    duration = duration_secs
        except IOError:
            pass
        finally:
            if video_file is not None:
                video_file.close()

        return duration


class VideoTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    pass


class UserVideoViewingTableTask(VideoTableDownstreamMixin, HiveTableTask):

    @property
    def table(self):
        return 'user_video_viewing'

    @property
    def columns(self):
        return [
            ('username', 'STRING'),
            ('course_id', 'STRING'),
            ('encoded_module_id', 'STRING'),
            ('start_timestamp', 'STRING'),
            ('start_offset', 'FLOAT'),
            ('end_offset', 'FLOAT'),
            ('end_reason', 'STRING'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return UserVideoViewingTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )

    def output(self):
        return self.requires().output()


class VideoUsageTask(EventLogSelectionDownstreamMixin, WarehouseMixin, MapReduceJobTask):

    output_root = luigi.Parameter()
    input_path = luigi.Parameter(default=None)

    def requires(self):
        if self.input_path:
            return ExternalURL(self.input_path)
        else:
            return UserVideoViewingTableTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path
            )

    def mapper(self, line):
        username, course_id, encoded_module_id, video_duration, _start_timestamp_str, start_offset, end_offset, _reason = line.split('\t')
        yield ((course_id, encoded_module_id), (username, start_offset, end_offset, video_duration))

    def reducer(self, key, viewings):
        course_id, encoded_module_id = key
        pipeline_video_id = '{0}|{1}'.format(course_id, encoded_module_id)
        usage_map = {}

        video_duration = 0
        for viewing in viewings:
            username, start_offset, end_offset, duration = viewing

            if duration > video_duration:
                video_duration = duration

            first_second = int(math.floor(float(start_offset)))
            first_segment = self.snap_to_last_segment_boundary(first_second)
            last_second = int(math.ceil(float(end_offset)))
            last_segment = self.snap_to_last_segment_boundary(last_second)
            stats = usage_map.setdefault(first_segment, {})
            stats['starts'] = stats.get('starts', 0) + 1
            stats = usage_map.setdefault(last_segment, {})
            stats['stops'] = stats.get('stops', 0) + 1
            for segment in xrange(first_segment, last_segment + 1):
                stats = usage_map.setdefault(segment, {})
                users = stats.setdefault('users', set())
                users.add(username)
                stats['views'] = stats.get('views', 0) + 1

        final_segment = self.snap_to_last_segment_boundary(int(math.ceil(float(video_duration))))
        start_views = usage_map.get(0, {}).get('views', 0)
        end_views = usage_map.get(final_segment, {}).get('views', 0)
        partial_views = abs(start_views - end_views)
        for segment in sorted(usage_map.keys()):
            stats = usage_map[segment]
            yield (
                pipeline_video_id,
                course_id,
                encoded_module_id,
                video_duration,
                VIDEO_VIEWING_SECONDS_PER_SEGMENT,
                start_views,
                end_views,
                partial_views,
                segment,
                len(stats.get('users', [])),
                stats.get('views', 0),
            )

    def snap_to_last_segment_boundary(self, second):
        return (int(second) / VIDEO_VIEWING_SECONDS_PER_SEGMENT)

    def output(self):
        return get_target_from_url(self.output_root)


class VideoUsageTableTask(VideoTableDownstreamMixin, HiveTableTask):

    @property
    def table(self):
        return 'video_usage'

    @property
    def columns(self):
        return [
            ('pipeline_video_id', 'STRING'),
            ('course_id', 'STRING'),
            ('encoded_module_id', 'STRING'),
            ('duration', 'INT'),
            ('segment_length', 'INT'),
            ('start_views', 'INT'),
            ('end_views', 'INT'),
            ('partial_views', 'INT'),
            ('segment', 'INT'),
            ('num_users', 'INT'),
            ('num_views', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return VideoUsageTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            output_root=self.partition_location
        )

    def output(self):
        return self.requires().output()


class InsertToMysqlVideoTimelineTask(VideoTableDownstreamMixin, HiveQueryToMysqlTask):

    @property
    def table(self):
        return "video_timeline"

    @property
    def query(self):
        return """
            SELECT
                pipeline_video_id,
                segment,
                num_users,
                num_views
            FROM video_usage
        """

    @property
    def columns(self):
        return [
            ('pipeline_video_id', 'VARCHAR(255)'),
            ('segment', 'INTEGER'),
            ('num_users', 'INTEGER'),
            ('num_views', 'INTEGER'),
        ]

    @property
    def required_table_tasks(self):
        return VideoUsageTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )

    @property
    def indexes(self):
        return [
            ('pipeline_video_id',),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member


class InsertToMysqlVideoTask(VideoTableDownstreamMixin, HiveQueryToMysqlTask):

    @property
    def table(self):
        return "video"

    @property
    def query(self):
        return """
            SELECT DISTINCT
                pipeline_video_id,
                course_id,
                encoded_module_id,
                duration,
                segment_length,
                start_views,
                end_views,
                partial_views
            FROM video_usage
        """

    @property
    def columns(self):
        return [
            ('pipeline_video_id', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('encoded_module_id', 'VARCHAR(255)'),
            ('duration', 'INTEGER'),
            ('segment_length', 'INTEGER'),
            ('start_views', 'INTEGER'),
            ('end_views', 'INTEGER'),
            ('partial_views', 'INTEGER'),
        ]

    @property
    def required_table_tasks(self):
        return VideoUsageTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )

    @property
    def indexes(self):
        return [
            ('course_id', 'encoded_module_id'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member


class InsertToMysqlAllVideoTask(VideoTableDownstreamMixin, luigi.WrapperTask):

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            InsertToMysqlVideoTimelineTask(**kwargs),
            InsertToMysqlVideoTask(**kwargs),
        )
