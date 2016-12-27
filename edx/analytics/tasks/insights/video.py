"""Tasks for aggregating statisics about video viewing."""

from collections import namedtuple
import json
import logging
import math
import re
import urllib

import ciso8601
import luigi
from luigi import configuration

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition, HiveTableTask, HiveQueryToMysqlTask
from edx.analytics.tasks.decorators import workflow_entry_point

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
# Any event that contains the events above must also contain this string.
VIDEO_EVENT_MINIMUM_STRING = '_video'

VIDEO_MAXIMUM_DURATION = 3600 * 10.0  # 10 hours, converted to seconds
VIDEO_UNKNOWN_DURATION = -1
VIDEO_VIEWING_SECONDS_PER_SEGMENT = 5
VIDEO_VIEWING_MINIMUM_LENGTH = 0.25  # seconds

VideoViewing = namedtuple('VideoViewing', [   # pylint: disable=invalid-name
    'start_timestamp', 'course_id', 'encoded_module_id', 'start_offset', 'video_duration'])


class UserVideoViewingTask(EventLogSelectionMixin, MapReduceJobTask):
    """Validates video-related events and identifies start-stop event pairs."""

    output_root = luigi.Parameter()

    # Cache for storing duration values fetched from Youtube.
    # Persist this across calls to the reducer.
    video_durations = {}

    def init_local(self):
        super(UserVideoViewingTask, self).init_local()
        # Providing an api_key is optional.
        self.api_key = configuration.get_config().get('google', 'api_key', None)
        # Reset this (mostly for the sake of tests).
        self.video_durations = {}

    def mapper(self, line):
        # Add a filter here to permit quicker rejection of unrelated events.
        if VIDEO_EVENT_MINIMUM_STRING not in line:
            return

        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            return

        if event_type not in VIDEO_EVENT_TYPES:
            return

        # This has already been checked when getting the event, so just fetch the value.
        timestamp = eventlog.get_event_time_string(event)

        # Strip username to remove trailing newlines that mess up Luigi.
        username = event.get('username', '').strip()
        if not username:
            log.error("Video event without username: %s", event)
            return

        course_id = eventlog.get_course_id(event)
        if course_id is None:
            log.warn('Video event without valid course_id: {0}'.format(line))
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            # This should already have been logged.
            return

        encoded_module_id = event_data.get('id')
        if encoded_module_id is None:
            log.warn('Video event without valid encoded_module_id (id): {0}'.format(line))
            return

        current_time = None
        old_time = None
        youtube_id = None
        if event_type == VIDEO_PLAYED:
            code = event_data.get('code')
            if code not in ('html5', 'mobile'):
                youtube_id = code
            current_time = self._check_time_offset(event_data.get('currentTime'), line)
            if current_time is None:
                return
        elif event_type == VIDEO_PAUSED:
            # Pause events may have a missing currentTime value if video is paused at the beginning,
            # so provide a default of zero.
            current_time = self._check_time_offset(event_data.get('currentTime', 0), line)
            if current_time is None:
                return
        elif event_type == VIDEO_SEEK:
            current_time = self._check_time_offset(event_data.get('new_time'), line)
            old_time = self._check_time_offset(event_data.get('old_time'), line)
            if current_time is None or old_time is None:
                return
        elif event_type == VIDEO_STOPPED:
            current_time = self._check_time_offset(event_data.get('currentTime'), line)
            if current_time is None:
                return

        if youtube_id is not None:
            youtube_id = youtube_id.encode('utf8')

        yield (
            (username.encode('utf8'), course_id.encode('utf8'), encoded_module_id.encode('utf8')),
            (timestamp, event_type, current_time, old_time, youtube_id)
        )

    def _check_time_offset(self, time_value, line):
        """Check that time can be converted to a float, and has a reasonable value."""
        if time_value is None:
            log.warn('Video with missing time_offset value: {0}'.format(line))
            return None

        try:
            time_value = float(time_value)
        except ValueError:
            log.warn('Video event with invalid time-offset value: {0}'.format(line))
            return None
        except TypeError:
            log.warn('Video event with invalid time-offset type: {0}'.format(line))
            return None

        # Some events have ridiculous (and dangerous) values for time.
        if time_value > VIDEO_MAXIMUM_DURATION:
            log.warn('Video event with huge time-offset value: {0}'.format(line))
            return None

        if time_value < 0.0:
            log.warn('Video event with negative time-offset value: {0}'.format(line))
            return None

        # We must screen out 'nan' and 'inf' values, as they do not "round-trip".
        # In Luigi, the mapper calls repr() and the reducer calls eval(), but
        # eval(repr(float('nan'))) throws a NameError rather than returning float('nan').
        if math.isnan(time_value) or math.isinf(time_value):
            log.warn('Video event with nan or inf time-offset value: {0}'.format(line))
            return None

        return time_value

    def reducer(self, key, events):
        """
        Constructs "viewing" records for each user in a course video module.

        Puts the user's video events in chronological order, and identifies pairs of
        play_video/non-play_video events.
        """
        username, course_id, encoded_module_id = key

        sorted_events = sorted(events)

        # When a user seeks forward while the video is playing, it is common to see an incorrect value for currentTime
        # in the play event emitted after the seek. The expected behavior here is play->seek->play with the second
        # play event being emitted almost immediately after the seek. This second play event should record the
        # currentTime after the seek is complete, not the currentTime before the seek started, however it often
        # records the time before the seek started. We choose to trust the seek_video event in these cases and the time
        # it claims to have moved the position to.
        last_viewing_end_event = None
        viewing = None
        for event in sorted_events:
            timestamp, event_type, current_time, old_time, youtube_id = event
            parsed_timestamp = ciso8601.parse_datetime(timestamp)
            if current_time is not None:
                current_time = float(current_time)
            if old_time is not None:
                old_time = float(old_time)

            def start_viewing():
                """Returns a 'viewing' object representing the point where a video began to be played."""
                video_duration = VIDEO_UNKNOWN_DURATION
                if youtube_id:
                    video_duration = self.video_durations.get(youtube_id)
                    if not video_duration:
                        video_duration = self.get_video_duration(youtube_id)
                        # Duration might still be unknown, but just store it.
                        self.video_durations[youtube_id] = video_duration

                if last_viewing_end_event is not None and last_viewing_end_event[1] == VIDEO_SEEK:
                    start_offset = last_viewing_end_event[2]
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
                """Returns a "viewing" record by combining the end_time with the current 'viewing' object."""

                # Check that end_time is within the bounds of the duration.
                # Note that duration may be an int, and end_time may be a float,
                # so just add +1 to avoid these round-off errors (instead of actually checking types).
                if viewing.video_duration != VIDEO_UNKNOWN_DURATION and end_time > (viewing.video_duration + 1):
                    log.error('End time of viewing past end of video.\nViewing Start: %r\nEvent: %r\nKey:%r',
                              viewing, event, key)
                    return None

                if end_time < viewing.start_offset:
                    log.error('End time is before the start time.\nViewing Start: %r\nEvent: %r\nKey:%r',
                              viewing, event, key)
                    return None

                if (end_time - viewing.start_offset) < VIDEO_VIEWING_MINIMUM_LENGTH:
                    log.error('Viewing too short and discarded.\nViewing Start: %r\nEvent: %r\nKey:%r',
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
                last_viewing_end_event = None
            elif viewing:
                viewing_end_time = None
                if event_type in (VIDEO_PAUSED, VIDEO_STOPPED):
                    # play -> pause or play -> stop
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
                    # Throw away the viewing even if it didn't yield a valid record. We assume that this is malformed
                    # data and untrustworthy.
                    viewing = None
                    last_viewing_end_event = event
            else:
                # This is a non-play video event outside of a viewing.  It is probably too frequent to be logged.
                pass

        # This happens too often!  Comment out for now...
        # if viewing is not None:
        #     log.error('Unexpected viewing started with no matching end.\n'
        #               'Viewing Start: %r\nLast Event: %r\nKey:%r', viewing, last_viewing_end_event, key)

    def output(self):
        return get_target_from_url(self.output_root)

    def get_video_duration(self, youtube_id):
        """
        For youtube videos, queries Google API for video duration information.

        This returns an "unknown" duration flag if no API key has been defined, or if the query fails.
        """
        duration = VIDEO_UNKNOWN_DURATION
        if self.api_key is None:
            return duration

        video_file = None
        try:
            video_url = "https://www.googleapis.com/youtube/v3/videos?id={0}&part=contentDetails&key={1}".format(
                youtube_id, self.api_key
            )
            video_file = urllib.urlopen(video_url)
            content = json.load(video_file)
            items = content.get('items', [])
            if len(items) > 0:
                duration_str = items[0].get(
                    'contentDetails', {'duration': 'MISSING_CONTENTDETAILS'}
                ).get('duration', 'MISSING_DURATION')
                matcher = re.match(r'PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?', duration_str)
                if not matcher:
                    log.error('Unable to parse duration returned for video %s: %s', youtube_id, duration_str)
                else:
                    duration_secs = int(matcher.group('hours') or 0) * 3600
                    duration_secs += int(matcher.group('minutes') or 0) * 60
                    duration_secs += int(matcher.group('seconds') or 0)
                    duration = duration_secs
            else:
                log.error('Unable to find items in response to duration request for youtube video: %s', youtube_id)
        except Exception:  # pylint: disable=broad-except
            log.exception("Unrecognized response from Youtube API")
        finally:
            if video_file is not None:
                video_file.close()

        return duration


class VideoTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the VideoUsageTask and its required tasks."""
    pass


class VideoUsageTask(VideoTableDownstreamMixin, MapReduceJobTask):
    """Aggregates usage statistics for video segments across individual user 'viewings'."""

    output_root = luigi.Parameter()
    dropoff_threshold = luigi.FloatParameter(config_path={'section': 'videos', 'name': 'dropoff_threshold'})

    def requires(self):
        # Define path so that data could be loaded into Hive, without actually requiring the load to be performed.
        table_name = 'user_video_viewing'
        dummy_partition = HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member
        partition_path_spec = dummy_partition.path_spec
        input_path = url_path_join(self.warehouse_path, table_name, partition_path_spec + '/')
        return UserVideoViewingTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=input_path,
        )

    def mapper(self, line):
        (
            username,
            course_id,
            encoded_module_id,
            video_duration,
            _start_timestamp_str,
            start_offset,
            end_offset,
            _reason
        ) = line.split('\t')
        yield ((course_id, encoded_module_id), (username, start_offset, end_offset, video_duration))

    def reducer(self, key, viewings):
        """
        Outputs a record for each watched segment in each video module in each course.

        Modules with no views will not be output.  Segments in video modules that were not watched
        are also not output.

        No attempt is made to ensure that videos within a video module are unique.  It is possible
        for a video to be changed or replaced over time.  It is possible for some versions to provide
        duration information and others to not.  If any viewing by a user is of a video with unknown
        duration, then the duration for the video's module is listed as unknown.  Otherwise we choose
        the maximum value across all videos for the video module.

        Output is designed to be loaded as a Hive table.  So videos with unknown duration
        use the Hive representation for Null to represent video duration.

        """
        course_id, encoded_module_id = key
        pipeline_video_id = '{0}|{1}'.format(course_id, encoded_module_id)
        usage_map = {}

        video_duration = 0
        for viewing in viewings:
            username, start_offset, end_offset, duration = viewing

            # Find the maximum actual video duration, but indicate that
            # it's unknown if any viewing was of a video with unknown duration.
            duration = float(duration)
            if video_duration == VIDEO_UNKNOWN_DURATION:
                pass
            elif duration == VIDEO_UNKNOWN_DURATION:
                video_duration = VIDEO_UNKNOWN_DURATION
            elif duration > video_duration:
                video_duration = duration

            first_segment = self.snap_to_last_segment_boundary(float(start_offset))
            last_segment = self.snap_to_last_segment_boundary(float(end_offset))
            for segment in xrange(first_segment, last_segment + 1):
                stats = usage_map.setdefault(segment, {})
                users = stats.setdefault('users', set())
                users.add(username)
                stats['views'] = stats.get('views', 0) + 1

        # If we don't know the duration of the video, just use the final segment that was
        # actually viewed to determine users_at_end.
        if video_duration == VIDEO_UNKNOWN_DURATION:
            final_segment = self.get_final_segment(usage_map)
            video_duration = ((final_segment + 1) * VIDEO_VIEWING_SECONDS_PER_SEGMENT) - 1
        else:
            final_segment = self.snap_to_last_segment_boundary(float(video_duration))

        # Output stats.
        users_at_start = len(usage_map.get(0, {}).get('users', []))
        users_at_end = len(usage_map.get(self.complete_end_segment(video_duration), {}).get('users', []))
        for segment in sorted(usage_map.keys()):
            stats = usage_map[segment]
            yield (
                pipeline_video_id,
                course_id,
                encoded_module_id,
                int(video_duration),
                VIDEO_VIEWING_SECONDS_PER_SEGMENT,
                users_at_start,
                users_at_end,
                segment,
                len(stats.get('users', [])),
                stats.get('views', 0),
            )
            if segment == final_segment:
                break

    def complete_end_segment(self, duration):
        """
        Calculates a complete end segment(if the user has watched till this segment,
        we consider the user to have watched the complete video) by cutting off the minimum of
        30 seconds and 5% of duration. Needed to cut off video credits etc.
        """
        complete_end_time = max(duration - 30, duration * 0.95)
        return self.snap_to_last_segment_boundary(complete_end_time)

    def get_final_segment(self, usage_map):
        """
        Identifies the final segment by looking for a sharp drop in number of users per segment.
        Needed as some events appear after the actual end of videos.
        """
        final_segment = last_segment = max(usage_map.keys())
        last_segment_num_users = len(usage_map[last_segment]['users'])
        for segment in sorted(usage_map.keys(), reverse=True)[1:]:
            stats = usage_map[segment]
            current_segment_num_users = len(stats.get('users', []))
            if last_segment_num_users <= current_segment_num_users * self.dropoff_threshold:
                final_segment = segment
                break
            last_segment_num_users = current_segment_num_users
            last_segment = segment
        return final_segment

    def snap_to_last_segment_boundary(self, second):
        """Maps a time_offset to a segment index."""
        return (int(second) / VIDEO_VIEWING_SECONDS_PER_SEGMENT)

    def output(self):
        return get_target_from_url(self.output_root)


class VideoUsageTableTask(VideoTableDownstreamMixin, HiveTableTask):
    """Imports data about video usage into a Hive table."""

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
            ('users_at_start', 'INT'),
            ('users_at_end', 'INT'),
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
    """Insert information about video segments from a Hive table into MySQL."""

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
    """Insert non-segment information about videos from a Hive table into MySQL."""

    @property
    def table(self):
        return "video"

    @property
    def query(self):
        return """
            SELECT
                pipeline_video_id,
                course_id,
                encoded_module_id,
                duration,
                segment_length,
                users_at_start,
                users_at_end,
                sum(num_views) * segment_length
            FROM video_usage
            GROUP BY
                pipeline_video_id,
                course_id,
                encoded_module_id,
                duration,
                segment_length,
                users_at_start,
                users_at_end
        """

    @property
    def columns(self):
        return [
            ('pipeline_video_id', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('encoded_module_id', 'VARCHAR(255)'),
            ('duration', 'INTEGER'),
            ('segment_length', 'INTEGER'),
            ('users_at_start', 'INTEGER'),
            ('users_at_end', 'INTEGER'),
            ('total_viewed_seconds', 'INTEGER'),
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


@workflow_entry_point
class InsertToMysqlAllVideoTask(VideoTableDownstreamMixin, luigi.WrapperTask):
    """Insert all video data into MySQL."""

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
