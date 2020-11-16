"""Tasks for aggregating statistics about video viewing."""
import datetime
import json
import logging
import math
import re
import textwrap
import urllib
from collections import namedtuple

import ciso8601
import luigi
from luigi import configuration
from luigi.contrib.hive import HiveQueryTask
from luigi.parameter import DateIntervalParameter

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import (
    BareHiveTableTask, HivePartition, HivePartitionTask, HiveTableTask, WarehouseMixin, hive_database_name
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import IntegerField, Record, StringField
from edx.analytics.tasks.util.url import UncheckedExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

VIDEO_CODES = frozenset([
    'html5',
    'mobile',
    'hls',
])
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


class VideoTimelineRecord(Record):
    """
    Video Segment Information used to populate the video_timeline table
    """

    pipeline_video_id = StringField(length=255,
                                    nullable=False,
                                    description='A concatenation of the course_id and the HTML encoded ID of the video.'
                                                ' Intended to uniquely identify an instance of a video in a particular '
                                                'course. Note that ideally we would use an XBlock usage_id here, but '
                                                'it isn\'t present on the legacy events.')
    segment = IntegerField(description='An integer representing the rank of the segment within the video. 0 is the '
                                       'first segment, 1 is the second etc. Note that this does not specify the length '
                                       'of the segment.')
    num_users = IntegerField(description='The number of unique users who watched any part of this segment of the '
                                         'video.')
    num_views = IntegerField(description='The total number of times any part of the segment was viewed, regardless of '
                                         'who was watching it.')


class VideoSegmentSummaryRecord(Record):
    """
    Video Segment Summary Information used to populate the video table
    """

    pipeline_video_id = StringField(length=255,
                                    nullable=False,
                                    description='A concatenation of the course_id and the HTML encoded ID of the video.'
                                                ' Intended to uniquely identify an instance of a video in a particular '
                                                'course. Note that ideally we would use an XBlock usage_id here, but '
                                                'it isn\'t present on the legacy events.')
    course_id = StringField(length=255,
                            nullable=False,
                            description='Course the video was displayed in. This is an opaque key serialized to '
                                        'a string.')
    encoded_module_id = StringField(length=255,
                                    nullable=False,
                                    description='This is the HTML encoded module ID for the video. Ideally this would '
                                                'be an XBlock usage_id, but that data is not present on legacy events.')
    duration = IntegerField(description='The video length in seconds. This can be inferred for some videos. We don\'t '
                                        'have reliable metadata for the length of videos in the source data.')
    segment_length = IntegerField(description='The length of each segment, in seconds.')
    users_at_start = IntegerField(description='The number of users who watched the first segment of the video.')
    users_at_end = IntegerField(description='The number of users who watched the end of the video. Note that this is '
                                            'not the number of users who watched the last segment of the video.')
    total_viewed_seconds = IntegerField(description='The total number of seconds viewed by all users across all '
                                                    'segments.')


class VideoSegmentDetailRecord(Record):
    """
    Video Segment Usage Detail
    """

    pipeline_video_id = StringField(length=255,
                                    nullable=False,
                                    description='A concatenation of the course_id and the HTML encoded ID of the video.'
                                                ' Intended to uniquely identify an instance of a video in a particular '
                                                'course. Note that ideally we would use an XBlock usage_id here, but '
                                                'it isn\'t present on the legacy events.')
    course_id = StringField(length=255,
                            nullable=False,
                            description='Course the video was displayed in. This is an opaque key serialized to '
                                        'a string.')
    encoded_module_id = StringField(length=255,
                                    nullable=False,
                                    description='This is the HTML encoded module ID for the video. Ideally this would '
                                                'be an XBlock usage_id, but that data is not present on legacy events.')
    duration = IntegerField(description='The video length in seconds. This can be inferred for some videos. We don\'t '
                                        'have reliable metadata for the length of videos in the source data.')
    segment_length = IntegerField(description='The length of each segment, in seconds.')
    users_at_start = IntegerField(description='The number of users who watched the first segment of the video.')
    users_at_end = IntegerField(description='The number of users who watched the end of the video. Note that this is '
                                            'not the number of users who watched the last segment of the video.')
    segment = IntegerField(description='An integer representing the rank of the segment within the video. 0 is the '
                                       'first segment, 1 is the second etc. Note that this does not specify the length '
                                       'of the segment.')
    num_users = IntegerField(description='The number of unique users who watched any part of this segment of the '
                                         'video.')
    num_views = IntegerField(description='The total number of times any part of the segment was viewed, regardless of '
                                         'who was watching it.')


class UserVideoViewingTask(EventLogSelectionMixin, MapReduceJobTask):
    """Validates video-related events and identifies start-stop event pairs."""

    output_root = luigi.Parameter()

    # Cache for storing duration values fetched from Youtube.
    # Persist this across calls to the reducer.
    video_durations = {}

    counter_category_name = 'Video Events'

    def init_local(self):
        super(UserVideoViewingTask, self).init_local()
        # Providing an api_key is optional.
        self.api_key = configuration.get_config().get('google', 'api_key', None)
        # Reset this (mostly for the sake of tests).
        self.video_durations = {}

    def mapper(self, line):
        # Add a filter here to permit quicker rejection of unrelated events.
        if VIDEO_EVENT_MINIMUM_STRING not in line:
            # self.incr_counter(self.counter_category_name, 'Discard Missing Video String', 1)
            return

        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value
        # self.incr_counter(self.counter_category_name, 'Inputs with Dates', 1)

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            self.incr_counter(self.counter_category_name, 'Discard Missing Event Type', 1)
            return

        if event_type not in VIDEO_EVENT_TYPES:
            # self.incr_counter(self.counter_category_name, 'Discard Non-Video Event Type', 1)
            return

        # self.incr_counter(self.counter_category_name, 'Input Video Events', 1)

        # This has already been checked when getting the event, so just fetch the value.
        timestamp = eventlog.get_event_time_string(event)

        user_id = event.get('context', {}).get('user_id')
        if not user_id:
            log.error("Video event without user_id in context: %s", event)
            return
        # Convert user_id to int if str
        if not isinstance(user_id, int):
            user_id = int(user_id)

        course_id = eventlog.get_course_id(event, from_url=True)
        if course_id is None:
            log.warn('Video event without valid course_id: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing course_id', 1)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            # This should already have been logged.
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Event Data', 1)
            return

        encoded_module_id = event_data.get('id', '').strip()  # we have seen id values with leading newline
        if not encoded_module_id:
            log.warn('Video event without valid encoded_module_id (id): {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing encoded_module_id', 1)
            return

        video_duration = event_data.get('duration', VIDEO_UNKNOWN_DURATION)
        if not video_duration:
            # events may have a 'duration' value of null, so use the same default for those as well.
            video_duration = VIDEO_UNKNOWN_DURATION

        # self.incr_counter(self.counter_category_name, 'Video Events Before Time Check', 1)

        current_time = None
        old_time = None
        youtube_id = None
        if event_type == VIDEO_PLAYED:
            code = event_data.get('code')
            if code not in VIDEO_CODES:
                youtube_id = code
            current_time = self._check_time_offset(event_data.get('currentTime'), line)
            if current_time is None:
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time From Play', 1)
                return
            # Slow: self.incr_counter(self.counter_category_name, 'Subset Play', 1)
        elif event_type == VIDEO_PAUSED:
            # Pause events may have a missing currentTime value if video is paused at the beginning,
            # so provide a default of zero.
            current_time = self._check_time_offset(event_data.get('currentTime', 0), line)
            if current_time is None:
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time From Pause', 1)
                return
            # Slow: self.incr_counter(self.counter_category_name, 'Subset Pause', 1)
        elif event_type == VIDEO_SEEK:
            current_time = self._check_time_offset(event_data.get('new_time'), line)
            old_time = self._check_time_offset(event_data.get('old_time'), line)
            if current_time is None or old_time is None:
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time From Seek', 1)
                return
            # Slow: self.incr_counter(self.counter_category_name, 'Subset Seek', 1)
        elif event_type == VIDEO_STOPPED:
            current_time = self._check_time_offset(event_data.get('currentTime'), line)
            if current_time is None:
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time From Stop', 1)
                return
            # Slow: self.incr_counter(self.counter_category_name, 'Subset Stop', 1)

        if youtube_id is not None:
            youtube_id = youtube_id.encode('utf8')

        # self.incr_counter(self.counter_category_name, 'Output Video Events from Mapper', 1)
        yield (
            (user_id, course_id.encode('utf8'), encoded_module_id.encode('utf8')),
            (timestamp, event_type, current_time, old_time, youtube_id, video_duration)
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
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Invalid Time-Offset Value', 1)
            return None
        except TypeError:
            log.warn('Video event with invalid time-offset type: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Invalid Time-Offset Type', 1)
            return None

        # Some events have ridiculous (and dangerous) values for time.
        if time_value > VIDEO_MAXIMUM_DURATION:
            log.warn('Video event with huge time-offset value: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Huge Time-Offset Value', 1)
            return None

        if time_value < 0.0:
            log.warn('Video event with negative time-offset value: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Negative Time-Offset Value', 1)
            return None

        # We must screen out 'nan' and 'inf' values, as they do not "round-trip".
        # In Luigi, the mapper calls repr() and the reducer calls eval(), but
        # eval(repr(float('nan'))) throws a NameError rather than returning float('nan').
        if math.isnan(time_value) or math.isinf(time_value):
            log.warn('Video event with nan or inf time-offset value: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Nan-Inf Time-Offset Value', 1)
            return None

        return time_value

    def reducer(self, key, events):
        """
        Constructs "viewing" records for each user in a course video module.

        Puts the user's video events in chronological order, and identifies pairs of
        play_video/non-play_video events.
        """
        user_id, course_id, encoded_module_id = key
        # self.incr_counter(self.counter_category_name, 'Input User_course_videos', 1)

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
            # self.incr_counter(self.counter_category_name, 'Input User_course_video events', 1)

            timestamp, event_type, current_time, old_time, youtube_id, duration = event
            parsed_timestamp = ciso8601.parse_datetime(timestamp)
            if current_time is not None:
                current_time = float(current_time)
            if old_time is not None:
                old_time = float(old_time)

            def start_viewing():
                """Returns a 'viewing' object representing the point where a video began to be played."""
                # self.incr_counter(self.counter_category_name, 'Viewing Start', 1)

                video_duration = duration
                # video_duration is set to VIDEO_UNKNOWN_DURATION only when duration is not present in
                # a video event, In that case fetch duration using youtube API if video is from youtube.
                if video_duration == VIDEO_UNKNOWN_DURATION and youtube_id:
                    # self.incr_counter(self.counter_category_name, 'Viewing Start with Video Id', 1)
                    video_duration = self.video_durations.get(youtube_id)
                    if not video_duration:
                        video_duration = self.get_video_duration(youtube_id)
                        # Duration might still be unknown, but just store it.
                        self.video_durations[youtube_id] = video_duration

                if last_viewing_end_event is not None and last_viewing_end_event[1] == VIDEO_SEEK:
                    start_offset = last_viewing_end_event[2]
                    # Slow: self.incr_counter(self.counter_category_name, 'Subset Viewing Start With Offset From Preceding Seek', 1)
                else:
                    start_offset = current_time
                    # Slow: self.incr_counter(self.counter_category_name, 'Subset Viewing Start With Offset From Current Play', 1)
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
                # Slow: self.incr_counter(self.counter_category_name, 'Viewing End', 1)

                if viewing.video_duration != VIDEO_UNKNOWN_DURATION and end_time > (viewing.video_duration + 1):
                    log.error('End time of viewing past end of video.\nViewing Start: %r\nEvent: %r\nKey:%r',
                              viewing, event, key)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End Time Past End Of Video', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                    return None

                if end_time < viewing.start_offset:
                    log.error('End time is before the start time.\nViewing Start: %r\nEvent: %r\nKey:%r',
                              viewing, event, key)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End Time Before Start Time', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                    return None

                if (end_time - viewing.start_offset) < VIDEO_VIEWING_MINIMUM_LENGTH:
                    log.error('Viewing too short and discarded.\nViewing Start: %r\nEvent: %r\nKey:%r',
                              viewing, event, key)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End Time Too Short', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                    return None

                return (
                    user_id,
                    viewing.course_id,
                    viewing.encoded_module_id,
                    viewing.video_duration,
                    viewing.start_timestamp.isoformat(),
                    viewing.start_offset,
                    end_time,
                    event_type,
                )

            if event_type == VIDEO_PLAYED:
                if viewing:
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing Start On Successive Play', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing Start', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                    pass
                viewing = start_viewing()
                last_viewing_end_event = None
            elif viewing:
                viewing_end_time = None
                if event_type in (VIDEO_PAUSED, VIDEO_STOPPED):
                    # play -> pause or play -> stop
                    viewing_end_time = current_time
                    # Slow: self.incr_counter(self.counter_category_name, 'Subset Viewing End By Stop Or Pause', 1)
                elif event_type == VIDEO_SEEK:
                    # play -> seek
                    viewing_end_time = old_time
                    # Slow: self.incr_counter(self.counter_category_name, 'Subset Viewing End By Seek', 1)
                else:
                    log.error('Unexpected event in viewing.\nViewing Start: %r\nEvent: %r\nKey:%r', viewing, event, key)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard End Viewing Unexpected Event', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End', 1)
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                if viewing_end_time is not None:
                    record = end_viewing(viewing_end_time)
                    if record:
                        # Slow: self.incr_counter(self.counter_category_name, 'Output Viewing', 1)
                        yield record
                    # Throw away the viewing even if it didn't yield a valid record. We assume that this is malformed
                    # data and untrustworthy.
                    viewing = None
                    last_viewing_end_event = event
            else:
                # This is a non-play video event outside of a viewing.  It is probably too frequent to be logged.
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Event Outside Of Viewing', 1)
                pass

        if viewing is not None:
            # This happens too often!  Comment out for now...
            # log.error('Unexpected viewing started with no matching end.\n'
            #           'Viewing Start: %r\nLast Event: %r\nKey:%r', viewing, last_viewing_end_event, key)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing Start With No Matching End', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing Start', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
            pass

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

        # Slow: self.incr_counter(self.counter_category_name, 'Subset Calls to Youtube API', 1)
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
                    # Slow: self.incr_counter(self.counter_category_name, 'Quality Unparseable Response From Youtube API', 1)
                else:
                    duration_secs = int(matcher.group('hours') or 0) * 3600
                    duration_secs += int(matcher.group('minutes') or 0) * 60
                    duration_secs += int(matcher.group('seconds') or 0)
                    duration = duration_secs
                    # Slow: self.incr_counter(self.counter_category_name, 'Subset Calls to Youtube API Succeeding', 1)
            else:
                log.error('Unable to find items in response to duration request for youtube video: %s', youtube_id)
                # Slow: self.incr_counter(self.counter_category_name, 'Quality No Items In Response From Youtube API', 1)
        except Exception:  # pylint: disable=broad-except
            log.exception("Unrecognized response from Youtube API")
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Unrecognized Response From Youtube API', 1)
        finally:
            if video_file is not None:
                video_file.close()

        return duration


class VideoTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the VideoUsageTask and its required tasks."""
    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'videos', 'name': 'overwrite_n_days'},
        significant=False,
        default=3,
    )


class UserVideoViewingByDateTask(OverwriteOutputMixin, VideoTableDownstreamMixin, MultiOutputMapReduceJobTask):
    "Task that reads in video viewings and outputs by date of viewing start time."

    output_root = None

    def __init__(self, *args, **kwargs):
        super(UserVideoViewingByDateTask, self).__init__(*args, **kwargs)

        overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)
        self.overwrite_interval = DateIntervalParameter().parse('{}-{}'.format(
            overwrite_from_date,
            self.interval.date_b
        ))

    def requires(self):
        output_path = self.hive_partition_path('video_viewing', self.interval.date_b)
        return UserVideoViewingTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.overwrite_interval,
            pattern=self.pattern,
            output_root=output_path,
        )

    def mapper(self, line):
        (
            _user_id,
            _course_id,
            _encoded_module_id,
            _video_duration,
            start_timestamp,
            _start_offset,
            _end_time,
            _event_type
        ) = line.split('\t')

        lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

        date_string = start_timestamp.split("T")[0]
        if date_string < lower_bound_date_string or date_string >= upper_bound_date_string:
            return

        yield date_string, line

    def multi_output_reducer(self, _date_string, values, output_file):
        for value in values:
            output_file.write(value)
            output_file.write('\n')

    def output_path_for_key(self, key):
        date_string = key
        return url_path_join(
            self.hive_partition_path('video_viewing_by_date', date_string),
            'video_viewing_{date}'.format(
                date=date_string,
            ),
        )

    def downstream_input_tasks(self):
        """
        MultiOutputMapReduceJobTask returns marker as output(which cannot be used as input in other jobs).
        This method returns the external tasks, which can then be used as input.
        """

        tasks = []
        for date in self.interval:  # pylint: disable=not-an-iterable
            url = self.output_path_for_key(date.isoformat())
            tasks.append(UncheckedExternalURL(url))

        return tasks

    def run(self):
        # Remove the marker file.
        self.remove_output_on_overwrite()
        # Also remove actual output files in case of overwrite.
        if self.overwrite:
            for date in self.overwrite_interval:
                url = self.output_path_for_key(date.isoformat())
                target = get_target_from_url(url)
                if target.exists():
                    target.remove()

        super(UserVideoViewingByDateTask, self).run()

        # Make sure an output file exists for each day within the interval.
        for date in self.overwrite_interval:
            url = self.output_path_for_key(date.isoformat())
            target = get_target_from_url(url)
            if not target.exists():
                target.open("w").close()  # touch the file


class VideoUsageTask(VideoTableDownstreamMixin, MapReduceJobTask):
    """Aggregates usage statistics for video segments across individual user 'viewings'."""

    output_root = luigi.Parameter()
    dropoff_threshold = luigi.FloatParameter(config_path={'section': 'videos', 'name': 'dropoff_threshold'})

    def requires_local(self):
        return UserVideoViewingByDateTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            overwrite_n_days=self.overwrite_n_days,
            overwrite=True,
        )

    def requires_hadoop(self):
        return self.requires_local().downstream_input_tasks()

    def mapper(self, line):
        (
            user_id,
            course_id,
            encoded_module_id,
            video_duration,
            _start_timestamp_str,
            start_offset,
            end_offset,
            _reason
        ) = line.split('\t')
        yield ((course_id, encoded_module_id), (user_id, start_offset, end_offset, video_duration))

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
            user_id, start_offset, end_offset, duration = viewing

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
                users.add(user_id)
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
            yield VideoSegmentDetailRecord(
                pipeline_video_id=pipeline_video_id,
                course_id=course_id,
                encoded_module_id=encoded_module_id,
                duration=int(video_duration),
                segment_length=VIDEO_VIEWING_SECONDS_PER_SEGMENT,
                users_at_start=users_at_start,
                users_at_end=users_at_end,
                segment=segment,
                num_users=len(stats.get('users', [])),
                num_views=stats.get('views', 0)
            ).to_string_tuple()
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
    def columns(self):  # pragma: no cover
        return VideoSegmentDetailRecord.get_hive_schema()

    @property
    def partition(self):  # pragma: no cover
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):  # pragma: no cover
        return VideoUsageTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            output_root=self.partition_location,
            overwrite_n_days=self.overwrite_n_days,
        )


class VideoTimelineTableTask(BareHiveTableTask):
    """Creates the Hive storage table used to hold video_timeline data."""

    @property
    def partition_by(self):  # pragma: no cover
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'video_timeline'

    @property
    def columns(self):  # pragma: no cover
        return VideoTimelineRecord.get_hive_schema()


class VideoTimelinePartitionTask(VideoTableDownstreamMixin, HivePartitionTask):
    """Creates the Hive storage partition used to hold video_timeline data."""

    @property
    def hive_table_task(self):  # pragma: no cover
        return VideoTimelineTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def partition_value(self):  # pragma: no cover
        """Use a dynamic partition value based on the date parameter."""
        return self.interval.date_b.isoformat()  # pylint: disable=no-member


class VideoTimelineDataTask(VideoTableDownstreamMixin, HiveQueryTask):
    """Execute the query on video_usage and persist the results into video_timeline."""

    @property
    def insert_query(self):  # pragma: no cover
        """The insert query that specifies the fields from the source table."""

        return """
            SELECT
                pipeline_video_id,
                segment,
                num_users,
                num_views
            FROM video_usage
        """

    def query(self):  # pragma: no cover
        full_insert_query = """
                    USE {database_name};
                    INSERT INTO TABLE {table}
                    PARTITION ({partition.query_spec})
                    {insert_query};
                    """.format(
            database_name=hive_database_name(),
            table=self.partition_task.hive_table_task.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),  # pylint: disable=no-member
        )

        return textwrap.dedent(full_insert_query)

    @property
    def partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return VideoTimelinePartitionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )

    @property
    def partition(self):  # pragma: no cover
        """A shorthand for the partition information on the upstream partition task."""
        return self.partition_task.partition  # pylint: disable=no-member

    def requires(self):  # pragma: no cover
        for requirement in super(VideoTimelineDataTask, self).requires():
            yield requirement
        yield self.partition_task

        yield VideoUsageTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
        )

    def output(self):  # pragma: no cover
        output_root = url_path_join(self.warehouse_path,
                                    self.partition_task.hive_table_task.table,
                                    self.partition.path_spec + '/')
        return get_target_from_url(output_root, marker=True)

    def on_success(self):  # pragma: no cover
        """Overload the success method to touch the _SUCCESS file.  Any class that uses a separate Marker file from the
        data file will need to override the base on_success() call to create this marker."""
        self.output().touch_marker()


class InsertToMysqlVideoTimelineTask(VideoTableDownstreamMixin, MysqlInsertTask):
    """Insert information about video timelines from a Hive table into MySQL."""

    overwrite = luigi.BoolParameter(
        default=True,
        description='Overwrite the table when writing to it by default. Allow users to override this behavior if they '
                    'want.',
        significant=False
    )
    allow_empty_insert = luigi.BoolParameter(
        default=False,
        description='Allow the video table to be empty (e.g. if no video activity has occurred)',
        config_path={'section': 'videos', 'name': 'allow_empty_insert'},
        significant=False,
    )

    @property
    def table(self):  # pragma: no cover
        return 'video_timeline'

    @property
    def insert_source_task(self):  # pragma: no cover
        return VideoTimelineDataTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
        )

    @property
    def columns(self):  # pragma: no cover
        return VideoTimelineRecord.get_sql_schema()

    @property
    def indexes(self):  # pragma: no cover
        return [
            ('pipeline_video_id',),
        ]


class VideoTableTask(BareHiveTableTask):
    """Creates the Hive storage table used to hold video data."""

    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'video'

    @property
    def columns(self):  # pragma: no cover
        return VideoSegmentSummaryRecord.get_hive_schema()


class VideoPartitionTask(VideoTableDownstreamMixin, HivePartitionTask):
    """Creates the Hive storage partition used to hold video data."""

    @property
    def hive_table_task(self):  # pragma: no cover
        return VideoTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.interval.date_b.isoformat()  # pylint: disable=no-member


class VideoDataTask(VideoTableDownstreamMixin, HiveQueryTask):
    """Execute the query on video_usage and persist the results into video."""

    @property
    def insert_query(self):  # pragma: no cover
        """The fields used in the source table."""
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

    def query(self):  # pragma: no cover
        full_insert_query = """
                    USE {database_name};
                    INSERT INTO TABLE {table}
                    PARTITION ({partition.query_spec})
                    {insert_query};
                """.format(
            database_name=hive_database_name(),
            table=self.partition_task.hive_table_task.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),  # pylint: disable=no-member
        )
        return textwrap.dedent(full_insert_query)

    @property
    def partition(self):  # pragma: no cover
        """A shorthand for the partition object on the upstream partition task."""
        return self.partition_task.partition  # pylint: disable=no-member

    @property
    def partition_task(self):  # pragma: no cover
        """Returns the task representing the work to create the partition."""
        return VideoPartitionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(VideoDataTask, self).requires():
            yield requirement
        yield self.partition_task

        yield VideoUsageTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
        )

    def output(self):  # pragma: no cover
        output_root = url_path_join(self.warehouse_path,
                                    self.partition_task.hive_table_task.table,
                                    self.partition.path_spec + '/')
        return get_target_from_url(output_root, marker=True)

    def on_success(self):  # pragma: no cover
        """Overload the success method to touch the _SUCCESS file.  Any class that uses a separate Marker file from the
        data file will need to override the base on_success() call to create this marker."""
        self.output().touch_marker()


class InsertToMysqlVideoTask(VideoTableDownstreamMixin, MysqlInsertTask):
    """Insert summary information into the video table in MySQL."""

    overwrite = luigi.BoolParameter(
        default=True,
        description='Overwrite the table when writing to it by default. Allow users to override this behavior if they '
                    'want.',
        significant=False
    )
    allow_empty_insert = luigi.BoolParameter(
        default=False,
        description='Allow the video table to be empty (e.g. if no video activity has occurred)',
        config_path={'section': 'videos', 'name': 'allow_empty_insert'},
        significant=False,
    )

    @property
    def table(self):  # pragma: no cover
        return 'video'

    @property
    def insert_source_task(self):  # pragma: no cover
        return VideoDataTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
        )

    @property
    def columns(self):  # pragma: no cover
        return VideoSegmentSummaryRecord.get_sql_schema()

    @property
    def indexes(self):  # pragma: no cover
        return [
            ('course_id', 'encoded_module_id'),
        ]


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
            'overwrite_n_days': self.overwrite_n_days,
        }
        yield (
            InsertToMysqlVideoTimelineTask(**kwargs),
            InsertToMysqlVideoTask(**kwargs),
        )
