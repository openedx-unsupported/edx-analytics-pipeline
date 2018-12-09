"""Test student engagement metrics"""

import json
from unittest import TestCase

from ddt import data, ddt, unpack
from mock import MagicMock, patch, sentinel

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.insights.video import (
    VIDEO_CODES, VIDEO_UNKNOWN_DURATION, VIDEO_VIEWING_SECONDS_PER_SEGMENT, UserVideoViewingTask,
    VideoSegmentDetailRecord, VideoUsageTask
)
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeLegacyKeysMixin, InitializeOpaqueKeysMixin


@ddt
class UserVideoViewingTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, TestCase):
    """Test video viewing mapper"""

    UTF8_BYTE_STRING = 'I\xd4\x89\xef\xbd\x94\xc3\xa9\xef\xbd\x92\xd0\xbb\xc3\xa3'

    task_class = UserVideoViewingTask

    def setUp(self):
        super(UserVideoViewingTaskMapTest, self).setUp()

        self.initialize_ids()
        self.video_id = 'i4x-foo-bar-baz'
        self.video_duration = 888
        self.event_templates = {
            'play_video': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "browser",
                "event_type": "play_video",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": '{"id": "%s", "currentTime": 23.4398, "code": "87389iouhdfh", "duration": %s}' % (
                    self.video_id,
                    self.video_duration
                ),
                "agent": "blah, blah, blah",
                "page": None
            },
            'pause_video': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "browser",
                "event_type": "pause_video",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": '{"id": "%s", "currentTime": 28, "code": "87389iouhdfh", "duration": %s}' % (
                    self.video_id,
                    self.video_duration
                ),
                "agent": "blah, blah, blah",
                "page": None
            },
            'stop_video': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "browser",
                "event_type": "stop_video",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": '{"id": "%s", "currentTime": 100, "code": "87389iouhdfh", "duration": %s}' % (
                    self.video_id,
                    self.video_duration
                ),
                "agent": "blah, blah, blah",
                "page": None
            },
            'seek_video': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "browser",
                "event_type": "seek_video",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": '{"id": "%s", "old_time": 14, "new_time": 10, "code": "87389iouhdfh", "duration": %s}' % (
                    self.video_id,
                    self.video_duration
                ),
                "agent": "blah, blah, blah",
                "page": None
            }
        }
        self.default_event_template = 'play_video'
        self.default_key = (self.DEFAULT_USER_ID, self.encoded_course_id, self.video_id.encode('utf8'))

    @data(
        {'time': "2013-12-01T15:38:32.805444"},
        {'time': None},
        {'event_type': None},
        {'event': None},
        {'event': ''},
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    @data(
        'time',
        'event_type',
        'event',
    )
    def test_events_with_missing_attribute(self, attribute_name):
        event_dict = self.create_event_dict()
        del event_dict[attribute_name]
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    @data(
        'page_close',
        'seq_goto',
        'seq_next',
        '/foo/bar',
        '/jsi18n/',
        'edx.course.enrollment.activated',
    )
    def test_filter_event_types(self, event_type):
        self.assert_no_map_output_for(self.create_event_log_line(event_type=event_type))

    @data(
        'play_video',
        'pause_video',
        'seek_video',
        'stop_video'
    )
    def test_allowed_event_types(self, template_name):
        mapper_output = tuple(self.task.mapper(self.create_event_log_line(template_name=template_name)))
        self.assertEquals(len(mapper_output), 1)

    def test_video_event_invalid_course_id(self):
        template = self.event_templates['play_video']
        template['context']['course_id'] = 'lsdkjfsdkljf'
        self.assert_no_map_output_for(self.create_event_log_line(template=template))

    def test_missing_event_data(self):
        self.assert_no_map_output_for(self.create_event_log_line(event=''))

    def test_play_video_youtube(self):
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 23.4398, None, '87389iouhdfh', self.video_duration)
        self.assert_single_map_output(self.create_event_log_line(), self.default_key, expected_value)

    @data(*tuple(VIDEO_CODES))
    def test_play_video_non_youtube(self, code):
        payload = {
            "id": self.video_id,
            "currentTime": 5,
            "code": code
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 5, None, None, -1)
        self.assert_single_map_output(self.create_event_log_line(event=event), self.default_key, expected_value)

    def test_play_video_without_current_time(self):
        payload = {
            "id": self.video_id,
            "code": "87389iouhdfh"
        }
        event = json.dumps(payload)
        self.assert_no_map_output_for(self.create_event_log_line(event=event))

    def test_pause_video(self):
        expected_value = (self.DEFAULT_TIMESTAMP, 'pause_video', 28, None, None, self.video_duration)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='pause_video'), self.default_key, expected_value)

    def test_pause_video_without_time(self):
        payload = {
            "id": self.video_id,
            "code": "foo"
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'pause_video', 0, None, None, -1)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='pause_video', event=event), self.default_key, expected_value)

    def test_pause_video_with_extremely_high_time(self):
        payload = {
            "id": self.video_id,
            "currentTime": "928374757012",
            "code": "foo"
        }
        event = json.dumps(payload)
        self.assert_no_map_output_for(self.create_event_log_line(template_name='pause_video', event=event))

    @data('old_time', 'new_time')
    def test_seek_video_invalid_time(self, field_to_remove):
        payload = {
            "id": self.video_id,
            "old_time": 5,
            "new_time": 10,
            "code": 'foo'
        }
        del payload[field_to_remove]
        event = json.dumps(payload)
        self.assert_no_map_output_for(self.create_event_log_line(template_name='seek_video', event=event))

    def test_seek_video(self):
        expected_value = (self.DEFAULT_TIMESTAMP, 'seek_video', 10, 14, None, self.video_duration)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='seek_video'), self.default_key, expected_value)

    def test_seek_video_string_time(self):
        payload = {
            "id": self.video_id,
            "old_time": "5",
            "new_time": 10,
            "code": 'foo'
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'seek_video', 10, 5, None, -1)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='seek_video', event=event), self.default_key, expected_value)

    def test_stop_video(self):
        expected_value = (self.DEFAULT_TIMESTAMP, 'stop_video', 100, None, None, self.video_duration)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='stop_video'), self.default_key, expected_value)

    def test_unicode_module_id(self):
        key = (self.DEFAULT_USER_ID, self.encoded_course_id, self.UTF8_BYTE_STRING)
        payload = {
            "id": self.UTF8_BYTE_STRING,
            "currentTime": 5,
            "code": 'foo'
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 5, None, 'foo', -1)
        self.assert_single_map_output(self.create_event_log_line(event=event), key, expected_value)

    def test_unicode_garbage_course_id(self):
        template = self.event_templates['play_video']
        template['context']['course_id'] = self.UTF8_BYTE_STRING
        self.assert_no_map_output_for(self.create_event_log_line(template=template))

    def test_unicode_code(self):
        payload = {
            "id": self.video_id,
            "currentTime": 5,
            "code": self.UTF8_BYTE_STRING
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 5, None, self.UTF8_BYTE_STRING, -1)
        self.assert_single_map_output(self.create_event_log_line(event=event), self.default_key, expected_value)

    @data(
        ('play_video', 922337203685.4775),
        ('pause_video', 922337203685.4775),
        ('stop_video', 922337203685.4775),
        ('play_video', 'lskdjflskdj'),
        ('pause_video', 'lskdjflskdj'),
        ('stop_video', 'lskdjflskdj'),
        ('play_video', None),
        ('pause_video', None),
        ('stop_video', None),
        ('play_video', ''),
        ('pause_video', ''),
        ('stop_video', ''),
        ('play_video', 'nan'),
        ('pause_video', 'nan'),
        ('stop_video', 'nan'),
        ('play_video', 'inf'),
        ('pause_video', 'inf'),
        ('stop_video', 'inf'),
        ('play_video', '-5'),
        ('pause_video', '-5'),
        ('stop_video', '-5'),
    )
    @unpack
    def test_invalid_time(self, template_name, time_value):
        payload = {
            "id": self.video_id,
            "currentTime": time_value,
            "code": 'html5'
        }
        event = json.dumps(payload)
        self.assert_no_map_output_for(self.create_event_log_line(template_name=template_name, event=event))

    @data(
        'play_video',
        'stop_video',
    )
    def test_missing_current_time(self, template_name):
        payload = {
            "id": self.video_id,
            # NO currentTime
            "code": 'html5'
        }
        event = json.dumps(payload)
        self.assert_no_map_output_for(self.create_event_log_line(template_name=template_name, event=event))

    def test_pause_missing_current_time(self):
        payload = {
            "id": self.video_id,
            # NO currentTime
            "code": 'html5'
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'pause_video', 0, None, None, -1)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='pause_video', event=event), self.default_key, expected_value)

    @data(
        'old_time',
        'new_time',
    )
    def test_seek_missing_current_time(self, field_name):
        payload = {
            "id": self.video_id,
            "old_time": 5,
            "new_time": 10,
            "code": 'html5'
        }
        del payload[field_name]
        event = json.dumps(payload)
        self.assert_no_map_output_for(self.create_event_log_line(template_name='seek_video', event=event))

    @data(
        ('old_time', 922337203685.4775),
        ('new_time', 922337203685.4775),
        ('old_time', 'lskdjflskdj'),
        ('new_time', 'lskdjflskdj'),
        ('old_time', None),
        ('new_time', None),
        ('old_time', ''),
        ('new_time', ''),
        ('old_time', 'nan'),
        ('new_time', 'nan'),
        ('old_time', 'inf'),
        ('new_time', 'inf'),
        ('old_time', '-5'),
        ('new_time', '-5'),
    )
    @unpack
    def test_seek_invalid_time(self, field_name, time_value):
        payload = {
            "id": self.video_id,
            "old_time": 5,
            "new_time": 10,
            "code": 'html5'
        }
        payload[field_name] = time_value
        event = json.dumps(payload)
        self.assert_no_map_output_for(self.create_event_log_line(template_name='seek_video', event=event))


class UserVideoViewingTaskLegacyMapTest(InitializeLegacyKeysMixin, UserVideoViewingTaskMapTest):
    """Test analysis of detailed video viewing analysis using legacy ID formats"""
    pass


class ViewingColumns(object):
    """Constants for columns in UserVideoViewingTask output."""

    USER_ID = 0
    COURSE_ID = 1
    VIDEO_MODULE_ID = 2
    VIDEO_DURATION = 3
    START_TIMESTAMP = 4
    START_OFFSET = 5
    END_OFFSET = 6
    REASON = 7


@ddt
class UserVideoViewingTaskReducerTest(ReducerTestMixin, TestCase):
    """Tests reducer for UserVideoViewingTask."""

    VIDEO_MODULE_ID = 'i4x-foo-bar-baz'

    task_class = UserVideoViewingTask

    def setUp(self):
        super(UserVideoViewingTaskReducerTest, self).setUp()
        self.user_id = 10
        self.reduce_key = (self.user_id, self.COURSE_ID, self.VIDEO_MODULE_ID)
        patcher = patch('edx.analytics.tasks.insights.video.urllib')
        self.mock_urllib = patcher.start()
        self.addCleanup(patcher.stop)

    def test_simple_viewing(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.USER_ID: self.user_id,
            ViewingColumns.COURSE_ID: self.COURSE_ID,
            ViewingColumns.VIDEO_MODULE_ID: self.VIDEO_MODULE_ID,
            ViewingColumns.VIDEO_DURATION: -1,
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
            ViewingColumns.REASON: 'pause_video'
        })

    def test_ordering(self):
        inputs = [
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None, -1),
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
            ViewingColumns.REASON: 'pause_video'
        })

    def test_viewing_with_sci_notation(self):  # REMOVE ME?
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', eval('1.2e+2'), None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.END_OFFSET: 120
        })

    def test_multiple_viewings(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None, -1),
            ('2013-12-17T00:00:07.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:12.00000Z', 'stop_video', 5, None, None, -1),
        ]
        self._check_output_by_key(inputs, [
            {
                ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
                ViewingColumns.START_OFFSET: 0,
                ViewingColumns.END_OFFSET: 3,
                ViewingColumns.REASON: 'pause_video'
            },
            {
                ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:07+00:00',
                ViewingColumns.START_OFFSET: 0,
                ViewingColumns.END_OFFSET: 5,
                ViewingColumns.REASON: 'stop_video'
            }
        ])

    def test_seek_backwards(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:03.00000Z', 'seek_video', 2, 3, None, -1),
            ('2013-12-17T00:00:03.10000Z', 'play_video', 2, None, 'html5', -1),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 5, None, None, -1),
        ]
        self._check_output_by_key(inputs, [
            {
                ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
                ViewingColumns.START_OFFSET: 0,
                ViewingColumns.END_OFFSET: 3,
                ViewingColumns.REASON: 'seek_video'
            },
            {
                ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:03.100000+00:00',
                ViewingColumns.START_OFFSET: 2,
                ViewingColumns.END_OFFSET: 5,
                ViewingColumns.REASON: 'pause_video'
            }
        ])

    def test_play_after_seek_forward_bug(self):
        # When a user seeks forward while the video is playing, it is common to see an incorrect value for currentTime
        # in the play event emitted after the seek. The expected behavior here is play->seek->play with the second
        # play event being emitted almost immediately after the seek. This second play event should record the
        # currentTime after the seek is complete, not the currentTime before the seek started, however it often
        # records the time before the seek started. We choose to trust the seek_video event in these cases and the time
        # it claims to have moved the position to.
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:03.00000Z', 'seek_video', 8, 3, None, -1),
            ('2013-12-17T00:00:03.10000Z', 'play_video', 3, None, 'html5', -1),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 11, None, None, -1),
        ]
        self._check_output_by_key(inputs, [
            {
                ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
                ViewingColumns.START_OFFSET: 0,
                ViewingColumns.END_OFFSET: 3,
                ViewingColumns.REASON: 'seek_video'
            },
            {
                ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:03.100000+00:00',
                ViewingColumns.START_OFFSET: 8,
                ViewingColumns.END_OFFSET: 11,
                ViewingColumns.REASON: 'pause_video'
            }
        ])

    def test_debounce_play(self):
        # we have had bugs before where many play events are emitted at the start of a video
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:00.10000Z', 'play_video', 1, None, 'html5', -1),
            ('2013-12-17T00:00:00.20000Z', 'play_video', 2, None, 'html5', -1),
            ('2013-12-17T00:00:00.30000Z', 'play_video', 3, None, 'html5', -1),
            ('2013-12-17T00:00:00.40000Z', 'play_video', 4, None, 'html5', -1),
            ('2013-12-17T00:00:00.50000Z', 'play_video', 5, None, 'html5', -1),
            ('2013-12-17T00:00:00.60000Z', 'play_video', 6, None, 'html5', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 9, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00.600000+00:00',
            ViewingColumns.START_OFFSET: 6,
            ViewingColumns.END_OFFSET: 9,
        })

    def test_ignored_events(self):
        inputs = [
            ('2013-12-17T00:00:01.00000Z', 'seek_video', 2, None, None, -1),
            ('2013-12-17T00:00:02.00000Z', 'pause_video', 1, None, None, -1),
            ('2013-12-17T00:00:03.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 3, None, None, -1),
            ('2013-12-17T00:00:07.00000Z', 'pause_video', 1, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:03+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
        })

    def test_first_event_seek(self):
        inputs = [
            ('2013-12-17T00:00:01.00000Z', 'seek_video', 2, 1, None, -1),
            ('2013-12-17T00:00:03.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 3, None, None, -1),
            ('2013-12-17T00:00:07.00000Z', 'pause_video', 1, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:03+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
        })

    def test_missing_end_event(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
        ]
        self.assert_no_output(inputs)

    def test_unexpected_event(self):
        inputs = [
            ('2013-12-17T00:00:03.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:05.00000Z', '/foobar', None, None, None, -1),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 3, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:03+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
        })

    def test_very_short_viewing(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5', -1),
            ('2013-12-17T00:00:00.10000Z', 'pause_video', 0.1, None, None, -1),
        ]
        self.assert_no_output(inputs)

    def test_inverted_viewing(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 3, None, 'html5', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 0, None, None, -1),
        ]
        self.assert_no_output(inputs)

    def test_get_duration(self):
        self.prepare_youtube_api_mock('PT1M2S')
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
            ViewingColumns.VIDEO_DURATION: 62,
            ViewingColumns.REASON: 'pause_video'
        })

    def prepare_youtube_api_mock(self, duration):
        """Mock calls to Google API to return a specific video duration value."""
        fake_buffer = """{
 "kind": "youtube#videoListResponse",
 "etag": "tbWC5XrSXxe1WOAx6MK9z4hHSU8/U18cvGr7ajKhffqbJnnrvHvXOOc",
 "pageInfo": {
  "totalResults": 1,
  "resultsPerPage": 1
 },
 "items": [
  {
   "kind": "youtube#video",
   "etag": "tbWC5XrSXxe1WOAx6MK9z4hHSU8/Uvha7bYP7IquCqUCznX-iZDBTfI",
   "id": "9bZkp7q19f0",
   "contentDetails": {
    "duration": "%s",
    "dimension": "2d",
    "definition": "hd",
    "caption": "false",
    "licensedContent": true
   }
  }
 ]
}
"""
        return self.prepare_youtube_api_mock_raw(fake_buffer % duration)

    def prepare_youtube_api_mock_raw(self, response_string):
        """Mock calls to Google API for video duration information."""
        self.task.api_key = 'foobar'
        mock_response = MagicMock(spec=file)
        mock_response.code = 200
        mock_response.read.side_effect = [response_string, '']
        self.mock_urllib.urlopen.return_value = mock_response

    def test_pause_after_end_of_video(self):
        self.prepare_youtube_api_mock('PT1M2S')
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 66, None, None, -1),
        ]
        self.assert_no_output(inputs)

    def test_pause_near_end_of_video(self):
        # we occasionally see pause events slightly after the end of the video
        self.prepare_youtube_api_mock('PT1M2S')
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 62.9, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 62.9,
            ViewingColumns.VIDEO_DURATION: 62,
            ViewingColumns.REASON: 'pause_video'
        })

    @data(
        '{lksjdf',
        '{}',
        '{"items": []}',
    )
    def test_invalid_json_youtube_api(self, response_string):
        self.prepare_youtube_api_mock_raw(response_string)
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
            ViewingColumns.VIDEO_DURATION: VIDEO_UNKNOWN_DURATION,
            ViewingColumns.REASON: 'pause_video'
        })

    def test_invalid_duration_format(self):
        self.prepare_youtube_api_mock('foobar')
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
            ViewingColumns.VIDEO_DURATION: VIDEO_UNKNOWN_DURATION,
            ViewingColumns.REASON: 'pause_video'
        })

    @data(
        ('PT2H30M1S', 9001),
        ('PT2M30S', 150),
        ('PT4S', 4),
        ('PT130S', 130)
    )
    @unpack
    def test_different_durations(self, duration_string, duration_secs):
        self.prepare_youtube_api_mock(duration_string)
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0', -1),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None, -1),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            ViewingColumns.START_OFFSET: 0,
            ViewingColumns.END_OFFSET: 3,
            ViewingColumns.VIDEO_DURATION: duration_secs,
            ViewingColumns.REASON: 'pause_video'
        })

    @data(
        ('3_yD_cEKoCk', 144),
        ('html5', 150),
        ('hls', 400)
    )
    @unpack
    def test_duration_in_event(self, video_type, duration_seconds):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, video_type, duration_seconds),
            ('2013-12-17T00:00:10.00000Z', 'pause_video', 10, None, video_type, duration_seconds),
        ]
        self._check_output_by_key(inputs, {
            ViewingColumns.VIDEO_DURATION: duration_seconds,
            ViewingColumns.REASON: 'pause_video'
        })


class VideoUsageTaskMapTest(MapperTestMixin, TestCase):
    """Test video usage mapper"""

    task_class = VideoUsageTask

    def test_simple(self):
        self.assert_single_map_output(
            '\t'.join(
                [
                    '1',
                    'foo/bar/baz',
                    'i4x-foo-bar',
                    '63',
                    '2015-04-17T00:00:00+00:00',
                    '10',
                    '15',
                    'pause_video'
                ]
            ),
            ('foo/bar/baz', 'i4x-foo-bar'),
            ('1', '10', '15', '63')
        )


class UsageColumns(object):
    """Constants for columns in VideoUsageTask output."""
    PIPELINE_VIDEO_ID = 0
    COURSE_ID = 1
    VIDEO_MODULE_ID = 2
    VIDEO_DURATION = 3
    SECONDS_PER_SEGMENT = 4
    USERS_AT_START = 5
    USERS_AT_END = 6
    SEGMENT = 7
    USERS_VIEWED = 8
    NUM_VIEWS = 9


@ddt
class VideoUsageTaskReducerTest(ReducerTestMixin, TestCase):
    """Test VideoUsageTask reducer."""

    VIDEO_MODULE_ID = 'i4x-foo-bar-baz'
    output_record_type = VideoSegmentDetailRecord

    task_class = VideoUsageTask

    def setUp(self):
        super(VideoUsageTaskReducerTest, self).setUp()
        self.reduce_key = (self.COURSE_ID, self.VIDEO_MODULE_ID)

    def test_single_viewing(self):
        inputs = [
            (1, 0, 4.99, VIDEO_UNKNOWN_DURATION),
        ]
        self._check_output_by_record_field(inputs, {
            "pipeline_video_id": self.COURSE_ID + '|' + self.VIDEO_MODULE_ID,
            "course_id": self.COURSE_ID,
            "encoded_module_id": self.VIDEO_MODULE_ID,
            "duration": "4",
            "segment_length": str(VIDEO_VIEWING_SECONDS_PER_SEGMENT),
            "users_at_start": "1",
            "users_at_end": "1",
            "segment": "0",
            "num_users": "1",
            "num_views": "1"
        })

    def test_adjacent_viewings(self):
        inputs = [
            (1, 0, 4.99, VIDEO_UNKNOWN_DURATION),
            (1, 5, 5.2, VIDEO_UNKNOWN_DURATION),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "segment": "0",
                "num_users": "1",
                "num_views": "1",
            },
            {
                "segment": "1",
                "num_users": "1",
                "num_views": "1",
            },
        ])

    def test_play_pause_play(self):
        # This may be unexpected, the user watched the entire segment from 0-4.9 seconds, however, they paused the video
        # at 4.6 seconds. This caused us to count 2 viewings in this bucket, since we treat the viewings as separate.
        # One could perform more sophisticated analysis to determine, if, in fact, this was a contiguous viewing or an
        # interrupted one.
        inputs = [
            (1, 0, 4.6, VIDEO_UNKNOWN_DURATION),
            (1, 4.6, 4.9, VIDEO_UNKNOWN_DURATION),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "segment": "0",
                "num_users": "1",
                "num_views": "2",
            }
        ])

    def test_overlapping_viewings(self):
        inputs = [
            (1, 0, 4.99, VIDEO_UNKNOWN_DURATION),
            (1, 4.8, 5.2, VIDEO_UNKNOWN_DURATION),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "segment": "0",
                "num_users": "1",
                "num_views": "2",
            },
            {
                "segment": "1",
                "num_users": "1",
                "num_views": "1",
            },
        ])

    def test_multi_segment_viewing(self):
        inputs = [
            (1, 0, 10.2, VIDEO_UNKNOWN_DURATION),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "segment": "0",
                "num_users": "1",
                "num_views": "1",
            },
            {
                "segment": "1",
                "num_users": "1",
                "num_views": "1",
            },
            {
                "segment": "2",
                "num_users": "1",
                "num_views": "1",
            },
        ])

    def test_overlapping_viewings_different_users(self):
        inputs = [
            (1, 0, 4.99, VIDEO_UNKNOWN_DURATION),
            (2, 4.8, 5.2, VIDEO_UNKNOWN_DURATION),
            (2, 4.2, 10.2, VIDEO_UNKNOWN_DURATION),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "segment": "0",
                "num_users": "2",
                "num_views": "3",
            },
            {
                "segment": "1",
                "num_users": "1",
                "num_views": "2",
            },
            {
                "segment": "2",
                "num_users": "1",
                "num_views": "1",
            },
        ])

    def test_view_counts_without_duration(self):
        inputs = [
            # These three viewings are in the first segment
            (1, 0, 1, VIDEO_UNKNOWN_DURATION),
            (2, 1.5, 2, VIDEO_UNKNOWN_DURATION),
            (1, 4, 4.99, VIDEO_UNKNOWN_DURATION),

            # These viewings are in neither the first nor the last segment
            (1, 6, 9.99, VIDEO_UNKNOWN_DURATION),
            (2, 6, 9.99, VIDEO_UNKNOWN_DURATION),

            # These viewings are in the last segment observed
            (1, 10.5, 11, VIDEO_UNKNOWN_DURATION),
            (2, 10.7, 11, VIDEO_UNKNOWN_DURATION),
        ]

        # Note that the start and end counts are denormalized into all results, so they should have an
        # identical value in every record. Also note that the middle records are ignored here.
        self._check_output_by_record_field(inputs, [
            {
                "duration": "14",
                "users_at_start": "2",
                "users_at_end": "2",
                "segment": "0",
                "num_users": "2",
                "num_views": "3",
            },
            {
                "duration": "14",
                "users_at_start": "2",
                "users_at_end": "2",
                "segment": "1",
                "num_users": "2",
                "num_views": "2",
            },
            {
                "duration": "14",
                "users_at_start": "2",
                "users_at_end": "2",
                "segment": "2",
                "num_users": "2",
                "num_views": "2",
            },
        ])

    def test_more_users_at_end_than_at_start(self):
        inputs = [
            # These three viewings are in the first segment
            (1, 0, 1, VIDEO_UNKNOWN_DURATION),
            (2, 1.5, 2, VIDEO_UNKNOWN_DURATION),
            (2, 2.5, 4, VIDEO_UNKNOWN_DURATION),

            # These viewings are in the last segment observed
            (1, 5, 6, VIDEO_UNKNOWN_DURATION),
            (2, 5, 6, VIDEO_UNKNOWN_DURATION),
            (2, 8, 9, VIDEO_UNKNOWN_DURATION),
            (3, 7, 9, VIDEO_UNKNOWN_DURATION),
        ]

        # Note that the start and end counts are denormalized into all results, so they should have an
        # identical value in every record.
        self._check_output_by_record_field(inputs, [
            {
                "users_at_start": "2",
                "users_at_end": "3",
                "segment": "0",
                "num_users": "2",
                "num_views": "3",
            },
            {
                "users_at_start": "2",
                "users_at_end": "3",
                "segment": "1",
                "num_users": "3",
                "num_views": "4",
            },
        ])

    def test_sparsity_of_output(self):
        inputs = [
            # These three viewings are in the first segment
            (1, 5, 9, 40),
            (1, 16, 19, 40),
        ]

        self._check_output_by_record_field(inputs, [
            # Note that segment 0 is omitted since we didn't see any activity there
            {
                "segment": "1",
                "num_users": "1",
                "num_views": "1",
            },
            # Note that segment 2 is omitted since we didn't see any activity there
            {
                "segment": "3",
                "num_users": "1",
                "num_views": "1",
            },
            # Note all segments up to the end of the video are omitted, again due to a lack of activity
        ])

    def test_multiple_known_durations(self):
        inputs = [
            (1, 0, 1, 10),
            (1, 0, 1, 50),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "duration": "50",
                "users_at_end": "0",
            }
        ])

    def test_unknown_duration(self):
        # Duration is always set now.
        inputs = [
            (1, 0, 4, VIDEO_UNKNOWN_DURATION),
            (1, 5, 9, VIDEO_UNKNOWN_DURATION),
            (1, 12, 16, VIDEO_UNKNOWN_DURATION),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "duration": "19",
                "users_at_end": "1",
            },
            {
                "duration": "19",
                "users_at_end": "1",
            },
            {
                "duration": "19",
                "users_at_end": "1",
            },
            {
                "duration": "19",
                "users_at_end": "1",
            }
        ])

    def test_end_view_with_duration(self):
        inputs = [
            (1, 6, 8, 9.2),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "duration": "9",
                "users_at_end": "1",
            }
        ])

    def test_num_users_at_end(self):
        inputs = [
            (1, 0, 4, VIDEO_UNKNOWN_DURATION),
            (2, 0, 4, VIDEO_UNKNOWN_DURATION),
            (3, 0, 4, VIDEO_UNKNOWN_DURATION),

            (1, 91, 98, VIDEO_UNKNOWN_DURATION),
            (2, 92, 95, VIDEO_UNKNOWN_DURATION),
            (3, 90, 99, VIDEO_UNKNOWN_DURATION),

            (4, 100, 103, VIDEO_UNKNOWN_DURATION),
        ]
        self._check_output_by_record_field(inputs, [
            {
                "segment": "0",
                "duration": "104",
                "users_at_start": "3",
                "users_at_end": "3",
                "num_views": "3",
            },
            {
                "segment": "18",
                "duration": "104",
                "users_at_start": "3",
                "users_at_end": "3",
                "num_views": "3",
            },
            {
                "segment": "19",
                "duration": "104",
                "users_at_start": "3",
                "users_at_end": "3",
                "num_views": "3",
            },
            # Even though segment 20 is viewed by one user, users_at_end would still be 3 as we consider
            # end time to be before the very end
            {
                "segment": "20",
                "duration": "104",
                "users_at_start": "3",
                "users_at_end": "3",
                "num_views": "1",
            },
        ])


@ddt
class GetFinalSegmentTest(TestCase):
    """Test video task's get_final_segment calculation."""

    def setUp(self):
        self.task = VideoUsageTask(interval=sentinel.ignored, output_root=sentinel.ignored)
        self.usage_map = self.generate_usage_map(20)

    def generate_segment(self, num_users, num_views):
        """Constructs an entry for a video segment."""
        return {
            'users': ['user{}'.format(i) for i in range(num_users)],
            'views': num_views
        }

    def generate_usage_map(self, num_segments):
        """Constructs a default usage_map, with constant users and views."""
        usage_map = {}
        for i in range(num_segments):
            usage_map[i] = self.generate_segment(20, 20)
        return usage_map

    @data(
        (15, 25, 14),
        (20, 30, 19),
    )
    @unpack
    def test_with_sudden_dropoff(self, start, end, result):
        """Sharp drop-off in num_users(20 to 1), triggers the cutoff"""
        for i in range(start, end):
            self.usage_map[i] = self.generate_segment(1, 1)
        self.assertEqual(self.task.get_final_segment(self.usage_map), result)

    def test_without_dropoff(self):
        """No dropoff"""
        self.assertEqual(self.task.get_final_segment(self.usage_map), 19)

    def test_with_gradual_dropoff(self):
        """Gradual dropoff of num_users, does not trigger the cutoff"""
        for i, j in zip(range(20, 39), range(19, 0, -1)):
            self.usage_map[i] = self.generate_segment(j, j)
        self.assertEqual(self.task.get_final_segment(self.usage_map), 38)

    def test_with_dropoff_no_cutoff(self):
        """Drop-off from 20 to 2 num_users, however, cutoff won't trigger as it does not meet the threshold."""
        self.usage_map[30] = self.generate_segment(2, 2)
        self.assertEqual(self.task.get_final_segment(self.usage_map), 30)


@ddt
class CompleteEndSegmentTest(TestCase):
    """Test video task's complete_end_segment calculation."""

    def setUp(self):
        self.task = VideoUsageTask(interval=sentinel.ignored, output_root=sentinel.ignored)

    @data(
        (10, 1),     # segment calculated from duration * 0.95
        (100, 19),   # segment calculated from duration * 0.95
        (650, 124),  # segment calculated from duration - 30
    )
    @unpack
    def test_complete_end_segment(self, duration, expected_segment):
        self.assertEqual(self.task.complete_end_segment(duration), expected_segment)
