"""Test student engagement metrics"""

import json

from mock import patch, MagicMock
from ddt import ddt, data

from edx.analytics.tasks.tests.config import with_luigi_config
from edx.analytics.tasks.video import UserVideoViewingTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin


@ddt
class UserVideoViewingTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, unittest.TestCase):
    """Base class for test analysis of detailed student engagement"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"
    UTF8_BYTE_STRING = 'I\xd4\x89\xef\xbd\x94\xc3\xa9\xef\xbd\x92\xd0\xbb\xc3\xa3'

    task_class = UserVideoViewingTask

    def setUp(self):
        super(UserVideoViewingTaskMapTest, self).setUp()

        self.initialize_ids()
        self.video_id = 'i4x-foo-bar-baz'
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
                "event": '{"id": "%s", "currentTime": 23.4398, "code": "87389iouhdfh"}' % self.video_id,
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
                "event": '{"id": "%s", "currentTime": 28, "code": "87389iouhdfh"}' % self.video_id,
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
                "event": '{"id": "%s", "currentTime": 100, "code": "87389iouhdfh"}' % self.video_id,
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
                "event": '{"id": "%s", "old_time": 14, "new_time": 10, "code": "87389iouhdfh"}' % self.video_id,
                "agent": "blah, blah, blah",
                "page": None
            }
        }
        self.default_event_template = 'play_video'
        self.default_key = ("test_user", self.course_id.encode('utf8'), self.video_id.encode('utf8'))

    @data(
        {'time': "2013-12-01T15:38:32.805444"},
        {'time': None},
        {'username': ''},
        {'username': None},
        {'event_type': None}
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

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
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 23.4398, None, '87389iouhdfh')
        self.assert_single_map_output(self.create_event_log_line(), self.default_key, expected_value)

    @data('html5', 'mobile')
    def test_play_video_non_youtube(self, code):
        payload = {
            "id": self.video_id,
            "currentTime": 5,
            "code": code
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 5, None, None)
        self.assert_single_map_output(self.create_event_log_line(event=event), self.default_key, expected_value)

    def test_play_video_without_current_time(self):
        payload = {
            "id": self.video_id,
            "code": "87389iouhdfh"
        }
        event = json.dumps(payload)
        self.assert_no_map_output_for(self.create_event_log_line(event=event))

    def test_pause_video(self):
        expected_value = (self.DEFAULT_TIMESTAMP, 'pause_video', 28, None, None)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='pause_video'), self.default_key, expected_value)

    def test_pause_video_without_time(self):
        payload = {
            "id": self.video_id,
            "code": "foo"
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'pause_video', 0, None, None)
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

    @data(
        'old_time', 'new_time'
    )
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
        expected_value = (self.DEFAULT_TIMESTAMP, 'seek_video', 10, 14, None)
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
        expected_value = (self.DEFAULT_TIMESTAMP, 'seek_video', 10, 5, None)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='seek_video', event=event), self.default_key, expected_value)

    def test_stop_video(self):
        expected_value = (self.DEFAULT_TIMESTAMP, 'stop_video', 100, None, None)
        self.assert_single_map_output(
            self.create_event_log_line(template_name='stop_video'), self.default_key, expected_value)

    def test_unicode_username(self):
        key = (self.UTF8_BYTE_STRING, self.course_id.encode('utf8'), self.video_id.encode('utf8'))
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 23.4398, None, '87389iouhdfh')
        self.assert_single_map_output(self.create_event_log_line(username=self.UTF8_BYTE_STRING), key, expected_value)

    def test_unicode_module_id(self):
        key = ("test_user", self.course_id.encode('utf8'), self.UTF8_BYTE_STRING)
        payload = {
            "id": self.UTF8_BYTE_STRING,
            "currentTime": 5,
            "code": 'foo'
        }
        event = json.dumps(payload)
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 5, None, 'foo')
        self.assert_single_map_output(self.create_event_log_line(event=event), key, expected_value)

    def test_unicode_course_id(self):
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
        expected_value = (self.DEFAULT_TIMESTAMP, 'play_video', 5, None, self.UTF8_BYTE_STRING)
        self.assert_single_map_output(self.create_event_log_line(event=event), self.default_key, expected_value)


class UserVideoViewingTaskLegacyMapTest(InitializeLegacyKeysMixin, UserVideoViewingTaskMapTest):
    """Test analysis of detailed video viewing analysis using legacy ID formats"""
    pass


class Columns(object):

    USERNAME = 0
    COURSE_ID = 1
    VIDEO_MODULE_ID = 2
    VIDEO_DURATION = 3
    START_TIMESTAMP = 4
    START_OFFSET = 5
    END_OFFSET = 6
    REASON = 7

class UserVideoViewingTaskReducerTest(ReducerTestMixin, unittest.TestCase):

    VIDEO_MODULE_ID = 'i4x-foo-bar-baz'

    task_class = UserVideoViewingTask

    def setUp(self):
        super(UserVideoViewingTaskReducerTest, self).setUp()
        self.reduce_key = (self.USERNAME, self.COURSE_ID, self.VIDEO_MODULE_ID)
        patcher = patch('edx.analytics.tasks.video.urllib')
        self.mock_urllib = patcher.start()
        self.addCleanup(patcher.stop)

    def test_simple_viewing(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None),
        ]
        self._check_output(inputs, {
            Columns.USERNAME: self.USERNAME,
            Columns.COURSE_ID: self.COURSE_ID,
            Columns.VIDEO_MODULE_ID: self.VIDEO_MODULE_ID,
            Columns.VIDEO_DURATION: -1,
            Columns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            Columns.START_OFFSET: 0,
            Columns.END_OFFSET: 3,
            Columns.REASON: 'pause_video'
        })

    def test_ordering(self):
        inputs = [
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None),
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
        ]
        self._check_output(inputs, {
            Columns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            Columns.START_OFFSET: 0,
            Columns.END_OFFSET: 3,
            Columns.REASON: 'pause_video'
        })

    def test_viewing_with_sci_notation(self):  # REMOVE ME?
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', eval('1.2e+2'), None, None),
        ]
        self._check_output(inputs, {
            Columns.END_OFFSET: 120
        })

    def test_multiple_viewings(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None),
            ('2013-12-17T00:00:07.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:12.00000Z', 'stop_video', 5, None, None),
        ]
        self._check_output(inputs, [
            {
                Columns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
                Columns.START_OFFSET: 0,
                Columns.END_OFFSET: 3,
                Columns.REASON: 'pause_video'
            },
            {
                Columns.START_TIMESTAMP: '2013-12-17T00:00:07+00:00',
                Columns.START_OFFSET: 0,
                Columns.END_OFFSET: 5,
                Columns.REASON: 'stop_video'
            }
        ])

    def test_seek_backwards(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:03.00000Z', 'seek_video', 2, 3, None),
            ('2013-12-17T00:00:03.10000Z', 'play_video', 2, None, 'html5'),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 5, None, None),
        ]
        self._check_output(inputs, [
            {
                Columns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
                Columns.START_OFFSET: 0,
                Columns.END_OFFSET: 3,
                Columns.REASON: 'seek_video'
            },
            {
                Columns.START_TIMESTAMP: '2013-12-17T00:00:03.100000+00:00',
                Columns.START_OFFSET: 2,
                Columns.END_OFFSET: 5,
                Columns.REASON: 'pause_video'
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
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:03.00000Z', 'seek_video', 8, 3, None),
            ('2013-12-17T00:00:03.10000Z', 'play_video', 3, None, 'html5'),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 11, None, None),
        ]
        self._check_output(inputs, [
            {
                Columns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
                Columns.START_OFFSET: 0,
                Columns.END_OFFSET: 3,
                Columns.REASON: 'seek_video'
            },
            {
                Columns.START_TIMESTAMP: '2013-12-17T00:00:03.100000+00:00',
                Columns.START_OFFSET: 8,
                Columns.END_OFFSET: 11,
                Columns.REASON: 'pause_video'
            }
        ])

    def test_debounce_play(self):
        # we have had bugs before where many play events are emitted at the start of a video
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:00.10000Z', 'play_video', 1, None, 'html5'),
            ('2013-12-17T00:00:00.20000Z', 'play_video', 2, None, 'html5'),
            ('2013-12-17T00:00:00.30000Z', 'play_video', 3, None, 'html5'),
            ('2013-12-17T00:00:00.40000Z', 'play_video', 4, None, 'html5'),
            ('2013-12-17T00:00:00.50000Z', 'play_video', 5, None, 'html5'),
            ('2013-12-17T00:00:00.60000Z', 'play_video', 6, None, 'html5'),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 9, None, None),
        ]
        self._check_output(inputs, {
            Columns.START_TIMESTAMP: '2013-12-17T00:00:00.600000+00:00',
            Columns.START_OFFSET: 6,
            Columns.END_OFFSET: 9,
        })

    def test_ignored_events(self):
        inputs = [
            ('2013-12-17T00:00:01.00000Z', 'seek_video', 2, None, None),
            ('2013-12-17T00:00:02.00000Z', 'pause_video', 1, None, None),
            ('2013-12-17T00:00:03.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 3, None, None),
            ('2013-12-17T00:00:07.00000Z', 'pause_video', 1, None, None),
        ]
        self._check_output(inputs, {
            Columns.START_TIMESTAMP: '2013-12-17T00:00:03+00:00',
            Columns.START_OFFSET: 0,
            Columns.END_OFFSET: 3,
        })

    def test_first_event_seek(self):
        inputs = [
            ('2013-12-17T00:00:01.00000Z', 'seek_video', 2, 1, None),
            ('2013-12-17T00:00:03.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 3, None, None),
            ('2013-12-17T00:00:07.00000Z', 'pause_video', 1, None, None),
        ]
        self._check_output(inputs, {
            Columns.START_TIMESTAMP: '2013-12-17T00:00:03+00:00',
            Columns.START_OFFSET: 0,
            Columns.END_OFFSET: 3,
        })

    def test_missing_end_event(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
        ]
        self.assert_no_output(inputs)

    def test_unexpected_event(self):
        inputs = [
            ('2013-12-17T00:00:03.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:05.00000Z', '/foobar', None, None, None),
            ('2013-12-17T00:00:06.00000Z', 'pause_video', 3, None, None),
        ]
        self._check_output(inputs, {
            Columns.START_TIMESTAMP: '2013-12-17T00:00:03+00:00',
            Columns.START_OFFSET: 0,
            Columns.END_OFFSET: 3,
        })

    def test_very_short_viewing(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, 'html5'),
            ('2013-12-17T00:00:00.10000Z', 'pause_video', 0.1, None, None),
        ]
        self.assert_no_output(inputs)

    def test_inverted_viewing(self):
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 3, None, 'html5'),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 0, None, None),
        ]
        self.assert_no_output(inputs)

    def test_get_duration(self):
        self.task.api_key = 'foobar'
        self.prepare_youtube_api_mock('PT1M2S')
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0'),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 3, None, None),
        ]
        self._check_output(inputs, {
            Columns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            Columns.START_OFFSET: 0,
            Columns.END_OFFSET: 3,
            Columns.VIDEO_DURATION: 62,
            Columns.REASON: 'pause_video'
        })

    def prepare_youtube_api_mock(self, duration):
        mock_response = MagicMock(spec=file)
        mock_response.code = 200
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
        mock_response.read.side_effect = [fake_buffer % duration, '']
        self.mock_urllib.urlopen.return_value = mock_response

    def test_pause_after_end_of_video(self):
        self.task.api_key = 'foobar'
        self.prepare_youtube_api_mock('PT1M2S')
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0'),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 66, None, None),
        ]
        self.assert_no_output(inputs)

    def test_pause_near_end_of_video(self):
        # we occasionally see pause events slightly after the end of the video
        self.task.api_key = 'foobar'
        self.prepare_youtube_api_mock('PT1M2S')
        inputs = [
            ('2013-12-17T00:00:00.00000Z', 'play_video', 0, None, '9bZkp7q19f0'),
            ('2013-12-17T00:00:03.00000Z', 'pause_video', 62.9, None, None),
        ]
        self._check_output(inputs, {
            Columns.START_TIMESTAMP: '2013-12-17T00:00:00+00:00',
            Columns.START_OFFSET: 0,
            Columns.END_OFFSET: 62.9,
            Columns.VIDEO_DURATION: 62,
            Columns.REASON: 'pause_video'
        })