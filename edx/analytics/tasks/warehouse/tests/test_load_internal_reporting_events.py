"""Test processing of events for loading into Hive, etc."""

from __future__ import absolute_import

import datetime
import json
import unittest

import ciso8601
import luigi
from ddt import data, ddt, unpack

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.obfuscate_util import backslash_encode_value
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeOpaqueKeysMixin
from edx.analytics.tasks.warehouse.load_internal_reporting_events import (
    VERSION, EventRecord, JsonEventRecord, PerDateSegmentEventRecordDataTask, PerDateTrackingEventRecordDataTask,
    SegmentEventRecordDataTask, TrackingEventRecordDataTask
)


class BaseTrackingEventRecordTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin):
    """Base class for test analysis of tracking log events."""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"
    EVENT_RECORD_TYPE = "unknown"

    def setUp(self):
        super(BaseTrackingEventRecordTaskMapTest, self).setUp()

        self.initialize_ids()
        self.video_id = 'i4x-foo-bar-baz'
        self.forum_id = 'a2cb123f9c2146f3211cdc6901acb00e'
        self.event_templates = {
            'problem_check': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "problem_check",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": {
                    "problem_id": self.problem_id,
                    "success": "incorrect",
                },
                "agent": "blah, blah, blah",
                "page": None
            },
            'play_video': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "browser",
                "name": "play_video",
                "event_type": "play_video",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                    "path": "/event",
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",

                "accept_language": "en-us",
                "event": "{\"code\": \"6FrbD6Ro5z8\", \"id\": \"01955efc9ba54c73a9aa7453a440cb06\", \"currentTime\": 630.437320479}",
                "agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9",
                "label": "course-v1:edX+DemoX+Demo_Course",
                "session": "83ce3bd69f7fc3b72b3b9f2142a8cd09",
                "referer": "long meaningful url",
                "page": "long meaningful url",
                "nonInteraction": 1,
            },
        }
        self.default_event_template = 'problem_check'
        self.create_task()

    def create_task(self, date=None):  # pylint: disable=arguments-differ
        """Allow arguments to be passed to the task constructor."""
        if not date:
            date = self.DEFAULT_DATE
        self.task = TrackingEventRecordDataTask(
            interval=luigi.DateIntervalParameter().parse(date),
            output_root='/fake/output',
            event_record_type=self.EVENT_RECORD_TYPE,
        )
        self.task.init_local()


@ddt
class TrackingEventRecordTaskMapTest(BaseTrackingEventRecordTaskMapTest, unittest.TestCase):
    """Test class for emission of tracking log events in EventRecord format."""

    EVENT_RECORD_TYPE = "EventRecord"

    @data(
        {'time': "2013-12-01T15:38:32.805444"},
        {'event_type': None},
        {'event': 'sdfasdf'}
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    def test_problem_check(self):
        template = self.event_templates['problem_check']
        event = self.create_event_log_line(template=template)
        expected_key = (self.DEFAULT_DATE, self.task.PROJECT_NAME)
        expected_dict = {
            'version': VERSION,
            'input_file': '',
            'project': self.task.PROJECT_NAME,
            'event_type': 'problem_check',
            'event_source': 'server',
            'event_category': 'unknown',
            'timestamp': '2013-12-17T15:38:32.805444+00:00',
            'received_at': '2013-12-17T15:38:32.805444+00:00',
            'date': self.DEFAULT_DATE,
            'host': 'test_host',
            'ip': '127.0.0.1',
            'username': 'test_user',
            'context_course_id': self.encoded_course_id,
            'context_org_id': self.encoded_org_id,
            'context_user_id': '10',
            'problem_id': self.encoded_problem_id,
            'success': 'incorrect',
            'agent': 'blah, blah, blah',
        }
        expected_value = EventRecord(**expected_dict).to_separated_values()
        self.assert_single_map_output(event, expected_key, expected_value)

    def test_play_video(self):
        template = self.event_templates['play_video']
        event = self.create_event_log_line(template=template)
        expected_key = (self.DEFAULT_DATE, self.task.PROJECT_NAME)
        expected_dict = {
            'version': VERSION,
            'input_file': '',
            'project': self.task.PROJECT_NAME,
            'event_type': 'play_video',
            'event_source': 'browser',
            'event_category': 'unknown',
            'timestamp': '2013-12-17T15:38:32.805444+00:00',
            'received_at': '2013-12-17T15:38:32.805444+00:00',
            'date': self.DEFAULT_DATE,
            'host': 'test_host',
            'ip': '127.0.0.1',
            'username': 'test_user',
            'context_course_id': self.encoded_course_id,
            'context_org_id': self.encoded_org_id,
            'context_user_id': '10',
            'context_path': '/event',
            'page': 'long meaningful url',
            'referer': 'long meaningful url',
            'accept_language': 'en-us',
            'agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/600.8.9 (KHTML, like Gecko) Version/8.0.8 Safari/600.8.9",
            'agent_type': 'desktop',
            'agent_device_name': 'Other',
            'agent_os': 'Mac OS X',
            'agent_browser': 'Safari',
            'agent_touch_capable': 'False',
            'session': '83ce3bd69f7fc3b72b3b9f2142a8cd09',
            'code': '6FrbD6Ro5z8',
            'id': '01955efc9ba54c73a9aa7453a440cb06',
            'currenttime': '630.437320479',
        }
        expected_value = EventRecord(**expected_dict).to_separated_values()
        self.assert_single_map_output(event, expected_key, expected_value)


@ddt
class TrackingJsonEventRecordTaskMapTest(BaseTrackingEventRecordTaskMapTest, unittest.TestCase):
    """Test class for emission of tracking log events in JsonEventRecord format."""

    EVENT_RECORD_TYPE = "JsonEventRecord"

    def get_raw_event(self, event_line):
        event = eventlog.parse_json_event(event_line)
        event_data = eventlog.get_event_data(event)
        if event_data is not None:
            event['event'] = event_data
        dump = json.dumps(event, sort_keys=True)
        encoded_dump = backslash_encode_value(dump)
        return encoded_dump

    def _get_event_record_from_mapper(self, kwargs):
        """Returns an EventRecord constructed from mapper output."""
        line = self.create_event_log_line(**kwargs)
        mapper_output = tuple(self.task.mapper(line))
        self.assertEquals(len(mapper_output), 1)
        row = mapper_output[0]
        self.assertEquals(len(row), 2)
        _actual_key, actual_value = row
        return JsonEventRecord.from_tsv(actual_value)

    @data(
        {'time': "2013-12-01T15:38:32.805444"},  # a good time, but lies outside the interval
        {'event_type': None},
        {'event': 'sdfasdf'},
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    @data(
        {'event_type': '/implicit/event/url'},
    )
    def test_implicit_events(self, kwargs):
        event = self.create_event_log_line(**kwargs)
        actual_record = self._get_event_record_from_mapper(kwargs)
        url = getattr(actual_record, 'url')
        self.assertEquals(url, kwargs['event_type'])
        self.assertEquals(getattr(actual_record, 'event_type'), 'edx.server.request')

    def test_problem_check(self):
        template = self.event_templates['problem_check']
        event = self.create_event_log_line(template=template)
        expected_key = (self.DEFAULT_DATE, self.task.PROJECT_NAME)
        expected_dict = {
            'input_file': '',
            'source': self.task.PROJECT_NAME,
            'event_type': 'problem_check',
            'emitter_type': 'server',
            'timestamp': ciso8601.parse_datetime('2013-12-17T15:38:32.805444+00:00'),
            'received_at': ciso8601.parse_datetime('2013-12-17T15:38:32.805444+00:00'),
            'date': datetime.date(*[int(x) for x in self.DEFAULT_DATE.split('-')]),
            'username': 'test_user',
            'course_id': self.encoded_course_id,
            'org_id': self.encoded_org_id,
            'user_id': '10',
            'raw_event': self.get_raw_event(event),
        }
        expected_value = JsonEventRecord(**expected_dict).to_separated_values()
        self.assert_single_map_output(event, expected_key, expected_value)

    def test_play_video(self):
        template = self.event_templates['play_video']
        event = self.create_event_log_line(template=template)
        expected_key = (self.DEFAULT_DATE, self.task.PROJECT_NAME)
        expected_dict = {
            'input_file': '',
            'source': self.task.PROJECT_NAME,
            'event_type': 'play_video',
            'emitter_type': 'browser',
            'timestamp': ciso8601.parse_datetime('2013-12-17T15:38:32.805444+00:00'),
            'received_at': ciso8601.parse_datetime('2013-12-17T15:38:32.805444+00:00'),
            'date': datetime.date(*[int(x) for x in self.DEFAULT_DATE.split('-')]),
            'username': 'test_user',
            'course_id': self.encoded_course_id,
            'org_id': self.encoded_org_id,
            'user_id': '10',
            'referrer': 'long meaningful url',
            'agent_type': 'desktop',
            'agent_device_name': 'Other',
            'agent_os': 'Mac OS X',
            'agent_browser': 'Safari',
            'agent_touch_capable': False,
            'raw_event': self.get_raw_event(event),
        }
        expected_value = JsonEventRecord(**expected_dict).to_separated_values()
        self.assert_single_map_output(event, expected_key, expected_value)


class BaseSegmentEventRecordTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, unittest.TestCase):
    """Base class for test analysis of segment events."""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32"
    DEFAULT_DATE = "2013-12-17"
    DEFAULT_ANONYMOUS_ID = "abcdef12-3456-789a-bcde-f0123456789a"
    DEFAULT_PROJECT = "segment_test"

    EVENT_RECORD_TYPE = "unknown"

    def setUp(self):
        super(BaseSegmentEventRecordTaskMapTest, self).setUp()

        self.initialize_ids()
        self.event_templates = {
            'android_screen': {
                "messageId": "fake_message_id",
                "type": "screen",
                "channel": "server",
                "context": {
                    "app": {
                        "build": 82,
                        "name": "edX",
                        "namespace": "org.edx.mobile",
                        "version": "2.3.0",
                    },
                    "traits": {
                        "anonymousId": self.DEFAULT_ANONYMOUS_ID
                    },
                    "library": {
                        "name": "analytics-android",
                        "version": "3.4.0",
                    },
                    "os": {
                        "name": "Android",
                        "version": "5.1.1",
                    },
                    "timezone": "America/New_York",
                    "screen": {
                        "density": 3.5,
                        "width": 1440,
                        "height": 2560,
                    },
                    "userAgent": "Dalvik/2.1.0 (Linux; U; Android 5.1.1; SAMSUNG-SM-N920A Build/LMY47X)",
                    "locale": "en-US",
                    "device": {
                        "id": "fake_device_id",
                        "manufacturer": "samsung",
                        "model": "SAMSUNG-SM-N920A",
                        "name": "noblelteatt",
                        "advertisingId": "fake_advertising_id",
                        "adTrackingEnabled": False,
                        "type": "android",
                    },
                    "network": {
                        "wifi": True,
                        "carrier": "AT&T",
                        "bluetooth": False,
                        "cellular": False,
                    },
                    "ip": "98.236.220.148"
                },
                "anonymousId": self.DEFAULT_ANONYMOUS_ID,
                "integrations": {
                    "All": True,
                    "Google Analytics": False,
                },
                "category": "",
                "name": "Launch",
                "properties": {
                    "data": {
                    },
                    "device-orientation": "portrait",
                    "navigation-mode": "full",
                    "context": {
                        "app_name": "edx.mobileapp.android",
                    },
                    "category": "screen",
                    "label": "Launch\x00",
                },
                "writeKey": "dummy_write_key",
                "projectId": self.DEFAULT_PROJECT,
                "timestamp": "{0}.700Z".format(self.DEFAULT_TIMESTAMP),
                "sentAt": "{0}.096Z".format(self.DEFAULT_TIMESTAMP),
                "receivedAt": "{0}.796Z".format(self.DEFAULT_TIMESTAMP),
                "originalTimestamp": "{0}-0400".format(self.DEFAULT_TIMESTAMP),
                "version": 2,
            }
        }
        self.default_event_template = 'android_screen'
        self.create_task()

    def create_task(self, date=None):  # pylint: disable=arguments-differ
        """Allow arguments to be passed to the task constructor."""
        if not date:
            date = self.DEFAULT_DATE
        self.task = SegmentEventRecordDataTask(
            interval=luigi.DateIntervalParameter().parse(date),
            output_root='/fake/output',
            event_record_type=self.EVENT_RECORD_TYPE,
        )
        self.task.init_local()


@ddt
class SegmentEventRecordTaskMapTest(BaseSegmentEventRecordTaskMapTest, unittest.TestCase):
    """Test class for emission of segment log events in JsonEventRecord format."""

    EVENT_RECORD_TYPE = "EventRecord"

    @data(
        {'receivedAt': "2013-12-01T15:38:32.805444Z"},
        {'type': 'track', 'event': None},
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    def _get_event_record_from_mapper(self, kwargs):
        """Returns an EventRecord constructed from mapper output."""
        line = self.create_event_log_line(**kwargs)
        mapper_output = tuple(self.task.mapper(line))
        self.assertEquals(len(mapper_output), 1)
        row = mapper_output[0]
        self.assertEquals(len(row), 2)
        _actual_key, actual_value = row
        return EventRecord.from_tsv(actual_value)

    @data(
        {'timestamp': '2016-7-26T13:26:23-0500'},
        {'timestamp': '2016-07-26T13:34:0026-0400'},
        {'timestamp': '2016-0007-29T12:15:34+0530'},
        {'timestamp': '2016-07-26 05:11:37 a.m. +0000'},
    )
    def test_funky_but_parsable_timestamps(self, kwargs):
        actual_record = self._get_event_record_from_mapper(kwargs)
        timestamp = getattr(actual_record, 'timestamp')
        self.assertNotEquals(timestamp, None)

    @data(
        {'timestamp': '2016-07-26 05:11:37 a.m. +000A'},
    )
    def test_unparsable_timestamps(self, kwargs):
        actual_record = self._get_event_record_from_mapper(kwargs)
        timestamp = getattr(actual_record, 'timestamp')
        self.assertEquals(timestamp, None)

    @data(
        ({'receivedAt': "2013-12-17T15:38:32.805444Z", 'requestTime': "2014-12-18T15:38:32.805444Z"}, "2013-12-17T15:38:32.805444+00:00"),
        ({'requestTime': "2014-12-01T15:38:32.805444Z"}, '2014-12-01T15:38:32.805444+00:00'),  # default to requestTime
        ({}, '2013-12-17T15:38:32.700000+00:00'),  # default to timestamp
    )
    @unpack
    def test_defaulting_arrival_timestamps(self, kwargs, expected_timestamp):
        template = self.event_templates['android_screen']
        del template['receivedAt']
        line = self.create_event_log_line(template=template, **kwargs)
        line_json = json.loads(line)
        timestamp = self.task.get_event_arrival_time(line_json)
        self.assertEquals(timestamp, expected_timestamp)

    def test_android_screen(self):
        template = self.event_templates['android_screen']
        event = self.create_event_log_line(template=template)
        expected_key = (self.DEFAULT_DATE, self.DEFAULT_PROJECT)
        expected_dict = {
            'version': VERSION,
            'input_file': '',
            'project': self.DEFAULT_PROJECT,
            'event_type': 'screen',
            'event_source': 'server',
            'event_category': 'screen',
            'timestamp': '2013-12-17T15:38:32.700000+00:00',
            'received_at': '2013-12-17T15:38:32.796000+00:00',
            'date': self.DEFAULT_DATE,
            'agent': 'Dalvik/2.1.0 (Linux; U; Android 5.1.1; SAMSUNG-SM-N920A Build/LMY47X)',
            'agent_type': 'tablet',
            'agent_device_name': 'Samsung SM-N920A',
            'agent_os': 'Android',
            'agent_browser': 'Android',
            'agent_touch_capable': 'True',
            'ip': '98.236.220.148',
            'channel': 'server',
            'anonymous_id': self.DEFAULT_ANONYMOUS_ID,
            'category': 'screen',
            'label': 'Launch\\0',
            'locale': 'en-US',
            'timezone': 'America/New_York',
            'app_name': 'edX',
            'app_version': '2.3.0',
            'os_name': 'Android',
            'os_version': '5.1.1',
            'device_manufacturer': 'samsung',
            'device_model': 'SAMSUNG-SM-N920A',
            'network_carrier': "AT&T",
            'screen_width': '1440',
            'screen_height': '2560',
        }
        expected_value = EventRecord(**expected_dict).to_separated_values()
        self.assert_single_map_output(event, expected_key, expected_value)


@ddt
class SegmentJsonEventRecordTaskMapTest(BaseSegmentEventRecordTaskMapTest, unittest.TestCase):
    """Test class for emission of segment log events in JsonEventRecord format."""

    EVENT_RECORD_TYPE = "JsonEventRecord"

    def get_raw_event(self, event_line):
        event = eventlog.parse_json_event(event_line)
        dump = json.dumps(event, sort_keys=True)
        encoded_dump = backslash_encode_value(dump)
        return encoded_dump

    @data(
        {'receivedAt': "2013-12-01T15:38:32.805444Z"},
        {'type': 'track', 'event': None},
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    def _get_event_record_from_mapper(self, kwargs):
        """Returns an EventRecord constructed from mapper output."""
        line = self.create_event_log_line(**kwargs)
        mapper_output = tuple(self.task.mapper(line))
        self.assertEquals(len(mapper_output), 1)
        row = mapper_output[0]
        self.assertEquals(len(row), 2)
        _actual_key, actual_value = row
        return JsonEventRecord.from_tsv(actual_value)

    @data(
        {'timestamp': '2016-7-26T13:26:23-0500'},
        {'timestamp': '2016-07-26T13:34:0026-0400'},
        {'timestamp': '2016-0007-29T12:15:34+0530'},
        {'timestamp': '2016-07-26 05:11:37 a.m. +0000'},
    )
    def test_funky_but_parsable_timestamps(self, kwargs):
        actual_record = self._get_event_record_from_mapper(kwargs)
        timestamp = getattr(actual_record, 'timestamp')
        self.assertNotEquals(timestamp, None)

    @data(
        {'timestamp': '2016-07-26 05:11:37 a.m. +000A'},
        # TODO: figure out why this no longer works!
        {'timestamp': '0300-01-01T00:00:00.000Z'},
    )
    def test_unparsable_timestamps(self, kwargs):
        actual_record = self._get_event_record_from_mapper(kwargs)
        timestamp = getattr(actual_record, 'timestamp')
        self.assertEquals(timestamp, None)

    @data(
        {'properties': {'course_id': 'course-v1:FooX+1.23x+2013_Spring'}},
        {'properties': {'courseid': 'course-v1:FooX+1.23x+2013_Spring'}},
        {'properties': {'course': 'course-v1:FooX+1.23x+2013_Spring'}},
        {'properties': {'label': 'course-v1:FooX+1.23x+2013_Spring'}},
        {'properties': {'url': 'https://courses.edx.org/courses/course-v1:FooX+1.23x+2013_Spring'}},
        {'properties': {'url': 'https://courses.edx.org/courses/FooX/1.23x/2013_Spring'}},
        {'properties': {'url': 'https://courses.edx.org/verify_student/start-flow/course-v1:FooX+1.23x+2013_Spring/'}},
        {'properties': {'url': 'https://courses.edx.org/course_modes/choose/course-v1:FooX+1.23x+2013_Spring/'}},
    )
    def test_course_ids(self, kwargs):
        actual_record = self._get_event_record_from_mapper(kwargs)
        course_id = getattr(actual_record, 'course_id')
        self.assertNotEquals(course_id, None)
        org_id = getattr(actual_record, 'org_id')
        self.assertNotEquals(org_id, None)

    def test_android_screen(self):
        template = self.event_templates['android_screen']
        event = self.create_event_log_line(template=template)
        expected_key = (self.DEFAULT_DATE, self.DEFAULT_PROJECT)
        expected_dict = {
            'input_file': '',
            'source': self.DEFAULT_PROJECT,
            'event_type': 'screen',
            'emitter_type': 'server',
            'timestamp': ciso8601.parse_datetime('2013-12-17T15:38:32.700000+00:00'),
            'received_at': ciso8601.parse_datetime('2013-12-17T15:38:32.796000+00:00'),
            'date': datetime.date(*[int(x) for x in self.DEFAULT_DATE.split('-')]),
            'agent_type': 'tablet',
            'agent_device_name': 'Samsung SM-N920A',
            'agent_os': 'Android',
            'agent_browser': 'Android',
            'agent_touch_capable': True,
            'anonymous_id': self.DEFAULT_ANONYMOUS_ID,
            'category': 'screen',
            'label': 'Launch\\0',
            'raw_event': self.get_raw_event(event),
        }
        expected_value = JsonEventRecord(**expected_dict).to_separated_values()
        self.assert_single_map_output(event, expected_key, expected_value)


class PerDateTrackingJsonEventRecordTaskMapTest(TrackingJsonEventRecordTaskMapTest):

    def create_task(self, date=None):  # pylint: disable=arguments-differ
        """Allow arguments to be passed to the task constructor."""
        if not date:
            date = self.DEFAULT_DATE
        self.task = PerDateTrackingEventRecordDataTask(
            date=luigi.DateParameter().parse(date),
            output_root='/fake/output',
            event_record_type=self.EVENT_RECORD_TYPE,
        )
        self.task.init_local()


class PerDateSegmentJsonEventRecordTaskMapTest(SegmentJsonEventRecordTaskMapTest):

    def create_task(self, date=None):  # pylint: disable=arguments-differ
        """Allow arguments to be passed to the task constructor."""
        if not date:
            date = self.DEFAULT_DATE
        self.task = PerDateSegmentEventRecordDataTask(
            date=luigi.DateParameter().parse(date),
            output_root='/fake/output',
            event_record_type=self.EVENT_RECORD_TYPE,
        )
        self.task.init_local()
