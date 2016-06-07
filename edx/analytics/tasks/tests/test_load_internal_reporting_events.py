"""Test processing of events for loading into Hive, etc."""

import json
import datetime

import luigi
from luigi import date_interval
from ddt import ddt, data, unpack
from mock import MagicMock

from edx.analytics.tasks.load_internal_reporting_events import (
    EventRecord,
    BaseEventRecordDataTask,
    TrackingEventRecordDataTask,
    SegmentEventRecordDataTask,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.target import FakeTarget


@ddt
class TrackingEventRecordTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, unittest.TestCase):
    """Base class for test analysis of detailed student engagement"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    def setUp(self):
        super(TrackingEventRecordTaskMapTest, self).setUp()

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
        }
        self.default_event_template = 'problem_check'
        self.create_task()

    def create_task(self, date=None):  # pylint: disable=arguments-differ
        """Allow arguments to be passed to the task constructor."""
        if not date:
            date = self.DEFAULT_DATE
        self.task = TrackingEventRecordDataTask(
            date=luigi.DateParameter().parse(date),
            output_root='/fake/output',
        )
        self.task.init_local()

    @data(
        {'time': "2013-12-01T15:38:32.805444"},
        {'event_type': None},
        {'event': 'sdfasdf'}
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))


@ddt
class SegmentEventRecordTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, unittest.TestCase):
    """Base class for test analysis of detailed student engagement"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32"
    DEFAULT_DATE = "2013-12-17"
    DEFAULT_ANONYMOUS_ID = "abcdef12-3456-789a-bcde-f0123456789a"
    DEFAULT_PROJECT = "segment_test"
    def setUp(self):
        super(SegmentEventRecordTaskMapTest, self).setUp()

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
                    "timezone": "America\/New_York",
                    "screen": {
                        "density": 3.5,
                        "width": 1440,
                        "height": 2560,
                    },
                    "userAgent": "Dalvik\/2.1.0 (Linux; U; Android 5.1.1; SAMSUNG-SM-N920A Build\/LMY47X)",
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
                    "label": "Launch",
                },
                "writeKey": "dummy_write_key",
                "projectId": self.DEFAULT_PROJECT,
                "timestamp": "{0}.796Z".format(self.DEFAULT_TIMESTAMP),                
                "sentAt": "{0}.000Z".format(self.DEFAULT_TIMESTAMP),
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
            date=luigi.DateParameter().parse(date),
            output_root='/fake/output',
        )
        self.task.init_local()

    @data(
        {'receivedAt': "2013-12-01T15:38:32.805444Z"},
        {'type': 'track', 'event': None},
        {'type': 'track', 'event': '/implicit/event/url'},
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    def test_android_screen(self):
        template = self.event_templates['android_screen']
        event = self.create_event_log_line(template=template)
        expected_key = (self.DEFAULT_DATE, self.DEFAULT_PROJECT)
        expected_dict = {
            'project': self.DEFAULT_PROJECT,
            'event_type': 'screen',
            'event_source': 'server',
            'event_category': 'screen',
            'timestamp': '2013-12-17T15:38:32+00:00',
            'received_at': '2013-12-17T15:38:32.796000+00:00',
            'date': self.DEFAULT_DATE,
        }
        expected_value = EventRecord(**expected_dict).to_separated_values()
        self.assert_single_map_output(
            event,
            expected_key,
            expected_value
        )
