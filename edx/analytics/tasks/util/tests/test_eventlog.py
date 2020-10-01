"""
Tests for utilities that parse event logs.
"""
from unittest import TestCase

from mock import patch

import edx.analytics.tasks.util.eventlog as eventlog


class ParseEventLogTest(TestCase):
    """
    Verify that event log parsing works correctly.
    """

    def test_parse_valid_json_event(self):
        line = '{"username": "successful"}'
        result = eventlog.parse_json_event(line)
        self.assertTrue(isinstance(result, dict))

    def test_parse_json_event_truncated(self):
        line = '{"username": "unsuccessful'
        result = eventlog.parse_json_event(line)
        self.assertIsNone(result)

    def test_parse_json_event_with_cruft(self):
        line = 'leading cruft here {"username": "successful"}  '
        result = eventlog.parse_json_event(line)
        self.assertTrue(isinstance(result, dict))

    def test_parse_json_event_with_nonascii(self):
        line = '{"username": "b\ufffdb"}'
        result = eventlog.parse_json_event(line)
        self.assertTrue(isinstance(result, dict))
        self.assertEquals(result['username'], u'b\ufffdb')


class TimestampTest(TestCase):
    """Verify timestamp-related functions."""

    def test_datestamp_from_timestamp(self):
        timestamp = "2013-12-17T15:38:32.805444"
        self.assertEquals(eventlog.timestamp_to_datestamp(timestamp), "2013-12-17")

    def test_missing_datetime(self):
        item = {"something else": "not an event"}
        self.assertIsNone(eventlog.get_event_time(item))

    def test_good_datetime_with_microseconds_and_timezone(self):
        item = {"time": "2013-12-17T15:38:32.805444+00:00"}
        dt_value = eventlog.get_event_time(item)
        self.assertIsNotNone(dt_value)
        self.assertEquals(eventlog.datetime_to_timestamp(dt_value), "2013-12-17T15:38:32.805444")
        self.assertEquals(eventlog.datetime_to_datestamp(dt_value), "2013-12-17")

    def test_good_datetime_with_timezone(self):
        item = {"time": "2013-12-17T15:38:32+00:00"}
        dt_value = eventlog.get_event_time(item)
        self.assertIsNotNone(dt_value)
        self.assertEquals(eventlog.datetime_to_timestamp(dt_value), "2013-12-17T15:38:32")
        self.assertEquals(eventlog.datetime_to_datestamp(dt_value), "2013-12-17")

    def test_good_datetime_with_microseconds(self):
        item = {"time": "2013-12-17T15:38:32.805444"}
        dt_value = eventlog.get_event_time(item)
        self.assertIsNotNone(dt_value)
        self.assertEquals(eventlog.datetime_to_timestamp(dt_value), "2013-12-17T15:38:32.805444")
        self.assertEquals(eventlog.datetime_to_datestamp(dt_value), "2013-12-17")

    def test_good_datetime_with_no_microseconds_or_timezone(self):
        item = {"time": "2013-12-17T15:38:32"}
        dt_value = eventlog.get_event_time(item)
        self.assertIsNotNone(dt_value)
        self.assertEquals(eventlog.datetime_to_timestamp(dt_value), "2013-12-17T15:38:32")
        self.assertEquals(eventlog.datetime_to_datestamp(dt_value), "2013-12-17")


class GetEventUsernameTest(TestCase):
    """Verify that get_event_username works as expected."""

    def test_missing_event_username(self):
        item = {"something else": "not an event"}
        self.assertIsNone(eventlog.get_event_username(item))

    def test_empty_event_username(self):
        item = {"username": "   "}
        self.assertIsNone(eventlog.get_event_username(item))

    def test_event_username_with_trailing_whitespace(self):
        item = {"username": "bub\n"}
        self.assertEquals(eventlog.get_event_username(item), u'bub')


class GetEventDataTest(TestCase):
    """Verify that get_event_data works as expected."""

    def setUp(self):
        super(GetEventDataTest, self).setUp()
        patcher = patch('edx.analytics.tasks.util.eventlog.log')
        self.mock_log = patcher.start()
        self.addCleanup(patcher.stop)

    def test_missing_event_data(self):
        item = {"something else": "not an event"}
        self.assertIsNone(eventlog.get_event_data(item))
        self.assertEquals(self.mock_log.error.call_count, 1)
        self.assertIn('missing event value', self.mock_log.error.call_args[0][0])

    def test_get_empty_string_event_data(self):
        item = {"event": ''}
        self.assertEquals(eventlog.get_event_data(item), {})
        self.assertEquals(self.mock_log.error.call_count, 0)
        self.assertEquals(self.mock_log.debug.call_count, 0)

    def test_get_truncated_post_string_event_data(self):
        post = '{"POST": "not JSON because of truncation %s'
        post512 = post % ('a' * (512 + 2 - len(post)))
        self.assertEquals(len(post512), 512)
        item = {"event": post512}
        self.assertIsNone(eventlog.get_event_data(item))
        self.assertEquals(self.mock_log.debug.call_count, 1)
        self.assertIn('truncated event value', self.mock_log.debug.call_args[0][0])

    def test_get_key_value_string_event_data(self):
        item = {"event": "a=3,b=4"}
        self.assertIsNone(eventlog.get_event_data(item))
        self.assertEquals(self.mock_log.debug.call_count, 1)
        self.assertIn('key-value pairs', self.mock_log.debug.call_args[0][0])

    def test_get_json_string_event_data(self):
        item = {"event": '{ "a string": "that is JSON"}'}
        self.assertEquals(eventlog.get_event_data(item), {"a string": "that is JSON"})
        self.assertEquals(self.mock_log.error.call_count, 0)
        self.assertEquals(self.mock_log.debug.call_count, 0)

    def test_get_bad_string_event_data(self):
        item = {"event": "a string but not JSON"}
        self.assertIsNone(eventlog.get_event_data(item))
        self.assertEquals(self.mock_log.error.call_count, 1)
        self.assertIn('unparsable event value', self.mock_log.error.call_args[0][0])

    def test_event_data_with_list_type(self):
        item = {"event": ["a list", "of strings"]}
        self.assertIsNone(eventlog.get_event_data(item))
        self.assertEquals(self.mock_log.debug.call_count, 1)
        self.assertIn('value of type list', self.mock_log.debug.call_args[0][0])

    def test_get_dict_event_data(self):
        item = {"event": {"a dict": "that has strings"}}
        self.assertEquals(eventlog.get_event_data(item), {"a dict": "that has strings"})
        self.assertEquals(self.mock_log.error.call_count, 0)
        self.assertEquals(self.mock_log.debug.call_count, 0)

    def test_event_data_with_unknown_type(self):
        item = {"event": 1}
        self.assertIsNone(eventlog.get_event_data(item))
        self.assertEquals(self.mock_log.error.call_count, 1)
        self.assertIn('unrecognized type', self.mock_log.error.call_args[0][0])


class GetCourseIdTest(TestCase):
    """Verify that get_course_id works as expected."""

    def test_course_id_from_server_url(self):
        event = {
            'event_source': 'server',
            'context': {},
            'event_type': '/courses/course-v1:DemoX+DemoX+T1_2014/about'
        }
        self.assertEquals(eventlog.get_course_id(event, from_url=True), 'course-v1:DemoX+DemoX+T1_2014')

    def test_course_id_from_url_legacy(self):
        event = {
            'event_source': 'server',
            'context': {},
            'event_type': '/courses/edX/Open_DemoX/edx_demo_course/info'
        }
        self.assertEquals(eventlog.get_course_id(event, from_url=True), 'edX/Open_DemoX/edx_demo_course')

    def test_course_id_from_browser_url(self):
        event = {
            'event_source': 'browser',
            'context': {},
            'page': 'http://test.edx.org/courses/course-v1:DemoX+DemoX+T1_2014/courseware/interactive_demonstrations'
        }
        self.assertEquals(eventlog.get_course_id(event, from_url=True), 'course-v1:DemoX+DemoX+T1_2014')

    def test_course_id_from_xblock_browser_url(self):
        event = {
            'event_source': 'browser',
            'context': {},
            'page': 'https://courses.edx.org/xblock/block-v1:DemoX+DemoX+T1_2014+type@vertical+block@3848270?p1=0&p2=0'
        }
        self.assertEquals(eventlog.get_course_id(event, from_url=True), 'course-v1:DemoX+DemoX+T1_2014')

    def test_course_id_from_invalid_xblock_browser_url(self):
        event = {
            'event_source': 'browser',
            'context': {},
            'page': 'https://courses.edx.org/xblock/block-v1:DemoX+DemoX+T1_2014?p1=0&p2=0'
        }
        self.assertIsNone(eventlog.get_course_id(event, from_url=True))

    def test_missing_context(self):
        event = {
            'event_source': 'server'
        }
        self.assertIsNone(eventlog.get_course_id(event))
