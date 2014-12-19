"""Tests overall count of events"""

import json

import luigi

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.overall_events import TotalEventsDailyTask
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin

from mock import patch


class TotalEventsTaskMapTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """Ensure events of various flavors are counted"""

    def setUp(self):

        fake_param = luigi.DateIntervalParameter()
        self.task = TotalEventsDailyTask(
            interval=fake_param.parse('2014-12-05'),
            output_root='/fake/output'
        )

        self.initialize_ids()
        self.task.init_local()

        self.event_type = "edx.course.enrollment.activated"
        self.timestamp = "2014-12-05T22:13:09.691008+00:00"
        self.user_id = 5

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        event_dict = {
            "username": "test_user",
            "host": "test_host",
            "session": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "event_source": "server",
            "name": self.event_type,
            "event_type": self.event_type,
            "context": {
                "course_id": self.course_id,
                "org_id": self.org_id,
                "user_id": self.user_id,
            },
            "time": "{0}+00:00".format(self.timestamp),
            "ip": "127.0.0.1",
            "event": {
                "course_id": self.course_id,
                "user_id": self.user_id,
                "mode": "honor",
            },
            "agent": "blah, blah, blah",
            "page": None
        }
        event_dict.update(**kwargs)
        return event_dict

    def test_explicit_event(self):
        line = self._create_event_log_line(user_id="")
        self.assertEquals(tuple(self.task.mapper(line)), (('2014-12-05', 1), ))

    def test_no_timestamp(self):
        line = self._create_event_log_line(timestamp="")
        self.assertEquals(tuple(self.task.mapper(line)), (('2014-12-05', 1), ))

    def test_bad_event(self):
        line = "bad event"
        self.assertEquals(tuple(self.task.mapper(line)), tuple())

    def test_event_no_ids(self):
        """
        Many events (generally implicit events) have typical edx IDs missing or unavailable
        because of their contexts. This test ensures these events are still counted.
        """
        self.empty_ids()
        line = self._create_event_log_line()
        self.assertEquals(tuple(self.task.mapper(line)), (('2014-12-05', 1), ))

    def test_implicit_event(self):
        event = {
            "username": "",
            "host": "precise64",
            "event_source": "server",
            "event_type": "/jsi18n/",
            "context": {
                "user_id": "",
                "org_id": "",
                "course_id": "",
                "path": "/jsi18n/"
            },
            "time": "2014-12-05T22:11:29.689805+00:00",
            "ip": "10.0.2.2",
            "event": "{\"POST\": {}, \"GET\": {}}",
            "agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/39.0.2171.71 Safari/537.36",
            "page": "null"
        }
        line = json.dumps(event)
        self.assertEquals(tuple(self.task.mapper(line)), (('2014-12-05', 1), ))

    def test_no_time_element(self):
        event = {
            "username": "faker",
            "event_source": "server",
            "name": "edx.course.enrollment.activated",
            "agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/39.0.2171.71 Safari/537.36",
            "page": "null",
            "host": "precise64",
            "session": "b8cf518e5233afb402419ef896263122",
            "context": {
                "user_id": 5,
                "org_id": "edX",
                "course_id": "edX/DemoX/Demo_Course",
                "path": "/change_enrollment"
            },
            "ip": "192.168.1.2",
            "event": {
                "course_id": "edX/DemoX/Demo_Course",
                "user_id": 5,
                "mode": "honor"
            },
            "event_type": "edx.course.enrollment.activated"
        }

        line = json.dumps(event)
        # Quiet internal luigi noise for our test output
        patcher = patch('luigi.hadoop.JobTask.incr_counter').start()
        self.assertEquals(tuple(self.task.mapper(line)), tuple())
        patcher.stop()


class TotalEventsTaskReducerTest(unittest.TestCase):
    """Ensure counts are aggregated"""

    def setUp(self):
        self.interval = '2014-12-17'
        fake_param = luigi.DateIntervalParameter()
        self.task = TotalEventsDailyTask(
            interval=fake_param.parse(self.interval),
            output_root="/fake/output"
        )
        self.key = '2014-12-17T00:00:01'

    def _check_output(self, key, values, expected):
        """Compare generated with expected output."""
        self.assertEquals(tuple(self.task.reducer(key, values)), expected)

    def test_one_event_count(self):
        key = '2014-12-17T00:00:01'
        values = [1, ]
        expected = ((key, 1), )
        self._check_output(key, values, expected)

    def test_multiple_events_same_day(self):
        key = '2014-12-17T00:00:01'
        values = [1, 1]
        expected = ((key, 2), )
        self._check_output(key, values, expected)
