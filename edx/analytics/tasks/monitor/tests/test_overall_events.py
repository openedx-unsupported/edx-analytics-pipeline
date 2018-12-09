"""Tests overall count of events"""

import json
import sys
from StringIO import StringIO
from unittest import TestCase

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.monitor.overall_events import TotalEventsDailyTask
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


class TotalEventsTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, TestCase):
    """Ensure events of various flavors are counted"""

    DATE = '2013-12-17'

    def setUp(self):
        self.task_class = TotalEventsDailyTask
        super(TotalEventsTaskMapTest, self).setUp()

        self.initialize_ids()
        self.task.init_local()

        self.event_type = "edx.course.enrollment.activated"
        self.timestamp = "{}T15:38:32.805444".format(self.DATE)
        self.user_id = 10

        self.event_templates = {
            'event': {
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

        }
        self.default_event_template = 'event'

    def test_explicit_event(self):
        line = self.create_event_log_line(user_id="")
        self.assert_single_map_output(line, self.DATE, 1)

    def test_no_timestamp(self):
        line = self.create_event_log_line(timestamp="")
        self.assert_single_map_output(line, self.DATE, 1)

    def test_bad_event(self):
        line = "bad event"
        self.assert_no_map_output_for(line)

    def test_event_no_ids(self):
        """
        Many events (generally implicit events) have typical edx IDs missing or unavailable
        because of their contexts. This test ensures these events are still counted.
        """
        self.empty_ids()
        line = self.create_event_log_line()
        self.assert_single_map_output(line, self.DATE, 1)

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
            "time": "{}T22:11:29.689805+00:00".format(self.DATE),
            "ip": "10.0.2.2",
            "event": "{\"POST\": {}, \"GET\": {}}",
            "agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/39.0.2171.71 Safari/537.36",
            "page": "null"
        }
        line = json.dumps(event)
        self.assert_single_map_output(line, self.DATE, 1)

    def test_no_time_element(self):
        event_line = self.create_event_dict()
        del event_line["time"]
        line = json.dumps(event_line)
        # When the time element is missing, luigi will print an error to stderr.
        # Capture stderr and assert it is what we expect. Also assert that we do not
        # count the event.
        test_stderr = StringIO()
        sys.stderr = test_stderr
        self.assert_no_map_output_for(line)
        test_stderr = test_stderr.getvalue().strip()
        self.assertEquals(test_stderr, 'reporter:counter:Event,Discard Missing Time Field,1')


class TotalEventsTaskReducerTest(ReducerTestMixin, TestCase):
    """Ensure counts are aggregated"""

    def setUp(self):
        self.task_class = TotalEventsDailyTask
        super(TotalEventsTaskReducerTest, self).setUp()

        self.reduce_key = '2013-12-17T00:00:01'

    def test_one_event_count(self):
        inputs = [1, ]
        expected = ((self.reduce_key, 1), )
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_events_same_day(self):
        inputs = [1, 1]
        expected = ((self.reduce_key, 2), )
        self._check_output_complete_tuple(inputs, expected)
