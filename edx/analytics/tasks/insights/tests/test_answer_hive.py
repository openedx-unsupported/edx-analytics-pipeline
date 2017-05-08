"""
Tests for tasks that calculate answer distributions.

"""
import json
import hashlib
import math
import os
import shutil
import StringIO
import tempfile
from unittest import TestCase

from mock import Mock, call
from opaque_keys.edx.locator import CourseLocator

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.insights.answer_hive import (
    AllProblemCheckEventsTask,
)
from edx.analytics.tasks.util.tests.config import with_luigi_config, OPTION_REMOVED
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


class AllProblemCheckEventsTaskBaseTest(MapperTestMixin, ReducerTestMixin, TestCase):
    """Base test class for testing AllProblemCheckEventsTask."""

    def initialize_ids(self):
        """Define set of id values for use in tests."""
        raise NotImplementedError

    def setUp(self):
        self.task_class = AllProblemCheckEventsTask
        super(AllProblemCheckEventsTaskBaseTest, self).setUp()

        self.initialize_ids()
        self.username = 'test_user'
        self.user_id = 24
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.earlier_timestamp = "2013-12-15T15:38:32.805444"

    @property
    def date_string(self):
        return self.timestamp.split('T')[0]

    @property
    def reduce_key(self):
        return (self.date_string, self.course_id)

    def _create_event_data_dict(self, **kwargs):
        """Returns event data dict with test values."""
        event_data = {
            "problem_id": self.problem_id,
            "attempts": 2,
            "answers": {self.answer_id: "3"},
            "correct_map": {
                self.answer_id: {
                    "queuestate": None,
                    "npoints": None,
                    "msg": "",
                    "correctness": "incorrect",
                    "hintmode": None,
                    "hint": ""
                },
            },
            "state": {
                "input_state": {self.answer_id: None},
                "correct_map": None,
                "done": False,
                "seed": 1,
                "student_answers": {self.answer_id: "1"},
            },
            "grade": 0,
            "max_grade": 1,
            "success": "incorrect",
        }
        self._update_with_kwargs(event_data, **kwargs)
        return event_data

    @staticmethod
    def _update_with_kwargs(data_dict, **kwargs):
        """Updates a dict from kwargs only if it modifies a top-level value."""
        for key, value in kwargs.iteritems():
            if key in data_dict:
                data_dict[key] = value

    def _create_event_context(self, **kwargs):
        """Returns context dict with test values."""
        context = {
            "course_id": self.course_id,
            "org_id": self.org_id,
            "user_id": self.user_id,
        }
        self._update_with_kwargs(context, **kwargs)
        return context

    def _create_problem_data_dict(self, **kwargs):
        """Returns problem_data with test values."""
        problem_data = self._create_event_data_dict(**kwargs)
        problem_data['timestamp'] = self.timestamp
        problem_data['context'] = self._create_event_context(**kwargs)

        self._update_with_kwargs(problem_data, **kwargs)
        return problem_data

    def create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        event_dict = {
            "username": self.username,
            "host": "test_host",
            "event_source": "server",
            "event_type": "problem_check",
            "context": self._create_event_context(**kwargs),
            "time": "{0}+00:00".format(self.timestamp),
            "ip": "127.0.0.1",
            "event": self._create_event_data_dict(**kwargs),
            "agent": "blah, blah, blah",
            "page": None
        }

        self._update_with_kwargs(event_dict, **kwargs)
        return event_dict


class AllProblemCheckEventsMapTest(InitializeOpaqueKeysMixin, AllProblemCheckEventsTaskBaseTest):
    """Tests to verify that event log parsing by mapper works correctly."""
    maxDiff = None

    def test_non_problem_check_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_unparseable_problem_check_event(self):
        line = 'this is garbage but contains problem_check'
        self.assert_no_map_output_for(line)

    def test_browser_event_source(self):
        line = self.create_event_log_line(event_source='browser')
        self.assert_no_map_output_for(line)

    def test_missing_event_source(self):
        line = self.create_event_log_line(event_source=None)
        self.assert_no_map_output_for(line)

    def test_missing_event_type(self):
        # Here, we make the event as a dictionary so we can edit individual attributes of the event.
        event_dict = self.create_event_dict()
        event_dict['old_event_type'] = event_dict['event_type']
        del event_dict['event_type']
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    def test_implicit_problem_check_event_type(self):
        line = self.create_event_log_line(event_type='implicit/event/ending/with/problem_check')
        self.assert_no_map_output_for(line)

    def test_bad_datetime(self):
        line = self.create_event_log_line(time='this is a bogus time')
        self.assert_no_map_output_for(line)

    def test_bad_event_data(self):
        line = self.create_event_log_line(event=["not an event"])
        self.assert_no_map_output_for(line)

    def test_missing_course_id(self):
        line = self.create_event_log_line(context={})
        self.assert_no_map_output_for(line)

    def test_illegal_course_id(self):
        line = self.create_event_log_line(course_id=";;;;bad/id/val")
        self.assert_no_map_output_for(line)

    def test_missing_problem_id(self):
        line = self.create_event_log_line(problem_id=None)
        self.assert_no_map_output_for(line)

    def test_missing_context(self):
        line = self.create_event_log_line(context=None)
        self.assert_no_map_output_for(line)

    def test_missing_username_ok(self):
        line = self.create_event_log_line(username=None)
        line = self.create_event_log_line()
        expected_data = self._create_problem_data_dict()
        expected_key = self.reduce_key
        expected_value = (expected_data,)
        self.assert_single_map_output_load_jsons(line, expected_key, expected_value)

    def test_answer_id_newline_ok(self):
        self.answer_id = 'foo\nbar'
        line = self.create_event_log_line()
        expected_data = self._create_problem_data_dict()
        expected_key = self.reduce_key
        expected_value = (expected_data,)
        self.assert_single_map_output_load_jsons(line, expected_key, expected_value)

    def test_answer_id_tab_ok(self):
        self.answer_id = 'foo\tbar'
        line = self.create_event_log_line()
        expected_data = self._create_problem_data_dict()
        expected_key = self.reduce_key
        expected_value = (expected_data,)
        self.assert_single_map_output_load_jsons(line, expected_key, expected_value)

    def test_good_problem_check_event(self):
        # Here, we make the event as a dictionary since we're comparing based on dictionaries anyway.
        event = self.create_event_dict()
        line = json.dumps(event)
        expected_data = self._create_problem_data_dict()
        expected_key = self.reduce_key
        expected_value = (expected_data,)
        self.assert_single_map_output_load_jsons(line, expected_key, expected_value)
