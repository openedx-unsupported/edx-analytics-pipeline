"""
Test the MapReduce task for cleaning event data prior to bulkloading into Vertica.

Note that because this task requires canonicalized events as input, we don't need to test any edge cases.

Note that because the mapper keys are generated deterministically in a non-meaningful way from the event
processing metadata, we don't use the provided assert_single_map_output function from MapperTestMixin and
instead just check the mapper output values.

Testing strategy:
    Single clean implicit event, when the task has remove_implicit=True, should have no output.
    Single clean implicit event, when the task has remove_implict=False, should appear in the 1-event output.
    Single clean explicit event, when the task has remove_implicit=True, should appear in the 1-event output.
    Single explicit event with a dictionary key longer than 256 should have no output.

    User agent canonicalization should canonicalize a good user agent properly.
    User agent canonicalization should leave empty fields (and not throw an exception) if the agent is malformed.
"""
import json
import StringIO
import hashlib
import os
import tempfile
import shutil
import math
import datetime

from mock import Mock, call
from opaque_keys.edx.locator import CourseLocator
from edx.analytics.tasks.clean_for_vertica import CleanForVerticaTask

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config, OPTION_REMOVED
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin

class TestCleanForVerticaMapper(MapperTestMixin, InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that the mapper for cleaning events for Vertica loading works as expected.
    """

    run_date = datetime.date(2013, 12, 17)
    MAX_KEY_LENGTH = 256

    def setUp(self):
        self.task_class = CleanForVerticaTask

        # super(TestCleanForVerticaMapper, self).setUp()

        # self.initialize_ids()
        self.course_id = "fooX/bar101X"
        self.org_id = "fooX"
        self.user_id = "test_user"
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.event_templates = {
            'sample_implicit': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "/test/implicit",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.user_id,
                },
                "time": "{0}+00:00".format(self.timestamp),
                "ip": "127.0.0.1",
                "event": {
                    "POST": "foo",
                    "GET": "bar"
                },
                "agent": "blah, blah, blah",
                "page": "test.page"
            },
            'sample_explicit': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "test_event_type",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.user_id,
                },
                "time": "{0}+00:00".format(self.timestamp),
                "ip": "127.0.0.1",
                "event": {
                    "POST": "foo",
                    "GET": "bar"
                },
                "agent": "blah, blah, blah",
                "page": "test.page"
            }
        }
        self.default_event_template = 'sample_explicit'
        self.expected_key = (self.course_id, self.user_id)


        # When CleanForVerticaTask's mapper adds metadata to the events, it expects
        # and uses certain environment variables, so we set those here.
        os.environ['map_input_file'] = 'test_map_input'
        self.maxDiff = None

    def assert_single_map_output_value(self, line, expected):
        """
        Assert that an input line generates exactly one output matching the expected value
        except in the metadata field, as that field contains unpredictable information
        like the timestamp of the Vertica cleaning task being run on the line.
        """
        mapper_output = tuple(self.task.mapper(line))
        self.assertEqual(len(mapper_output), 1, "Expected only a single mapper output.")
        row = mapper_output[0]
        actual_key, actual_value = row
        actual_value_dict = json.loads(actual_value)
        expected_dict = json.loads(expected)
        actual_value_dict.pop('metadata')
        self.assertEqual(expected_dict, actual_value_dict)

    def test_implicit_removed_when_desired(self):
        """If we have an implicit event, we remove it if the remove_implicit flag is set to True."""
        self.task = CleanForVerticaTask(date=self.run_date, remove_implicit=True)
        line = self.create_event_log_line(template_name='sample_implicit')
        self.assert_no_map_output_for(line)

    def test_implicit_kept_when_desired(self):
        """If we have an implicit event, we keep it if the remove_implicit flag is set to False."""
        self.task = CleanForVerticaTask(date=self.run_date, remove_implicit=False)
        line = self.create_event_log_line(template_name='sample_implicit')
        processed_line_expected = {"username": "test_user",
                                   "event_source": "server",
                                   "event_type": "/test/implicit",
                                   "ip": "127.0.0.1",
                                   "event": {"POST": "foo", "GET": "bar"},
                                   "agent": "blah, blah, blah",
                                   "template_name": "sample_implicit",
                                   "host": "test_host",
                                   "context": {
                                       "course_id": "fooX/bar101X",
                                       "org_id": "fooX",
                                       "user_id": "test_user"
                                   },
                                   "time": "2013-12-17T15:38:32.805444+00:00",
                                   "page": "test.page"
                                   }
        self.assert_single_map_output_value(line, json.dumps(processed_line_expected))

    def test_explicit_key(self):
        """If we have an explicit event, we keep it even if the remove_implicit flag is set to True."""
        self.task = CleanForVerticaTask(date=self.run_date, remove_implicit=False)
        line = self.create_event_log_line(template_name='sample_explicit')
        processed_line_expected = {"username": "test_user",
                                   "event_source": "server",
                                   "event_type": "test_event_type",
                                   "ip": "127.0.0.1",
                                   "event": {"POST": "foo", "GET": "bar"},
                                   "agent": "blah, blah, blah",
                                   "template_name": "sample_explicit",
                                   "host": "test_host",
                                   "context": {
                                       "course_id": "fooX/bar101X",
                                       "org_id": "fooX",
                                       "user_id": "test_user"
                                   },
                                   "time": "2013-12-17T15:38:32.805444+00:00",
                                   "page": "test.page"
                                   }
        self.assert_single_map_output_value(line, json.dumps(processed_line_expected))

    def test_truncate_massive_keys(self):
        """
        If a key in an event is longer than 256 characters, we should truncate it.

        This condition is necessary as long as the Vertica loader being used uses
        PARSER fjsonparser, as that parser can't handle json keys longer than 256.
        """
        self.task = CleanForVerticaTask(date=self.run_date, remove_implicit=False)
        line = self.create_event_log_line(event={"""
                                                    This is an event that is too long, too long, too long, too long.
                                                    This is an event that is too long, too long, too long, too long.
                                                    This is an event that is too long, too long, too long, too long.
                                                """: "[]"})
        mapper_output = tuple(self.task.mapper(line))
        actual_key, actual_value = mapper_output[0]
        cleaned_line = json.loads(actual_value)

        self.assertLessEqual(len(cleaned_line.get('event').keys()[0]), self.MAX_KEY_LENGTH)
