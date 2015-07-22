"""
Test the MapReduce task for cleaning event data prior to bulkloading into Vertica.

Note that because this task requires canonicalized events as input, we don't need to test any edge cases.

Testing strategy:
    Single clean implicit event, when the task has remove_implicit=True, should have no output.
    Single clean implicit event, when the task has remove_implict=False, should appear in the 1-event output.
    Single clean explicit event, when the task has remove_implicit=True, should appear in the 1-event output.
    Single explicit event with a dictionary key longer than 256 should have no output.
    Metadata added in the clean_for_vertica step should match expected.
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

    # This dictionary stores the default values for arguments to various task constructors; if not told otherwise,
    # the task constructor will pull needed values from this dictionary.
    # DEFAULT_ARGS = {
    #     'output_root': '/fake/output',
    #     'end_date': datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
    #     'geolocation_data': 'test://data/data.file',
    #     'mapreduce_engine': 'local',
    #     'user_country_output': 'test://output/',
    #     'name': 'test',
    #     'src': ['test://input/'],
    #     'dest': 'test://output/'
    # }
    #
    #
    def setUp(self):
        self.task_class = CleanForVerticaTask

        # super(TestCleanForVerticaMapper, self).setUp()

        # self.initialize_ids()
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
                "page": None
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
                "page": None
            }
        }
        self.default_event_template = 'sample_explicit'
        self.expected_key = (self.course_id, self.user_id)



    def test_implicit_removed(self):
        """If we have an implicit event, we remove it by default."""
        line = self.create_event_log_line()
        self.assert_no_map_output_for(line)
