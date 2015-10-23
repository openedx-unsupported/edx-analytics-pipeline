"""
Tests for event analysis.
"""

from edx.analytics.tasks.event_analysis import EventAnalysisTask, get_key_names
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin

from opaque_keys.edx.locator import CourseLocator


class EventAnalysisBaseTest(InitializeOpaqueKeysMixin, MapperTestMixin, unittest.TestCase):
    """Base class for EventAnalysis Task test."""

    DATE = '2013-12-17'

    def setUp(self):
        self.task_class = EventAnalysisTask
        super(EventAnalysisBaseTest, self).setUp()
        self.initialize_ids()
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.event_templates = {
            'event': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": 'edx.course.enrollment.activated',
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                },
                "time": "{0}+00:00".format(self.timestamp),
                "ip": "127.0.0.1",
                "event": {
                    "course_id": self.course_id,
                    "mode": "honor",
                },
            }

        }
        self.default_event_template = 'event'
        self.expected_key = (self.DATE, self.course_id)


class EventAnalysisMapTest(EventAnalysisBaseTest):
    """Test for EventAnalysis mapper()"""

    def test_bad_event(self):
        line = "some garbage"
        self.assert_no_map_output_for(line)

    def test_missing_course_id(self):
        line = self.create_event_log_line(context={"course_id": ''})
        self.assert_no_map_output_for(line)

    def test_without_timestamp(self):
        line = self.create_event_log_line(time='')
        self.assert_no_map_output_for(line)

    def test_output_for_unwanted_event(self):
        self.create_task(course_id=['Foo'])
        line = self.create_event_log_line()
        self.assert_no_map_output_for(line)

    def test_single_output(self):
        line = self.create_event_log_line()
        # self.assert_single_map_output(line, self.expected_key, line)
        mapper_output = tuple(self.task.mapper(line))
        expected_event_type = 'edx.course.enrollment.activated'
        self.assertEquals(len(mapper_output), 4)
        self.assertEquals(mapper_output, (
            ('event.course_id(str)', (expected_event_type, 'server')),
            ('event.mode(str)', (expected_event_type, 'server')),
            ('context.course_id(str)', (expected_event_type, 'server')),
            ('context.org_id(str)', (expected_event_type, 'server')),
        ))


class EventAnalysisLegacyMapTest(InitializeLegacyKeysMixin, EventAnalysisMapTest):
    """Run same mapper() tests, but using legacy values for keys."""
    pass


class EventAnalysisKeyNameTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """Test for get_key_names for EventAnalysis Task."""

    def setUp(self):
        super(EventAnalysisKeyNameTest, self).setUp()
        self.initialize_ids()

    def test_empty(self):
        event = {}
        keys = get_key_names(event, 'event')
        self.assertEquals(keys, ['event(emptydict)'])

    def test_submission(self):
        event = {
            'submission': {
                self.answer_id: {
                    'variant': 'whee'
                }
            }
        }
        keys = get_key_names(event, 'event')
        self.assertEquals(keys, ['event.submission.(hex32)_(int1)_(int1).variant(str)'])


class EventAnalysisLegacyKeyNameTest(InitializeLegacyKeysMixin, EventAnalysisKeyNameTest):
    """Test for get_key_names for EventAnalysis Task."""

    def test_submission(self):
        event = {
            'submission': {
                self.answer_id: {
                    'variant': 'whee'
                }
            }
        }
        keys = get_key_names(event, 'event')
        self.assertEquals(keys, ['event.submission.(i4x-string).variant(str)'])
