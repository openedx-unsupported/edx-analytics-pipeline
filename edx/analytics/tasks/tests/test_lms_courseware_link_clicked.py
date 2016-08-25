"""Test enrollment computations"""

import luigi

from edx.analytics.tasks.lms_courseware_link_clicked import (
    LMSCoursewareLinkClickedTask,
    LINK_CLICKED
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


class LMSCoursewareLinkClickedTaskMapTest(MapperTestMixin, InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.task_class = LMSCoursewareLinkClickedTask
        super(LMSCoursewareLinkClickedTaskMapTest, self).setUp()

        self.initialize_ids()

        self.user_id = 21
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.datestamp = "2013-12-17"

        self.event_templates = {
            'link_clicked_event': {
                "host": "test_host",
                "event_source": "server",
                "event_type": LINK_CLICKED,
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.user_id,
                },
                "time": "{0}+00:00".format(self.timestamp),
                "ip": "127.0.0.1",
                "event": {
                    "current_url": "http://example.com/",
                    "target_url": "http://another.example.com/",
                }
            }
        }
        self.default_event_template = 'link_clicked_event'

        self.expected_key = (self.course_id, self.datestamp)

    def test_non_link_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_unparseable_enrollment_event(self):
        line = 'this is garbage but contains {}'.format(LINK_CLICKED)
        self.assert_no_map_output_for(line)

    def test_incomplete_events(self):
        line = self.create_event_log_line(event_type="")
        self.assert_no_map_output_for(line)

        line = self.create_event_log_line(event="")
        self.assert_no_map_output_for(line)

        line = self.create_event_log_line(context={})
        self.assert_no_map_output_for(line)

        line = self.create_event_log_line(context={"course_id": ""})
        self.assert_no_map_output_for(line)

        line = self.create_event_log_line(context={"course_id": "garbage course key"})
        self.assert_no_map_output_for(line)

        line = self.create_event_log_line(event={
            "current_url": "",
            "target_url": "https://courses.example.com/blargh"
            }
        )
        self.assert_no_map_output_for(line)

        line = self.create_event_log_line(event={
            "current_url": "http://courses.example.com/blah",
            "target_url": ""
            }
        )
        self.assert_no_map_output_for(line)

    def test_link_clicked_event_count_per_course(self):
        line = self.create_event_log_line()
        self.assert_single_map_output(line, (self.course_id, self.datestamp), 1)

    def test_internal_link_clicked_event_count(self):
        """
        Tests mapping for different types of internal link:
            * Links starting with /
            * Links to top-level page
            * Links to a different protocol (http vs https)
            * Links with no explicit protocol
        """
        line = self.create_event_log_line(event={
            "current_url": "http://courses.example.com/blah",
            "target_url": "https://courses.example.com/blargh"
            }
        )
        self.assert_single_map_output(line, (self.course_id, self.datestamp), 0)

        line = self.create_event_log_line(event={
            "current_url": "http://courses.example.com/blah",
            "target_url": "/internal/example"
        })

        self.assert_single_map_output(line, (self.course_id, self.datestamp), 0)

        line = self.create_event_log_line(event={
            "current_url": "http://courses.example.com/blah",
            "target_url": "courses.example.com"
        })

        self.assert_single_map_output(line, (self.course_id, self.datestamp), 0)

        line = self.create_event_log_line(event={
            "current_url": "http://courses.example.com/blah",
            "target_url": "/"
        })

        self.assert_single_map_output(line, (self.course_id, self.datestamp), 0)


class LinkClickedEventMapTask(InitializeLegacyKeysMixin, unittest.TestCase):
    pass


class LinkClickedTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        self.task_class = LMSCoursewareLinkClickedTask
        super(LinkClickedTaskReducerTest, self).setUp()

        # Create the task locally, since we only need to check certain attributes
        self.create_link_clicked_task()
        self.user_id = 0
        self.course_id = 'foo/bar/baz'
        self.datestamp = "2013-12-17"
        self.reduce_key = (self.course_id, self.datestamp)

    def test_no_events(self):
        self._check_output_complete_tuple([], ((self.course_id, self.datestamp, 0, 0),))

    def create_link_clicked_task(self, interval='2013-01-01'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = LMSCoursewareLinkClickedTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
        )

    def test_multiple_events_for_course(self):
        inputs = [1, 1, 1, 1, 0, 0, 0]
        expected = ((self.course_id, self.datestamp, 4, 7),)
        self._check_output_complete_tuple(inputs, expected)

    def test_external_events_only(self):
        inputs = [1, 1, 1, 1, 1, 1, 1, 1]
        expected = ((self.course_id, self.datestamp, 8, 8),)
        self._check_output_complete_tuple(inputs, expected)

    def test_internal_events_only(self):
        inputs = [0, 0, 0]
        expected = ((self.course_id, self.datestamp, 0, 3),)
        self._check_output_complete_tuple(inputs, expected)
