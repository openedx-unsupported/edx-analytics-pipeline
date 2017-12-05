"""
Tests for event export by course tasks
"""
from unittest import TestCase

from opaque_keys.edx.locator import CourseLocator

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin
from edx.analytics.tasks.export.event_exports_by_course import EventExportByCourseTask
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeLegacyKeysMixin, InitializeOpaqueKeysMixin


class EventExportByCourseBaseTest(InitializeOpaqueKeysMixin, MapperTestMixin, TestCase):
    """Base class for EventExportByCourse Task test."""

    DATE = '2013-12-17'

    def setUp(self):
        self.task_class = EventExportByCourseTask
        super(EventExportByCourseBaseTest, self).setUp()
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
        self.expected_key = (self.DATE, self.encoded_course_id)


class EventExportByCourseMapTest(EventExportByCourseBaseTest):
    """Test fot EventExportByCourse mapper()"""

    def test_bad_event(self):
        line = "some garbage"
        self.assert_no_map_output_for(line)

    def test_missing_course_id(self):
        line = self.create_event_log_line(context={"course_id": ''})
        self.assert_no_map_output_for(line)

    def test_single_output(self):
        line = self.create_event_log_line()
        self.assert_single_map_output(line, self.expected_key, line)

    def test_without_timestamp(self):
        line = self.create_event_log_line(time='')
        self.assert_no_map_output_for(line)

    def test_output_for_unwanted_event(self):
        self.create_task(course=['Foo'])
        line = self.create_event_log_line()
        self.assert_no_map_output_for(line)


class EventExportByCourseLegacyMapTest(InitializeLegacyKeysMixin, EventExportByCourseMapTest):
    """Run same mapper() tests, but using legacy values for keys."""
    pass


class EventExportByCourseOutputPathTest(EventExportByCourseBaseTest):
    """Test for output_path for EventExportByCourse Task."""

    def test_output_path_for_legacy_key(self):
        self.course_id = 'Foo/Bar/Baz'
        path = self.task.output_path_for_key((self.DATE, self.course_id))
        self.assertEquals('/fake/output/Foo_Bar_Baz/events/Foo_Bar_Baz-events-2013-12-17.log.gz', path)

    def test_output_path_for_opaque_key(self):
        self.course_id = str(CourseLocator(org='Foo', course='Bar', run='Baz'))
        path = self.task.output_path_for_key((self.DATE, self.course_id))
        self.assertEquals('/fake/output/Foo_Bar_Baz/events/Foo_Bar_Baz-events-2013-12-17.log.gz', path)
