"""
Tests for tasks that collect enrollment events.

"""
import json

from edx.analytics.tasks.course_enroll import (
    CourseEnrollmentEventsPerDayMixin,
    CourseEnrollmentChangesPerDayMixin,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


class CourseEnrollEventMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.task_class = CourseEnrollmentEventsPerDayMixin

        super(CourseEnrollEventMapTest, self).setUp()

        self.initialize_ids()
        self.user_id = 21
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.event_templates = {
            'course_enroll': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "edx.course.enrollment.activated",
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
        self.default_event_template = 'course_enroll'
        self.expected_key = (self.course_id, self.user_id)

    def test_non_enrollment_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_unparseable_enrollment_event(self):
        line = 'this is garbage but contains edx.course.enrollment'
        self.assert_no_map_output_for(line)

    def test_missing_event_type(self):
        event_dict = self.create_event_dict()
        event_dict['old_event_type'] = event_dict['event_type']
        del event_dict['event_type']
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    def test_nonenroll_event_type(self):
        line = self.create_event_log_line(event_type='edx.course.enrollment.unknown')
        self.assert_no_map_output_for(line)

    def test_bad_datetime(self):
        line = self.create_event_log_line(time='this is a bogus time')
        self.assert_no_map_output_for(line)

    def test_bad_event_data(self):
        line = self.create_event_log_line(event=["not an event"])
        self.assert_no_map_output_for(line)

    def test_illegal_course_id(self):
        line = self.create_event_log_line(event={"course_id": ";;;;bad/id/val", "user_id": self.user_id})
        self.assert_no_map_output_for(line)

    def test_missing_user_id(self):
        line = self.create_event_log_line(event={"course_id": self.course_id})
        self.assert_no_map_output_for(line)

    def test_good_enroll_event(self):
        line = self.create_event_log_line()
        expected_value = (self.timestamp, 1)
        self.assert_single_map_output(line, self.expected_key, expected_value)

    def test_good_unenroll_event(self):
        line = self.create_event_log_line(event_type='edx.course.enrollment.deactivated')
        expected_value = (self.timestamp, -1)
        self.assert_single_map_output(line, self.expected_key, expected_value)


class CourseEnrollEventReduceTest(unittest.TestCase, ReducerTestMixin):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        super(CourseEnrollEventReduceTest, self).setUp()

        self.task = CourseEnrollmentEventsPerDayMixin()
        self.reduce_key = ('course', 'user')

    def test_single_enrollment(self):
        inputs = [('2013-01-01T00:00:01', 1), ]
        expected = ((('course', '2013-01-01'), 1),)
        self._check_output_complete_tuple(inputs, expected)

    def test_single_unenrollment(self):
        inputs = [('2013-01-01T00:00:01', -1), ]
        expected = ((('course', '2013-01-01'), -1),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_events_on_same_day(self):
        # run first with no output expected:
        inputs = [
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', -1),
            ('2013-01-01T00:00:03', 1),
            ('2013-01-01T00:00:04', -1),
        ]
        self.assert_no_output(inputs)

        # then run with output expected:
        inputs = [
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', -1),
            ('2013-01-01T00:00:03', -1),
            ('2013-01-01T00:00:04', 1),
        ]
        expected = ((('course', '2013-01-01'), 1),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_events_out_of_order(self):
        # Make sure that events are sorted by the reducer.
        inputs = [
            ('2013-01-01T00:00:04', -1),
            ('2013-01-01T00:00:03', 1),
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', -1),
        ]
        self.assert_no_output(inputs)

    def test_multiple_enroll_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', 1),
            ('2013-01-01T00:00:03', 1),
            ('2013-01-01T00:00:04', 1),
        ]
        expected = ((('course', '2013-01-01'), 1),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_unenroll_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', -1),
            ('2013-01-01T00:00:02', -1),
            ('2013-01-01T00:00:03', -1),
            ('2013-01-01T00:00:04', -1),
        ]
        expected = ((('course', '2013-01-01'), -1),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_enroll_events_on_many_days(self):
        inputs = [
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', 1),
            ('2013-01-02T00:00:03', 1),
            ('2013-01-02T00:00:04', 1),
            ('2013-01-04T00:00:05', 1),
        ]
        expected = ((('course', '2013-01-01'), 1),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_events_on_many_days(self):
        # Run with an arbitrary list of events.
        inputs = [
            ('2013-01-01T1', 1),
            ('2013-01-01T2', -1),
            ('2013-01-01T3', 1),
            ('2013-01-01T4', -1),
            ('2013-01-02', 1),
            ('2013-01-03', 1),
            ('2013-01-04T1', 1),
            ('2013-01-04T2', -1),
            ('2013-01-05', -1),
            ('2013-01-06', -1),
            ('2013-01-07', 1),
            ('2013-01-08T1', 1),
            ('2013-01-08T2', 1),
            ('2013-01-09T1', -1),
            ('2013-01-09T2', -1),
        ]
        expected = (
            (('course', '2013-01-02'), 1),
            (('course', '2013-01-04'), -1),
            (('course', '2013-01-07'), 1),
            (('course', '2013-01-09'), -1),
        )
        self._check_output_complete_tuple(inputs, expected)


class CourseEnrollChangesReduceTest(ReducerTestMixin, unittest.TestCase):
    """
    Verify that CourseEnrollmentChangesPerDayMixin.reduce() works correctly.
    """
    def setUp(self):
        self.task_class = CourseEnrollmentChangesPerDayMixin
        super(CourseEnrollChangesReduceTest, self).setUp()

        self.reduce_key = ('course', '2013-01-01')

    def test_no_user_counts(self):
        expected = ((self.reduce_key, 0),)
        self._check_output_complete_tuple([], expected)

    def test_single_user_count(self):
        expected = ((self.reduce_key, 1),)
        self._check_output_complete_tuple([1], expected)

    def test_multiple_user_count(self):
        inputs = [1, 1, 1, -1, 1]
        expected = ((self.reduce_key, 3),)
        self._check_output_complete_tuple(inputs, expected)
