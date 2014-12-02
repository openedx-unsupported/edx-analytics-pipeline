"""
Tests for tasks that collect enrollment events.

"""
import json

from edx.analytics.tasks.course_enroll import (
    CourseEnrollmentEventsPerDayMixin,
    CourseEnrollmentChangesPerDayMixin,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


class CourseEnrollEventMapTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.initialize_ids()
        self.task = CourseEnrollmentEventsPerDayMixin()
        self.user_id = 21
        self.timestamp = "2013-12-17T15:38:32.805444"

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        event_dict = {
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
        event_dict.update(**kwargs)
        return event_dict

    def assert_no_output_for(self, line):
        """Assert that an input line generates no output."""
        self.assertEquals(tuple(self.task.mapper(line)), tuple())

    def test_non_enrollment_event(self):
        line = 'this is garbage'
        self.assert_no_output_for(line)

    def test_unparseable_enrollment_event(self):
        line = 'this is garbage but contains edx.course.enrollment'
        self.assert_no_output_for(line)

    def test_missing_event_type(self):
        event_dict = self._create_event_dict()
        event_dict['old_event_type'] = event_dict['event_type']
        del event_dict['event_type']
        line = json.dumps(event_dict)
        self.assert_no_output_for(line)

    def test_nonenroll_event_type(self):
        line = self._create_event_log_line(event_type='edx.course.enrollment.unknown')
        self.assert_no_output_for(line)

    def test_bad_datetime(self):
        line = self._create_event_log_line(time='this is a bogus time')
        self.assert_no_output_for(line)

    def test_bad_event_data(self):
        line = self._create_event_log_line(event=["not an event"])
        self.assert_no_output_for(line)

    def test_illegal_course_id(self):
        line = self._create_event_log_line(event={"course_id": ";;;;bad/id/val", "user_id": self.user_id})
        self.assert_no_output_for(line)

    def test_missing_user_id(self):
        line = self._create_event_log_line(event={"course_id": self.course_id})
        self.assert_no_output_for(line)

    def test_anonymous_user(self):
        line = self._create_event_log_line(
            event_type='edx.course.enrollment.activated',
            username='anonymous'
        )
        self.assert_no_output_for(line)

    def test_good_enroll_event(self):
        line = self._create_event_log_line()
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, 1)),)
        self.assertEquals(event, expected)

    def test_good_unenroll_event(self):
        line = self._create_event_log_line(event_type='edx.course.enrollment.deactivated')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, -1)),)
        self.assertEquals(event, expected)


class CourseEnrollEventReduceTest(unittest.TestCase):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        self.task = CourseEnrollmentEventsPerDayMixin()
        self.key = ('course', 'user')

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def _check_output(self, inputs, expected):
        """Compare generated with expected output."""
        self.assertEquals(self._get_reducer_output(inputs), expected)

    def test_no_events(self):
        self._check_output([], tuple())

    def test_single_enrollment(self):
        inputs = [('2013-01-01T00:00:01', 1), ]
        expected = ((('course', '2013-01-01'), 1),)
        self._check_output(inputs, expected)

    def test_single_unenrollment(self):
        inputs = [('2013-01-01T00:00:01', -1), ]
        expected = ((('course', '2013-01-01'), -1),)
        self._check_output(inputs, expected)

    def test_multiple_events_on_same_day(self):
        # run first with no output expected:
        inputs = [
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', -1),
            ('2013-01-01T00:00:03', 1),
            ('2013-01-01T00:00:04', -1),
        ]
        expected = tuple()
        self._check_output(inputs, expected)

        # then run with output expected:
        inputs = [
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', -1),
            ('2013-01-01T00:00:03', -1),
            ('2013-01-01T00:00:04', 1),
        ]
        expected = ((('course', '2013-01-01'), 1),)
        self._check_output(inputs, expected)

    def test_multiple_events_out_of_order(self):
        # Make sure that events are sorted by the reducer.
        inputs = [
            ('2013-01-01T00:00:04', -1),
            ('2013-01-01T00:00:03', 1),
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', -1),
        ]
        expected = tuple()
        self._check_output(inputs, expected)

    def test_multiple_enroll_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', 1),
            ('2013-01-01T00:00:03', 1),
            ('2013-01-01T00:00:04', 1),
        ]
        expected = ((('course', '2013-01-01'), 1),)
        self._check_output(inputs, expected)

    def test_multiple_unenroll_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', -1),
            ('2013-01-01T00:00:02', -1),
            ('2013-01-01T00:00:03', -1),
            ('2013-01-01T00:00:04', -1),
        ]
        expected = ((('course', '2013-01-01'), -1),)
        self._check_output(inputs, expected)

    def test_multiple_enroll_events_on_many_days(self):
        inputs = [
            ('2013-01-01T00:00:01', 1),
            ('2013-01-01T00:00:02', 1),
            ('2013-01-02T00:00:03', 1),
            ('2013-01-02T00:00:04', 1),
            ('2013-01-04T00:00:05', 1),
        ]
        expected = ((('course', '2013-01-01'), 1),)
        self._check_output(inputs, expected)

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
        self._check_output(inputs, expected)


class CourseEnrollChangesReduceTest(unittest.TestCase):
    """
    Verify that CourseEnrollmentChangesPerDayMixin.reduce() works correctly.
    """
    def setUp(self):
        self.task = CourseEnrollmentChangesPerDayMixin()
        self.key = ('course', '2013-01-01')

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def test_no_user_counts(self):
        self.assertEquals(self._get_reducer_output([]), ((self.key, 0),))

    def test_single_user_count(self):
        self.assertEquals(self._get_reducer_output([1]), ((self.key, 1),))

    def test_multiple_user_count(self):
        inputs = [1, 1, 1, -1, 1]
        self.assertEquals(self._get_reducer_output(inputs), ((self.key, 3),))
