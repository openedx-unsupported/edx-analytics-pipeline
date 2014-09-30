"""Test enrollment computations"""

import json

import luigi

from edx.analytics.tasks.enrollments import (
    CourseEnrollmentTask,
    DEACTIVATED,
    ACTIVATED,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


class CourseEnrollmentTaskMapTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.initialize_ids()

        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentTask(
            interval=fake_param.parse('2013-12-17'),
            output_root='/fake/output'
        )
        self.task.init_local()

        self.user_id = 21
        self.timestamp = "2013-12-17T15:38:32.805444"

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        org_id = self.course_id.split('/')[0]
        event_dict = {
            "username": "test_user",
            "host": "test_host",
            "event_source": "server",
            "event_type": "edx.course.enrollment.activated",
            "context": {
                "course_id": self.course_id,
                "org_id": org_id,
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

    def test_good_enroll_event(self):
        line = self._create_event_log_line()
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, ACTIVATED)),)
        self.assertEquals(event, expected)

    def test_good_unenroll_event(self):
        line = self._create_event_log_line(event_type=DEACTIVATED)
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, DEACTIVATED)),)
        self.assertEquals(event, expected)


class CourseEnrollmentTaskReducerTest(unittest.TestCase):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        self.create_task()
        self.user_id = 0
        self.course_id = 'foo/bar/baz'
        self.key = (self.course_id, self.user_id)

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def _check_output(self, inputs, expected):
        """Compare generated with expected output."""
        self.assertEquals(self._get_reducer_output(inputs), expected)

    def test_no_events(self):
        self._check_output([], tuple())

    def test_single_enrollment(self):
        inputs = [('2013-01-01T00:00:01', ACTIVATED), ]
        expected = (('2013-01-01', self.course_id, self.user_id, 1, 1),)
        self._check_output(inputs, expected)

    def create_task(self, interval='2013-01-01'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
        )

    def test_single_unenrollment(self):
        inputs = [('2013-01-01T00:00:01', DEACTIVATED), ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, -1),)
        self._check_output(inputs, expected)

    def test_multiple_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED),
            ('2013-01-01T00:00:02', DEACTIVATED),
            ('2013-01-01T00:00:03', ACTIVATED),
            ('2013-01-01T00:00:04', DEACTIVATED),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, 0),)
        self._check_output(inputs, expected)

        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED),
            ('2013-01-01T00:00:02', DEACTIVATED),
            ('2013-01-01T00:00:03', DEACTIVATED),
            ('2013-01-01T00:00:04', ACTIVATED),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 1, 1),)
        self._check_output(inputs, expected)

    def test_oversized_interval_unenrollment(self):
        self.create_task('2012-12-30-2013-01-04')
        inputs = [
            ('2013-01-01T00:00:01', DEACTIVATED),
        ]
        expected = (
            ('2012-12-30', self.course_id, self.user_id, 1, 1),
            ('2012-12-31', self.course_id, self.user_id, 1, 0),
            ('2013-01-01', self.course_id, self.user_id, 0, -1),
        )
        self._check_output(inputs, expected)

    def test_oversized_interval_enrollment(self):
        self.create_task('2012-12-30-2013-01-04')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1),
            ('2013-01-02', self.course_id, self.user_id, 1, 0),
            ('2013-01-03', self.course_id, self.user_id, 1, 0),
        )
        self._check_output(inputs, expected)

    def test_missing_days(self):
        self.create_task('2012-12-30-2013-01-07')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED),
            ('2013-01-04T00:00:01', DEACTIVATED),
            ('2013-01-06T00:00:01', ACTIVATED),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1),
            ('2013-01-02', self.course_id, self.user_id, 1, 0),
            ('2013-01-03', self.course_id, self.user_id, 1, 0),
            ('2013-01-04', self.course_id, self.user_id, 0, -1),
            ('2013-01-06', self.course_id, self.user_id, 1, 1),
        )
        self._check_output(inputs, expected)

    def test_multiple_events_out_of_order(self):
        # Make sure that events are sorted by the reducer.
        inputs = [
            ('2013-01-01T00:00:04', DEACTIVATED),
            ('2013-01-01T00:00:03', ACTIVATED),
            ('2013-01-01T00:00:01', ACTIVATED),
            ('2013-01-01T00:00:02', DEACTIVATED),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, 0),)
        self._check_output(inputs, expected)

    def test_multiple_enroll_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED),
            ('2013-01-01T00:00:02', ACTIVATED),
            ('2013-01-01T00:00:03', ACTIVATED),
            ('2013-01-01T00:00:04', ACTIVATED),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 1, 1),)
        self._check_output(inputs, expected)

    def test_multiple_unenroll_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', DEACTIVATED),
            ('2013-01-01T00:00:02', DEACTIVATED),
            ('2013-01-01T00:00:03', DEACTIVATED),
            ('2013-01-01T00:00:04', DEACTIVATED),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, -1),)
        self._check_output(inputs, expected)

    def test_multiple_enroll_events_on_many_days(self):
        self.create_task('2013-01-01-2013-01-05')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED),
            ('2013-01-01T00:00:02', ACTIVATED),
            ('2013-01-02T00:00:03', ACTIVATED),
            ('2013-01-02T00:00:04', ACTIVATED),
            ('2013-01-04T00:00:05', DEACTIVATED),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1),
            ('2013-01-02', self.course_id, self.user_id, 1, 0),
            ('2013-01-03', self.course_id, self.user_id, 1, 0),
            ('2013-01-04', self.course_id, self.user_id, 0, -1),
        )
        self._check_output(inputs, expected)

    def test_multiple_events_on_many_days(self):
        self.create_task('2013-01-01-2013-01-10')
        inputs = [
            ('2013-01-01T1', ACTIVATED),
            ('2013-01-01T2', DEACTIVATED),
            ('2013-01-01T3', ACTIVATED),
            ('2013-01-01T4', DEACTIVATED),
            ('2013-01-02', ACTIVATED),
            ('2013-01-03', ACTIVATED),
            ('2013-01-04T1', ACTIVATED),
            ('2013-01-04T2', DEACTIVATED),
            ('2013-01-05', DEACTIVATED),
            ('2013-01-06', DEACTIVATED),
            ('2013-01-07', ACTIVATED),
            ('2013-01-08T1', ACTIVATED),
            ('2013-01-08T2', ACTIVATED),
            ('2013-01-09T1', DEACTIVATED),
            ('2013-01-09T2', DEACTIVATED),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 0, 0),
            ('2013-01-02', self.course_id, self.user_id, 1, 1),
            ('2013-01-03', self.course_id, self.user_id, 1, 0),
            ('2013-01-04', self.course_id, self.user_id, 0, -1),
            ('2013-01-05', self.course_id, self.user_id, 0, 0),
            ('2013-01-06', self.course_id, self.user_id, 0, 0),
            ('2013-01-07', self.course_id, self.user_id, 1, 1),
            ('2013-01-08', self.course_id, self.user_id, 1, 0),
            ('2013-01-09', self.course_id, self.user_id, 0, -1),
        )
        self._check_output(inputs, expected)

    def test_oversized_interval_both_sides(self):
        self.create_task('2012-12-30-2013-01-06')
        inputs = [
            ('2013-01-01T00:00:01', DEACTIVATED),
            ('2013-01-03T00:00:01', ACTIVATED),
        ]
        expected = (
            ('2012-12-30', self.course_id, self.user_id, 1, 1),
            ('2012-12-31', self.course_id, self.user_id, 1, 0),
            ('2013-01-01', self.course_id, self.user_id, 0, -1),
            ('2013-01-03', self.course_id, self.user_id, 1, 1),
            ('2013-01-04', self.course_id, self.user_id, 1, 0),
            ('2013-01-05', self.course_id, self.user_id, 1, 0),
        )
        self._check_output(inputs, expected)
