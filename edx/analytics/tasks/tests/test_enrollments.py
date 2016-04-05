"""Test enrollment computations"""

import json

import luigi

from edx.analytics.tasks.enrollments import (
    CourseEnrollmentTask,
    DEACTIVATED,
    ACTIVATED,
    MODE_CHANGED
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


class CourseEnrollmentTaskMapTest(MapperTestMixin, InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.task_class = CourseEnrollmentTask
        super(CourseEnrollmentTaskMapTest, self).setUp()

        self.initialize_ids()

        self.user_id = 21
        self.timestamp = "2013-12-17T15:38:32.805444"

        self.event_templates = {
            'enrollment_event': {
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

                }
            }
        }
        self.default_event_template = 'enrollment_event'

        self.expected_key = (self.course_id, self.user_id)

    def test_non_enrollment_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_unparseable_enrollment_event(self):
        line = 'this is garbage but contains edx.course.enrollment'
        self.assert_no_map_output_for(line)

    def test_missing_event_type(self):
        event_dict = self._create_event_dict()
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
        expected_value = (self.timestamp, ACTIVATED, 'honor')
        self.assert_single_map_output(line, self.expected_key, expected_value)

    def test_good_unenroll_event(self):
        line = self.create_event_log_line(event_type=DEACTIVATED)
        expected_value = (self.timestamp, DEACTIVATED, 'honor')
        self.assert_single_map_output(line, self.expected_key, expected_value)


class CourseEnrollmentTaskMapTest(InitializeLegacyKeysMixin, unittest.TestCase):
    pass


class CourseEnrollmentTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        self.task_class = CourseEnrollmentTask
        super(CourseEnrollmentTaskReducerTest, self).setUp()

        # Create the task locally, since we only need to check certain attributes
        self.create_enrollment_task()
        self.user_id = 0
        self.course_id = 'foo/bar/baz'
        self.reduce_key = (self.course_id, self.user_id)

    def test_no_events(self):
        self.assert_no_output([])

    def test_single_enrollment(self):
        inputs = [('2013-01-01T00:00:01', ACTIVATED, 'honor'), ]
        expected = (('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),)
        self._check_output_complete_tuple(inputs, expected)

    def create_enrollment_task(self, interval='2013-01-01'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
        )

    def test_single_unenrollment(self):
        inputs = [('2013-01-01T00:00:01', DEACTIVATED, 'honor'), ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, 0, 'honor'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:02', DEACTIVATED, 'honor'),
            ('2013-01-01T00:00:03', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:04', DEACTIVATED, 'honor'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, 0, 'honor'),)
        self._check_output_complete_tuple(inputs, expected)

        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:02', DEACTIVATED, 'honor'),
            ('2013-01-01T00:00:03', DEACTIVATED, 'honor'),
            ('2013-01-01T00:00:04', ACTIVATED, 'honor'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_oversized_interval_unenrollment(self):
        self.create_enrollment_task('2012-12-30-2013-01-04')
        inputs = [
            ('2013-01-01T00:00:01', DEACTIVATED, 'honor'),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-03', self.course_id, self.user_id, 0, 0, 'honor'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_oversized_interval_enrollment(self):
        self.create_enrollment_task('2012-12-30-2013-01-04')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 1, 0, 'honor'),
            ('2013-01-03', self.course_id, self.user_id, 1, 0, 'honor'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_missing_days(self):
        self.create_enrollment_task('2012-12-30-2013-01-07')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-04T00:00:01', DEACTIVATED, 'honor'),
            ('2013-01-06T00:00:01', ACTIVATED, 'honor'),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 1, 0, 'honor'),
            ('2013-01-03', self.course_id, self.user_id, 1, 0, 'honor'),
            ('2013-01-04', self.course_id, self.user_id, 0, -1, 'honor'),
            ('2013-01-05', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-06', self.course_id, self.user_id, 1, 1, 'honor'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_events_out_of_order(self):
        # Make sure that events are sorted by the reducer.
        inputs = [
            ('2013-01-01T00:00:04', DEACTIVATED, 'honor'),
            ('2013-01-01T00:00:03', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:02', DEACTIVATED, 'honor'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, 0, 'honor'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_enroll_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:02', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:03', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:04', ACTIVATED, 'honor'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_unenroll_events_on_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', DEACTIVATED, 'honor'),
            ('2013-01-01T00:00:02', DEACTIVATED, 'honor'),
            ('2013-01-01T00:00:03', DEACTIVATED, 'honor'),
            ('2013-01-01T00:00:04', DEACTIVATED, 'honor'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, 0, 'honor'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_enroll_events_on_many_days(self):
        self.create_enrollment_task('2013-01-01-2013-01-05')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:02', ACTIVATED, 'honor'),
            ('2013-01-02T00:00:03', ACTIVATED, 'honor'),
            ('2013-01-02T00:00:04', ACTIVATED, 'honor'),
            ('2013-01-04T00:00:05', DEACTIVATED, 'honor'),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 1, 0, 'honor'),
            ('2013-01-03', self.course_id, self.user_id, 1, 0, 'honor'),
            ('2013-01-04', self.course_id, self.user_id, 0, -1, 'honor'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_events_on_many_days(self):
        self.create_enrollment_task('2013-01-01-2013-01-10')
        inputs = [
            ('2013-01-01T1', ACTIVATED, 'honor'),
            ('2013-01-01T2', DEACTIVATED, 'honor'),
            ('2013-01-01T3', ACTIVATED, 'honor'),
            ('2013-01-01T4', DEACTIVATED, 'honor'),
            ('2013-01-02', ACTIVATED, 'honor'),
            ('2013-01-03', ACTIVATED, 'honor'),
            ('2013-01-04T1', ACTIVATED, 'honor'),
            ('2013-01-04T2', DEACTIVATED, 'honor'),
            ('2013-01-05', DEACTIVATED, 'honor'),
            ('2013-01-06', DEACTIVATED, 'honor'),
            ('2013-01-07', ACTIVATED, 'honor'),
            ('2013-01-08T1', ACTIVATED, 'honor'),
            ('2013-01-08T2', ACTIVATED, 'honor'),
            ('2013-01-09T1', DEACTIVATED, 'honor'),
            ('2013-01-09T2', DEACTIVATED, 'honor'),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-03', self.course_id, self.user_id, 1, 0, 'honor'),
            ('2013-01-04', self.course_id, self.user_id, 0, -1, 'honor'),
            ('2013-01-05', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-06', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-07', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-08', self.course_id, self.user_id, 1, 0, 'honor'),
            ('2013-01-09', self.course_id, self.user_id, 0, -1, 'honor'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_oversized_interval_both_sides(self):
        self.create_enrollment_task('2012-12-30-2013-01-06')
        inputs = [
            ('2013-01-01T00:00:01', DEACTIVATED, 'honor'),
            ('2013-01-03T00:00:01', ACTIVATED, 'honor'),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-03', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-04', self.course_id, self.user_id, 1, 0, 'honor'),
            ('2013-01-05', self.course_id, self.user_id, 1, 0, 'honor'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_oversized_interval_both_sides_unenrolled_at_end(self):
        self.create_enrollment_task('2012-12-30-2013-01-06')
        inputs = [
            ('2013-01-01T00:00:01', DEACTIVATED, 'honor'),
            ('2013-01-03T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-04T00:00:01', DEACTIVATED, 'honor'),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 0, 0, 'honor'),
            ('2013-01-03', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-04', self.course_id, self.user_id, 0, -1, 'honor'),
            ('2013-01-05', self.course_id, self.user_id, 0, 0, 'honor'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_mode_change_same_day(self):
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:02', DEACTIVATED, 'honor'),
            ('2013-01-01T00:00:03', ACTIVATED, 'verified'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 1, 1, 'verified'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_mode_change_multi_day(self):
        self.create_enrollment_task('2013-01-01-2013-01-03')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-02T00:00:02', DEACTIVATED, 'honor'),
            ('2013-01-02T00:00:03', ACTIVATED, 'verified'),
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 1, 0, 'verified'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_mode_change_missed_event(self):
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:03', ACTIVATED, 'verified'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 1, 1, 'verified'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_mode_change_first_deactivated(self):
        inputs = [
            ('2013-01-01T00:00:01', DEACTIVATED, 'audit'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, 0, 'audit'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_mode_change_last_deactivated(self):
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-01T00:00:02', DEACTIVATED, 'audit'),
        ]
        expected = (('2013-01-01', self.course_id, self.user_id, 0, 0, 'audit'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_explicit_mode_change_multi_day(self):
        self.create_enrollment_task('2013-01-01-2013-01-03')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-02T00:00:02', MODE_CHANGED, 'verified')
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 1, 0, 'verified'),
        )
        self._check_output_complete_tuple(inputs, expected)

    def test_explicit_mode_change_multiple(self):
        self.create_enrollment_task('2013-01-01-2013-01-03')
        inputs = [
            ('2013-01-01T00:00:01', ACTIVATED, 'honor'),
            ('2013-01-02T00:00:02', MODE_CHANGED, 'verified'),
            ('2013-01-02T00:00:03', MODE_CHANGED, 'honor'),
            ('2013-01-02T00:00:04', MODE_CHANGED, 'audit')
        ]
        expected = (
            ('2013-01-01', self.course_id, self.user_id, 1, 1, 'honor'),
            ('2013-01-02', self.course_id, self.user_id, 1, 0, 'audit'),
        )
        self._check_output_complete_tuple(inputs, expected)
