"""Test enrollment computations"""

import json

import luigi

from edx.analytics.tasks.enrollment_validation import (
    CourseEnrollmentValidationTask,
    DEACTIVATED,
    ACTIVATED,
    VALIDATED,
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin
from edx.analytics.tasks.util.datetime_util import add_microseconds
from edx.analytics.tasks.util.event_factory import SyntheticEventFactory


class CourseEnrollmentValidationTaskMapTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.initialize_ids()

        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentValidationTask(
            interval=fake_param.parse('2013-12-17'),
            output_root='/fake/output'
        )
        self.task.init_local()

        self.user_id = 21
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.mode = 'honor'

        self.factory = SyntheticEventFactory(
            timestamp=self.timestamp,
            event_source='server',
            event_type=ACTIVATED,
            synthesizer='enrollment_from_db',
            reason='db entry',
            user_id=self.user_id,
            course_id=self.course_id,
            org_id=self.org_id,
        )

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        event_data = {
            'course_id': self.course_id,
            'user_id': self.user_id,
            'mode': self.mode,
        }
        event = self.factory.create_event_dict(event_data, **kwargs)
        return event

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
        expected = (((self.course_id, self.user_id), (self.timestamp, ACTIVATED, self.mode, None)),)
        self.assertEquals(event, expected)

    def test_good_unenroll_event(self):
        line = self._create_event_log_line(event_type=DEACTIVATED)
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, DEACTIVATED, self.mode, None)),)
        self.assertEquals(event, expected)

    def test_good_validation_event(self):
        validation_info = {
            'is_active': True,
            'created': '2012-07-24T12:37:32.000000',
            'dump_start': '2014-10-08T04:52:48.154228',
            'dump_end': '2014-10-08T04:57:38.145282',
        }
        event_info = {
            "course_id": self.course_id,
            "user_id": self.user_id,
            "mode": self.mode,
        }
        event_info.update(validation_info)
        line = self._create_event_log_line(event_type=VALIDATED, event=event_info)
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.user_id), (self.timestamp, VALIDATED, self.mode, validation_info)),)
        self.assertEquals(event, expected)


class CourseEnrollmentValidationTaskLegacyMapTest(InitializeLegacyKeysMixin, CourseEnrollmentValidationTaskMapTest):
    """TODO:"""
    pass


class BaseCourseEnrollmentValidationTaskReducerTest(unittest.TestCase):
    """Provide common methods for testing CourseEnrollmentValidationTask reducer."""

    def setUp(self):
        self.mode = 'Honor'

    @property
    def key(self):
        """Returns key value to simulate output from mapper to pass to reducer."""
        user_id = 0
        course_id = 'foo/bar/baz'
        return (course_id, user_id)

    def create_task(self, generate_before=True, event_output=False):
        """Create a task for testing purposes."""
        interval = '2013-01-01-2014-10-10'
        fake_param = luigi.DateIntervalParameter()
        self.task = CourseEnrollmentValidationTask(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
            generate_before=generate_before,
            event_output=event_output,
        )
        self.task.init_local()

    def _activated(self, timestamp):
        """Creates an ACTIVATED event."""
        return (timestamp, ACTIVATED, self.mode, None)

    def _deactivated(self, timestamp):
        """Creates a DEACTIVATED event."""
        return (timestamp, DEACTIVATED, self.mode, None)

    def _validated(self, timestamp, is_active, created, dump_duration_in_secs=300):
        """Creates a VALIDATED event."""
        dump_end = timestamp
        dump_start = add_microseconds(timestamp, int(dump_duration_in_secs) * -100000)
        validation_info = {
            'is_active': is_active,
            'created': created,
            'dump_start': dump_start,
            'dump_end': dump_end,
        }
        return (timestamp, VALIDATED, self.mode, validation_info)

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def check_output(self, inputs, expected):
        """Compare generated with expected output."""
        expected_with_key = tuple([(key, self.key + value) for key, value in expected])
        self.assertEquals(self._get_reducer_output(inputs), expected_with_key)


class CourseEnrollmentValidationTaskReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        super(CourseEnrollmentValidationTaskReducerTest, self).setUp()
        self.create_task(generate_before=True)

    def test_no_events(self):
        self.check_output([], tuple())

    def test_missing_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_enroll_unenroll(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing deactivation (between 4/1 and 9/1)
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(inactive)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
            ('2013-04-01',
             ('2013-04-01T00:00:01.123457', DEACTIVATED, "start => validate(inactive)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_single_unenrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123457', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-05-01T00:00:01.123456'),
            # missing activation
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "start => deactivate",
              '2013-04-01T00:00:01.123456', '2013-05-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_single_unenrollment_without_ms(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123457', False, '2013-04-01T00:00:01'),
            self._deactivated('2013-05-01T00:00:01.123456')
            # missing activation
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01', ACTIVATED, "start => deactivate",
              '2013-04-01T00:00:01', '2013-05-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_single_unvalidated_unenrollment(self):
        inputs = [
            self._deactivated('2013-05-01T00:00:01.123456')
            # missing activation
        ]
        expected = (
            ('2013-05-01',
             ('2013-05-01T00:00:01.123455', ACTIVATED, "start => deactivate",
              None, '2013-05-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_rollover_unvalidated_unenrollment(self):
        inputs = [
            self._deactivated('2013-05-01T00:00:01.000000')
            # missing activation
        ]
        expected = (
            ('2013-05-01',
             ('2013-05-01T00:00:00.999999', ACTIVATED, "start => deactivate",
              None, '2013-05-01T00:00:01.000000')),
        )
        self.check_output(inputs, expected)

    def test_unvalidated_unenrollment_without_ms(self):
        inputs = [
            self._deactivated('2013-05-01T00:00:01'),
            # missing activation
        ]
        expected = (
            ('2013-05-01',
             ('2013-05-01T00:00:00.999999', ACTIVATED, "start => deactivate",
              None, '2013-05-01T00:00:01')),
        )
        self.check_output(inputs, expected)

    def test_single_enroll_unenroll(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-05-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_unenroll_during_dump(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:00.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_unenroll_during_dump_reverse(self):
        inputs = [
            self._activated('2013-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:00.123456'),
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_single_unenroll_enroll(self):
        inputs = [
            self._activated('2013-09-01T00:00:01.123456'),
            self._deactivated('2013-05-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_multiple_validation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._validated('2013-07-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_multiple_validation_without_enroll(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._validated('2013-07-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-07-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_enroll_unenroll_with_validations(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._validated('2013-07-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_missing_activate_between_validation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation
            self._validated('2013-08-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-05-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', ACTIVATED, "validate(inactive) => validate(active)",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_deactivate_between_validation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', DEACTIVATED, "validate(active) => validate(inactive)",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_deactivate_from_validation(self):
        inputs = [
            self._activated('2013-09-01T00:00:01.123456'),
            # missing deactivation
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', DEACTIVATED, "validate(active) => activate",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_deactivate_from_activation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123457', DEACTIVATED, "activate => validate(inactive)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_rollover_deactivate_from_activation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.999999'),
            # missing deactivation
            self._activated('2013-04-01T00:00:01.999999'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:02.000000', DEACTIVATED, "activate => validate(inactive)",
              '2013-04-01T00:00:01.999999', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_activate_from_deactivation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', ACTIVATED, "deactivate => validate(active)",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_activate_between_deactivation(self):
        inputs = [
            self._deactivated('2013-09-01T00:00:01.123456'),
            # missing activation
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', ACTIVATED, "deactivate => deactivate",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_deactivate_between_activation(self):
        inputs = [
            self._activated('2013-09-01T00:00:01.123456'),
            # missing deactivation
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-01-01',
             ('2013-01-01T00:00:01.123457', DEACTIVATED, "activate => activate",
              '2013-01-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_activate_from_validation(self):
        inputs = [
            self._deactivated('2013-10-01T00:00:01.123456'),
            # missing activation
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-09-01',
             ('2013-09-01T00:00:01.123457', ACTIVATED, "validate(inactive) => deactivate",
              '2013-09-01T00:00:01.123456', '2013-10-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)


class CourseEnrollmentValidationTaskEventReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        super(CourseEnrollmentValidationTaskEventReducerTest, self).setUp()
        self.create_task(event_output=True)

    def test_missing_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        events = self._get_reducer_output(inputs)
        self.assertEquals(len(events), 1)
        datestamp, encoded_event = events[0]
        self.assertEquals(datestamp, '2013-04-01')
        event = json.loads(encoded_event)

        self.assertEquals(event.get('event_type'), ACTIVATED)
        self.assertEquals(event.get('time'), '2013-04-01T00:00:01.123456')

        synthesized = event.get('synthesized')
        self.assertEquals(synthesized.get('reason'), "start => validate(active)")
        self.assertEquals(synthesized.get('after_time'), '2013-04-01T00:00:01.123456')
        self.assertEquals(synthesized.get('before_time'), '2013-09-01T00:00:01.123456')


class GenerateBeforeDisabledTaskReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events before first validation event are properly skipped.
    """
    def setUp(self):
        super(GenerateBeforeDisabledTaskReducerTest, self).setUp()
        self.create_task(generate_before=False)

    def test_no_events(self):
        self.check_output([], tuple())

    def test_missing_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_early_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2012-04-01T00:00:01.123456'),
            # missing activation (4/1/12)
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_single_deactivation(self):
        inputs = [
            self._deactivated('2013-10-01T00:00:01.123456'),
            # missing activation
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_missing_deactivate_from_activation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # NO LONGER expect no event.
        # self.check_output(inputs, tuple())
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123457', DEACTIVATED, "activate => validate(inactive)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_activate_from_validation(self):
        inputs = [
            self._validated('2013-10-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "start => deactivate",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_missing_early_activate_from_validation(self):
        inputs = [
            self._validated('2013-10-01T00:00:01.123456', False, '2012-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:01.123456'),
            # missing activation (4/1/12)
        ]
        # expect no event.
        self.check_output(inputs, tuple())

    def test_missing_activate_after_validation(self):
        inputs = [
            self._deactivated('2013-10-01T00:00:01.123456'),
            # missing activation
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-09-01',
             ('2013-09-01T00:00:01.123457', ACTIVATED, "validate(inactive) => deactivate",
              '2013-09-01T00:00:01.123456', '2013-10-01T00:00:01.123456')),
        )
        self.check_output(inputs, expected)

    def test_unenroll_during_dump(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:00.123456'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:00.123455')),
        )
        self.check_output(inputs, expected)
