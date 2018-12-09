"""Test enrollment computations"""

import datetime
import json
import os
import shutil
import tempfile
from unittest import TestCase

import luigi

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.monitor.enrollment_validation import (
    ACTIVATED, DEACTIVATED, MODE_CHANGED, VALIDATED, CourseEnrollmentValidationTask,
    CreateAllEnrollmentValidationEventsTask, CreateEnrollmentValidationEventsForTodayTask
)
from edx.analytics.tasks.util.datetime_util import add_microseconds
from edx.analytics.tasks.util.event_factory import SyntheticEventFactory
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeLegacyKeysMixin, InitializeOpaqueKeysMixin


class CourseEnrollmentValidationTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.task_class = CourseEnrollmentValidationTask
        super(CourseEnrollmentValidationTaskMapTest, self).setUp()

        self.initialize_ids()

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

        self.expected_key = (self.encoded_course_id, self.user_id)

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        event_data = {
            'course_id': self.course_id,
            'user_id': self.user_id,
            'mode': self.mode,
        }
        event = self.factory.create_event_dict(event_data, **kwargs)
        return event

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
        line = self._create_event_log_line(event_type='edx.course.enrollment.unknown')
        self.assert_no_map_output_for(line)

    def test_bad_datetime(self):
        line = self._create_event_log_line(time='this is a bogus time')
        self.assert_no_map_output_for(line)

    def test_bad_event_data(self):
        line = self._create_event_log_line(event=["not an event"])
        self.assert_no_map_output_for(line)

    def test_illegal_course_id(self):
        line = self._create_event_log_line(event={"course_id": ";;;;bad/id/val", "user_id": self.user_id})
        self.assert_no_map_output_for(line)

    def test_missing_user_id(self):
        line = self._create_event_log_line(event={"course_id": self.course_id})
        self.assert_no_map_output_for(line)

    def test_good_enroll_event(self):
        line = self._create_event_log_line()
        expected_value = (self.timestamp, ACTIVATED, self.mode, None)
        self.assert_single_map_output(line, self.expected_key, expected_value)

    def test_good_unenroll_event(self):
        line = self._create_event_log_line(event_type=DEACTIVATED)
        expected_value = (self.timestamp, DEACTIVATED, self.mode, None)
        self.assert_single_map_output(line, self.expected_key, expected_value)

    def test_good_mode_change_event(self):
        line = self._create_event_log_line(event_type=MODE_CHANGED)
        expected_value = (self.timestamp, MODE_CHANGED, self.mode, None)
        self.assert_single_map_output(line, self.expected_key, expected_value)

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
        expected_value = (self.timestamp, VALIDATED, self.mode, validation_info)
        self.assert_single_map_output(line, self.expected_key, expected_value)


class CourseEnrollmentValidationTaskLegacyMapTest(InitializeLegacyKeysMixin, CourseEnrollmentValidationTaskMapTest):
    """Tests to verify that event log parsing by mapper works correctly with legacy course_id."""
    pass


class BaseCourseEnrollmentValidationTaskReducerTest(ReducerTestMixin, TestCase):
    """Provide common methods for testing CourseEnrollmentValidationTask reducer."""

    def setUp(self):
        self.task_class = CourseEnrollmentValidationTask
        super(BaseCourseEnrollmentValidationTaskReducerTest, self).setUp()
        self.reduce_key = ('foo/bar/baz', 0)
        self.mode = 'honor'

    def create_validation_task(self, generate_before=True, tuple_output=True, include_nonstate_changes=True,
                               earliest_timestamp=None, expected_validation=None):
        """Create a task for testing purposes."""
        interval = '2013-01-01-2014-10-10'

        interval_value = luigi.DateIntervalParameter().parse(interval)
        earliest_timestamp_value = luigi.DateHourParameter().parse(earliest_timestamp) if earliest_timestamp else None
        expected_validation_value = (
            luigi.DateHourParameter().parse(expected_validation) if expected_validation else None
        )

        self.task = CourseEnrollmentValidationTask(
            interval=interval_value,
            output_root="/fake/output",
            generate_before=generate_before,
            tuple_output=tuple_output,
            include_nonstate_changes=include_nonstate_changes,
            earliest_timestamp=earliest_timestamp_value,
            expected_validation=expected_validation_value,
        )
        self.task.init_local()

    def _activated(self, timestamp, mode=None):
        """Creates an ACTIVATED event."""
        return (timestamp, ACTIVATED, mode or self.mode, None)

    def _deactivated(self, timestamp, mode=None):
        """Creates a DEACTIVATED event."""
        return (timestamp, DEACTIVATED, mode or self.mode, None)

    def _mode_changed(self, timestamp, mode=None):
        """Creates a MODE_CHANGED event."""
        return (timestamp, MODE_CHANGED, mode or self.mode, None)

    def _validated(self, timestamp, is_active, created, mode=None, dump_duration_in_secs=300):
        """Creates a VALIDATED event."""
        dump_end = timestamp
        dump_start = add_microseconds(timestamp, int(dump_duration_in_secs) * -100000)
        validation_info = {
            'is_active': is_active,
            'created': created,
            'dump_start': dump_start,
            'dump_end': dump_end,
        }
        return (timestamp, VALIDATED, mode or self.mode, validation_info)


class CourseEnrollmentValidationTaskReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        super(CourseEnrollmentValidationTaskReducerTest, self).setUp()
        self.create_validation_task(generate_before=True)

    def test_no_events(self):
        self.assert_no_output([])

    def test_missing_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "honor", "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_enroll_unenroll(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing deactivation (between 4/1 and 9/1)
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "honor", "start => validate(inactive)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
            ('2013-04-01',
             ('2013-04-01T00:00:01.123457', DEACTIVATED, "honor", "start => validate(inactive)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_single_unenrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123457', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-05-01T00:00:01.123456'),
            # missing activation
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "honor", "start => deactivate",
              '2013-04-01T00:00:01.123456', '2013-05-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_single_unenrollment_without_ms(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123457', False, '2013-04-01T00:00:01'),
            self._deactivated('2013-05-01T00:00:01.123456')
            # missing activation
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01', ACTIVATED, "honor", "start => deactivate",
              '2013-04-01T00:00:01', '2013-05-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_single_unvalidated_unenrollment(self):
        inputs = [
            self._deactivated('2013-05-01T00:00:01.123456')
            # missing activation
        ]
        expected = (
            ('2013-05-01',
             ('2013-05-01T00:00:01.123455', ACTIVATED, "honor", "start => deactivate",
              None, '2013-05-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_rollover_unvalidated_unenrollment(self):
        inputs = [
            self._deactivated('2013-05-01T00:00:01.000000')
            # missing activation
        ]
        expected = (
            ('2013-05-01',
             ('2013-05-01T00:00:00.999999', ACTIVATED, "honor", "start => deactivate",
              None, '2013-05-01T00:00:01.000000')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_unvalidated_unenrollment_without_ms(self):
        inputs = [
            self._deactivated('2013-05-01T00:00:01'),
            # missing activation
        ]
        expected = (
            ('2013-05-01',
             ('2013-05-01T00:00:00.999999', ACTIVATED, "honor", "start => deactivate",
              None, '2013-05-01T00:00:01')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_single_enroll_unenroll(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-05-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_unenroll_during_dump(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:00.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_unenroll_during_dump_reverse(self):
        inputs = [
            self._activated('2013-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:00.123456'),
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_single_unenroll_enroll(self):
        inputs = [
            self._activated('2013-09-01T00:00:01.123456'),
            self._deactivated('2013-05-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_multiple_validation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._validated('2013-07-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_multiple_validation_without_enroll(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._validated('2013-07-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "honor", "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-07-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_enroll_unenroll_with_validations(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._validated('2013-07-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

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
             ('2013-08-01T00:00:01.123457', ACTIVATED, "honor", "validate(inactive) => validate(active)",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_deactivate_between_validation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', DEACTIVATED, "honor", "validate(active) => validate(inactive)",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_deactivate_from_validation(self):
        inputs = [
            self._activated('2013-09-01T00:00:01.123456'),
            # missing deactivation
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', DEACTIVATED, "honor", "validate(active) => activate",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_deactivate_from_activation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123457', DEACTIVATED, "honor", "activate => validate(inactive)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_rollover_deactivate_from_activation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.999999'),
            # missing deactivation
            self._activated('2013-04-01T00:00:01.999999'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:02.000000', DEACTIVATED, "honor", "activate => validate(inactive)",
              '2013-04-01T00:00:01.999999', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_activate_from_deactivation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', ACTIVATED, "honor", "deactivate => validate(active)",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_activate_between_deactivation(self):
        inputs = [
            self._deactivated('2013-09-01T00:00:01.123456'),
            # missing activation
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-08-01',
             ('2013-08-01T00:00:01.123457', ACTIVATED, "honor", "deactivate => deactivate",
              '2013-08-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_deactivate_between_activation(self):
        inputs = [
            self._activated('2013-09-01T00:00:01.123456'),
            # missing deactivation
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-01-01',
             ('2013-01-01T00:00:01.123457', DEACTIVATED, "honor", "activate => activate",
              '2013-01-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

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
             ('2013-09-01T00:00:01.123457', ACTIVATED, "honor", "validate(inactive) => deactivate",
              '2013-09-01T00:00:01.123456', '2013-10-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_mode_change_via_activate(self):
        inputs = [
            self._activated('2013-10-01T00:00:01.123456', mode='verified'),
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_activate_missing_mode_change(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456', mode='verified'),
            # missing mode-change
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123457', MODE_CHANGED, "verified",
              "activate => validate(active) (honor=>verified)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_initial_mode_change(self):
        inputs = [
            self._activated('2013-04-01T00:00:01.123456', mode='verified'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_activate_duplicate_mode_change(self):
        inputs = [
            self._mode_changed('2013-05-01T00:00:01.123456', mode='honor'),
            self._activated('2013-04-01T00:00:01.123456', mode='honor'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_activate_with_mode_change(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456', mode='verified'),
            self._mode_changed('2013-05-01T00:00:01.123456', mode='verified'),
            self._activated('2013-04-01T00:00:01.123456', mode='honor'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_validate_with_missing_mode_change(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456', mode='audited'),
            # missing mode change
            self._mode_changed('2013-05-01T00:00:01.123456', mode='verified'),
            self._activated('2013-04-01T00:00:01.123456', mode='honor'),
        ]
        expected = (
            ('2013-05-01',
             ('2013-05-01T00:00:01.123457', MODE_CHANGED, "audited",
              "mode_change => validate(active) (verified=>audited)",
              '2013-05-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_activate_with_only_mode_change(self):
        inputs = [
            self._mode_changed('2013-05-01T00:00:01.123456', mode='verified'),
            self._activated('2013-04-01T00:00:01.123456', mode='honor'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_only_mode_change(self):
        inputs = [
            self._mode_changed('2013-05-01T00:00:01.123456', mode='verified'),
        ]
        # expect no event.
        self.assert_no_output(inputs)


class CourseEnrollmentValidationTaskEventReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        super(CourseEnrollmentValidationTaskEventReducerTest, self).setUp()
        self.create_validation_task(tuple_output=False)

    def test_missing_single_enrollment(self):
        """
        Tests the event formatting, so the assertions are done bit-by-bit rather than with the tuple check of output.
        """
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


class EarliestTimestampTaskReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events before first validation event are properly skipped.
    """
    def setUp(self):
        super(EarliestTimestampTaskReducerTest, self).setUp()
        self.create_validation_task(earliest_timestamp="2013-01-01T11")

    def test_no_events(self):
        self.assert_no_output([])

    def test_missing_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1/13)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "honor", "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_early_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2012-04-01T00:00:01.123456'),
            # missing activation (4/1/12)
        ]
        expected = (
            ('2013-01-01',
             ('2013-01-01T11:00:00.000000', ACTIVATED, "honor", "start => validate(active)",
              '2012-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)


class ExpectedValidationTaskReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events before first validation event are properly skipped.
    """
    def setUp(self):
        super(ExpectedValidationTaskReducerTest, self).setUp()
        self.create_validation_task(expected_validation="2014-10-01T11")

    def test_no_events(self):
        self.assert_no_output([])

    def test_missing_validation_from_activation(self):
        inputs = [
            # missing validation
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123457', DEACTIVATED, "honor", "activate => missing",
              '2013-04-01T00:00:01.123456', '2014-10-01T11:00:00.000000')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_validation_from_deactivation(self):
        inputs = [
            self._deactivated('2013-09-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-09-01',
             ('2013-09-01T00:00:01.123457', DEACTIVATED, "honor", "deactivate => missing",
              '2013-09-01T00:00:01.123456', '2014-10-01T11:00:00.000000')),
        )
        self._check_output_tuple_with_key(inputs, expected)


class GenerateBeforeDisabledTaskReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events before first validation event are properly skipped.
    """
    def setUp(self):
        super(GenerateBeforeDisabledTaskReducerTest, self).setUp()
        self.create_validation_task(generate_before=False)

    def test_no_events(self):
        self.assert_no_output([])

    def test_missing_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "honor", "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_early_single_enrollment(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2012-04-01T00:00:01.123456'),
            # missing activation (4/1/12)
        ]
        # expect no event.
        self._check_output_tuple_with_key(inputs, tuple())

    def test_single_deactivation(self):
        inputs = [
            self._deactivated('2013-10-01T00:00:01.123456'),
            # missing activation
        ]
        # expect no event.
        self._check_output_tuple_with_key(inputs, tuple())

    def test_missing_deactivate_from_activation(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing deactivation
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123457', DEACTIVATED, "honor", "activate => validate(inactive)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_activate_from_validation(self):
        inputs = [
            self._validated('2013-10-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:01.123456'),
            # missing activation (4/1)
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "honor", "start => deactivate",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_missing_early_activate_from_validation(self):
        inputs = [
            self._validated('2013-10-01T00:00:01.123456', False, '2012-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:01.123456'),
            # missing activation (4/1/12)
        ]
        # expect no event.
        self.assert_no_output(inputs)

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
             ('2013-09-01T00:00:01.123457', ACTIVATED, "honor", "validate(inactive) => deactivate",
              '2013-09-01T00:00:01.123456', '2013-10-01T00:00:01.123456')),
        )
        self._check_output_tuple_with_key(inputs, expected)

    def test_unenroll_during_dump(self):
        inputs = [
            self._validated('2013-09-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-09-01T00:00:00.123456'),
        ]
        expected = (
            ('2013-04-01',
             ('2013-04-01T00:00:01.123456', ACTIVATED, "honor", "start => validate(active)",
              '2013-04-01T00:00:01.123456', '2013-09-01T00:00:00.123455')),
        )
        self._check_output_tuple_with_key(inputs, expected)


class ExcludeNonstateChangesTaskReducerTest(BaseCourseEnrollmentValidationTaskReducerTest):
    """
    Tests to verify that events transitions that don't change state are properly ignored.

    Cases include:

    * activate => activate
    * validate(active) => activate
    * deactivate => deactivate
    * validate(inactive) => deactivate
    * start => validate(inactive)

    """
    def setUp(self):
        super(ExcludeNonstateChangesTaskReducerTest, self).setUp()
        self.create_validation_task(generate_before=False, include_nonstate_changes=False)

    def test_no_events(self):
        self.assert_no_output([])

    def test_missing_deactivate_between_activation(self):
        inputs = [
            self._activated('2013-09-01T00:00:01.123456'),
            # missing deactivation
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_missing_deactivate_before_activation(self):
        inputs = [
            self._activated('2013-09-01T00:00:01.123456'),
            # missing deactivation
            self._validated('2013-08-01T00:00:01.123456', True, '2013-04-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_missing_activate_between_deactivation(self):
        inputs = [
            self._deactivated('2013-09-01T00:00:01.123456'),
            # missing activation
            self._deactivated('2013-08-01T00:00:01.123456'),
            self._activated('2013-01-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_missing_activate_before_deactivation(self):
        inputs = [
            self._deactivated('2013-09-01T00:00:01.123456'),
            # missing activation
            self._validated('2013-08-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            self._deactivated('2013-05-01T00:00:01.123456'),
            self._activated('2013-04-01T00:00:01.123456'),
        ]
        # expect no event.
        self.assert_no_output(inputs)

    def test_single_deactivation_validation(self):
        inputs = [
            self._validated('2013-08-01T00:00:01.123456', False, '2013-04-01T00:00:01.123456'),
            # missing activation and deactivation?
        ]
        # expect no event.
        self.assert_no_output(inputs)


class CreateAllEnrollmentValidationEventsTest(TestCase):
    """Test that requirements for creating enrollment validation events are generated correctly."""

    def setUp(self):
        # Define a real output directory, so it can
        # be removed if existing.
        def cleanup(dirname):
            """Remove the temp directory only if it exists."""
            if os.path.exists(dirname):
                shutil.rmtree(dirname)

        self.output_root = tempfile.mkdtemp()
        self.addCleanup(cleanup, self.output_root)

    def test_requires_includes_today(self):
        today_datestring = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        fake_param = luigi.DateIntervalParameter()
        interval_string = '2013-12-17-{}'.format(today_datestring)
        interval = fake_param.parse(interval_string)
        task = CreateAllEnrollmentValidationEventsTask(
            interval=interval,
            output_root=self.output_root,
            warehouse_path=self.output_root,
        )
        required = task.requires()
        self.assertTrue(any([isinstance(dep, CreateEnrollmentValidationEventsForTodayTask) for dep in required]))

    def test_requires_is_empty(self):
        interval = luigi.DateIntervalParameter().parse('2013-12-17-2014-01-15')
        task = CreateAllEnrollmentValidationEventsTask(
            interval=interval,
            output_root=self.output_root,
            warehouse_path=self.output_root,
        )
        required = task.requires()
        self.assertEquals(required, [])

    def test_requires_is_not_empty(self):
        interval = luigi.DateIntervalParameter().parse('2013-12-17-2014-01-15')
        warehouse_path = self.output_root
        source_dir = os.path.join(warehouse_path, "student_courseenrollment", "dt=2014-01-01")
        os.makedirs(source_dir)
        self.assertTrue(os.path.exists(source_dir))
        task = CreateAllEnrollmentValidationEventsTask(
            interval=interval,
            output_root=self.output_root,
            warehouse_path=warehouse_path,
        )
        required = task.requires()
        self.assertEquals(len(required), 1)
        one_required = required[0]
        self.assertTrue(one_required.source_dir, source_dir)
        self.assertTrue(one_required.output_root, self.output_root)
