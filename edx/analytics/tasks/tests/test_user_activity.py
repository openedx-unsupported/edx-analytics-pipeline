"""
Tests for tasks that collect enrollment events.

"""
import datetime
import json

from luigi import date_interval

from edx.analytics.tasks.user_activity import (
    UserActivityTask,
    CourseActivityWeeklyTask,
    CourseActivityMonthlyTask,
    ACTIVE_LABEL,
    PROBLEM_LABEL,
    PLAY_VIDEO_LABEL,
    POST_FORUM_LABEL,
)

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


class UserActivityTaskMapTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.initialize_ids()
        self.interval_string = '2013-12-01-2013-12-31'
        self.task = UserActivityTask(
            interval=date_interval.Custom.parse(self.interval_string),
            output_root='/tmp/foo'
        )
        # We're not really using Luigi properly here to create
        # the task, so set up manually.
        self.task.init_local()

        self.username = "test_user"
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.expected_date_string = '2013-12-17'
        self.event_type = "edx.dummy.event"

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        event_dict = {
            "username": self.username,
            "host": "test_host",
            "event_source": "server",
            "event_type": self.event_type,
            "context": {
                "course_id": self.course_id,
                "org_id": self.org_id,
                "user_id": 21,
            },
            "time": "{0}+00:00".format(self.timestamp),
            "ip": "127.0.0.1",
            "event": {},
            "agent": "blah, blah, blah",
            "page": None
        }
        event_dict.update(**kwargs)
        return event_dict

    def assert_no_output_for(self, line):
        """Assert that an input line generates no output."""
        self.assertEquals(tuple(self.task.mapper(line)), tuple())

    def test_unparseable_event(self):
        line = 'this is garbage'
        self.assert_no_output_for(line)

    def test_bad_datetime(self):
        line = self._create_event_log_line(time='this is a bogus time')
        self.assert_no_output_for(line)

    def test_datetime_out_of_range(self):
        line = self._create_event_log_line(time='2014-05-04T01:23:45')
        self.assert_no_output_for(line)

    def test_missing_context(self):
        event_dict = self._create_event_dict()
        del event_dict['context']
        line = json.dumps(event_dict)
        self.assert_no_output_for(line)

    def test_missing_course_id(self):
        line = self._create_event_log_line(context={})
        self.assert_no_output_for(line)

    def test_illegal_course_id(self):
        line = self._create_event_log_line(context={"course_id": ";;;;bad/id/val"})
        self.assert_no_output_for(line)

    def test_empty_username(self):
        line = self._create_event_log_line(username='')
        self.assert_no_output_for(line)

    def test_whitespace_username(self):
        line = self._create_event_log_line(username='   ')
        self.assert_no_output_for(line)

    def test_good_dummy_event(self):
        line = self._create_event_log_line()
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),)
        self.assertEquals(event, expected)

    def test_play_video_event(self):
        line = self._create_event_log_line(event_source='browser', event_type='play_video')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),
                    ((self.course_id, self.username, self.expected_date_string, PLAY_VIDEO_LABEL), 1))
        self.assertEquals(event, expected)

    def test_problem_event(self):
        line = self._create_event_log_line(event_source='server', event_type='problem_check')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),
                    ((self.course_id, self.username, self.expected_date_string, PROBLEM_LABEL), 1))
        self.assertEquals(event, expected)

    def test_post_forum_event(self):
        line = self._create_event_log_line(event_source='server', event_type='blah/blah/threads/create')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),
                    ((self.course_id, self.username, self.expected_date_string, POST_FORUM_LABEL), 1))
        self.assertEquals(event, expected)

    def test_exclusion_of_events_by_source(self):
        line = self._create_event_log_line(event_source='task')
        self.assert_no_output_for(line)

    def test_exclusion_of_events_by_type(self):
        excluded_event_types = [
            'edx.course.enrollment.activated',
            'edx.course.enrollment.deactivated',
            'edx.course.enrollment.upgrade.clicked',
            'edx.course.enrollment.upgrade.succeeded',
            'edx.course.enrollment.reverify.started',
            'edx.course.enrollment.reverify.submitted',
            'edx.course.enrollment.reverify.reviewed',
        ]
        for event_type in excluded_event_types:
            line = self._create_event_log_line(event_source='server', event_type=event_type)
            self.assert_no_output_for(line)

    def test_multiple(self):
        lines = [
            self._create_event_log_line(event_source='browser', event_type='play_video'),
            self._create_event_log_line(event_source='mobile', event_type='play_video'),
            self._create_event_log_line(
                time="2013-12-24T00:00:00.000000", event_source='server', event_type='problem_check'),
            self._create_event_log_line(time="2013-12-16T04:00:00.000000")
        ]
        outputs = []
        for line in lines:
            for output in self.task.mapper(line):
                outputs.append(output)

        expected = (
            ((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),
            ((self.course_id, self.username, self.expected_date_string, PLAY_VIDEO_LABEL), 1),
            ((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),
            ((self.course_id, self.username, self.expected_date_string, PLAY_VIDEO_LABEL), 1),
            ((self.course_id, self.username, '2013-12-24', ACTIVE_LABEL), 1),
            ((self.course_id, self.username, '2013-12-24', PROBLEM_LABEL), 1),
            ((self.course_id, self.username, '2013-12-16', ACTIVE_LABEL), 1),
        )
        self.assertItemsEqual(outputs, expected)


class UserActivityTaskMapLegacyTest(InitializeLegacyKeysMixin, UserActivityTaskMapTest):
    """Tests to verify that event log parsing by mapper works correctly with legacy ids."""
    pass


class UserActivityPerIntervalReduceTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """
    Tests to verify that UserActivityPerIntervalTask reducer works correctly.
    """
    def setUp(self):
        self.initialize_ids()
        self.username = 'test_user'
        self.interval_string = '2013-12-01-2013-12-31'
        self.task = UserActivityTask(
            interval=date_interval.Custom.parse(self.interval_string),
            output_root='/tmp/foo'
        )
        self.key = (self.course_id, self.username, '2013-12-04')

    def _get_reducer_output(self, key, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(key, values))

    def _check_output(self, key, values, expected):
        """Compare generated with expected output."""
        self.assertEquals(self._get_reducer_output(key, values), expected)

    def test_no_events(self):
        self._check_output(tuple(), [], tuple())

    def test_single_event(self):
        key = (self.course_id, self.username, '2013-12-01', ACTIVE_LABEL)
        values = [1]
        expected = (((self.course_id, self.username, '2013-12-01', ACTIVE_LABEL), 1),)
        self._check_output(key, values, expected)

    def test_multiple(self):
        key = (self.course_id, self.username, '2013-12-01', ACTIVE_LABEL)
        values = [1, 2, 1]
        expected = (((self.course_id, self.username, '2013-12-01', ACTIVE_LABEL), 4),)
        self._check_output(key, values, expected)


class CourseActivityWeeklyTaskTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """Ensure the date interval is computed correctly for monthly tasks."""

    def setUp(self):
        self.initialize_ids()

    def test_zero_weeks(self):
        task = CourseActivityWeeklyTask(
            end_date=datetime.date(2014, 1, 1),
            weeks=0
        )
        with self.assertRaises(ValueError):
            task.interval

    def test_single_week(self):
        task = CourseActivityWeeklyTask(
            end_date=datetime.date(2014, 1, 1),
            weeks=1
        )
        self.assertEquals(task.interval, date_interval.Custom.parse('2013-12-23-2013-12-30'))

    def test_multi_week(self):
        task = CourseActivityWeeklyTask(
            end_date=datetime.date(2014, 1, 6),
            weeks=2
        )
        self.assertEquals(task.interval, date_interval.Custom.parse('2013-12-23-2014-01-06'))

    def test_leap_year(self):
        task = CourseActivityWeeklyTask(
            end_date=datetime.date(2012, 2, 29),
            weeks=52
        )
        self.assertEquals(task.interval, date_interval.Custom.parse('2011-02-28-2012-02-27'))


class CourseActivityMonthlyTaskTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """Ensure the date interval is computed correctly for monthly tasks."""

    def setUp(self):
        self.initialize_ids()

    def test_zero_months(self):
        task = CourseActivityMonthlyTask(
            end_date=datetime.date(2014, 1, 31),
            months=0
        )
        with self.assertRaises(ValueError):
            task.interval

    def test_single_month(self):
        task = CourseActivityMonthlyTask(
            end_date=datetime.date(2014, 1, 31),
            months=1
        )
        self.assertEquals(task.interval, date_interval.Custom.parse('2013-12-01-2014-01-01'))

    def test_multi_month(self):
        task = CourseActivityMonthlyTask(
            end_date=datetime.date(2014, 1, 31),
            months=2
        )
        self.assertEquals(task.interval, date_interval.Custom.parse('2013-11-01-2014-01-01'))

    def test_leap_year(self):
        task = CourseActivityMonthlyTask(
            end_date=datetime.date(2012, 2, 29),
            months=12
        )
        self.assertEquals(task.interval, date_interval.Custom.parse('2011-02-01-2012-02-01'))
