"""
Tests for tasks that collect enrollment events.

"""
import json

from luigi import date_interval

from edx.analytics.tasks.user_activity import (
    CategorizeUserActivityTask,
    ACTIVE_LABEL,
    PROBLEM_LABEL,
    PLAY_VIDEO_LABEL,
    POST_FORUM_LABEL,
)

from edx.analytics.tasks.tests import unittest


class CategorizeUserActivityTaskMapTest(unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.task = CategorizeUserActivityTask(
            interval=date_interval.Month.parse('2013-12'),
            output_root='s3://fake/warehouse/user_activity_daily/dt=2013-12-01'
        )
        # We're not really using Luigi properly here to create
        # the task, so set up manually.
        self.task.init_local()

        self.course_id = "Foox/Bar101/2013_Spring"
        self.username = "test_user"
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.event_type = "edx.dummy.event"

    @property
    def date(self):
        """Just the date extracted from self.timestamp"""
        return self.timestamp.split('T')[0]

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        org_id = self.course_id.split('/')[0]
        event_dict = {
            "username": self.username,
            "host": "test_host",
            "event_source": "server",
            "event_type": self.event_type,
            "context": {
                "course_id": self.course_id,
                "org_id": org_id,
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
        expected = (((self.course_id, self.username, self.date), ACTIVE_LABEL),)
        self.assertEquals(event, expected)

    def test_play_video_event(self):
        line = self._create_event_log_line(event_source='browser', event_type='play_video')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.date), ACTIVE_LABEL),
                    ((self.course_id, self.username, self.date), PLAY_VIDEO_LABEL))
        self.assertEquals(event, expected)

    def test_problem_event(self):
        line = self._create_event_log_line(event_source='server', event_type='problem_check')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.date), ACTIVE_LABEL),
                    ((self.course_id, self.username, self.date), PROBLEM_LABEL))
        self.assertEquals(event, expected)

    def test_post_forum_event(self):
        line = self._create_event_log_line(event_source='server', event_type='blah/blah/threads/create')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.date), ACTIVE_LABEL),
                    ((self.course_id, self.username, self.date), POST_FORUM_LABEL))
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


class CategorizeUserActivityTaskReduceTest(unittest.TestCase):
    """
    Tests to verify that UserActivityPerDayTask reducer works correctly.
    """
    def setUp(self):
        self.course_id = 'course_id'
        self.username = 'test_user'
        self.interval_string = '2014-08-11-2014-08-15'
        self.task = CategorizeUserActivityTask(
            interval=date_interval.Custom.parse(self.interval_string),
            output_root='s3://fake/warehouse/user_activity_daily/dt=2014-08-15'
        )

    def _get_reducer_output(self, key, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(key, values))

    def _check_output(self, key, inputs, expected):
        """Compare generated with expected output."""
        output = self._get_reducer_output(key, inputs)
        self.assertItemsEqual(output, expected)

    def test_date_out_of_interval(self):
        key_08_12 = (self.course_id, self.username, "2014-08-18")
        expected = tuple()
        self._check_output(key_08_12, [], expected)

    def test_single_date(self):
        key_08_12 = (self.course_id, self.username, "2014-08-12")
        inputs_08_12 = (ACTIVE_LABEL, PROBLEM_LABEL)
        expected_08_12 = (
            ('2014-08-12', self.course_id, self.username, ACTIVE_LABEL, 1),
            ('2014-08-12', self.course_id, self.username, PROBLEM_LABEL, 1),
        )
        self._check_output(key_08_12, inputs_08_12, expected_08_12)

    def test_multiple_dates(self):
        key_08_12 = (self.course_id, self.username, "2014-08-12")
        inputs_08_12 = (
            ACTIVE_LABEL, ACTIVE_LABEL, PROBLEM_LABEL, PROBLEM_LABEL, PROBLEM_LABEL, PLAY_VIDEO_LABEL, PLAY_VIDEO_LABEL
        )
        expected_08_12 = (
            ('2014-08-12', self.course_id, self.username, ACTIVE_LABEL, 2),
            ('2014-08-12', self.course_id, self.username, PROBLEM_LABEL, 3),
            ('2014-08-12', self.course_id, self.username, PLAY_VIDEO_LABEL, 2),
        )
        self._check_output(key_08_12, inputs_08_12, expected_08_12)

        key_08_13 = (self.course_id, self.username, "2014-08-13")
        inputs_08_13 = (ACTIVE_LABEL, PLAY_VIDEO_LABEL)
        expected_08_13 = (
            ('2014-08-13', self.course_id, self.username, ACTIVE_LABEL, 1),
            ('2014-08-13', self.course_id, self.username, PLAY_VIDEO_LABEL, 1),
        )
        self._check_output(key_08_13, inputs_08_13, expected_08_13)

        key_08_14 = (self.course_id, self.username, "2014-08-14")
        inputs_08_14 = (ACTIVE_LABEL, PLAY_VIDEO_LABEL, PROBLEM_LABEL, PROBLEM_LABEL, ACTIVE_LABEL, ACTIVE_LABEL)
        expected_08_14 = (
            ('2014-08-14', self.course_id, self.username, ACTIVE_LABEL, 3),
            ('2014-08-14', self.course_id, self.username, PROBLEM_LABEL, 2),
            ('2014-08-14', self.course_id, self.username, PLAY_VIDEO_LABEL, 1),
        )
        self._check_output(key_08_14, inputs_08_14, expected_08_14)
