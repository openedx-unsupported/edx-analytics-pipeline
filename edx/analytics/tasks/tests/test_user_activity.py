"""
Tests for tasks that collect enrollment events.

"""
import json

from luigi import date_interval

from edx.analytics.tasks.user_activity import (
    UserActivityPerIntervalTask,
    ACTIVE_LABEL,
    PROBLEM_LABEL,
    PLAY_VIDEO_LABEL,
    POST_FORUM_LABEL,
    CountLastElementMixin,
)

from edx.analytics.tasks.tests import unittest


class UserActivityPerIntervalMapTest(unittest.TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.interval_string = '2013-12-01-2013-12-31'
        self.task = UserActivityPerIntervalTask(
            interval=date_interval.Custom.parse(self.interval_string),
        )
        # We're not really using Luigi properly here to create
        # the task, so set up manually.
        self.task.init_local()

        self.course_id = "Foox/Bar101/2013_Spring"
        self.username = "test_user"
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.expected_interval_string = '2013-12-17-2013-12-24'
        self.event_type = "edx.dummy.event"

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
        expected = (((self.course_id, self.username, self.expected_interval_string), ACTIVE_LABEL),)
        self.assertEquals(event, expected)

    def test_play_video_event(self):
        line = self._create_event_log_line(event_source='browser', event_type='play_video')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_interval_string), ACTIVE_LABEL),
                    ((self.course_id, self.username, self.expected_interval_string), PLAY_VIDEO_LABEL))
        self.assertEquals(event, expected)

    def test_problem_event(self):
        line = self._create_event_log_line(event_source='server', event_type='problem_check')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_interval_string), ACTIVE_LABEL),
                    ((self.course_id, self.username, self.expected_interval_string), PROBLEM_LABEL))
        self.assertEquals(event, expected)

    def test_post_forum_event(self):
        line = self._create_event_log_line(event_source='server', event_type='blah/blah/threads/create')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_interval_string), ACTIVE_LABEL),
                    ((self.course_id, self.username, self.expected_interval_string), POST_FORUM_LABEL))
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

    def test_multiple_weeks(self):
        lines = [
            self._create_event_log_line(event_source='browser', event_type='play_video'),
            self._create_event_log_line(
                time="2013-12-24T00:00:00.000000", event_source='server', event_type='problem_check'),
            self._create_event_log_line(time="2013-12-16T04:00:00.000000")
        ]
        outputs = []
        for line in lines:
            for output in self.task.mapper(line):
                outputs.append(output)

        expected = (
            ((self.course_id, self.username, self.expected_interval_string), ACTIVE_LABEL),
            ((self.course_id, self.username, self.expected_interval_string), PLAY_VIDEO_LABEL),
            ((self.course_id, self.username, '2013-12-24-2013-12-31'), ACTIVE_LABEL),
            ((self.course_id, self.username, '2013-12-24-2013-12-31'), PROBLEM_LABEL),
            ((self.course_id, self.username, '2013-12-10-2013-12-17'), ACTIVE_LABEL),
        )
        self.assertItemsEqual(outputs, expected)


class UserActivityPerIntervalReduceTest(unittest.TestCase):
    """
    Tests to verify that UserActivityPerIntervalTask reducer works correctly.
    """
    def setUp(self):
        self.course_id = 'course_id'
        self.username = 'test_user'
        self.interval_string = '2013-12-01-2013-12-31'
        self.task = UserActivityPerIntervalTask(
            interval=date_interval.Custom.parse(self.interval_string),
        )
        self.key = (self.course_id, self.username, self.interval_string)

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def _check_output(self, inputs, expected):
        """Compare generated with expected output."""
        self.assertEquals(self._get_reducer_output(inputs), expected)

    def test_no_events(self):
        self._check_output([], tuple())

    def test_single_event(self):
        inputs = [ACTIVE_LABEL]
        expected = ((self.course_id, '2013-12-01', '2013-12-31', ACTIVE_LABEL, self.username),)
        self._check_output(inputs, expected)

    def test_multiple_events_different(self):
        inputs = [ACTIVE_LABEL, PROBLEM_LABEL]
        expected = ((self.course_id, '2013-12-01', '2013-12-31', ACTIVE_LABEL, self.username),
                    (self.course_id, '2013-12-01', '2013-12-31', PROBLEM_LABEL, self.username),)
        self._check_output(inputs, expected)

    def test_multiple_events_with_dupes(self):
        inputs = [ACTIVE_LABEL, PROBLEM_LABEL, ACTIVE_LABEL, PROBLEM_LABEL]
        expected = ((self.course_id, '2013-12-01', '2013-12-31', ACTIVE_LABEL, self.username),
                    (self.course_id, '2013-12-01', '2013-12-31', PROBLEM_LABEL, self.username),)
        self._check_output(inputs, expected)


class CountLastElementMixinTest(unittest.TestCase):
    """
    Tests to verify that CountLastElementMixin works correctly.
    """
    def setUp(self):
        self.interval_string = '2013-12-01-2013-12-31'
        self.task = CountLastElementMixin()
        self.key = ('course', '2013-01-01')

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.key, values))

    def test_mapper(self):
        inputs = ['a', 'b', 'c', 'd']
        expected = ((('a', 'b', 'c'), 1),)
        output = tuple(self.task.mapper('\t'.join(inputs)))
        self.assertEquals(output, expected)

    def test_no_counts(self):
        self.assertEquals(self._get_reducer_output([]), ((self.key, 0),))

    def test_single_count(self):
        self.assertEquals(self._get_reducer_output([1]), ((self.key, 1),))

    def test_multiple_counts(self):
        inputs = [1, 1, 1, 1, 1]
        self.assertEquals(self._get_reducer_output(inputs), ((self.key, 5),))
