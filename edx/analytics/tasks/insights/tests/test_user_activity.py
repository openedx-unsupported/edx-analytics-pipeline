"""
Tests for tasks that collect enrollment events.

"""
import datetime
import json
from unittest import TestCase

from ddt import ddt, data, unpack
from luigi import date_interval

from edx.analytics.tasks.insights.user_activity import (
    UserActivityTask,
    CourseActivityWeeklyTask,
    CourseActivityMonthlyTask,
    ACTIVE_LABEL,
    PROBLEM_LABEL,
    PLAY_VIDEO_LABEL,
    POST_FORUM_LABEL,
)

from edx.analytics.tasks.common.tests.map_reduce_mixins import ReducerTestMixin, MapperTestMixin
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


@ddt
class UserActivityTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.task_class = UserActivityTask
        self.interval = '2013-12-01-2013-12-31'
        super(UserActivityTaskMapTest, self).setUp()

        self.initialize_ids()

        self.username = "test_user"
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.expected_date_string = '2013-12-17'
        self.event_type = "edx.dummy.event"

        self.event_templates = {
            'user_activity': {
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
        }
        self.default_event_template = 'user_activity'

    def test_unparseable_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_bad_datetime(self):
        line = self.create_event_log_line(time='this is a bogus time')
        self.assert_no_map_output_for(line)

    def test_datetime_out_of_range(self):
        line = self.create_event_log_line(time='2014-05-04T01:23:45')
        self.assert_no_map_output_for(line)

    def test_missing_context(self):
        event_dict = self.create_event_dict()
        del event_dict['context']
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    def test_missing_course_id(self):
        line = self.create_event_log_line(context={})
        self.assert_no_map_output_for(line)

    def test_illegal_course_id(self):
        line = self.create_event_log_line(context={"course_id": ";;;;bad/id/val"})
        self.assert_no_map_output_for(line)

    def test_empty_username(self):
        line = self.create_event_log_line(username='')
        self.assert_no_map_output_for(line)

    def test_whitespace_username(self):
        line = self.create_event_log_line(username='   ')
        self.assert_no_map_output_for(line)

    def test_good_dummy_event(self):
        line = self.create_event_log_line()
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),)
        self.assertEquals(event, expected)

    def test_play_video_event(self):
        line = self.create_event_log_line(event_source='browser', event_type='play_video')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),
                    ((self.course_id, self.username, self.expected_date_string, PLAY_VIDEO_LABEL), 1))
        self.assertEquals(event, expected)

    def test_problem_event(self):
        line = self.create_event_log_line(event_source='server', event_type='problem_check')
        event = tuple(self.task.mapper(line))
        expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),
                    ((self.course_id, self.username, self.expected_date_string, PROBLEM_LABEL), 1))
        self.assertEquals(event, expected)

    @data(('edx.forum.thread.created', True), ('edx.forum.response.created', True), ('edx.forum.comment.created', True),
          ('edx.forum.thread.voted', False))
    @unpack
    def test_post_forum_event(self, event_type, is_labeled_forum):
        line = self.create_event_log_line(event_source='server', event_type=event_type)
        event = tuple(self.task.mapper(line))
        if is_labeled_forum:
            expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),
                        ((self.course_id, self.username, self.expected_date_string, POST_FORUM_LABEL), 1))
        else:
            # The voted event is not a "discussion activity" and thus does not get the POST_FORUM_LABEL
            expected = (((self.course_id, self.username, self.expected_date_string, ACTIVE_LABEL), 1),)
        self.assertEquals(event, expected)

    def test_exclusion_of_events_by_source(self):
        line = self.create_event_log_line(event_source='task')
        self.assert_no_map_output_for(line)

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
            line = self.create_event_log_line(event_source='server', event_type=event_type)
            self.assert_no_map_output_for(line)

    def test_multiple(self):
        lines = [
            self.create_event_log_line(event_source='browser', event_type='play_video'),
            self.create_event_log_line(event_source='mobile', event_type='play_video'),
            self.create_event_log_line(
                time="2013-12-24T00:00:00.000000", event_source='server', event_type='problem_check'),
            self.create_event_log_line(time="2013-12-16T04:00:00.000000")
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


class UserActivityPerIntervalReduceTest(InitializeOpaqueKeysMixin, ReducerTestMixin, TestCase):
    """
    Tests to verify that UserActivityPerIntervalTask reducer works correctly.
    """
    def setUp(self):
        self.task_class = UserActivityTask
        self.interval = '2013-12-01-2013-12-31'
        super(UserActivityPerIntervalReduceTest, self).setUp()

        self.initialize_ids()
        self.username = 'test_user'

        self.reduce_key = (self.course_id, self.username, '2013-12-04')

    def test_no_events(self):
        self.reduce_key = ()
        self.assert_no_output([])

    def test_single_event(self):
        self.reduce_key = (self.course_id, self.username, '2013-12-01', ACTIVE_LABEL)
        values = [1]
        expected = (((self.course_id, self.username, '2013-12-01', ACTIVE_LABEL), 1),)
        self._check_output_complete_tuple(values, expected)

    def test_multiple(self):
        self.reduce_key = (self.course_id, self.username, '2013-12-01', ACTIVE_LABEL)
        values = [1, 2, 1]
        expected = (((self.course_id, self.username, '2013-12-01', ACTIVE_LABEL), 4),)
        self._check_output_complete_tuple(values, expected)


class CourseActivityWeeklyTaskTest(InitializeOpaqueKeysMixin, TestCase):
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


class CourseActivityMonthlyTaskTest(InitializeOpaqueKeysMixin, TestCase):
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
