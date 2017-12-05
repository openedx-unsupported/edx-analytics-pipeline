"""Test student engagement metrics"""

import json
from unittest import TestCase

import luigi
from ddt import data, ddt, unpack

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.data_api.student_engagement import SUBSECTION_VIEWED_MARKER, StudentEngagementTask
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeLegacyKeysMixin, InitializeOpaqueKeysMixin


class BaseStudentEngagementTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, TestCase):
    """Base class for test analysis of detailed student engagement"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    def setUp(self):
        super(BaseStudentEngagementTaskMapTest, self).setUp()

        self.initialize_ids()
        self.video_id = 'i4x-foo-bar-baz'
        self.event_templates = {
            'play_video': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "browser",
                "event_type": "play_video",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": '{"id": "%s", "currentTime": "23.4398", "code": "87389iouhdfh"}' % self.video_id,
                "agent": "blah, blah, blah",
                "page": None
            },
            'problem_check': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "problem_check",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": {
                    "problem_id": self.problem_id,
                    "success": "incorrect",
                },
                "agent": "blah, blah, blah",
                "page": None
            }
        }
        self.default_event_template = 'problem_check'
        self.default_key = (self.DEFAULT_DATE, self.course_id, 'test_user')

    def create_task(self, interval=None, interval_type=None):
        """Allow arguments to be passed to the task constructor."""
        if not interval:
            interval = self.DEFAULT_DATE
        self.task = StudentEngagementTask(
            interval=luigi.DateIntervalParameter().parse(interval),
            output_root='/fake/output',
            interval_type=interval_type,
        )
        self.task.init_local()

    def assert_date_mappings(self, expected_end_date, actual_event_date):
        """Asserts that an event_date is mapped to the expected date in the key."""
        self.assert_single_map_output(
            self.create_event_log_line(time="{}T15:38:32.805444".format(actual_event_date)),
            (expected_end_date, self.course_id, 'test_user'),
            (self.problem_id, 'problem_check', '{}', actual_event_date)
        )


@ddt
class StudentEngagementTaskMapTest(BaseStudentEngagementTaskMapTest):
    """Test analysis of detailed student engagement"""

    def setUp(self):
        super(StudentEngagementTaskMapTest, self).setUp()
        self.create_task()

    @data(
        {'time': "2013-12-01T15:38:32.805444"},
        {'username': ''},
        {'event_type': None},
        {'context': {'course_id': 'lskdjfslkdj'}},
        {'event': 'sdfasdf'}
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    def test_browser_problem_check_event(self):
        template = self.event_templates['problem_check']
        self.assert_no_map_output_for(self.create_event_log_line(template=template, event_source='browser'))

    def test_incorrect_problem_check(self):
        self.assert_single_map_output(
            json.dumps(self.event_templates['problem_check']),
            self.default_key,
            (self.problem_id, 'problem_check', '{}', self.DEFAULT_DATE)
        )

    def test_correct_problem_check(self):
        template = self.event_templates['problem_check']
        template['event']['success'] = 'correct'
        self.assert_single_map_output(
            json.dumps(template),
            self.default_key,
            (self.problem_id, 'problem_check', json.dumps({'correct': True}), self.DEFAULT_DATE)
        )

    def test_missing_problem_id(self):
        template = self.event_templates['problem_check']
        del template['event']['problem_id']
        self.assert_no_map_output_for(self.create_event_log_line(template=template))

    def test_missing_video_id(self):
        template = self.event_templates['play_video']
        template['event'] = '{"currentTime": "23.4398", "code": "87389iouhdfh"}'
        self.assert_no_map_output_for(self.create_event_log_line(template=template))

    def test_play_video(self):
        self.assert_single_map_output(
            json.dumps(self.event_templates['play_video']),
            self.default_key,
            (self.video_id, 'play_video', '{}', self.DEFAULT_DATE)
        )

    def test_implicit_event(self):
        self.assert_single_map_output(
            self.create_event_log_line(event_type='/jsi18n/', event_source='server'),
            self.default_key,
            ('', '/jsi18n/', '{}', self.DEFAULT_DATE)
        )

    def test_course_event(self):
        self.assert_single_map_output(
            self.create_event_log_line(event_type='/courses/foo/bar/', event_source='server'),
            self.default_key,
            ('', '/courses/foo/bar/', '{}', self.DEFAULT_DATE)
        )

    def test_section_view_event(self):
        event_type = u'/courses/{0}/courseware/foo/'.format(self.course_id)
        self.assert_single_map_output(
            self.create_event_log_line(event_type=event_type, event_source='server'),
            self.default_key,
            ('', event_type, '{}', self.DEFAULT_DATE)
        )

    def test_subsection_event(self):
        self.assert_last_subsection_viewed_recognized('foo/bar/')

    def assert_last_subsection_viewed_recognized(self, end_of_path):
        """Assert that given a path ending the event is recognized as a subsection view"""
        event_type = u'/courses/{0}/courseware/{1}'.format(self.course_id, end_of_path)
        self.assert_single_map_output(
            self.create_event_log_line(event_type=event_type, event_source='server'),
            self.default_key,
            ('', 'marker:last_subsection_viewed', json.dumps({
                'path': event_type,
                'timestamp': self.DEFAULT_TIMESTAMP,
            }), self.DEFAULT_DATE)
        )

    def test_subsection_sequence_num_event(self):
        self.assert_last_subsection_viewed_recognized('foo/bar/10')

    def test_subsection_jquery_event(self):
        self.assert_last_subsection_viewed_recognized('foo/bar/jquery.js')


@ddt
class WeeklyStudentEngagementTaskMapTest(BaseStudentEngagementTaskMapTest):
    """Test mapping of dates to weekly intervals in student engagement."""

    INTERVAL_START = "2013-11-01"
    INTERVAL_END = "2014-01-02"

    def setUp(self):
        super(WeeklyStudentEngagementTaskMapTest, self).setUp()
        interval = "{}-{}".format(self.INTERVAL_START, self.INTERVAL_END)
        self.create_task(interval=interval, interval_type="weekly")

    @data(
        ("2014-01-01", "2014-01-01"),
        ("2013-12-25", "2013-12-25"),
        ("2014-01-01", "2013-12-27"),
        ("2013-12-25", "2013-12-23"),
    )
    @unpack
    def test_date_mappings(self, expected_end_date, actual_event_date):
        self.assert_date_mappings(expected_end_date, actual_event_date)


@ddt
class AllStudentEngagementTaskMapTest(BaseStudentEngagementTaskMapTest):
    """Test mapping of dates to overall interval in student engagement."""

    INTERVAL_START = "2013-11-01"
    INTERVAL_END = "2014-01-02"

    def setUp(self):
        super(AllStudentEngagementTaskMapTest, self).setUp()
        interval = "{}-{}".format(self.INTERVAL_START, self.INTERVAL_END)
        self.create_task(interval=interval, interval_type="all")

    @data(
        ("2014-01-01", "2014-01-01"),
        ("2014-01-01", "2013-12-25"),
        ("2014-01-01", "2013-12-27"),
        ("2014-01-01", "2013-12-23"),
    )
    @unpack
    def test_date_mappings(self, expected_end_date, actual_event_date):
        self.assert_date_mappings(expected_end_date, actual_event_date)


class StudentEngagementTaskLegacyMapTest(InitializeLegacyKeysMixin, StudentEngagementTaskMapTest):
    """Test analysis of detailed student engagement using legacy ID formats"""
    pass


@ddt
class StudentEngagementTaskReducerTest(ReducerTestMixin, TestCase):
    """
    Tests to verify that engagement data is reduced properly
    """

    task_class = StudentEngagementTask

    WAS_ACTIVE_COLUMN = 3
    PROBLEMS_ATTEMPTED_COLUMN = 4
    PROBLEM_ATTEMPTS_COLUMN = 5
    PROBLEMS_CORRECT_COLUMN = 6
    VIDEOS_PLAYED_COLUMN = 7
    FORUM_POSTS_COLUMN = 8
    FORUM_REPLIES_COLUMN = 9
    FORUM_COMMENTS_COLUMN = 10
    FORUM_UPVOTES_COLUMN = 11
    FORUM_DOWNVOTES_COLUMN = 12
    FORUM_UPVOTES_RECEIVED_COLUMN = 13
    FORUM_DOWNVOTES_RECEIVED_COLUMN = 14
    TEXTBOOK_PAGES_COLUMN = 15
    LAST_SUBSECTION_COLUMN = 16

    def setUp(self):
        super(StudentEngagementTaskReducerTest, self).setUp()

        self.reduce_key = (self.DATE, self.COURSE_ID, self.USERNAME)

    def test_any_activity(self):
        inputs = [
            ('', '/foo', '{}', self.DATE)
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 0,
            self.PROBLEM_ATTEMPTS_COLUMN: 0,
            self.PROBLEMS_CORRECT_COLUMN: 0,
            self.VIDEOS_PLAYED_COLUMN: 0,
            self.FORUM_POSTS_COLUMN: 0,
            self.FORUM_REPLIES_COLUMN: 0,
            self.FORUM_COMMENTS_COLUMN: 0,
            self.TEXTBOOK_PAGES_COLUMN: 0,
            self.LAST_SUBSECTION_COLUMN: '',
        })

    def test_single_problem_attempted(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', json.dumps({'correct': True}), self.DATE)
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 1,
            self.PROBLEM_ATTEMPTS_COLUMN: 1,
            self.PROBLEMS_CORRECT_COLUMN: 1,
        })

    def test_single_problem_attempted_incorrect(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', '{}', self.DATE)
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 1,
            self.PROBLEM_ATTEMPTS_COLUMN: 1,
            self.PROBLEMS_CORRECT_COLUMN: 0,
        })

    def test_single_problem_attempted_multiple_events(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', json.dumps({'correct': True}), self.DATE),
            ('i4x://foo/bar/baz', 'problem_check', json.dumps({'correct': True}), self.DATE),
            ('i4x://foo/bar/baz', 'problem_check', '{}', self.DATE)
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 1,
            self.PROBLEM_ATTEMPTS_COLUMN: 3,
            self.PROBLEMS_CORRECT_COLUMN: 1,
        })

    def test_multiple_problems_attempted(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', json.dumps({'correct': True}), self.DATE),
            ('i4x://foo/bar/baz2', 'problem_check', json.dumps({'correct': True}), self.DATE),
            ('i4x://foo/bar/baz', 'problem_check', '{}', self.DATE)
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 2,
            self.PROBLEM_ATTEMPTS_COLUMN: 3,
            self.PROBLEMS_CORRECT_COLUMN: 2,
        })

    def test_single_video_played(self):
        inputs = [
            ('foobarbaz', 'play_video', '{}', self.DATE),
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.VIDEOS_PLAYED_COLUMN: 1,
        })

    def test_multiple_video_plays_same_video(self):
        inputs = [
            ('foobarbaz', 'play_video', '{}', self.DATE),
            ('foobarbaz', 'play_video', '{}', self.DATE),
            ('foobarbaz', 'play_video', '{}', self.DATE),
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.VIDEOS_PLAYED_COLUMN: 1,
        })

    def test_other_video_events(self):
        inputs = [
            ('foobarbaz', 'pause_video', '{}', self.DATE),
            ('foobarbaz2', 'seek_video', '{}', self.DATE),
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.VIDEOS_PLAYED_COLUMN: 0,
        })

    @data(
        ('edx.forum.thread.created', FORUM_POSTS_COLUMN),
        ('edx.forum.response.created', FORUM_REPLIES_COLUMN),
        ('edx.forum.comment.created', FORUM_COMMENTS_COLUMN),
        ('book', TEXTBOOK_PAGES_COLUMN),
    )
    @unpack
    def test_count_events(self, event_type, column_num):
        inputs = [
            ('', event_type, '{}', self.DATE),
        ]
        self._check_output_by_key(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            column_num: 1,
        })

    @data(
        ('edx.forum.thread.created', FORUM_POSTS_COLUMN),
        ('edx.forum.response.created', FORUM_REPLIES_COLUMN),
        ('edx.forum.comment.created', FORUM_COMMENTS_COLUMN),
        ('book', TEXTBOOK_PAGES_COLUMN),
    )
    @unpack
    def test_multiple_counted_events(self, event_type, column_num):
        inputs = [
            ('', event_type, '{}', self.DATE),
            ('', event_type, '{}', self.DATE),
        ]
        self._check_output_by_key(inputs, {
            column_num: 2,
        })

    @data(
        ('edx.forum.thread.voted', 'up', FORUM_UPVOTES_COLUMN),
        ('edx.forum.thread.vote_received', 'up', FORUM_UPVOTES_RECEIVED_COLUMN),
        ('edx.forum.response.voted', 'up', FORUM_UPVOTES_COLUMN),
        ('edx.forum.response.vote_received', 'up', FORUM_UPVOTES_RECEIVED_COLUMN),
        ('edx.forum.thread.voted', 'down', FORUM_DOWNVOTES_COLUMN),
        ('edx.forum.thread.vote_received', 'down', FORUM_DOWNVOTES_RECEIVED_COLUMN),
        ('edx.forum.response.voted', 'down', FORUM_DOWNVOTES_COLUMN),
        ('edx.forum.response.vote_received', 'down', FORUM_DOWNVOTES_RECEIVED_COLUMN),
    )
    @unpack
    def test_forum_vote_events(self, event_type, vote_type, column_num):
        inputs = [
            ('', event_type, json.dumps({'undo_vote': False, 'vote_value': vote_type}), self.DATE),
        ]
        self._check_output_by_key(inputs, [
            {self.WAS_ACTIVE_COLUMN: 1, column_num: 1},
        ])

    def test_last_subsection(self):
        inputs = [
            ('', SUBSECTION_VIEWED_MARKER, json.dumps({
                'path': 'foobar',
                'timestamp': '2014-12-01T00:00:00.000000',
            }), self.DATE),
        ]
        self._check_output_by_key(inputs, {
            self.LAST_SUBSECTION_COLUMN: 'foobar',
        })

    def test_multiple_subsection_views(self):
        inputs = [
            ('', SUBSECTION_VIEWED_MARKER, json.dumps({
                'path': 'finalpath',
                'timestamp': '2014-12-01T00:00:04.000000',
            }), self.DATE),
            ('', SUBSECTION_VIEWED_MARKER, json.dumps({
                'path': 'foobar',
                'timestamp': '2014-12-01T00:00:00.000000',
            }), self.DATE),
            ('', SUBSECTION_VIEWED_MARKER, json.dumps({
                'path': 'foobar1',
                'timestamp': '2014-12-01T00:00:03.000000',
            }), self.DATE),
        ]
        self._check_output_by_key(inputs, {
            self.LAST_SUBSECTION_COLUMN: 'finalpath',
        })
