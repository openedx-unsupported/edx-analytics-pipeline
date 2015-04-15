"""Test student engagement metrics"""

import json

import luigi
from ddt import ddt, data, unpack

from edx.analytics.tasks.student_engagement import StudentEngagementTask, SUBSECTION_VIEWED_MARKER
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


class StudentEngagementTaskMapTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """Test analysis of detailed student engagement"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    def setUp(self):
        self.initialize_ids()

        fake_param = luigi.DateIntervalParameter()
        self.task = StudentEngagementTask(
            interval=fake_param.parse(self.DEFAULT_DATE),
            output_root='/fake/output'
        )
        self.task.init_local()

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
        self.default_key = (self.DEFAULT_DATE, self.course_id, 'test_user')

    def test_invalid_events(self):
        self.assert_no_map_output_for(self._create_event_log_line(time="2013-12-01T15:38:32.805444"))
        self.assert_no_map_output_for(self._create_event_log_line(username=''))
        self.assert_no_map_output_for(self._create_event_log_line(event_type=None))
        self.assert_no_map_output_for(self._create_event_log_line(context={'course_id': 'lskdjfslkdj'}))
        self.assert_no_map_output_for(self._create_event_log_line(event='sdfasdf'))

    def assert_no_map_output_for(self, line):
        """Assert that an input line generates no output."""

        self.assertEquals(
            tuple(self.task.mapper(line)),
            tuple()
        )

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        event_dict = kwargs.pop('template', self.event_templates['play_video']).copy()
        event_dict.update(**kwargs)
        return event_dict

    def test_browser_problem_check_event(self):
        template = self.event_templates['problem_check']
        self.assert_no_map_output_for(self._create_event_log_line(template=template, event_source='browser'))

    def test_incorrect_problem_check(self):
        self.assert_single_map_output(
            json.dumps(self.event_templates['problem_check']),
            self.default_key,
            (self.problem_id, 'problem_check', {}, self.DEFAULT_DATE)
        )

    def assert_single_map_output(self, line, expected_key, expected_value):
        """Assert that an input line generates exactly one output record with the expected key and value"""
        mapper_output = tuple(self.task.mapper(line))
        self.assertEquals(len(mapper_output), 1)
        row = mapper_output[0]
        self.assertEquals(len(row), 2)
        actual_key, actual_value = row
        self.assertEquals(expected_key, actual_key)
        self.assertEquals(expected_value, actual_value)

    def test_correct_problem_check(self):
        template = self.event_templates['problem_check']
        template['event']['success'] = 'correct'
        self.assert_single_map_output(
            json.dumps(template),
            self.default_key,
            (self.problem_id, 'problem_check', {'correct': True}, self.DEFAULT_DATE)
        )

    def test_missing_problem_id(self):
        template = self.event_templates['problem_check']
        del template['event']['problem_id']
        self.assert_no_map_output_for(self._create_event_log_line(template=template))

    def test_missing_video_id(self):
        template = self.event_templates['play_video']
        template['event'] = '{"currentTime": "23.4398", "code": "87389iouhdfh"}'
        self.assert_no_map_output_for(self._create_event_log_line(template=template))

    def test_play_video(self):
        self.assert_single_map_output(
            json.dumps(self.event_templates['play_video']),
            self.default_key,
            (self.video_id, 'play_video', {}, self.DEFAULT_DATE)
        )

    def test_implicit_event(self):
        self.assert_single_map_output(
            self._create_event_log_line(event_type='/jsi18n/', event_source='server'),
            self.default_key,
            ('', '/jsi18n/', {}, self.DEFAULT_DATE)
        )

    def test_course_event(self):
        self.assert_single_map_output(
            self._create_event_log_line(event_type='/courses/foo/bar/', event_source='server'),
            self.default_key,
            ('', '/courses/foo/bar/', {}, self.DEFAULT_DATE)
        )

    def test_section_view_event(self):
        event_type = '/courses/{0}/courseware/foo/'.format(self.course_id)
        self.assert_single_map_output(
            self._create_event_log_line(event_type=event_type, event_source='server'),
            self.default_key,
            ('', event_type, {}, self.DEFAULT_DATE)
        )

    def test_subsection_event(self):
        self.assert_last_subsection_viewed_recognized('foo/bar/')

    def assert_last_subsection_viewed_recognized(self, end_of_path):
        """Assert that given a path ending the event is recognized as a subsection view"""
        event_type = '/courses/{0}/courseware/{1}'.format(self.course_id, end_of_path)
        self.assert_single_map_output(
            self._create_event_log_line(event_type=event_type, event_source='server'),
            self.default_key,
            ('', 'marker:last_subsection_viewed', {
                'path': event_type,
                'timestamp': self.DEFAULT_TIMESTAMP,
            }, self.DEFAULT_DATE)
        )

    def test_subsection_sequence_num_event(self):
        self.assert_last_subsection_viewed_recognized('foo/bar/10')

    def test_subsection_jquery_event(self):
        self.assert_last_subsection_viewed_recognized('foo/bar/jquery.js')


class StudentEngagementTaskLegacyMapTest(InitializeLegacyKeysMixin, unittest.TestCase):
    """Test analysis of detailed student engagement using legacy ID formats"""
    pass


@ddt
class StudentEngagementTaskReducerTest(unittest.TestCase):
    """
    Tests to verify that engagement data is reduced properly
    """

    DATE = '2013-12-17'
    COURSE_ID = 'foo/bar/baz'
    USERNAME = 'test_user'
    REDUCE_KEY = (DATE, COURSE_ID, USERNAME)

    WAS_ACTIVE_COLUMN = 3
    PROBLEMS_ATTEMPTED_COLUMN = 4
    PROBLEM_ATTEMPTS_COLUMN = 5
    PROBLEMS_CORRECT_COLUMN = 6
    VIDEOS_PLAYED_COLUMN = 7
    FORUM_POSTS_COLUMN = 8
    FORUM_REPLIES_COLUMN = 9
    FORUM_COMMENTS_COLUMN = 10
    TEXTBOOK_PAGES_COLUMN = 11
    LAST_SUBSECTION_COLUMN = 12

    def setUp(self):
        fake_param = luigi.DateIntervalParameter()
        self.task = StudentEngagementTask(
            interval=fake_param.parse(self.DATE),
            output_root='/fake/output'
        )
        self.task.init_local()

    def test_no_events(self):
        output = self._get_reducer_output([])
        self.assertEquals(len(output), 0)

    def _get_reducer_output(self, inputs):
        """Run the reducer and return the ouput"""
        return tuple(self.task.reducer(self.REDUCE_KEY, inputs))

    def test_any_activity(self):
        inputs = [
            ('', '/foo', {}, self.DATE)
        ]
        self._check_output(inputs, {
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

    def _check_output(self, inputs, column_values):
        """Compare generated with expected output."""
        output = self._get_reducer_output(inputs)
        self.assertEquals(len(output), 1)
        for column_num, expected_value in column_values.iteritems():
            self.assertEquals(output[0][column_num], expected_value)

    def test_single_problem_attempted(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', {'correct': True}, self.DATE)
        ]
        self._check_output(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 1,
            self.PROBLEM_ATTEMPTS_COLUMN: 1,
            self.PROBLEMS_CORRECT_COLUMN: 1,
        })

    def test_single_problem_attempted_incorrect(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', {}, self.DATE)
        ]
        self._check_output(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 1,
            self.PROBLEM_ATTEMPTS_COLUMN: 1,
            self.PROBLEMS_CORRECT_COLUMN: 0,
        })

    def test_single_problem_attempted_multiple_events(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', {'correct': True}, self.DATE),
            ('i4x://foo/bar/baz', 'problem_check', {'correct': True}, self.DATE),
            ('i4x://foo/bar/baz', 'problem_check', {}, self.DATE)
        ]
        self._check_output(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 1,
            self.PROBLEM_ATTEMPTS_COLUMN: 3,
            self.PROBLEMS_CORRECT_COLUMN: 1,
        })

    def test_multiple_problems_attempted(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', {'correct': True}, self.DATE),
            ('i4x://foo/bar/baz2', 'problem_check', {'correct': True}, self.DATE),
            ('i4x://foo/bar/baz', 'problem_check', {}, self.DATE)
        ]
        self._check_output(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.PROBLEMS_ATTEMPTED_COLUMN: 2,
            self.PROBLEM_ATTEMPTS_COLUMN: 3,
            self.PROBLEMS_CORRECT_COLUMN: 2,
        })

    def test_single_video_played(self):
        inputs = [
            ('foobarbaz', 'play_video', {}, self.DATE),
        ]
        self._check_output(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.VIDEOS_PLAYED_COLUMN: 1,
        })

    def test_multiple_video_plays_same_video(self):
        inputs = [
            ('foobarbaz', 'play_video', {}, self.DATE),
            ('foobarbaz', 'play_video', {}, self.DATE),
            ('foobarbaz', 'play_video', {}, self.DATE),
        ]
        self._check_output(inputs, {
            self.WAS_ACTIVE_COLUMN: 1,
            self.VIDEOS_PLAYED_COLUMN: 1,
        })

    def test_other_video_events(self):
        inputs = [
            ('foobarbaz', 'pause_video', {}, self.DATE),
            ('foobarbaz2', 'seek_video', {}, self.DATE),
        ]
        self._check_output(inputs, {
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
            ('', event_type, {}, self.DATE),
        ]
        self._check_output(inputs, {
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
            ('', event_type, {}, self.DATE),
            ('', event_type, {}, self.DATE),
        ]
        self._check_output(inputs, {
            column_num: 2,
        })

    def test_last_subsection(self):
        inputs = [
            ('', SUBSECTION_VIEWED_MARKER, {'path': 'foobar', 'timestamp': '2014-12-01T00:00:00.000000'}, self.DATE),
        ]
        self._check_output(inputs, {
            self.LAST_SUBSECTION_COLUMN: 'foobar',
        })

    def test_multiple_subsection_views(self):
        inputs = [
            ('', SUBSECTION_VIEWED_MARKER, {'path': 'finalpath', 'timestamp': '2014-12-01T00:00:04.000000'}, self.DATE),
            ('', SUBSECTION_VIEWED_MARKER, {'path': 'foobar', 'timestamp': '2014-12-01T00:00:00.000000'}, self.DATE),
            ('', SUBSECTION_VIEWED_MARKER, {'path': 'foobar1', 'timestamp': '2014-12-01T00:00:03.000000'}, self.DATE),
        ]
        self._check_output(inputs, {
            self.LAST_SUBSECTION_COLUMN: 'finalpath',
        })
