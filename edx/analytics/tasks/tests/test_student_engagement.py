"""Test student engagement metrics"""

import json

import luigi

from edx.analytics.tasks.student_engagement import StudentEngagementTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin


class StudentEngagementTaskMapTest(InitializeOpaqueKeysMixin, unittest.TestCase):
    """Test analysis of detailed student engagement"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"

    def setUp(self):
        self.initialize_ids()

        fake_param = luigi.DateIntervalParameter()
        self.task = StudentEngagementTask(
            interval=fake_param.parse('2013-12-17'),
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
        self.default_key = ('2013-12-17', self.course_id, 'test_user')

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
            (self.problem_id, 'problem_check', {})
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
            (self.problem_id, 'problem_check', {'correct': True})
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
            (self.video_id, 'play_video', {})
        )

    def test_implicit_event(self):
        self.assert_single_map_output(
            self._create_event_log_line(event_type='/jsi18n/', event_source='server'),
            self.default_key,
            ('', '/jsi18n/', {})
        )

    def test_course_event(self):
        self.assert_single_map_output(
            self._create_event_log_line(event_type='/courses/foo/bar/', event_source='server'),
            self.default_key,
            ('', '/courses/foo/bar/', {})
        )

    def test_section_view_event(self):
        event_type = '/courses/{0}/courseware/foo/'.format(self.course_id)
        self.assert_single_map_output(
            self._create_event_log_line(event_type=event_type, event_source='server'),
            self.default_key,
            ('', event_type, {})
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
            })
        )

    def test_subsection_sequence_num_event(self):
        self.assert_last_subsection_viewed_recognized('foo/bar/10')

    def test_subsection_jquery_event(self):
        self.assert_last_subsection_viewed_recognized('foo/bar/jquery.js')


class StudentEngagementTaskLegacyMapTest(InitializeLegacyKeysMixin, unittest.TestCase):
    """Test analysis of detailed student engagement using legacy ID formats"""
    pass


class StudentEngagementTaskReducerTest(unittest.TestCase):
    """
    Tests to verify that engagement data is reduced properly
    """

    DATE = '2013-12-17'
    COURSE_ID = 'foo/bar/baz'
    USERNAME = 'test_user'
    REDUCE_KEY = (DATE, COURSE_ID, USERNAME)

    WAS_ACTIVE_COLUMN_NUM = 3
    PROBLEMS_ATTEMPTED_COLUMN = 4

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
            ('', '/foo', {})
        ]
        self._check_output(inputs, self.WAS_ACTIVE_COLUMN_NUM, 1)

    def _check_output(self, inputs, column_num, expected_value):
        """Compare generated with expected output."""
        output = self._get_reducer_output(inputs)
        self.assertEquals(len(output), 1)
        self.assertEquals(output[0][column_num], expected_value)

    def test_single_problem_attempted(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', {})
        ]
        self._check_output(inputs, self.PROBLEMS_ATTEMPTED_COLUMN, 1)

    def test_single_problem_attempted_multiple_events(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', {}),
            ('i4x://foo/bar/baz', 'problem_check', {}),
            ('i4x://foo/bar/baz', 'problem_check', {})
        ]
        self._check_output(inputs, self.PROBLEMS_ATTEMPTED_COLUMN, 1)

    def test_multiple_problems(self):
        inputs = [
            ('i4x://foo/bar/baz', 'problem_check', {}),
            ('i4x://foo/bar/baz2', 'problem_check', {}),
            ('i4x://foo/bar/baz', 'problem_check', {})
        ]
        self._check_output(inputs, self.PROBLEMS_ATTEMPTED_COLUMN, 2)
