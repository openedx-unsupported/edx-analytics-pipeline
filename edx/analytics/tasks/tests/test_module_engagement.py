"""Test metrics for student engagement with modules"""

import json

import luigi
from ddt import ddt, data, unpack

from edx.analytics.tasks.module_engagement import ModuleEngagementDataTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin


@ddt
class ModuleEngagementTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, unittest.TestCase):
    """Base class for test analysis of detailed student engagement"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    def setUp(self):
        super(ModuleEngagementTaskMapTest, self).setUp()

        self.initialize_ids()
        self.video_id = 'i4x-foo-bar-baz'
        self.forum_id = 'a2cb123f9c2146f3211cdc6901acb00e'
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
            },
            'edx.forum.object.created': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "edx.forum.comment.created",
                "name": "edx.forum.comment.created",
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "event": {
                    "commentable_id": self.forum_id,
                },
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "ip": "127.0.0.1",
                "page": None,
            }
        }
        self.default_event_template = 'problem_check'
        self.create_task()

    def create_task(self, date=None):  # pylint: disable=arguments-differ
        """Allow arguments to be passed to the task constructor."""
        if not date:
            date = self.DEFAULT_DATE
        self.task = ModuleEngagementDataTask(
            date=luigi.DateParameter().parse(date),
            output_root='/fake/output',
        )
        self.task.init_local()

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
            self.get_expected_output_key('problem', self.problem_id, 'attempted'),
            1
        )

    def get_expected_output_key(self, entity_type, entity_id, action):
        """Generate the expected key"""
        return self.course_id, 'test_user', self.DEFAULT_DATE, entity_type, entity_id, action

    def test_correct_problem_check(self):
        template = self.event_templates['problem_check']
        template['event']['success'] = 'correct'

        self.assert_map_output(
            json.dumps(template),
            [
                (self.get_expected_output_key('problem', self.problem_id, 'completed'), 1),
                (self.get_expected_output_key('problem', self.problem_id, 'attempted'), 1)
            ]
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
            self.get_expected_output_key('video', self.video_id, 'played'),
            1
        )

    def test_missing_forum_id(self):
        template = self.event_templates['edx.forum.object.created']
        del template['event']['commentable_id']
        self.assert_no_map_output_for(self.create_event_log_line(template=template))

    @data(
        ('edx.forum.comment.created', 'commented'),
        ('edx.forum.response.created', 'responded'),
        ('edx.forum.thread.created', 'created'),
    )
    @unpack
    def test_forum_posting_events(self, event_type, expected_action):
        template = self.event_templates['edx.forum.object.created']
        template['event_type'] = event_type
        template['name'] = event_type
        self.assert_single_map_output(
            json.dumps(template),
            self.get_expected_output_key('forum', self.forum_id, expected_action),
            1
        )


class ModuleEngagementTaskMapLegacyKeysTest(InitializeLegacyKeysMixin, ModuleEngagementTaskMapTest):
    """Also test with legacy keys"""
    pass


@ddt
class ModuleEngagementTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """
    Tests to verify that engagement data is reduced properly
    """

    task_class = ModuleEngagementDataTask

    def setUp(self):
        super(ModuleEngagementTaskReducerTest, self).setUp()

        self.reduce_key = (self.COURSE_ID, 'test_user', self.DATE, 'problem', 'foobar', 'completed')

    def test_replacement_of_count(self):
        inputs = [1, 1, 1, 1]
        self._check_output_complete_tuple(
            inputs,
            (('\t'.join(self.reduce_key), 4),)
        )
