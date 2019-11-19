"""Test active users computation"""

from __future__ import absolute_import

from unittest import TestCase

import luigi

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeOpaqueKeysMixin
from edx.analytics.tasks.warehouse.load_internal_reporting_active_users import ActiveUsersTask


class ActiveUsersMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """

    def setUp(self):
        self.task_class = ActiveUsersTask
        self.interval = '2017-02-01-2017-02-28'
        super(ActiveUsersMapTest, self).setUp()

        self.initialize_ids()
        self.username = 'test_user'
        self.timestamp = "2017-02-17T15:38:32.805444"

        self.event_templates = {
            'active_user_event': {
                "username": self.username,
                "host": "test_host",
                "event_source": "server",
                "event_type": "test_event",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": '',
                },
                "time": "{0}+00:00".format(self.timestamp),
            }
        }
        self.default_event_template = 'active_user_event'

    def test_unparseable_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_missing_username(self):
        line = self.create_event_log_line(username='')
        self.assert_no_map_output_for(line)

    def test_mapper(self):
        line = self.create_event_log_line()
        self.assert_single_map_output(line, ('2017-02-13', '2017-02-20', 'test_user'), 1)


class ActiveUsersReduceTest(InitializeOpaqueKeysMixin, ReducerTestMixin, TestCase):
    """
    Tests to verify that ActiveUsersTask reducer works correctly.
    """

    def setUp(self):
        self.task = ActiveUsersTask(
            interval=luigi.DateIntervalParameter().parse('2017-02-01-2017-02-28'),
        )

        self.reduce_key = ('2017-02-13', '2017-02-20', 'test_user')

    def test_reducer(self):
        inputs = [(1), (1), (1)]
        expected = (self.reduce_key,)
        self._check_output_complete_tuple(inputs, expected)
