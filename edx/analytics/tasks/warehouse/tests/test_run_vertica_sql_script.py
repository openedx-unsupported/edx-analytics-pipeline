"""
Ensure we can write to Vertica data sources.
"""
from __future__ import absolute_import

import textwrap

import luigi
import luigi.task

from mock import call
from mock import MagicMock
from mock import patch
from mock import sentinel

from edx.analytics.tasks.run_vertica_sql_script import RunVerticaSqlScriptTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget
from edx.analytics.tasks.tests.config import with_luigi_config


class RunVerticaSqlScriptTaskTest(unittest.TestCase):
    """
    Ensure we can connect to and write data to Vertica data sources.
    """

    def setUp(self):
        patcher = patch('edx.analytics.tasks.run_vertica_sql_script.vertica_python.vertica')
        self.mock_vertica_connector = patcher.start()
        self.addCleanup(patcher.stop)

    def create_task(self, credentials=None, source_script=None):
        """
        Emulate execution of a generic RunVerticaSqlScriptTask.
        """
        # Make sure to flush the instance cache so we create a new task object.
        luigi.task.Register.clear_instance_cache()
        task = RunVerticaSqlScriptTask(
            credentials=sentinel.ignored,
            script_name='my simple script',
            source_script=sentinel.ignored,
        )

        if not credentials:
            credentials = '''\
                {
                    "host": "db.example.com",
                    "port": 5433,
                    "user": "exampleuser",
                    "password": "example password"
                }'''

        # This SQL doesn't actually run, but I've used real SQL to provide context. :)
        source = '''
        DELETE TABLE my_schema.my_table;
        CREATE TABLE my_schema.my_table AS SELECT foo, bar, baz FROM my_schema.another_table;
        '''

        fake_input = {
            'credentials': FakeTarget(value=textwrap.dedent(credentials)),
            'source_script': FakeTarget(value=textwrap.dedent(source))
        }

        fake_output = MagicMock(return_value=self.mock_vertica_connector)
        self.mock_vertica_connector.marker_schema = "name_of_marker_schema"
        self.mock_vertica_connector.marker_table = "name_of_marker_table"

        task.input = MagicMock(return_value=fake_input)
        task.output = fake_output
        return task

    def test_run_with_default_credentials(self):
        self.create_task(credentials='{}').run()

    def test_run(self):
        self.create_task().run()
        mock_conn = self.mock_vertica_connector.connect()
        self.assertTrue(mock_conn.cursor().execute.called)
        self.assertFalse(mock_conn.rollback.called)
        self.assertTrue(mock_conn.commit.called)
        self.assertTrue(mock_conn.close.called)

    def test_run_with_failure(self):
        task = self.create_task()
        task.output().touch = MagicMock(side_effect=Exception("Failed to update marker"))
        with self.assertRaises(Exception):
            task.run()
        mock_conn = self.mock_vertica_connector.connect()
        self.assertTrue(mock_conn.cursor().execute.called)
        self.assertTrue(mock_conn.rollback.called)
        self.assertFalse(mock_conn.commit.called)
        self.assertTrue(mock_conn.close.called)
