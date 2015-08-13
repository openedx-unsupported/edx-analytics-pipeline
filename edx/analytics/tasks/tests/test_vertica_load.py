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

from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget
from edx.analytics.tasks.tests.config import with_luigi_config


class CopyToVerticaDummyTable(VerticaCopyTask):
    """
    Define table for testing.
    """
    @property
    def table(self):
        return "dummy_table"

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255)'),
            ('interval_start', 'DATETIME'),
            ('interval_end', 'DATETIME'),
            ('label', 'VARCHAR(255)'),
            ('count', 'INT'),
        ]

    @property
    def insert_source_task(self):
        return None


class CopyToPredefinedVerticaDummyTable(CopyToVerticaDummyTable):
    """
    Define table for testing without definitions (since table is externally defined).
    """

    @property
    def columns(self):
        return ['course_id', 'interval_start', 'interval_end', 'label', 'count']


class VerticaCopyTaskTest(unittest.TestCase):
    """
    Ensure we can connect to and write data to Vertica data sources.
    """

    def setUp(self):
        patcher = patch('edx.analytics.tasks.vertica_load.vertica_python.vertica')
        self.mock_vertica_connector = patcher.start()
        self.addCleanup(patcher.stop)

    def create_task(self, credentials=None, source=None, overwrite=False, cls=CopyToVerticaDummyTable):
        """
         Emulate execution of a generic VerticaCopyTask.
        """
        # Make sure to flush the instance cache so we create
        # a new task object.
        luigi.task.Register.clear_instance_cache()
        task = cls(
            credentials=sentinel.ignored,
            overwrite=overwrite
        )

        if not credentials:
            credentials = '''\
                {
                    "host": "db.example.com",
                    "port": 5433,
                    "user": "exampleuser",
                    "password": "example password"
                }'''

        if not source:
            source = self._get_source_string(1)

        fake_input = {
            'credentials': FakeTarget(value=textwrap.dedent(credentials)),
            'insert_source': FakeTarget(value=textwrap.dedent(source))
        }

        fake_output = MagicMock(return_value=self.mock_vertica_connector)

        task.input = MagicMock(return_value=fake_input)
        task.output = fake_output
        return task

    def test_run_with_default_credentials(self):
        self.create_task(credentials='{}').run()

    @with_luigi_config('vertica-export', 'schema', 'foobar')
    def test_parameters_from_config(self):
        task = CopyToVerticaDummyTable(credentials=sentinel.credentials)
        self.assertEquals(task.schema, 'foobar')

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

    def test_create_table(self):
        connection = MagicMock()
        self.create_task().create_table(connection)
        connection.cursor().execute.assert_called_once_with(
            "CREATE TABLE IF NOT EXISTS testing.dummy_table "
            "(id AUTO_INCREMENT,course_id VARCHAR(255),"
            "interval_start DATETIME,interval_end DATETIME,label VARCHAR(255),"
            "count INT,created TIMESTAMP DEFAULT NOW(),PRIMARY KEY (id))"
        )

    def test_create_table_without_column_definition(self):
        connection = MagicMock()
        task = self.create_task(cls=CopyToPredefinedVerticaDummyTable)
        with self.assertRaises(NotImplementedError):
            task.create_table(connection)

    def test_create_table_without_table_definition(self):
        connection = MagicMock()
        task = self.create_task(cls=VerticaCopyTask)
        with self.assertRaises(NotImplementedError):
            task.create_table(connection)

    def _get_source_string(self, num_rows=1):
        """Returns test data to be input to database table."""
        template = 'course{num}\t2014-05-01\t2014-05-08\tACTIVE\t{count}\n'
        row_strings = [template.format(num=str(num + 1), count=str(num + 50)) for num in xrange(num_rows)]
        source = ''.join(row_strings)
        return source

    def _get_expected_query(self):
        """Returns query that should be generated for copying into the table."""
        query = ("COPY {schema}.dummy_table (course_id,interval_start,interval_end,label,count) "
                 "FROM STDIN DELIMITER AS E'\t' NULL AS '\\N' DIRECT ABORT ON ERROR NO COMMIT;"
                 .format(schema=self.create_task().schema))
        return query

    def _get_expected_query_args(self, num_rows=1):
        """Returns query args that should be generated for given number of rows of input."""
        expected_row_args = []
        for num in xrange(num_rows):
            expected_row_args.append('course{num}'.format(num=str(num + 1)))
            expected_row_args.append('2014-05-01')
            expected_row_args.append('2014-05-08')
            expected_row_args.append('ACTIVE')
            expected_row_args.append('{count}'.format(count=str(num + 50)))
        return expected_row_args

    def test_copy_single_row(self):
        task = self.create_task(source=self._get_source_string(1))
        cursor = MagicMock()
        task.copy_data_table_from_target(cursor)
        query = cursor.copy.call_args[0][0]
        self.assertEquals(query, self._get_expected_query())
        file_to_copy = cursor.copy.call_args[0][1]
        with task.input()['insert_source'].open('r') as expected_data:
            expected_source = expected_data.read()
        sent_source = file_to_copy.read()
        self.assertEquals(sent_source, expected_source)

    def test_copy_multiple_rows(self):
        task = self.create_task(source=self._get_source_string(4))
        cursor = MagicMock()
        task.copy_data_table_from_target(cursor)
        query = cursor.copy.call_args[0][0]
        self.assertEquals(query, self._get_expected_query())
        file_to_copy = cursor.copy.call_args[0][1]
        with task.input()['insert_source'].open('r') as expected_data:
            expected_source = expected_data.read()
        sent_source = file_to_copy.read()
        self.assertEquals(sent_source, expected_source)

    def test_copy_to_predefined_table(self):
        task = self.create_task(cls=CopyToPredefinedVerticaDummyTable)
        cursor = MagicMock()
        task.copy_data_table_from_target(cursor)
        query = cursor.copy.call_args[0][0]
        self.assertEquals(query, self._get_expected_query())
        file_to_copy = cursor.copy.call_args[0][1]
        with task.input()['insert_source'].open('r') as expected_data:
            expected_source = expected_data.read()
        sent_source = file_to_copy.read()
        self.assertEquals(sent_source, expected_source)

    @with_luigi_config(('vertica-export', 'schema', 'foobar'))
    def test_create_schema(self):
        task = self.create_task()
        task.run()

        mock_cursor = self.mock_vertica_connector.connect.return_value.cursor.return_value
        mock_cursor.execute.assert_has_calls([
            call("CREATE SCHEMA IF NOT EXISTS foobar"),
            call(
                "CREATE TABLE IF NOT EXISTS foobar.dummy_table "
                "(id AUTO_INCREMENT,course_id VARCHAR(255),"
                "interval_start DATETIME,interval_end DATETIME,label VARCHAR(255),"
                "count INT,created TIMESTAMP DEFAULT NOW(),PRIMARY KEY (id))"
            )
        ])
