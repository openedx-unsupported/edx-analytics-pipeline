"""
Ensure we can write to MySQL data sources.
"""
from __future__ import absolute_import

import textwrap
import unittest

import luigi
import luigi.task
from mock import MagicMock, PropertyMock, call, patch, sentinel

from edx.analytics.tasks.common.mysql_load import MysqlInsertTask, coerce_for_mysql_connect
from edx.analytics.tasks.util.tests.config import with_luigi_config
from edx.analytics.tasks.util.tests.target import FakeTarget


class InsertToMysqlDummyTable(MysqlInsertTask):
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


class InsertIntoMysqlDummyTableWithIndexes(InsertToMysqlDummyTable):
    @property
    def indexes(self):
        return [
            ('course_id',),
            ('interval_start', 'interval_end'),
        ]


class InsertToPredefinedMysqlDummyTable(InsertToMysqlDummyTable):
    """
    Define table for testing without definitions (since table is externally defined).
    """
    @property
    def columns(self):
        return ['course_id', 'interval_start', 'interval_end', 'label', 'count']


class MysqlInsertTaskTestCase(unittest.TestCase):
    """
    Ensure we can connect to and write data to MySQL data sources.
    """

    def setUp(self):
        patcher = patch('edx.analytics.tasks.common.mysql_load.mysql.connector')
        self.mock_mysql_connector = patcher.start()
        self.addCleanup(patcher.stop)

    def create_task(self, credentials=None, source=None, insert_chunk_size=100, overwrite=False, cls=InsertToMysqlDummyTable):
        """
         Emulate execution of a generic MysqlTask.
        """
        # Make sure to flush the instance cache so we create
        # a new task object.
        luigi.task.Register.clear_instance_cache()
        task = cls(
            credentials=sentinel.ignored,
            insert_chunk_size=insert_chunk_size,
            overwrite=overwrite
        )

        if not credentials:
            credentials = '''\
                {
                    "host": "db.example.com",
                    "port": "3306",
                    "username": "exampleuser",
                    "password": "example password"
                }'''

        if not source:
            source = self._get_source_string(1)

        fake_input = {
            'credentials': FakeTarget(value=textwrap.dedent(credentials)),
        }
        task.input = MagicMock(return_value=fake_input)

        cls.insert_source_task = PropertyMock(return_value=MagicMock())
        task.insert_source_task.output = MagicMock(return_value=FakeTarget(value=textwrap.dedent(source)))

        return task

    def test_connect_with_credential_syntax_error(self):
        with self.assertRaises(ValueError):
            list(self.create_task(credentials='{').run())

    def test_run_with_default_credentials(self):
        list(self.create_task(credentials='{}').run())

    @with_luigi_config('database-export', 'database', 'foobar')
    def test_parameters_from_config(self):
        t = InsertToMysqlDummyTable(credentials=sentinel.credentials)
        self.assertEquals(t.database, 'foobar')

    def test_run(self):
        task = self.create_task()
        list(task.run())

        self.assertTrue(self.mock_mysql_connector.connect().cursor().execute.called)
        self.assertFalse(self.mock_mysql_connector.connect().rollback.called)
        self.assertTrue(self.mock_mysql_connector.connect().commit.called)
        self.assertTrue(self.mock_mysql_connector.connect().close.called)

    def test_run_with_failure(self):
        task = self.create_task()
        task.output().touch = MagicMock(side_effect=Exception("Failed to update marker"))
        with self.assertRaises(Exception):
            list(task.run())

        self.assertTrue(self.mock_mysql_connector.connect().cursor().execute.called)
        self.assertTrue(self.mock_mysql_connector.connect().rollback.called)
        self.assertFalse(self.mock_mysql_connector.connect().commit.called)
        self.assertTrue(self.mock_mysql_connector.connect().close.called)

    def test_create_table(self):
        connection = MagicMock()
        self.create_task().create_table(connection)
        connection.cursor().execute.assert_called_once_with(
            "CREATE TABLE IF NOT EXISTS dummy_table "
            "(id BIGINT(20) NOT NULL AUTO_INCREMENT,course_id VARCHAR(255),"
            "interval_start DATETIME,interval_end DATETIME,label VARCHAR(255),"
            "count INT,created TIMESTAMP DEFAULT NOW(),PRIMARY KEY (id))"
        )

    def test_create_table_without_column_definition(self):
        connection = MagicMock()
        task = self.create_task(cls=InsertToPredefinedMysqlDummyTable)
        with self.assertRaises(NotImplementedError):
            task.create_table(connection)

    def test_create_table_without_table_definition(self):
        connection = MagicMock()
        task = self.create_task(cls=MysqlInsertTask)
        with self.assertRaises(NotImplementedError):
            task.create_table(connection)

    def _get_source_string(self, num_rows=1):
        """Returns test data to be input to database table."""
        template = 'course{num}\t2014-05-01\t2014-05-08\tACTIVE\t{count}\n'
        row_strings = [template.format(num=str(num + 1), count=str(num + 50)) for num in xrange(num_rows)]
        source = ''.join(row_strings)
        return source

    def _get_expected_query(self, num_rows=1):
        """Returns query that should be generated for given number of rows."""
        row_value = '(%s,%s,%s,%s,%s)'
        all_parameters = ",".join([row_value] * num_rows)

        query = ('INSERT INTO dummy_table '
                 '(course_id,interval_start,interval_end,label,count) '
                 'VALUES {all_parameters}'
                 ).format(all_parameters=all_parameters)
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

    def test_insert_single_row(self):
        task = self.create_task(source=self._get_source_string(1))
        cursor = MagicMock()
        task.insert_rows(cursor)
        query = cursor.execute.call_args[0][0]
        self.assertEquals(query, self._get_expected_query(1))
        row_args = cursor.execute.call_args[0][1]
        self.assertEquals(row_args, self._get_expected_query_args(1))

    def test_insert_multiple_rows(self):
        task = self.create_task(source=self._get_source_string(4))
        cursor = MagicMock()
        task.insert_rows(cursor)
        query = cursor.execute.call_args[0][0]
        self.assertEquals(query, self._get_expected_query(4))
        row_args = cursor.execute.call_args[0][1]
        self.assertEquals(row_args, self._get_expected_query_args(4))

    def test_insert_multiple_rows_in_chunks(self):
        task = self.create_task(source=self._get_source_string(4), insert_chunk_size=2)
        cursor = MagicMock()
        task.insert_rows(cursor)
        execute_calls = cursor.execute.mock_calls
        self.assertEquals(len(execute_calls), 2)
        expected_query = self._get_expected_query(2)
        self.assertEquals(execute_calls[0][1][0], expected_query)
        self.assertEquals(execute_calls[1][1][0], expected_query)
        expected_row_args = self._get_expected_query_args(4)
        self.assertEquals(execute_calls[0][1][1], expected_row_args[:10])
        self.assertEquals(execute_calls[1][1][1], expected_row_args[10:])

    def test_insert_multiple_rows_not_square(self):
        # Insert a stray tab character, so that the input line will
        # find an extra column in the first row.
        source = self._get_source_string(4).replace('ACTIVE', 'AC\tTIVE', 1)
        task = self.create_task(source=source)
        with self.assertRaises(Exception):
            task.insert_rows(MagicMock())

    def test_insert_row_to_predefined_table(self):
        task = self.create_task(cls=InsertToPredefinedMysqlDummyTable)
        cursor = MagicMock()
        task.insert_rows(cursor)
        query = cursor.execute.call_args[0][0]
        self.assertEquals(query, self._get_expected_query(1))
        row_args = cursor.execute.call_args[0][1]
        self.assertEquals(row_args, self._get_expected_query_args(1))

    @with_luigi_config(('database-export', 'database', 'foobar'))
    def test_create_database(self):
        task = self.create_task()
        list(task.run())

        mock_cursor = self.mock_mysql_connector.connect.return_value.cursor.return_value
        mock_cursor.execute.assert_has_calls([
            call("CREATE DATABASE IF NOT EXISTS foobar"),
            call(
                "CREATE TABLE IF NOT EXISTS dummy_table "
                "(id BIGINT(20) NOT NULL AUTO_INCREMENT,course_id VARCHAR(255),"
                "interval_start DATETIME,interval_end DATETIME,label VARCHAR(255),"
                "count INT,created TIMESTAMP DEFAULT NOW(),PRIMARY KEY (id))"
            )
        ])

    def test_table_with_index(self):
        task = self.create_task(cls=InsertIntoMysqlDummyTableWithIndexes)
        connection = MagicMock()
        task.create_table(connection)
        connection.cursor.return_value.execute.assert_called_once_with(
            "CREATE TABLE IF NOT EXISTS dummy_table "
            "(id BIGINT(20) NOT NULL AUTO_INCREMENT,course_id VARCHAR(255),"
            "interval_start DATETIME,interval_end DATETIME,label VARCHAR(255),"
            "count INT,created TIMESTAMP DEFAULT NOW(),PRIMARY KEY (id),"
            "INDEX (course_id),INDEX (interval_start,interval_end))"
        )

    def test_overwrite_with_empty_results(self):
        # A source of '' will result in a default source of one row, so add whitespace.
        task = self.create_task(source='   ', overwrite=True)
        with self.assertRaisesRegexp(Exception, 'Cannot overwrite a table with an empty result set.'):
            task.insert_rows(MagicMock())


class MySQLLoadHelperFuncTests(unittest.TestCase):
    """
    Unit tests for helper functions
    """
    COERCE_TEST_CASES = [
        (None, None),
        ('None', None),
        (u'None', None),
        (1, 1),
        ('abc', u'abc'),
        ('\xe5\x8c\x85\xe5\xad\x90', u'\u5305\u5b50'),
        (u'\u5305\u5b50', u'\u5305\u5b50'),
    ]

    def test_coerce_for_mysql_connect(self):
        for input, output in self.COERCE_TEST_CASES:
            self.assertEqual(coerce_for_mysql_connect(input), output)
