"""
Ensure we can read from MySQL data sources.
"""
from __future__ import absolute_import

import datetime
import textwrap
import unittest

import luigi
from mock import MagicMock, patch, sentinel
from pandas import read_csv

from edx.analytics.tasks.common.mysql_dump import MysqlSelectTask, mysql_datetime
from edx.analytics.tasks.util.tests.config import with_luigi_config
from edx.analytics.tasks.util.tests.target import FakeTarget
from edx.analytics.tasks.util.url import ExternalURL


class ConversionTestCase(unittest.TestCase):
    """
    Ensure we can reliably convert native python data types to strings.
    """

    def setUp(self):
        self.task = MysqlSelectTask(
            credentials=sentinel.ignored,
            destination=sentinel.ignored
        )

    def test_convert_datetime(self):
        self.assert_converted_string_equals(
            datetime.datetime.strptime('2014-01-02', '%Y-%m-%d').date(), '2014-01-02'
        )

    def assert_converted_string_equals(self, obj, expected_string):
        """
        Args:
            obj (mixed): Any object.
            expected_string (str): The expected string representation of `obj`.

        Raises:
            AssertionError: iff the string resulting from converting `obj` to a string does not match the
                expected string.
        """
        self.assertEquals(self.task.convert(obj), expected_string)

    def test_convert_integer(self):
        self.assert_converted_string_equals(
            10, '10'
        )

    def test_convert_none(self):
        self.assert_converted_string_equals(
            None, '-'
        )

    def test_convert_unicode(self):
        self.assert_converted_string_equals(
            u'\u0669(\u0361\u0e4f\u032f\u0361\u0e4f)\u06f6',
            u'\u0669(\u0361\u0e4f\u032f\u0361\u0e4f)\u06f6'.encode('utf-8')
        )


class MysqlSelectTaskTestCase(unittest.TestCase):
    """
    Ensure we can connect to and read data from MySQL data sources.
    """

    def setUp(self):
        patcher = patch('edx.analytics.tasks.common.mysql_dump.mysql.connector')
        self.mock_mysql_connector = patcher.start()
        self.addCleanup(patcher.stop)

        mock_conn = self.mock_mysql_connector.connect.return_value  # pylint: disable=maybe-no-member

        self.mock_cursor = mock_conn.cursor.return_value

        # By default, emulate 0 results returned
        self.mock_cursor.fetchone.return_value = None

    def run_task(self, credentials=None, query=None):
        """
        Emulate execution of a generic MysqlSelectTask.
        """
        if not credentials:
            credentials = '''\
                {
                    "host": "db.example.com",
                    "port": "3306",
                    "username": "exampleuser",
                    "password": "example password"
                }'''

        if not query:
            query = 'SELECT 1'

        # Create a dummy task that simply returns the parameters given
        class TestTask(MysqlSelectTask):
            """A generic MysqlSelectTask that wraps the parameters from the enclosing function"""

            database = "exampledata"

            @property
            def query(self):
                return query

            @property
            def filename(self):
                return None  # pragma: no cover

        task = TestTask(
            credentials=sentinel.ignored,
            destination=sentinel.ignored
        )

        fake_input = {
            'credentials': FakeTarget(value=textwrap.dedent(credentials))
        }
        task.input = MagicMock(return_value=fake_input)

        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)

        task.run()

        try:
            parsed = read_csv(output_target.buffer,
                              header=None,
                              sep="\t",
                              na_values=['-'],
                              encoding='utf-8')
        except ValueError:
            parsed = None

        return parsed

    def test_connect_with_missing_credentials(self):
        with self.assertRaises(KeyError):
            self.run_task('{}')

    def test_connect_with_credential_syntax_error(self):
        with self.assertRaises(ValueError):
            self.run_task('{')

    def test_connect_with_complete_credentials(self):
        self.run_task()

    def test_execute_query(self):
        self.mock_cursor.fetchone.side_effect = [
            (2L,),
            (3L,),
            (10L,),
            None
        ]

        output = self.run_task(query=sentinel.query)

        self.mock_cursor.execute.assert_called_once_with(sentinel.query, tuple())
        self.assertEquals(output[0][0], 2)
        self.assertEquals(output[0][1], 3)
        self.assertEquals(output[0][2], 10)

    def test_unicode_results(self):
        unicode_string = u'\u0669(\u0361\u0e4f\u032f\u0361\u0e4f)\u06f6'
        self.mock_cursor.fetchone.side_effect = [
            (unicode_string,),
            None
        ]

        output = self.run_task(query=sentinel.query)

        self.assertEquals(output[0][0], unicode_string)

    def test_default_attributes(self):
        destination = 'file:///tmp/foo'

        class GenericTask(MysqlSelectTask):
            """A dummy task used to ensure defaults are reasonable"""

            @property
            def filename(self):
                return 'bar'

        task = GenericTask(
            credentials=sentinel.credentials,
            destination=destination,
            database=sentinel.database,
        )

        self.assertEquals(task.credentials, sentinel.credentials)
        self.assertEquals(task.database, sentinel.database)
        self.assertEquals(task.destination, destination)
        self.assertEquals(task.query, 'SELECT 1')
        self.assertEquals(task.query_parameters, tuple())
        self.assertIsInstance(task.requires()['credentials'], ExternalURL)
        self.assertEquals(task.requires()['credentials'].url, sentinel.credentials)
        self.assertIsInstance(task.output(), luigi.LocalTarget)
        self.assertEquals(task.output().path, '/tmp/foo/bar')  # pylint: disable=maybe-no-member

    @with_luigi_config('database-import', 'database', 'foobar')
    def test_parameters_from_config(self):
        t = MysqlSelectTask(credentials=sentinel.credentials, destination=sentinel.destination)
        self.assertEquals(t.database, 'foobar')

    def test_mysql_timestamp(self):
        string_timestamp = '2014-01-02 13:10:11'
        timestamp = datetime.datetime.strptime(string_timestamp, '%Y-%m-%d %H:%M:%S')
        self.assertEquals(mysql_datetime(timestamp), string_timestamp)
