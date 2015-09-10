"""Tests for Sqoop import task."""

import textwrap
import json

from mock import MagicMock, patch, sentinel, Mock

from edx.analytics.tasks.sqoop import SqoopImportFromMysql
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget


class SqoopImportFromMysqlTestCase(unittest.TestCase):
    """
    Ensure we can pass the right arguments to Sqoop.
    """

    def setUp(self):
        patcher = patch('edx.analytics.tasks.sqoop.SqoopPasswordTarget')
        self.mock_sqoop_password_target = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_sqoop_password_target().path = "/temp/password_file"

        patcher2 = patch("luigi.hadoop.run_and_track_hadoop_job")
        self.mock_run = patcher2.start()
        self.addCleanup(patcher2.stop)

    def create_task(self, num_mappers=None, where=None, verbose=False,
                    columns=None, null_string=None, fields_terminated_by=None, delimiter_replacement=None,
                    overwrite=False, direct=True, mysql_delimiters=True):
        """Create a SqoopImportFromMysql with specified options."""
        task = SqoopImportFromMysql(
            credentials=sentinel.ignored,
            database='exampledata',
            destination="/fake/destination",
            table_name="example_table",
            num_mappers=num_mappers,
            where=where,
            verbose=verbose,
            columns=columns if columns is not None else [],
            null_string=null_string,
            fields_terminated_by=fields_terminated_by,
            delimiter_replacement=delimiter_replacement,
            overwrite=overwrite,
            direct=direct,
            mysql_delimiters=mysql_delimiters,
        )
        return task

    def run_task(self, task, credentials=None):
        """Emulate execution of a Sqoop import from Mysql."""
        if not credentials:
            credentials = '''\
                {
                    "host": "db.example.com",
                    "port": "3306",
                    "username": "exampleuser",
                    "password": "example password"
                }'''
        fake_input = {
            'credentials': FakeTarget(value=textwrap.dedent(credentials))
        }
        task.input = MagicMock(return_value=fake_input)

        metadata_output_target = FakeTarget()
        task.metadata_output = MagicMock(return_value=metadata_output_target)

        task.run()

    def create_and_run_task(self, credentials=None, num_mappers=None, where=None, verbose=False,
                            columns=None, null_string=None, fields_terminated_by=None, delimiter_replacement=None,
                            overwrite=False, direct=True, mysql_delimiters=True):
        """Create a SqoopImportFromMysql task with specified options, and then run it."""
        task = self.create_task(
            num_mappers=num_mappers,
            where=where,
            verbose=verbose,
            columns=columns,
            null_string=null_string,
            fields_terminated_by=fields_terminated_by,
            delimiter_replacement=delimiter_replacement,
            overwrite=overwrite,
            direct=direct,
            mysql_delimiters=mysql_delimiters,
        )
        self.run_task(task, credentials)

    def get_call_args_after_run(self):
        """Returns args for first call to Hadoop."""
        return self.mock_run.call_args[0][0]

    def test_connect_with_missing_credentials(self):
        with self.assertRaises(KeyError):
            self.create_and_run_task('{}')
        self.assertTrue(self.mock_sqoop_password_target().remove.called)
        self.assertFalse(self.mock_run.called)

    def test_connect_with_credential_syntax_error(self):
        with self.assertRaises(ValueError):
            self.create_and_run_task('{')
        self.assertTrue(self.mock_sqoop_password_target().remove.called)
        self.assertFalse(self.mock_run.called)

    def test_connect_with_complete_credentials(self):
        self.create_and_run_task()
        arglist = self.get_call_args_after_run()
        self.assertTrue(self.mock_run.called)
        expected_arglist = [
            'sqoop',
            'import',
            '--connect',
            'jdbc:mysql://db.example.com/exampledata',
            '--username',
            'exampleuser',
            '--password-file',
            '/temp/password_file',
            '--table',
            'example_table',
            '--target-dir',
            '/fake/destination',
            '--direct',
            '--mysql-delimiters'
        ]
        self.assertEquals(arglist, expected_arglist)
        self.assertTrue(self.mock_sqoop_password_target().remove.called)

    def test_verbose_arguments(self):
        self.create_and_run_task(verbose=True)
        arglist = self.get_call_args_after_run()
        self.assertIn('--verbose', arglist)

    def test_connect_with_where_args(self):
        self.create_and_run_task(where='id < 50')
        arglist = self.get_call_args_after_run()
        self.assertEquals(arglist[-4], '--where')
        self.assertEquals(arglist[-3], 'id < 50')

    def test_connect_with_num_mappers(self):
        self.create_and_run_task(num_mappers=50)
        arglist = self.get_call_args_after_run()
        self.assertEquals(arglist[-4], '--num-mappers')
        self.assertEquals(arglist[-3], '50')

    def test_connect_with_columns(self):
        self.create_and_run_task(columns=['column1', 'column2'])
        arglist = self.get_call_args_after_run()
        self.assertEquals(arglist[-4], '--columns')
        self.assertEquals(arglist[-3], 'column1,column2')

    def test_connect_with_null_string(self):
        self.create_and_run_task(null_string='\\\\N')
        arglist = self.get_call_args_after_run()
        self.assertEquals(arglist[-6], '--null-string')
        self.assertEquals(arglist[-5], '\\\\N')
        self.assertEquals(arglist[-4], '--null-non-string')
        self.assertEquals(arglist[-3], '\\\\N')

    def test_connect_with_fields_terminations(self):
        self.create_and_run_task(fields_terminated_by='\x01')
        arglist = self.get_call_args_after_run()
        self.assertEquals(arglist[-4], '--fields-terminated-by')
        self.assertEquals(arglist[-3], '\x01')

    def test_connect_with_delimiter_replacement(self):
        self.create_and_run_task(delimiter_replacement=' ')
        arglist = self.get_call_args_after_run()
        self.assertEquals(arglist[-4], '--hive-delims-replacement')
        self.assertEquals(arglist[-3], ' ')

    def test_connect_without_mysql_delimiters(self):
        self.create_and_run_task(mysql_delimiters=False)
        arglist = self.get_call_args_after_run()
        self.assertNotIn('--mysql-delimiters', arglist)

    def test_connect_without_direct(self):
        self.create_and_run_task(direct=False)
        arglist = self.get_call_args_after_run()
        self.assertNotIn('--direct', arglist)

    def test_metadata(self):
        task = self.create_task()
        self.run_task(task)
        self.assertFalse(task.complete())
        metadata_target = task.metadata_output()
        metadata_output = metadata_target.buffer.read()
        metadata_dict = json.loads(metadata_output)
        self.assertIn('start_time', metadata_dict)
        self.assertIn('end_time', metadata_dict)

    def test_overwrite(self):
        kwargs = {'overwrite': True}
        task = self.create_task(**kwargs)
        output_target = Mock()
        task.output = Mock(return_value=output_target)
        output_target.exists = Mock(return_value=True)
        output_target.complete = Mock(return_value=True)

        self.assertFalse(task.complete())
        self.assertFalse(task.output().exists.called)

        self.run_task(task)

        self.assertTrue(task.output().exists.called)
        self.assertTrue(task.output().remove.called)
        self.assertTrue(task.attempted_removal)
        self.assertTrue(task.complete())
