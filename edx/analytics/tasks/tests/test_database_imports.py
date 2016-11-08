"""
Ensure we can write from MySQL to Hive data sources.
"""

import datetime
import textwrap

from mock import patch, Mock
from ddt import ddt, data, unpack

from edx.analytics.tasks.database_imports import (
    ImportStudentCourseEnrollmentTask, ImportIntoHiveTableTask, LoadMysqlToVerticaTableTask, ImportMysqlToVerticaTask
)
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config


@ddt
class ImportMysqlToVerticaTaskTest(unittest.TestCase):
    """Test for ImportMysqlToVerticaTask."""

    def setUp(self):
        self.task = ImportMysqlToVerticaTask(exclude=('auth_user$', 'courseware_studentmodule*', 'oauth*'))

    @data(
        ('auth_user', True),
        ('auth_userprofile', False),
        ('courseware_studentmodule', True),
        ('courseware_studentmodulehistory', True),
        ('oauth2_accesstoken', True),
        ('oauth_provider_token', True),
    )
    @unpack
    def test_should_exclude_table(self, table, expected):
        actual = self.task.should_exclude_table(table)
        self.assertEqual(actual, expected)


class LoadMysqlToVerticaTableTaskTest(unittest.TestCase):
    """Test for LoadMysqlToVerticaTableTask."""

    @patch('edx.analytics.tasks.database_imports.get_mysql_query_results')
    def test_table_schema(self, mysql_query_results_mock):
        desc_table = [
            ('id', 'int(11)', 'NO', 'PRI', None, 'auto_increment'),
            ('name', 'varchar(255)', 'NO', 'MUL', None, ''),
            ('meta', 'longtext', 'NO', '', None, ''),
            ('width', 'smallint(6)', 'YES', 'MUL', None, ''),
            ('allow_certificate', 'tinyint(1)', 'NO', '', None, ''),
            ('user_id', 'bigint(20) unsigned', 'NO', '', None, ''),
            ('profile_image_uploaded_at', 'datetime', 'YES', '', None, ''),
            ('change_date', 'datetime(6)', 'YES', '', None, ''),
            ('total_amount', 'double', 'NO', '', None, ''),
            ('expiration_date', 'date', 'YES', '', None, ''),
            ('ip_address', 'char(39)', 'YES', '', None, ''),
            ('unit_cost', 'decimal(30,2)', 'NO', '', None, '')
        ]
        mysql_query_results_mock.return_value = desc_table

        task = LoadMysqlToVerticaTableTask(table_name='test_table')

        expected_schema = [
            ('"id"', 'int NOT NULL'),
            ('"name"', 'varchar(255) NOT NULL'),
            ('"meta"', 'LONG VARCHAR NOT NULL'),
            ('"width"', 'smallint'),
            ('"allow_certificate"', 'tinyint NOT NULL'),
            ('"user_id"', 'bigint NOT NULL'),
            ('"profile_image_uploaded_at"', 'datetime'),
            ('"change_date"', 'datetime'),
            ('"total_amount"', 'DOUBLE PRECISION NOT NULL'),
            ('"expiration_date"', 'date'),
            ('"ip_address"', 'char(39)'),
            ('"unit_cost"', 'decimal(30,2) NOT NULL'),
        ]

        self.assertEqual(task.vertica_compliant_schema(), expected_schema)


class ImportStudentCourseEnrollmentTestCase(unittest.TestCase):
    """Tests to validate ImportStudentCourseEnrollmentTask."""

    def test_base_class(self):
        task = ImportIntoHiveTableTask(**{})
        with self.assertRaises(NotImplementedError):
            task.table_name()

    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        kwargs = {'import_date': datetime.datetime.strptime('2014-07-01', '%Y-%m-%d').date()}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS student_courseenrollment;
            CREATE EXTERNAL TABLE student_courseenrollment (
                id INT,user_id INT,course_id STRING,created TIMESTAMP,is_active BOOLEAN,mode STRING
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/student_courseenrollment';
            ALTER TABLE student_courseenrollment ADD PARTITION (dt = '2014-07-01');
            """
        )
        self.assertEquals(query, expected_query)

    def test_overwrite(self):
        kwargs = {'overwrite': True}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        self.assertFalse(task.complete())

    def test_no_overwrite(self):
        # kwargs = {'overwrite': False}
        kwargs = {}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        with patch('edx.analytics.tasks.database_imports.HivePartitionTarget') as mock_target:
            output = mock_target()
            # Make MagicMock act more like a regular mock, so that flatten() does the right thing.
            del output.__iter__
            del output.__getitem__
            output.exists = Mock(return_value=False)
            self.assertFalse(task.complete())
            self.assertTrue(output.exists.called)
            output.exists = Mock(return_value=True)
            self.assertTrue(task.complete())
            self.assertTrue(output.exists.called)
