"""
Tests for internal reporting database tasks.
"""
from mock import patch
from ddt import ddt, data, unpack

from edx.analytics.tasks.load_internal_reporting_database import LoadMysqlToVerticaTableTask, ImportMysqlToVerticaTask
from edx.analytics.tasks.tests import unittest


@ddt
class ImportMysqlToVerticaTaskTest(unittest.TestCase):
    """Test for ImportMysqlToVerticaTask."""

    def setUp(self):
        self.task = ImportMysqlToVerticaTask(
            exclude=('auth_user$', 'courseware_studentmodule*', 'oauth*'),
            marker_schema=None
        )

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

    @patch('edx.analytics.tasks.load_internal_reporting_database.get_mysql_query_results')
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
