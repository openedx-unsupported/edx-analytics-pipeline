"""
Tests for internal reporting database tasks.
"""
from unittest import TestCase

from ddt import data, ddt, unpack
from mock import patch

from edx.analytics.tasks.warehouse.load_internal_reporting_database import (
    ImportMysqlDatabaseToBigQueryDatasetTask, ImportMysqlToVerticaTask, LoadMysqlToBigQueryTableTask,
    LoadMysqlToVerticaTableTask
)


@ddt
class ImportMysqlToVerticaTaskTest(TestCase):
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


class LoadMysqlToVerticaTableTaskTest(TestCase):
    """Test for LoadMysqlToVerticaTableTask."""

    @patch('edx.analytics.tasks.warehouse.load_internal_reporting_database.get_mysql_query_results')
    def test_table_schema(self, mysql_query_results_mock):
        desc_table = [
            ('id', 'int(11)', 'NO', 'PRI', None, 'auto_increment'),
            ('name', 'varchar(255)', 'NO', 'MUL', None, ''),
            ('meta', 'longtext', 'NO', '', None, ''),
            ('width', 'smallint(6)', 'YES', 'MUL', None, ''),
            ('test_tiny', 'tinyint(4)', 'YES', 'MUL', None, ''),
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
            ('"test_tiny"', 'tinyint'),
            ('"allow_certificate"', 'BOOLEAN NOT NULL'),
            ('"user_id"', 'bigint NOT NULL'),
            ('"profile_image_uploaded_at"', 'datetime'),
            ('"change_date"', 'datetime'),
            ('"total_amount"', 'DOUBLE PRECISION NOT NULL'),
            ('"expiration_date"', 'date'),
            ('"ip_address"', 'char(39)'),
            ('"unit_cost"', 'decimal(30,2) NOT NULL'),
        ]

        self.assertEqual(task.vertica_compliant_schema(), expected_schema)


@ddt
class ImportMysqlToBigQueryTaskTest(TestCase):
    """Test for ImportMysqlDatabaseToBigQueryDatasetTask."""

    def setUp(self):
        self.task = ImportMysqlDatabaseToBigQueryDatasetTask(
            exclude=('auth_user$', 'courseware_studentmodule*', 'oauth*'),
            dataset_id='dummy',
            credentials='dummy',
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


class LoadMysqlToBigQueryTableTaskTest(TestCase):
    """Test for LoadMysqlToBigQueryTableTask."""

    @patch('edx.analytics.tasks.warehouse.load_internal_reporting_database.get_mysql_query_results')
    def test_table_schema(self, mysql_query_results_mock):
        desc_table = [
            ('id', 'int(11)', 'NO', 'PRI', None, 'auto_increment'),
            ('name', 'varchar(255)', 'NO', 'MUL', None, ''),
            ('meta', 'longtext', 'NO', '', None, ''),
            ('width', 'smallint(6)', 'YES', 'MUL', None, ''),
            ('test_tiny', 'tinyint(4)', 'YES', 'MUL', None, ''),
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

        task = LoadMysqlToBigQueryTableTask(
            table_name='test_table',
            dataset_id='dummy',
            credentials='dummy',
        )

        expected_schemas = [
            {'name': 'id', 'type': 'int64', 'mode': 'required'},
            {'name': 'name', 'type': 'string', 'mode': 'required'},
            {'name': 'meta', 'type': 'string', 'mode': 'required'},
            {'name': 'width', 'type': 'int64', 'mode': 'nullable'},
            {'name': 'test_tiny', 'type': 'int64', 'mode': 'nullable'},
            {'name': 'allow_certificate', 'type': 'bool', 'mode': 'required'},
            {'name': 'user_id', 'type': 'int64', 'mode': 'required'},
            {'name': 'profile_image_uploaded_at', 'type': 'datetime', 'mode': 'nullable'},
            {'name': 'change_date', 'type': 'datetime', 'mode': 'nullable'},
            {'name': 'total_amount', 'type': 'float64', 'mode': 'required'},
            {'name': 'expiration_date', 'type': 'date', 'mode': 'nullable'},
            {'name': 'ip_address', 'type': 'string', 'mode': 'nullable'},
            {'name': 'unit_cost', 'type': 'float64', 'mode': 'required'},
        ]

        schema = task.schema
        # schema_list = [(field.name, field.field_type, field.mode) for field in task.schema]
        actual_schemas = [field.to_api_repr() for field in task.schema]
        for actual_schema, expected_schema in zip(actual_schemas, expected_schemas):
            self.assertDictEqual(actual_schema, expected_schema)
