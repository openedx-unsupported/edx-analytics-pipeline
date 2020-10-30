"""
Tests for internal reporting database tasks.
"""
from unittest import TestCase

from ddt import data, ddt, unpack
from edx.analytics.tasks.warehouse.load_internal_reporting_database import (
    ExportMysqlDatabaseToS3Task, LoadMysqlTableFromS3ToVerticaTask
)
from mock import patch


@ddt
class ExportMysqlDatabaseToS3TaskTest(TestCase):
    """Test for ExportMysqlDatabaseToS3Task."""

    def setUp(self):
        self.task = ExportMysqlDatabaseToS3Task(
            exclude=('auth_user$', 'courseware_studentmodule*', 'oauth*'),
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


class LoadMysqlTableFromS3ToVerticaTaskTest(TestCase):
    """Test for LoadMysqlTableFromS3ToVerticaTask."""

    @patch('edx.analytics.tasks.warehouse.load_internal_reporting_database.LoadMysqlTableFromS3ToVerticaTask.mysql_table_schema')
    @patch('edx.analytics.tasks.warehouse.load_internal_reporting_database.LoadMysqlTableFromS3ToVerticaTask.get_table_metadata')
    def test_table_schema(self, get_table_metadata_mock, mysql_table_schema_mock):
        desc_table = [
            ('id', 'int(11)', 'NO'),
            ('name', 'varchar(255)', 'NO'),
            ('meta', 'longtext', 'NO'),
            ('width', 'smallint(6)', 'YES'),
            ('test_tiny', 'tinyint(4)', 'YES'),
            ('allow_certificate', 'tinyint(1)', 'NO'),
            ('user_id', 'bigint(20) unsigned', 'NO'),
            ('profile_image_uploaded_at', 'datetime', 'YES'),
            ('change_date', 'datetime(6)', 'YES'),
            ('total_amount', 'double', 'NO'),
            ('expiration_date', 'date', 'YES'),
            ('ip_address', 'char(39)', 'YES'),
            ('unit_cost', 'decimal(30,2)', 'NO')
        ]
        mysql_table_schema_mock.return_value = desc_table
        get_table_metadata_mock.return_value = None

        task = LoadMysqlTableFromS3ToVerticaTask(table_name='test_table')

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
