"""Tests for some hive related utilities"""

from datetime import date

from mock import sentinel

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.util import hive


class HivePartitionTest(unittest.TestCase):
    """Ensure the partition serializes properly to various formats."""

    def setUp(self):
        self.partition = hive.HivePartition('dt', '2014-01-01')

    def test_as_dict(self):
        self.assertEquals(self.partition.as_dict(), {'dt': '2014-01-01'})

    def test_query_spec(self):
        self.assertEquals(self.partition.query_spec, "dt='2014-01-01'")

    def test_path_spec(self):
        self.assertEquals(self.partition.path_spec, "dt=2014-01-01")

    def test_str(self):
        self.assertEquals(str(self.partition), "dt=2014-01-01")


class HivePartitionParameterTest(unittest.TestCase):
    """Ensure the partition can be read from a string."""

    def test_partition_parameter(self):
        partition = hive.HivePartitionParameter().parse('dt=2014-01-01')
        self.assertEquals(partition.key, 'dt')
        self.assertEquals(partition.value, '2014-01-01')
        self.assertEquals(str(partition), 'dt=2014-01-01')


class HiveQueryToMysqlTaskTest(unittest.TestCase):
    """Test some of the tricky logic in HiveQueryToMysqlTask"""

    def test_hive_columns(self):
        class TestQuery(hive.HiveQueryToMysqlTask):  # pylint: disable=abstract-method
            """Sample task with just a column definition."""
            columns = [
                ('one', 'VARCHAR(255) NOT NULL AUTO_INCREMENT UNIQUE'),
                ('two', 'VARCHAR'),
                ('three', 'DATETIME NOT NULL'),
                ('four', 'DATE'),
                ('five', 'INTEGER'),
                ('six', 'INT'),
                ('seven', 'DOUBLE'),
                ('eight', 'tinyint'),
                ('nine', 'longtext')
            ]

        self.assertEquals(TestQuery().hive_columns, [
            ('one', 'STRING'),
            ('two', 'STRING'),
            ('three', 'TIMESTAMP'),
            ('four', 'STRING'),
            ('five', 'INT'),
            ('six', 'INT'),
            ('seven', 'DOUBLE'),
            ('eight', 'TINYINT'),
            ('nine', 'STRING')
        ])

    def test_other_tables(self):
        class TestOtherTables(hive.HiveQueryToMysqlTask):  # pylint: disable=abstract-method
            """Sample task that relies on other tables."""
            @property
            def required_table_tasks(self):
                return (
                    sentinel.table_1,
                    sentinel.table_2
                )

            query = 'SELECT 1'
            table = 'test_table'
            columns = [('one', 'VARCHAR')]
            partition = hive.HivePartition('dt', '2014-01-01')

        requirements = TestOtherTables().requires()
        self.assertEquals(requirements['other_tables'], (sentinel.table_1, sentinel.table_2))


class HiveWarehouseMixinTest(unittest.TestCase):
    """Test the partition path generation"""

    def setUp(self):
        self.mixin = hive.WarehouseMixin()
        self.mixin.warehouse_path = 's3://warehouse/'

    def test_hive_partition_from_string(self):
        self.assertEqual(
            's3://warehouse/foo/dt=bar/',
            self.mixin.hive_partition_path('foo', 'bar')
        )

    def test_hive_partition_custom_key(self):
        self.assertEqual(
            's3://warehouse/foo/baz=bar/',
            self.mixin.hive_partition_path('foo', 'bar', partition_key='baz')
        )

    def test_hive_partition_date_key(self):
        self.assertEqual(
            's3://warehouse/foo/dt=2016-01-02/',
            self.mixin.hive_partition_path('foo', date(2016, 1, 2))
        )
