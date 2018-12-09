"""Tests for some hive related utilities"""

from datetime import date
from unittest import TestCase

from edx.analytics.tasks.util import hive


class HivePartitionTest(TestCase):
    """Ensure the partition serializes properly to various formats."""

    def setUp(self):
        self.partition = hive.HivePartition('dt', '2014-01-01')

    def test_as_dict(self):
        self.assertEquals(self.partition.as_dict(), {'dt': '2014-01-01'})

    def test_query_spec(self):
        self.assertEquals(self.partition.query_spec, "`dt`='2014-01-01'")

    def test_path_spec(self):
        self.assertEquals(self.partition.path_spec, "dt=2014-01-01")

    def test_str(self):
        self.assertEquals(str(self.partition), "dt=2014-01-01")


class HivePartitionParameterTest(TestCase):
    """Ensure the partition can be read from a string."""

    def test_partition_parameter(self):
        partition = hive.HivePartitionParameter().parse('dt=2014-01-01')
        self.assertEquals(partition.key, 'dt')
        self.assertEquals(partition.value, '2014-01-01')
        self.assertEquals(str(partition), 'dt=2014-01-01')


class HiveWarehouseMixinTest(TestCase):
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
