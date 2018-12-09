"""
End to end test of the CourseBlocksPartitionTask task.
"""

import datetime
import logging

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, get_target_for_local_server
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class CourseBlocksPartitionTaskAcceptanceTest(AcceptanceTestCase):
    """
    Tests the CourseBlocksPartitionTask.
    """

    DAILY_PARTITION_FORMAT = '%Y-%m-%d'
    DATE = datetime.date(2016, 9, 8)

    def setUp(self):
        """Copy the input data into place."""
        super(CourseBlocksPartitionTaskAcceptanceTest, self).setUp()
        self.partition = "dt=" + self.DATE.strftime(self.DAILY_PARTITION_FORMAT)

        # Copy raw input from the course_blocks REST API task into warehouse
        self.copy_raw_input('course_blocks')

    def copy_raw_input(self, table_name):
        """Copy raw REST API json data for the given table into warehouse."""
        file_name = table_name + '.json'
        self.upload_file(url_path_join(self.data_dir, 'input', file_name),
                         url_path_join(self.warehouse_path, table_name + '_raw', self.partition, file_name))

    def copy_hive_input(self, table_name):
        """Copy processed hive data for the given table into warehouse."""
        daily_partition = "dt=" + self.DATE.strftime(self.DAILY_PARTITION_FORMAT)
        for file_name in ('part-00000', 'part-00001', '_SUCCESS'):
            self.upload_file(url_path_join(self.data_dir, 'output', table_name, file_name),
                             url_path_join(self.warehouse_path, table_name, daily_partition, file_name))

    def test_partition_task_raw_input(self):
        """Run the CourseBlocksPartitionTask using raw Course List input data."""
        self.copy_raw_input('course_list')
        self.validate_partition_task()

    def test_partition_task_hive_input(self):
        """
        Run the CourseBlocksPartitionTask using hive Course List input data.

        This simulates what happens when the course blocks task is run more often than the course list task.
        """
        self.copy_hive_input('course_list')
        self.validate_partition_task()

    def validate_partition_task(self):
        """Run the CourseBlocksPartitionTask and test its output."""
        date_time = self.DATE.strftime('%Y-%m-%dT%H%M%S')
        input_root = url_path_join(self.warehouse_path, 'course_list', self.partition)

        self.task.launch([
            'CourseBlocksPartitionTask',
            '--datetime', date_time,
            '--input-root', input_root,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.maxDiff = None
        self.validate_hive()

    def validate_hive(self):
        """Ensure hive partition was created as expected."""
        table_name = 'course_blocks'
        output_dir = url_path_join(self.data_dir, 'output', table_name)
        for file_name in ('_SUCCESS', 'part-00000', 'part-00001'):
            actual_output_file = url_path_join(self.warehouse_path, table_name, self.partition, file_name)
            actual_output_target = get_target_for_local_server(actual_output_file)
            self.assertTrue(actual_output_target.exists(), '{} not created'.format(file_name))
            actual_output = actual_output_target.open('r').read()

            expected_output_file = url_path_join(output_dir, file_name)
            expected_output_target = get_target_for_local_server(expected_output_file)
            expected_output = expected_output_target.open('r').read()
            self.assertEqual(actual_output, expected_output)
