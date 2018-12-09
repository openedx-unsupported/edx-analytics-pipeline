"""
End to end test of the course list hive partition task.
"""

import datetime
import logging

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, get_target_for_local_server
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class CourseListPartitionTaskAcceptanceTest(AcceptanceTestCase):
    """
    Tests the CourseListPartitionTask.
    """

    DAILY_PARTITION_FORMAT = '%Y-%m-%d'
    DATE = datetime.date(2016, 9, 8)

    def setUp(self):
        """Copy the input data into place."""
        super(CourseListPartitionTaskAcceptanceTest, self).setUp()

        # Copy course list REST API data
        file_name = 'course_list.json'
        daily_partition = self.DATE.strftime(self.DAILY_PARTITION_FORMAT)
        self.upload_file(url_path_join(self.data_dir, 'input', file_name),
                         url_path_join(self.warehouse_path, 'course_list_raw', "dt=" + daily_partition, file_name))

    def test_partition_task(self):
        """Run the CourseListPartitionTask and test its output."""
        date_time = self.DATE.strftime('%Y-%m-%dT%H%M%S')
        self.task.launch([
            'CourseListPartitionTask',
            '--datetime', date_time,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.maxDiff = None
        self.validate_hive()

    def validate_hive(self):
        """Ensure hive partition was created as expected."""

        table_name = 'course_list'
        output_dir = url_path_join(self.data_dir, 'output', table_name)
        daily_partition = self.DATE.strftime(self.DAILY_PARTITION_FORMAT)
        for file_name in ('_SUCCESS', 'part-00000', 'part-00001'):
            actual_output_file = url_path_join(self.warehouse_path, table_name, "dt=" + daily_partition, file_name)
            actual_output_target = get_target_for_local_server(actual_output_file)
            self.assertTrue(actual_output_target.exists(), '{} not created'.format(file_name))
            actual_output = actual_output_target.open('r').read()

            expected_output_file = url_path_join(output_dir, file_name)
            expected_output_target = get_target_for_local_server(expected_output_file)
            expected_output = expected_output_target.open('r').read()
            self.assertEqual(actual_output, expected_output)
