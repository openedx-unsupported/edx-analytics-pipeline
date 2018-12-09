"""
End to end test of problem response reporting workflow.
"""

import datetime
import logging
import os
import time

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, get_target_for_local_server
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class ProblemResponseReportWorkflowAcceptanceTest(AcceptanceTestCase):
    """
    Tests the ProblemResponseReportWorkflow.
    """

    TRACKING_LOG = 'problem_response_tracking.log'
    DAILY_PARTITION_FORMAT = '%Y-%m-%d'
    HOURLY_PARTITION_FORMAT = '%Y-%m-%dT%H'
    DATE = datetime.date(2016, 9, 8)

    def setUp(self):
        """Copy the input data into place."""
        super(ProblemResponseReportWorkflowAcceptanceTest, self).setUp()

        # Copy course list and course blocks REST API data
        for table_name in ('course_list', 'course_blocks'):
            daily_partition = "dt=" + self.DATE.strftime(self.DAILY_PARTITION_FORMAT)
            file_name = table_name + '.json'
            self.upload_file(url_path_join(self.data_dir, 'input', file_name),
                             url_path_join(self.warehouse_path, table_name + '_raw', daily_partition, file_name))

        # Copy tracking logs into hdfs
        self.upload_tracking_log(url_path_join(self.data_dir, 'input', self.TRACKING_LOG), self.DATE)

    def test_problem_response_report(self):
        """Run the ProblemResponseReportWorkflow task and test its output."""
        marker_path = url_path_join(self.test_out, 'marker-{}'.format(str(time.time())))
        report_date = self.DATE.strftime('%Y-%m-%d')
        self.task.launch([
            'ProblemResponseReportWorkflow',
            '--interval', '2016-09-01-2016-09-08',
            '--date', report_date,
            '--marker', marker_path,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.maxDiff = None
        self.validate_marker(marker_path)
        self.validate_hive()
        self.validate_reports()

    def validate_marker(self, marker_path):
        """Ensure marker file was created."""
        marker_file = url_path_join(marker_path, '_SUCCESS')
        marker_target = get_target_for_local_server(marker_file)
        self.assertTrue(marker_target.exists())

    def validate_hive(self):
        """Ensure hive partition was created."""
        hourly_partition = self.DATE.strftime(self.HOURLY_PARTITION_FORMAT)
        hive_partition = url_path_join(self.warehouse_path, "problem_response_location",
                                       "dt=" + hourly_partition)
        partition_target = get_target_for_local_server(hive_partition)
        self.assertTrue(partition_target.exists())

    def validate_reports(self):
        """Check the generated reports against the expected output files."""
        actual_output_targets = self.get_targets_from_remote_path(self.report_output_root)
        self.assertEqual(len(actual_output_targets), 2)

        for course_id in ('OpenCraft_PRDemo1_2016', 'OpenCraft_PRDemo2_2016'):
            report_file_name = '{}_problem_response.csv'.format(course_id)
            actual_output_targets = self.get_targets_from_remote_path(self.report_output_root, "*{}".format(report_file_name))
            self.assertEqual(len(actual_output_targets), 1, '{} not created in {}'.format(report_file_name, self.report_output_root))
            actual_output = actual_output_targets[0].open('r').read()

            expected_output_file = os.path.join(self.data_dir, 'output', 'problem_response', report_file_name)
            expected_output = get_target_for_local_server(expected_output_file).open('r').read()
            self.assertEqual(actual_output, expected_output)
