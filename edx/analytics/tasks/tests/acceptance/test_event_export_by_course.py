"""
End to end test of event exports by course.
"""

import datetime
import logging
import os
import shutil
import tempfile

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.tests.acceptance.services import fs
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class EventExportByCourseAcceptanceTest(AcceptanceTestCase):
    """Validate data flow for bulk export of events by course"""

    INPUT_FILE = 'event_export_tracking.log'
    NUM_REDUCERS = 1

    def test_events_export_by_course(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 5, 15))

        self.task.launch([
            'EventExportByCourseTask',
            '--output-root', self.test_out,
            '--interval', '2014',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.output_files = self.get_non_ccx_output_files()
        self.download_output_files()
        self.validate_output()

    def test_events_export_by_course_with_ccx(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 5, 15))

        self.task.launch(
            [
                'EventExportByCourseTask',
                '--output-root', self.test_out,
                '--interval', '2014',
                '--n-reduce-tasks', str(self.NUM_REDUCERS),
            ],
            config_override={
                'ccx': {
                    'enabled': True
                }
            }
        )

        self.output_files = self.get_ccx_output_files()
        self.download_output_files()
        self.validate_output()

    def get_non_ccx_output_files(self):
        return [
            {
                'date': '2014-05-15',
                'course_id': 'AcceptanceX_A101_T12014'
            },
            {
                'date': '2014-06-01',
                'course_id': 'AcceptanceX_A101_T12014'
            },
            {
                'date': '2014-05-15',
                'course_id': 'AcceptanceX_DemoX_T1_2014'
            },
            {
                'date': '2014-04-18',
                'course_id': 'edX_Open_DemoX_edx_demo_course'
            },
            {
                'date': '2014-05-15',
                'course_id': 'edX_Open_DemoX_edx_demo_course'
            },
            {
                'date': '2014-05-16',
                'course_id': 'edX_Open_DemoX_edx_demo_course'
            },
        ]

    def get_ccx_output_files(self):
        output_files = self.get_non_ccx_output_files()
        output_files.extend([
            {
                'date': '2014-05-27',
                'course_id': 'AcceptanceX_Demo_ccx_T2_2014_ccx_101'
            },
            {
                'date': '2014-05-27',
                'course_id': 'AcceptanceX_Demo_ccx_T2_2014_ccx_102'
            },
        ])
        return output_files

    def download_output_files(self):
        output_targets = self.get_targets_from_remote_path(self.test_out)

        self.assertEqual(len(output_targets), len(self.output_files))

        self.temporary_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temporary_dir)

        self.downloaded_output_dir = os.path.join(self.temporary_dir, 'output')
        os.makedirs(self.downloaded_output_dir)

        for output_file in self.output_files:
            local_file_name = self.generate_file_name(output_file)
            remote_url = url_path_join(self.test_out, output_file['course_id'], "events", local_file_name + '.gz')
            downloaded_output_path = self.download_file_to_local_directory(remote_url, self.downloaded_output_dir)

            decompressed_file_name = downloaded_output_path[:-len('.gz')]
            output_file['downloaded_path'] = decompressed_file_name
            fs.decompress_file(downloaded_output_path, decompressed_file_name)

    def validate_output(self):
        for output_file in self.output_files:
            local_file_name = self.generate_file_name(output_file)
            expected_output = os.path.join(self.data_dir, 'output', local_file_name)
            actual_output = output_file['downloaded_path']
            self.assertEventLogEqual(expected_output, actual_output)

    def generate_file_name(self, output_file):
        """Generates file_name for a given course and date."""
        file_name = '{course}-events-{date}.log'.format(
            course=output_file['course_id'],
            date=output_file['date'],
        )

        return file_name
