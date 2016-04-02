"""
End to end test of event exports by course.
"""

import os
import logging
import tempfile
import shutil
import datetime

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_s3_available
from edx.analytics.tasks.tests.acceptance.services import fs, shell
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.s3_util import get_file_from_key, generate_s3_sources

log = logging.getLogger(__name__)


class EventExportByCourseAcceptanceTest(AcceptanceTestCase):
    """Validate data flow for bulk export of events by course"""

    INPUT_FILE = 'event_export_tracking.log'
    NUM_REDUCERS = 1

    @when_s3_available
    def test_events_export_by_course(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 5, 15))

        self.task.launch([
            'EventExportByCourseTask',
            '--output-root', self.test_out,
            '--interval', '2014',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.output_files = [
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
        self.download_output_files()
        self.validate_output()

    def download_output_files(self):
        self.assertEqual(len(list(generate_s3_sources(self.s3_client.s3, self.test_out))), len(self.output_files))

        self.temporary_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temporary_dir)

        self.downloaded_outputs = os.path.join(self.temporary_dir, 'output')
        os.makedirs(self.downloaded_outputs)

        for output_file in self.output_files:
            local_file_name = self.generate_file_name(output_file)

            remote_url = url_path_join(self.test_out, output_file['course_id'], "events", local_file_name + '.gz')

            downloaded_output_path = get_file_from_key(self.s3_client, remote_url, self.downloaded_outputs)

            if downloaded_output_path is None:
                self.fail('Unable to find expected output file {0}'.format(remote_url))

            decompressed_file_name = downloaded_output_path[:-len('.gz')]
            output_file['downloaded_path'] = decompressed_file_name
            fs.decompress_file(downloaded_output_path, decompressed_file_name)

    def validate_output(self):
        for output_file in self.output_files:
            local_file_name = self.generate_file_name(output_file)
            shell.run(['diff', output_file['downloaded_path'], os.path.join(self.data_dir, 'output', local_file_name)])

    def generate_file_name(self, output_file):
        """Generates file_name for a given course and date."""
        file_name = '{course}-events-{date}.log'.format(
            course=output_file['course_id'],
            date=output_file['date'],
        )

        return file_name
