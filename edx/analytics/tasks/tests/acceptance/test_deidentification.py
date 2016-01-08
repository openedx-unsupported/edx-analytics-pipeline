"""End to end test of deidentification."""

import datetime
import logging
import os
import tempfile
import shutil
import json


from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.pathutil import PathSetTask
from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.tests.acceptance.services import fs, shell
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
from edx.analytics.tasks.util.file_util import copy_file_to_file

log = logging.getLogger(__name__)


class DeidentificationAcceptanceTest(AcceptanceTestCase):
    """Validate data flow for deidentification process."""

    COURSE_ID = 'course-v1:edX+DemoX+Demo_Course'
    INPUT_FILE = 'deidentification_tracking.log'
    EXPORT_DATE = '2015-11-13'
    INTERVAL = '2015-10-01-2015-12-31'

    def setUp(self):
        super(DeidentificationAcceptanceTest, self).setUp()
        self.temporary_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temporary_dir)
        self.dump_root = os.path.join(self.test_src, 'course_exports', 'raw')
        self.filename_safe_course_id = get_filename_safe_course_id(self.COURSE_ID)

    def setup_state_files(self):
        """Upload input fixture data files, needed to mimic the output produced by course-exporter which is not a part of this test."""

        state_files_dir = os.path.join(self.data_dir, 'input', 'deidentification', 'state')
        for filename in os.listdir(state_files_dir):
            local_filepath = os.path.join(state_files_dir, filename)
            dst = url_path_join(self.dump_root, self.filename_safe_course_id, 'state', self.EXPORT_DATE, filename)
            self.upload_file(local_filepath, dst)

    def test_deidentification(self):
        self.run_event_export_task()
        self.setup_state_files()
        self.run_data_deidentification_task()
        self.run_events_deidentification_task()
        self.maxDiff = None
        self.validate_data_deidentification()
        self.validate_events_deidentification()

    def run_event_export_task(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2015, 10, 15))
        self.task.launch([
            'EventExportByCourseTask',
            '--output-root', self.dump_root,
            '--interval', self.INTERVAL,
            '--n-reduce-tasks', str(self.NUM_REDUCERS)
        ])

    def run_data_deidentification_task(self):
        self.task.launch([
            'DataDeidentificationTask',
            '--course', self.filename_safe_course_id,
            '--output-root', self.test_out,
            '--dump-root', self.dump_root
        ])

    def run_events_deidentification_task(self):
        self.task.launch([
            'EventDeidentificationTask',
            '--course', self.filename_safe_course_id,
            '--output-root', self.test_out,
            '--dump-root', self.dump_root,
            '--interval', self.INTERVAL
        ])

    def validate_data_deidentification(self):
        for output_filepath in self.download_output_files(url_path_join(self.test_out, self.filename_safe_course_id, 'state', self.EXPORT_DATE)):
            output_filename = os.path.basename(output_filepath)
            expected_output_filepath = os.path.join(self.data_dir, 'output', 'deidentification', 'state', output_filename)

            if output_filename.endswith('mongo'):
                with open(output_filepath) as mongo_output_file:
                    output_json = [json.loads(line) for line in mongo_output_file]
                with open(expected_output_filepath) as expected_output_file:
                    expected_output_json = [json.loads(line) for line in expected_output_file]
                self.assertEqual(output_json, expected_output_json)
            else:
                shell.run(['diff', output_filepath, expected_output_filepath])

    def validate_events_deidentification(self):
        for output_filepath in self.download_output_files(url_path_join(self.test_out, self.filename_safe_course_id, 'events')):
            output_filename = os.path.basename(output_filepath)
            decompressed_file_name = output_filepath[:-len('.gz')]
            fs.decompress_file(output_filepath, decompressed_file_name)
            expected_events_filepath = os.path.join(self.data_dir, 'output', 'deidentification', 'events', output_filename[:-len('.gz')])

            with open(decompressed_file_name) as events_output_file:
                events_output = [json.loads(line) for line in events_output_file]
            with open(expected_events_filepath) as expected_events_file:
                expected_events = [json.loads(line) for line in expected_events_file]
            self.assertItemsEqual(events_output, expected_events)

    def download_output_files(self, path):
        """Downloads the files on the path and copies them to a temporary directory."""
        output_filepaths = []
        for output_target in PathSetTask([path]).output():
            output_filename = os.path.basename(output_target.path)
            output_filepath = os.path.join(self.temporary_dir, output_filename)
            output_filepaths.append(output_filepath)

            # TODO: Fix this hack solution properly.
            # We need the target to be of type S3Target instead of S3HdfsTarget on Jenkins.
            output_target = get_target_from_url(output_target.path.replace('s3://', 's3+https://'))

            with output_target.open('r') as input_file:
                with open(output_filepath, 'w') as output_file:
                    copy_file_to_file(input_file, output_file)

        return output_filepaths
