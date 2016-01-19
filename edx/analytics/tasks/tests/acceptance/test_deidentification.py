"""End to end test of deidentification."""

import datetime
import logging
import os
import tempfile
import shutil
import json
import tarfile


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
    FORMAT_VERSION = 'rdx-test'
    PIPELINE_VERSION = '9c3b6bc9cf1c1b26e39b6eba2ca0d1e76829862d'

    def setUp(self):
        super(DeidentificationAcceptanceTest, self).setUp()
        self.temporary_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temporary_dir)
        self.dump_root = url_path_join(self.test_src, 'course_exports', 'raw')
        self.filename_safe_course_id = get_filename_safe_course_id(self.COURSE_ID)
        self.test_gpg_key_dir = url_path_join(self.test_root, 'gpg-keys')

    def setup_state_files(self):
        """Upload input fixture data files, needed to mimic the output produced by course-exporter which is not a part of this test."""

        state_files_dir = os.path.join(self.data_dir, 'input', 'deidentification', 'state')
        for filename in os.listdir(state_files_dir):
            local_filepath = os.path.join(state_files_dir, filename)
            dst_url = url_path_join(self.dump_root, self.filename_safe_course_id, 'state', self.EXPORT_DATE, filename)
            self.upload_file(local_filepath, dst_url)

    def upload_gpg_keys(self):
        """Uploads test gpg keys, needed for encryption."""
        gpg_key_dir = os.path.join('gpg-keys')
        for key_filename in os.listdir(gpg_key_dir):
            local_filepath = os.path.join(gpg_key_dir, key_filename)
            destination_url = url_path_join(self.test_gpg_key_dir, key_filename)

            if not key_filename.endswith('.key'):
                self.upload_file(local_filepath, destination_url)

    def run_event_export_task(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2015, 10, 15))
        self.task.launch([
            'EventExportByCourseTask',
            '--output-root', self.dump_root,
            '--interval', self.INTERVAL,
            '--n-reduce-tasks', str(self.NUM_REDUCERS)
        ])

    def test_deidentification(self):
        """Test deid workflow."""
        self.run_event_export_task()
        self.setup_state_files()
        self.upload_gpg_keys()
        self.run_deidentification_task()
        self.run_deidentified_package_task()
        self.maxDiff = None
        self.validate_deidentification()

    def run_deidentification_task(self):
        """Run DeidentifiedCourseTask."""
        self.task.launch([
            'DeidentifiedCourseTask',
            '--course', self.filename_safe_course_id,
            '--dump-root', self.dump_root,
            '--deidentified-output-root', url_path_join(self.test_root, 'deidentified-output'),
            '--format-version', self.FORMAT_VERSION,
            '--pipeline-version', self.PIPELINE_VERSION
        ])

    def run_deidentified_package_task(self):
        """Run DeidentifiedPackageTask."""
        self.task.launch([
            'DeidentifiedPackageTask',
            '--course', self.filename_safe_course_id,
            '--deidentified-output-root', url_path_join(self.test_root, 'deidentified-output'),
            '--gpg-key-dir', self.test_gpg_key_dir,
            '--gpg-master-key', 'daemon+master@edx.org',
            '--output-root', self.test_out,
            '--recipient', 'daemon@edx.org',
            '--format-version', self.FORMAT_VERSION
        ])

    def validate_deidentification(self):
        """Validates deid workflow."""
        output_target = PathSetTask([self.test_out], ['*.tar.gz.gpg']).output()[0]
        output_filename = os.path.basename(output_target.path)
        output_filepath = os.path.join(self.temporary_dir, output_filename)

        if output_target.path.startswith('s3://'):
            output_target = get_target_from_url(output_target.path.replace('s3://', 's3+https://'))

        with output_target.open('r') as input_file:
            with open(output_filepath, 'w') as output_file:
                copy_file_to_file(input_file, output_file)

        decrypted_filepath = output_filepath[:-len('.gpg')]
        fs.decrypt_file(output_filepath, decrypted_filepath, 'insecure_secret.key')

        with tarfile.open(decrypted_filepath, 'r:gz') as tfile:
            tfile.extractall(self.temporary_dir)

        # Validate package metadata info.
        metadata_filepath = os.path.join(self.temporary_dir, 'metadata_file.json')
        with open(metadata_filepath) as metadata_file:
            metadata_info = json.load(metadata_file)
        self.assertItemsEqual(metadata_info['format_version'], self.FORMAT_VERSION)
        self.assertItemsEqual(metadata_info['pipeline_version'], self.PIPELINE_VERSION)

        self.validate_data_deidentification()
        self.validate_events_deidentification()

    def validate_data_deidentification(self):
        """Validates data deid."""
        data_dir = os.path.join(self.temporary_dir, 'state', self.EXPORT_DATE)
        for data_filename in os.listdir(data_dir):
            data_filepath = os.path.join(data_dir, data_filename)
            expected_output_filepath = os.path.join(self.data_dir, 'output', 'deidentification', 'state', data_filename)

            if data_filename.endswith('mongo'):
                with open(data_filepath) as mongo_output_file:
                    output_json = [json.loads(line) for line in mongo_output_file]
                with open(expected_output_filepath) as expected_output_file:
                    expected_output_json = [json.loads(line) for line in expected_output_file]
                self.assertItemsEqual(output_json, expected_output_json)
            else:
                shell.run(['diff', data_filepath, expected_output_filepath])

    def validate_events_deidentification(self):
        """Validates events deid."""
        events_dir = os.path.join(self.temporary_dir, 'events')
        for events_filename in os.listdir(events_dir):
            events_filepath = os.path.join(events_dir, events_filename)
            decompressed_filepath = events_filepath[:-len('.gz')]
            fs.decompress_file(events_filepath, decompressed_filepath)

            expected_events_filepath = os.path.join(self.data_dir, 'output', 'deidentification', 'events', events_filename[:-len('.gz')])

            with open(decompressed_filepath) as events_output_file:
                events_output = [json.loads(line) for line in events_output_file]
            with open(expected_events_filepath) as expected_events_file:
                expected_events = [json.loads(line) for line in expected_events_file]
            self.assertItemsEqual(events_output, expected_events)
