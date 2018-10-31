"""
End to end test of event exports.
"""

import logging
import os
import shutil
import tempfile
import textwrap

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, as_list_param
from edx.analytics.tasks.tests.acceptance.services import fs
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class EventExportAcceptanceTest(AcceptanceTestCase):
    """Validate data flow for bulk export of events for research purposes."""

    INPUT_FILE = 'event_export_tracking.log'
    PROD_FOLDER = 'FakeServerGroup'
    EDGE_FOLDER = 'EdgeFakeServerGroup'
    NUM_REDUCERS = 1

    def setUp(self):
        super(EventExportAcceptanceTest, self).setUp()

        self.test_config_root = url_path_join(self.test_root, 'config')

        self.test_config = url_path_join(self.test_config_root, 'default.yaml')
        self.test_gpg_key_dir = url_path_join(self.test_config_root, 'gpg-keys')

        self.input_paths = [
            url_path_join(self.test_src, 'edx', self.PROD_FOLDER, 'tracking.log-20140515.gz'),
            url_path_join(self.test_src, 'edx', 'OtherFolder', 'tracking.log-20140515.gz'),
            url_path_join(self.test_src, 'edge', self.EDGE_FOLDER, 'tracking.log-20140516-12345456.gz')
        ]

        self.upload_data()
        self.write_config()
        self.upload_public_keys()

    def upload_data(self):
        with fs.gzipped_file(os.path.join(self.data_dir, 'input', self.INPUT_FILE)) as compressed_file_name:
            for dst in self.input_paths:
                self.upload_file(compressed_file_name, dst)

    def write_config(self):
        content = textwrap.dedent(
            """
            ---
            organizations:
              edX:
                recipients:
                  - daemon@edx.org
              AcceptanceX:
                recipients:
                  - daemon+2@edx.org
            """
        )
        self.upload_file_with_content(self.test_config, content)

    def upload_public_keys(self):
        gpg_key_dir = os.path.join('gpg-keys')
        for key_filename in os.listdir(gpg_key_dir):
            full_local_path = os.path.join(gpg_key_dir, key_filename)
            remote_url = url_path_join(self.test_gpg_key_dir, key_filename)

            if not key_filename.endswith('.key'):
                self.upload_file(full_local_path, remote_url)

    def test_event_log_exports_using_manifest(self):
        config_override = {
            'manifest': {
                'threshold': 1
            }
        }

        folders = {
            'edx': self.PROD_FOLDER,
            'edge': self.EDGE_FOLDER
        }
        for environment in ['edx', 'edge']:
            self.task.launch([
                'EventExportTask',
                '--source', as_list_param(url_path_join(self.test_src, environment)),
                '--output-root', self.test_out,
                '--config', self.test_config,
                '--environment', environment,
                '--interval', '2014-05',
                '--gpg-key-dir', self.test_gpg_key_dir,
                '--gpg-master-key', 'daemon+master@edx.org',
                '--required-path-text', folders[environment],
                '--n-reduce-tasks', str(self.NUM_REDUCERS),
            ], config_override)

        self.validate_output()

    def validate_output(self):
        for site in ['edge', 'edx']:
            for use_master_key in [False, True]:
                self.validate_output_file('2014-05-15', 'edx', site, use_master_key)
                self.validate_output_file('2014-05-16', 'edx', site, use_master_key)
                self.validate_output_file('2014-05-15', 'acceptancex', site, use_master_key)

    def validate_output_file(self, date, org_id, site, use_master_key=False):
        if use_master_key:
            key_filename = 'insecure_master_secret.key'
        else:
            if org_id == 'edx':
                key_filename = 'insecure_secret.key'
            else:
                key_filename = 'insecure_secret_2.key'

        self.temporary_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temporary_dir)

        self.downloaded_output_dir = os.path.join(self.temporary_dir, 'output')
        os.makedirs(self.downloaded_output_dir)

        local_file_name = '{org}-{site}-events-{date}.log'.format(
            org=org_id,
            site=site,
            date=date,
        )

        year = str(date).split("-")[0]

        remote_url = url_path_join(self.test_out, org_id, site, "events", year, local_file_name + '.gz.gpg')
        downloaded_output_path = self.download_file_to_local_directory(remote_url, self.downloaded_output_dir)

        # first decrypt file
        decrypted_file_name = downloaded_output_path[:-len('.gpg')]
        fs.decrypt_file(downloaded_output_path, decrypted_file_name, key_filename)

        # now decompress file
        decompressed_file_name = decrypted_file_name[:-len(',gz')]
        fs.decompress_file(decrypted_file_name, decompressed_file_name)

        original_filename = os.path.join(self.data_dir, 'output', local_file_name)
        self.assertEventLogEqual(decompressed_file_name, original_filename)
