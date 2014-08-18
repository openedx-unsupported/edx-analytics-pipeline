"""
Run end-to-end acceptance tests. The goal of these tests is to emulate (as closely as possible) user actions and
validate user visible outputs.

"""

import datetime
import logging
import os
import tempfile
import textwrap
import shutil

import boto
import gnupg

from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.tests.acceptance.services import shell


log = logging.getLogger(__name__)


class ExportAcceptanceTest(AcceptanceTestCase):
    """Validate the research data export pipeline for a single course and organization."""

    ENVIRONMENT = 'acceptance'
    TABLE = 'courseware_studentmodule'
    COURSE_ID = 'edX/E929/2014_T1'

    def setUp(self):
        super(ExportAcceptanceTest, self).setUp()

        # These variables will be set later
        self.temporary_dir = None
        self.external_files_dir = None
        self.working_dir = None

        self.output_prefix = 'automation/{ident}/'.format(ident=self.identifier)

        self.exported_filename = '{safe_course_id}-{table}-{suffix}-analytics.sql'.format(
            safe_course_id=self.COURSE_ID.replace('/', '-'),
            table=self.TABLE,
            suffix=self.ENVIRONMENT,
        )

        self.org_id = self.COURSE_ID.split('/')[0].lower()

        self.create_temporary_directories()

    def create_temporary_directories(self):
        """Create temporary local filesystem paths for usage by the test and launched applications."""
        self.temporary_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temporary_dir)

        self.external_files_dir = os.path.join(self.temporary_dir, 'external')
        self.working_dir = os.path.join(self.temporary_dir, 'work')

        for dir_path in [self.external_files_dir, self.working_dir]:
            os.makedirs(dir_path)

    @unittest.skipIf(os.getenv('EXPORTER') is None, "requires private Exporter code")
    def test_database_export(self):
        # An S3 bucket to store the output in.
        assert('exporter_output_bucket' in self.config)

        self.load_data_from_file()
        self.run_export_task()
        self.run_legacy_exporter()
        self.validate_exporter_output()

    def load_data_from_file(self):
        """
        External Effect: Drops courseware_studentmodule table and loads it with data from a static file.

        """
        self.import_db.execute_sql_file(
            os.path.join(self.data_dir, 'input', 'load_{table}.sql'.format(table=self.TABLE))
        )

    def run_export_task(self):
        """
        Preconditions: Populated courseware_studentmodule table in the MySQL database.
        External Effect: Generates a single text file with the contents of courseware_studentmodule from the MySQL
            database for the test course and stores it in S3.

        Intermediate output will be stored in s3://<tasks_output_url>/intermediate/. This directory
            will contain the complete data set from the MySQL database with all courses interleaved in the data files.

        The final output file will be stored in s3://<tasks_output_url>/edX-E929-2014_T1-courseware_studentmodule-acceptance-analytics.sql
        """
        self.task.launch([
            'StudentModulePerCourseAfterImportWorkflow',
            '--credentials', self.import_db.credentials_file_url,
            '--dump-root', url_path_join(self.test_src, 'intermediate'),
            '--output-root', url_path_join(self.test_src, self.ENVIRONMENT),
            '--output-suffix', self.ENVIRONMENT,
            '--num-mappers', str(self.NUM_MAPPERS),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

    def run_legacy_exporter(self):
        """
        Preconditions: A text file for courseware_studentmodule has been generated and stored in the external file path.
        External Effect: Runs the legacy exporter which assembles the data package, encrypts it, and uploads it to S3.

        Reads <temporary_dir>/external/<day of month>/edX-E929-2014_T1-courseware_studentmodule-acceptance-analytics.sql
            and copies it in to the data package.

        Writes the configuration to <temporary_dir>/acceptance.yml.

        Uploads the package to s3://<exporter_output_bucket>/<output_prefix>edx-<year>-<month>-<day>.zip

        """
        config_file_path = os.path.join(self.temporary_dir, 'acceptance.yml')
        self.write_exporter_config(config_file_path)

        # The exporter expects this directory to already exist.
        os.makedirs(os.path.join(self.working_dir, 'course-data'))

        command = [
            os.getenv('EXPORTER'),
            '--work-dir', self.working_dir,
            '--bucket', self.config.get('exporter_output_bucket'),
            '--course-id', self.COURSE_ID,
            '--external-prefix', self.test_src,
            '--output-prefix', self.output_prefix,
            config_file_path,
            '--env', self.ENVIRONMENT,
            '--org', self.org_id,
            '--task', 'StudentModuleTask'
        ]
        shell.run(command)

    def write_exporter_config(self, config_file_path):
        """Write out the configuration file that the exporter expects to the filesystem."""
        config_text = textwrap.dedent("""\
            options: {{}}

            defaults:
              gpg_keys: gpg-keys
              sql_user: {sql_user}
              sql_db: {sql_db}
              sql_password: {sql_password}

            environments:
              {environment}:
                name: {environment}-analytics
                sql_host: {sql_host}
                external_files: {external_files}

            organizations:
              {org_id}:
                recipient: daemon@edx.org
            """)
        config_text = config_text.format(
            sql_user=self.import_db.credentials['username'],
            sql_db=self.import_db.database_name,
            sql_password=self.import_db.credentials['password'],
            environment=self.ENVIRONMENT,
            sql_host=self.import_db.credentials['host'],
            external_files=self.external_files_dir,
            org_id=self.org_id,
        )

        with open(config_file_path, 'w') as config_file:
            config_file.write(config_text)

    def validate_exporter_output(self):
        """
        Preconditions: A complete data package has been uploaded to S3.
        External Effect: Downloads the complete data package, decompresses it, decrypts it and then compares it to the
            static expected output ignoring the ordering of the records in both files.

        Downloads s3://<exporter_output_bucket>/<output_prefix>edx-<year>-<month>-<day>.zip to <temporary_dir>/work/validation/.

        """
        validation_dir = os.path.join(self.working_dir, 'validation')
        os.makedirs(validation_dir)

        today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        bucket = boto.connect_s3().get_bucket(self.config.get('exporter_output_bucket'))
        export_id = '{org}-{date}'.format(org=self.org_id, date=today)
        filename = export_id + '.zip'
        key = bucket.lookup(self.output_prefix + filename)
        if key is None:
            self.fail(
                'Expected output from legacy exporter not found. Url = s3://{bucket}/{pre}{filename}'.format(
                    bucket=self.config.get('exporter_output_bucket'),
                    pre=self.output_prefix,
                    filename=filename
                )
            )
        exporter_archive_path = os.path.join(validation_dir, filename)
        key.get_contents_to_filename(exporter_archive_path)

        shell.run(['unzip', exporter_archive_path, '-d', validation_dir])

        gpg_dir = os.path.join(self.working_dir, 'gnupg')
        os.makedirs(gpg_dir)
        os.chmod(gpg_dir, 0700)

        gpg = gnupg.GPG(gnupghome=gpg_dir)
        with open(os.path.join('gpg-keys', 'insecure_secret.key'), 'r') as key_file:
            gpg.import_keys(key_file.read())

        exported_file_path = os.path.join(validation_dir, self.exported_filename)
        with open(os.path.join(validation_dir, export_id, self.exported_filename + '.gpg'), 'r') as encrypted_file:
            gpg.decrypt_file(encrypted_file, output=exported_file_path)

        sorted_filename = exported_file_path + '.sorted'
        shell.run(['sort', '-o', sorted_filename, exported_file_path])

        expected_output_path = os.path.join(self.data_dir, 'output', self.exported_filename + '.sorted')
        shell.run(['diff', sorted_filename, expected_output_path])
