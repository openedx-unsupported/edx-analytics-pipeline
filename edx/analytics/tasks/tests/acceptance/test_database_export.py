"""
Run end-to-end acceptance tests. The goal of these tests is to emulate (as closely as possible) user actions and
validate user visible outputs.

"""

import datetime
import logging
import os
import shutil
import tempfile
import textwrap
import urlparse

import gnupg

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_exporter_available
from edx.analytics.tasks.tests.acceptance.services import shell
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id, get_org_id_for_course
from edx.analytics.tasks.util.s3_util import ScalableS3Client
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class ExportAcceptanceTest(AcceptanceTestCase):
    """Validate the research data export pipeline for a single course and organization."""

    ENVIRONMENT = 'acceptance'
    TABLE = 'courseware_studentmodule'
    COURSE_ID = 'edX/E929/2014_T1'
    COURSE_ID2 = 'course-v1:testX+DemoX+2014_T2'

    def setUp(self):
        super(ExportAcceptanceTest, self).setUp()

        # These variables will be set later
        self.temporary_dir = None
        self.external_files_dir = None
        self.working_dir = None

        self.output_prefix = 'automation/{ident}/'.format(ident=self.identifier)

        self.create_temporary_directories()

    def create_temporary_directories(self):
        """Create temporary local filesystem paths for usage by the test and launched applications."""
        self.temporary_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temporary_dir)

        self.external_files_dir = os.path.join(self.temporary_dir, 'external')
        self.working_dir = os.path.join(self.temporary_dir, 'work')
        self.validation_dir = os.path.join(self.working_dir, 'validation')
        self.gpg_dir = os.path.join(self.working_dir, 'gnupg')

        for dir_path in [self.external_files_dir, self.working_dir, self.validation_dir, self.gpg_dir]:
            os.makedirs(dir_path)

        os.chmod(self.gpg_dir, 0700)

        # The exporter expects this directory to already exist.
        os.makedirs(os.path.join(self.working_dir, 'course-data'))

    @when_exporter_available
    def test_database_export(self):
        # An S3 bucket to store the output in.
        assert('exporter_output_bucket' in self.config)

        self.load_data_from_file()
        self.run_export_task()

        for course_id in [self.COURSE_ID2, self.COURSE_ID]:
            org_id = get_org_id_for_course(course_id).lower()
            self.run_legacy_exporter(org_id, course_id)

            exported_filename = '{safe_course_id}-{table}-{suffix}-analytics.sql'.format(
                safe_course_id=get_filename_safe_course_id(course_id, '-'),
                table=self.TABLE,
                suffix=self.ENVIRONMENT,
            )
            self.validate_exporter_output(org_id, exported_filename)

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

    def run_legacy_exporter(self, org_id, course_id):
        """
        Preconditions: A text file for courseware_studentmodule has been generated and stored in the external file path.
        External Effect: Runs the legacy exporter which assembles the data package, encrypts it, and uploads it to S3.

        Reads <temporary_dir>/external/<day of month>/edX-E929-2014_T1-courseware_studentmodule-acceptance-analytics.sql
            and copies it in to the data package.

        Writes the configuration to <temporary_dir>/acceptance.yml.

        Uploads the package to s3://<exporter_output_bucket>/<output_prefix>edx-<year>-<month>-<day>.zip

        """
        env_config_file_path = os.path.join(self.temporary_dir, '{}_env_acceptance.yml'.format(org_id))
        org_config_file_path = os.path.join(self.temporary_dir, '{}_org_acceptance.yml'.format(org_id))

        self.write_exporter_config(org_id, course_id, env_config_file_path, org_config_file_path)

        src_url_tuple = urlparse.urlparse(self.test_src)

        command = [
            os.getenv('EXPORTER'),
            '--work-dir', self.working_dir,
            '--output-bucket', self.config.get('exporter_output_bucket'),
            '--pipeline-bucket', src_url_tuple.netloc,
            '--external-prefix', src_url_tuple.path.lstrip('/'),
            '--output-prefix', self.output_prefix,
            env_config_file_path,
            org_config_file_path,
            '--env', self.ENVIRONMENT,
            '--org', org_id,
            '--task', 'StudentModuleTask'
        ]
        shell.run(command)

    def write_exporter_config(self, org_id, course_id, env_config_file_path, org_config_file_path):
        """Write out the configuration file that the exporter expects to the filesystem."""
        env_config_text = textwrap.dedent("""\
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
            """)
        env_config_text = env_config_text.format(
            sql_user=self.import_db.credentials['username'],
            sql_db=self.import_db.database_name,
            sql_password=self.import_db.credentials['password'],
            environment=self.ENVIRONMENT,
            sql_host=self.import_db.credentials['host'],
            external_files=self.external_files_dir,
        )
        with open(env_config_file_path, 'w') as env_config_file:
            env_config_file.write(env_config_text)

        org_config_text = textwrap.dedent("""\
            organizations:
              {org_id}:
                recipients:
                  - daemon@edx.org
                courses:
                  - {course_id}
        """)
        org_config_text = org_config_text.format(
            org_id=org_id,
            course_id=course_id,
        )
        with open(org_config_file_path, 'w') as org_config_file:
            org_config_file.write(org_config_text)

    def validate_exporter_output(self, org_id, exported_filename):
        """
        Preconditions: A complete data package has been uploaded to S3.
        External Effect: Downloads the complete data package, decompresses it, decrypts it and then compares it to the
            static expected output ignoring the ordering of the records in both files.

        Downloads s3://<exporter_output_bucket>/<output_prefix><org_id>-<year>-<month>-<day>.zip to <temporary_dir>/work/validation/.

        """
        today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        bucket = ScalableS3Client().s3.get_bucket(self.config.get('exporter_output_bucket'))
        export_id = '{org}-{date}'.format(org=org_id, date=today)
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
        exporter_archive_path = os.path.join(self.validation_dir, filename)
        key.get_contents_to_filename(exporter_archive_path)

        shell.run(['unzip', exporter_archive_path, '-d', self.validation_dir])

        gpg = gnupg.GPG(gnupghome=self.gpg_dir)
        with open(os.path.join('gpg-keys', 'insecure_secret.key'), 'r') as key_file:
            gpg.import_keys(key_file.read())

        exported_file_path = os.path.join(self.validation_dir, exported_filename)
        with open(os.path.join(self.validation_dir, export_id, exported_filename + '.gpg'), 'r') as encrypted_file:
            gpg.decrypt_file(encrypted_file, output=exported_file_path)

        sorted_filename = exported_file_path + '.sorted'
        shell.run(['sort', '-o', sorted_filename, exported_file_path])

        expected_output_path = os.path.join(self.data_dir, 'output', exported_filename + '.sorted')
        shell.run(['diff', sorted_filename, expected_output_path])
