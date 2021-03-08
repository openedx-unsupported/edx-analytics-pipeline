import csv
import hashlib
import json
import logging
import os
import shutil
import unittest

import boto
import pandas
from pandas.util.testing import assert_frame_equal, assert_series_equal

from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.tests.acceptance.services import db, elasticsearch_service, fs, hive, task
from edx.analytics.tasks.util.s3_util import ScalableS3Client
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)

# Decorators for tagging tests


def when_s3_available(function):
    s3_available = getattr(when_s3_available, 's3_available', None)
    if s3_available is None:
        try:
            connection = ScalableS3Client().s3
            # ^ The above line will not error out if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
            # are set, so it can't be used to check if we have a valid connection to S3. Instead:
            connection.get_all_buckets()
        except (boto.exception.S3ResponseError, boto.exception.NoAuthHandlerFound):
            s3_available = False
        else:
            s3_available = True
        finally:
            when_s3_available.s3_available = s3_available  # Cache result to avoid having to compute it again
    return unittest.skipIf(
        not s3_available, 'S3 is not available'
    )(function)


def when_exporter_available(function):
    return unittest.skipIf(
        os.getenv('EXPORTER') is None, 'Private Exporter code is not available'
    )(function)


def when_geolocation_data_available(function):
    config = get_test_config()
    geolocation_data = config.get('geolocation_data')
    geolocation_data_available = bool(geolocation_data)
    if geolocation_data_available:
        geolocation_data_available = get_target_for_local_server(geolocation_data).exists()
    return unittest.skipIf(
        not geolocation_data_available, 'Geolocation data is not available'
    )(function)


def when_elasticsearch_available(function):
    config = get_test_config()
    es_available = bool(config.get('elasticsearch_host'))
    return unittest.skipIf(
        not es_available, 'Elasticsearch service is not available'
    )(function)

# Utility functions


def get_test_config():
    config_json = os.getenv('ACCEPTANCE_TEST_CONFIG')
    try:
        with open(config_json, 'r') as config_json_file:
            config = json.load(config_json_file)
    except (IOError, TypeError):
        try:
            config = json.loads(config_json)
        except TypeError:
            config = {}
    return config


def get_target_for_local_server(url):
    # The machine running the acceptance test suite may not have hadoop installed on it, so convert S3 paths (which
    # are normally handled by the hadoop DFS client) to S3+https paths, which are handled by the python native S3
    # client.
    return get_target_from_url(url.replace('s3://', 's3+https://'))


def modify_target_for_local_server(target):
    # The machine running the acceptance test suite may not have hadoop installed on it (e.g. Jenkins), so convert
    # S3 paths (which are normally handled by the hadoop DFS client) to S3+https paths, which are handled by the python
    # native S3 client.  But avoid creating a new target for a HDFS target, because the path has had the scheme stripped.
    if target.path.startswith('s3://'):
        return get_target_for_local_server(target.path)
    else:
        return target


def as_list_param(value, escape_quotes=True):
    """Convenience method to convert a single string to a format expected by a Luigi ListParameter."""
    if escape_quotes:
        return '[\\"{}\\"]'.format(value)
    else:
        return json.dumps([value, ])


def read_csv_fixture_as_list(fixture_file_path):
    with open(fixture_file_path) as fixture_file:
        reader = csv.reader(fixture_file)
        next(reader)  # skip header
        fixture_data = list(reader)
    return fixture_data


class AcceptanceTestCase(unittest.TestCase):

    acceptance = 1
    NUM_MAPPERS = 4
    NUM_REDUCERS = 2

    def setUp(self):
        try:
            self.s3_client = ScalableS3Client()
        except Exception:
            self.s3_client = None

        self.config = get_test_config()

        for env_var in ('TASKS_REPO', 'TASKS_BRANCH', 'IDENTIFIER', 'JOB_FLOW_NAME', 'IS_REMOTE'):
            if env_var in os.environ:
                self.config[env_var.lower()] = os.environ[env_var]

        if 'is_remote' in self.config:
            self.config['is_remote'] = self.config['is_remote'].lower() not in ('0', 'false', 'f')
        else:
            self.config['is_remote'] = True

        if self.config['is_remote']:
            # The name of an existing job flow to run the test on
            assert('job_flow_name' in self.config or 'host' in self.config)
            # The git URL of the pipeline repository to check this code out from.
            assert('tasks_repo' in self.config)
            # The branch of the pipeline repository to test. Note this can differ from the branch that is currently
            # checked out and running this code.
            assert('tasks_branch' in self.config)
            # Where to store logs generated by the pipeline
            assert('tasks_log_path' in self.config)
            # The user to connect to the job flow over SSH with.
            assert('connection_user' in self.config)

        # Where the pipeline should output data, should be a URL pointing to a directory.
        assert('tasks_output_url' in self.config)
        # Allow for parallel execution of the test by specifying a different identifier. Using an identical identifier
        # allows for old virtualenvs to be reused etc, which is why a random one is not simply generated with each run.
        assert('identifier' in self.config)
        # A URL to a JSON file that contains most of the connection information for the MySQL database.
        assert('credentials_file_url' in self.config)
        # A URL to a build of the oddjob third party library
        assert 'oddjob_jar' in self.config
        # A URL to a maxmind compatible geolocation database file
        assert 'geolocation_data' in self.config

        self.data_dir = os.path.join(os.path.dirname(__file__), 'fixtures')

        url = self.config['tasks_output_url']
        m = hashlib.md5()
        m.update(self.config['identifier'])
        self.identifier = m.hexdigest()
        self.test_root = url_path_join(url, self.identifier, self.__class__.__name__)

        self.test_src = url_path_join(self.test_root, 'src')
        self.test_out = url_path_join(self.test_root, 'out')

        # Use a local dir for devstack testing, or s3 for production testing.
        self.report_output_root = self.config.get('report_output_root',
                                                  url_path_join(self.test_out, 'reports'))

        self.catalog_path = 'http://acceptance.test/api/courses/v2'
        database_name = 'test_' + self.identifier
        schema = 'test_' + self.identifier
        import_database_name = 'acceptance_import_' + database_name
        export_database_name = 'acceptance_export_' + database_name
        otto_database_name = 'acceptance_otto_' + database_name
        elasticsearch_alias = 'alias_test_' + self.identifier
        self.warehouse_path = url_path_join(self.test_root, 'warehouse')
        self.edx_rest_api_cache_root = url_path_join(self.test_src, 'edx-rest-api-cache')
        task_config_override = {
            'hive': {
                'database': database_name,
                'warehouse_path': self.warehouse_path
            },
            'map-reduce': {
                'marker': url_path_join(self.test_root, 'marker')
            },
            'manifest': {
                'path': url_path_join(self.test_root, 'manifest'),
                'lib_jar': self.config['oddjob_jar'],
            },
            'database-import': {
                'credentials': self.config['credentials_file_url'],
                'destination': self.warehouse_path,
                'database': import_database_name
            },
            'database-export': {
                'credentials': self.config['credentials_file_url'],
                'database': export_database_name
            },
            'otto-database-import': {
                'credentials': self.config['credentials_file_url'],
                'database': otto_database_name
            },
            'course-catalog': {
                'catalog_path': self.catalog_path
            },
            'geolocation': {
                'geolocation_data': self.config['geolocation_data']
            },
            'event-logs': {
                'source': as_list_param(self.test_src, escape_quotes=False),
                'pattern': as_list_param(".*tracking.log-(?P<date>\\d{8}).*\\.gz", escape_quotes=False),
            },
            'segment-logs': {
                'source': as_list_param(self.test_src, escape_quotes=False),
                'pattern': as_list_param(".*segment.log-(?P<date>\\d{8}).*\\.gz", escape_quotes=False),
            },
            'course-structure': {
                'api_root_url': 'acceptance.test',
                'access_token': 'acceptance'
            },
            'module-engagement': {
                'alias': elasticsearch_alias
            },
            'elasticsearch': {},
            'problem-response': {
                'report_fields': '["username","problem_id","answer_id","location","question","score","max_score",'
                                 '"correct","answer","total_attempts","first_attempt_date","last_attempt_date"]',
                'report_field_list_delimiter': '"|"',
                'report_field_datetime_format': '%Y-%m-%dT%H:%M:%SZ',
                'report_output_root': self.report_output_root,
                'partition_format': '%Y-%m-%dT%H',
            },
            'edx-rest-api': {
                'client_id': 'oauth_id',
                'client_secret': 'oauth_secret',
                'oauth_username': 'test_user',
                'oauth_password': 'password',
                'auth_url': 'http://acceptance.test',
            },
            'course-blocks': {
                'api_root_url': 'http://acceptance.test/api/courses/v1/blocks/',
            },
            'course-list': {
                'api_root_url': 'http://acceptance.test/api/courses/v1/courses/',
            },
        }

        if 'elasticsearch_host' in self.config:
            task_config_override['elasticsearch']['host'] = self.config['elasticsearch_host']
        if 'elasticsearch_connection_class' in self.config:
            task_config_override['elasticsearch']['connection_type'] = self.config['elasticsearch_connection_class']
        if 'manifest_input_format' in self.config:
            task_config_override['manifest']['input_format'] = self.config['manifest_input_format']
        if 'hive_version' in self.config:
            task_config_override['hive']['version'] = self.config['hive_version']

        log.info('Running test: %s', self.id())
        log.info('Using executor: %s', self.config['identifier'])
        log.info('Generated Test Identifier: %s', self.identifier)

        self.import_db = db.DatabaseService(self.config, import_database_name)
        self.export_db = db.DatabaseService(self.config, export_database_name)
        self.otto_db = db.DatabaseService(self.config, otto_database_name)
        self.task = task.TaskService(self.config, task_config_override, self.identifier)
        self.hive = hive.HiveService(self.task, self.config, database_name)
        self.elasticsearch = elasticsearch_service.ElasticsearchService(self.config, elasticsearch_alias)

        try:
            self.reset_external_state()
        except Exception:
            pass

        max_diff = os.getenv('MAX_DIFF', None)
        if max_diff is not None:
            if max_diff.lower() == "infinite":
                self.maxDiff = None
            else:
                self.maxDiff = int(max_diff)

    @property
    def should_reset_state(self):
        return os.getenv('DISABLE_RESET_STATE', 'false').lower() != 'true'

    def reset_external_state(self):
        if not self.should_reset_state:
            return

        root_target = get_target_for_local_server(self.test_root)
        if root_target.exists():
            root_target.remove()
        self.import_db.reset()
        self.export_db.reset()
        self.otto_db.reset()
        self.hive.reset()
        self.elasticsearch.reset()

    def upload_tracking_log(self, input_file_name, file_date, template_context=None):
        # Define a tracking log path on S3 that will be matched by the standard event-log pattern."
        input_file_path = url_path_join(
            self.test_src,
            'FakeServerGroup',
            'tracking.log-{0}.gz'.format(file_date.strftime('%Y%m%d'))
        )

        raw_file_path = os.path.join(self.data_dir, 'input', input_file_name)
        with fs.template_rendered_file(raw_file_path, template_context) as rendered_file_name:
            with fs.gzipped_file(rendered_file_name) as compressed_file_name:
                self.upload_file(compressed_file_name, input_file_path)

    def upload_file(self, local_file_name, remote_file_path):
        log.debug('Uploading %s to %s', local_file_name, remote_file_path)
        with get_target_from_url(remote_file_path).open('w') as remote_file:
            with open(local_file_name, 'r') as local_file:
                shutil.copyfileobj(local_file, remote_file)

    def upload_file_with_content(self, remote_file_path, content):
        log.debug('Writing %s from string', remote_file_path)
        with get_target_from_url(remote_file_path).open('w') as remote_file:
            remote_file.write(content)

    def execute_sql_fixture_file(self, sql_file_name, database=None):
        if database is None:
            database = self.import_db
        log.debug('Executing SQL fixture %s on %s', sql_file_name, database.database_name)
        database.execute_sql_file(os.path.join(self.data_dir, 'input', sql_file_name))

    def assertEventLogEqual(self, expected_filepath, actual_filepath):
        """Compares event log files to confirm they are equal."""
        # Brute force:  read in entire file, and then compare dicts.
        with open(expected_filepath) as expected_output_file:
            with open(actual_filepath) as actual_output_file:
                expected = sorted([json.loads(eventline) for eventline in expected_output_file])
                actual = sorted([json.loads(eventline) for eventline in actual_output_file])
                self.assertListEqual(expected, actual)

    @staticmethod
    def assert_data_frames_equal(data, expected):
        """Compare two pandas DataFrames and display diagnostic output if they don't match."""
        try:
            assert_frame_equal(data, expected)
        except AssertionError:
            # For some reason the version of pands we have pinned throws an error if you try to print it
            # or to_string() it. Thus these shenanigans.
            print '----- The report generated this data: -----'
            for index, row in data.iterrows():
                print("  {}".format(index))
                for col in row:
                    print("     {}".format(col))
            print '----- vs expected: -----'
            for index, row in expected.iterrows():
                print("  {}".format(index))
                for col in row:
                    print("     {}".format(col))

            if data.shape != expected.shape:
                print "Data shapes differ."
            else:
                for index, _series in data.iterrows():
                    # Try to print a more helpful/localized difference message:
                    try:
                        assert_series_equal(data.iloc[index, :], expected.iloc[index, :])
                    except AssertionError:
                        print "First differing row: {index}".format(index=index)
            raise

    @staticmethod
    def get_targets_from_remote_path(remote_path, pattern='*'):
        output_targets = PathSetTask([remote_path], [pattern]).output()
        modified = [modify_target_for_local_server(output_target) for output_target in output_targets]
        return modified

    @staticmethod
    def download_file_to_local_directory(remote_file_path, local_file_dir_name):
        log.debug('Downloading %s to %s', remote_file_path, local_file_dir_name)
        filename = os.path.basename(remote_file_path)
        local_file_path = url_path_join(local_file_dir_name, filename)
        with get_target_for_local_server(remote_file_path).open('r') as remote_file:
            with open(local_file_path, 'w') as local_file:
                shutil.copyfileobj(remote_file, local_file)
        return local_file_path

    @staticmethod
    def read_dfs_directory(url):
        """Given the URL to a directory, read all of the files from it and concatenate them."""
        output_targets = AcceptanceTestCase.get_targets_from_remote_path(url)
        raw_output = []
        for output_target in output_targets:
            raw_output.append(output_target.open('r').read())

        return ''.join(raw_output)
