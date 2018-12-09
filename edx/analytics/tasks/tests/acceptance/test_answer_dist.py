"""
End to end test of answer distribution.
"""

import logging
import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, as_list_param
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class BaseAnswerDistributionAcceptanceTest(AcceptanceTestCase):
    """Base class for setting up answer dist acceptance tests"""

    INPUT_FILE = 'answer_dist_acceptance_tracking.log'
    INPUT_FORMAT = 'org.edx.hadoop.input.ManifestTextInputFormat'
    NUM_REDUCERS = 1

    def setUp(self):
        super(BaseAnswerDistributionAcceptanceTest, self).setUp()

        assert 'oddjob_jar' in self.config

        self.oddjob_jar = self.config['oddjob_jar']
        self.input_format = self.config.get('manifest_input_format', self.INPUT_FORMAT)

        self.upload_data()

    def upload_data(self):
        src = os.path.join(self.data_dir, 'input', self.INPUT_FILE)
        dst = url_path_join(self.test_src, self.INPUT_FILE)

        # Upload test data file
        self.upload_file(src, dst)


class AnswerDistributionAcceptanceTest(BaseAnswerDistributionAcceptanceTest):
    """Acceptance test for the CSV-generating Answer Distribution Task"""

    def test_answer_distribution(self):
        self.task.launch([
            'AnswerDistributionOneFilePerCourseTask',
            '--src', as_list_param(self.test_src),
            '--dest', url_path_join(self.test_root, 'dst'),
            '--name', 'test',
            '--output-root', self.test_out,
            '--include', as_list_param('"*"'),
            '--manifest', url_path_join(self.test_root, 'manifest.txt'),
            '--base-input-format', self.input_format,
            '--lib-jar', as_list_param(self.oddjob_jar),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])
        self.validate_output()

    def validate_output(self):

        output_targets = self.get_targets_from_remote_path(self.test_out)

        # There are 3 courses in the test data
        self.assertEqual(len(output_targets), 3)

        # Check that the results have data
        def get_count(line):
            return int(line.split(',')[3])
        for output_target in output_targets:
            with output_target.open() as f:
                lines = [l for l in f][1:]  # Skip header
                self.assertTrue(len(lines) > 0)

                # Check that at least one of the count columns is non zero
                self.assertTrue(any(get_count(l) > 0 for l in lines))


class AnswerDistributionMysqlAcceptanceTests(BaseAnswerDistributionAcceptanceTest):
    """Acceptance tests for Answer Distribution Tasks -> MySQL"""

    def test_answer_distribution_mysql(self):
        self.task.launch([
            'AnswerDistributionToMySQLTaskWorkflow',
            '--src', as_list_param(self.test_src),
            '--dest', url_path_join(self.test_root, 'dst'),
            '--name', 'test',
            '--include', as_list_param('"*"'),
            '--manifest', url_path_join(self.test_root, 'manifest.txt'),
            '--base-input-format', self.input_format,
            '--lib-jar', as_list_param(self.oddjob_jar),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--credentials', self.export_db.credentials_file_url,
        ])

        self.validate_output()

    def validate_output(self):
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT DISTINCT(`course_id`) from answer_distribution')
            uniq_course_ids = cursor.fetchall()
            # There are 3 courses in the test data
            self.assertEqual(len(uniq_course_ids), 3)

            # the fetchall above returns a list of singleton tuples, so we use course_id[0] below
            for course_id in uniq_course_ids:
                cursor.execute(
                    'SELECT COUNT(*) FROM answer_distribution where `course_id`="{}" and `last_response_count`>0'.format(course_id[0])
                )
                count = cursor.fetchone()[0]
                # Check that at least one of the count columns is non zero
                self.assertGreaterEqual(count, 1)
