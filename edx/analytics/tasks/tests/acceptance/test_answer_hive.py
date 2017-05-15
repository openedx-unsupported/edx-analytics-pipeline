"""
End to end test of answer distribution via Hive.
"""

import datetime
import logging
import os
import re

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
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
        self.interval = '2014-08-26-2014-08-27'

        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 8, 26))


class AnswerDistributionHiveAcceptanceTest(BaseAnswerDistributionAcceptanceTest):
    """Acceptance test for the CSV-generating Answer Distribution Task"""

    def test_answer_distribution(self):
        self.task.launch([
            'AnswerDistOneFilePerCourseTask',
            '--source', self.test_src,
            '--warehouse-path', url_path_join(self.test_root, 'dst'),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', self.interval,
            '--output-root', self.test_out,
        ])

        expected_rows = [
            ['course-v1:edX+DemoX+Test_2014', '932e6f2ce8274072a355a94560216d1a_2_1', 'False',
             'Feeling sleepy can cause white rabbits to appear.', 'choice_0', '1', '0',
             'block-v1:edX+DemoX+Test_2014+type@problem+block@932e6f2ce8274072a355a94560216d1a',
             'Perchance to Dream'],
            ['course-v1:edX+DemoX+Test_2014', '932e6f2ce8274072a355a94560216d1a_2_1', 'False',
             'There is foreshadowing of a tea party.', 'choice_1', '0', '0',
             'block-v1:edX+DemoX+Test_2014+type@problem+block@932e6f2ce8274072a355a94560216d1a',
             'Perchance to Dream'],
            ['course-v1:edX+DemoX+Test_2014', '932e6f2ce8274072a355a94560216d1a_2_1', 'True',
             'There is an implication that the strangeness to follow can be considered like a dream.',
             'choice_2', '0', '1',
             'block-v1:edX+DemoX+Test_2014+type@problem+block@932e6f2ce8274072a355a94560216d1a',
             'Perchance to Dream'],
            ['course-v1:edX+DemoX+Test_2014', '9cee77a606ea4c1aa5440e0ea5d0f618_2_1', 'False',
             '[late penalties|instructor forgiveness]', '[choice_0|choice_1]', '1', '1',
             'block-v1:edX+DemoX+Test_2014+type@problem+block@9cee77a606ea4c1aa5440e0ea5d0f618',
             'Interactive Questions'],
        ]

        output_target = self.get_targets_from_remote_path(url_path_join(self.test_root, 'dst', 'latest_answer_dist'))[0]

        with output_target.open() as output_file:
            # columns are sometimes separated by more than one tab
            actual_rows = [re.split('\t+', row.strip()) for row in output_file]
            for i, expected_row in enumerate(expected_rows):
                actual_row = actual_rows[i]
                self.assertListEqual(expected_row, actual_row)
