"""
End to end test of answer distribution via Hive.
"""

import datetime
import logging
import os

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
        self.interval = '2014-05-01-2014-08-27'

        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 7, 30))


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
