"""
End to end test of the internal reporting CourseProgramMetadataInsertToMysqlTask.
"""

from __future__ import absolute_import

import logging
import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class InternalReportingProgramCourseLoadAcceptanceTest(AcceptanceTestCase):  # pragma: no cover
    """End-to-end test of the workflow to load the `program_course` table."""

    DATE = '2016-09-08'

    def setUp(self):
        super(InternalReportingProgramCourseLoadAcceptanceTest, self).setUp()
        input_location = os.path.join(self.data_dir,
                                      'input',
                                      'course_runs.json')
        output_location = url_path_join(self.warehouse_path,
                                        'discovery_api_raw',
                                        'dt=' + self.DATE,
                                        'course_runs.json')
        # The furthest upstream dependency is PullDiscoveryCourseRunsAPIData.
        # We fixture the expected output of that Task here.
        self.upload_file(input_location, output_location)

    def test_import_program_courses_into_mysql(self):
        """Tests the workflow for the `course_meta_program` table, end to end."""
        self.task.launch(['CourseProgramMetadataInsertToMysqlTask',
                          '--date', self.DATE])

        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT * FROM course_program_metadata')
            # discard the `id` and `created` columns
            actual = [row[1: -1] for row in cursor.fetchall()]

        expected = [
            ('edX/Open_DemoX/edx_demo_course', 'acb243a0-1234-5abe-099e-ffcae2a340d4', 'XSeries', 'Testing'),
            ('course-v1:edX+Testing102x+1T2017', 'acb243a0-1234-5abe-099e-ffcae2a340d4', 'XSeries', 'Testing')
        ]
        self.assertEqual(expected, actual)
