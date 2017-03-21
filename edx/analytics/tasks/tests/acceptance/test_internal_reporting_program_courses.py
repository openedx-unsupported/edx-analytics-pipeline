"""
End to end test of the internal reporting ImportProgramCoursesIntoMysql.
"""

import os
import logging

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class InternalReportingProgramCourseLoadAcceptanceTest(AcceptanceTestCase):
    """End-to-end test of the workflow to load the `program_course` table."""

    DATE = '2016-09-08'

    def setUp(self):
        super(InternalReportingProgramCourseLoadAcceptanceTest, self).setUp()
        input_location = os.path.join(self.data_dir,
                                      'input',
                                      'program_course_catalog.json')
        output_location = url_path_join(self.warehouse_path,
                                        'course_catalog_raw',
                                        'dt=' + self.DATE,
                                        'course_catalog.json')
        # The furthest upstream dependency is PullCourseCatalogAPIData.
        # We fixture the expected output of that Task here.
        self.upload_file(input_location, output_location)

    def test_import_program_courses_into_mysql(self):
        """Tests the workflow for the `program_course` table, end to end."""
        self.task.launch(['ImportProgramCoursesIntoMysql', '--date', self.DATE])

        columns = ['program_id', 'program_type', 'program_title',
                   'catalog_course', 'catalog_course_title', 'course_id',
                   'org_id', 'partner_short_code']

        with self.export_db.cursor() as cursor:
            cursor.execute(
                '''
                  SELECT {columns}
                  FROM   course_meta_program
                '''.format(columns=','.join(columns))
            )
            actual = cursor.fetchall()

        expected = [
            ('acb243a0-1234-5abe-099e-ffcae2a340d4', 'XSeries', 'Testing',
             'edX+Open_DemoX', 'All about acceptance testing!',
             'edX/Open_DemoX/edx_demo_course', 'edX', 'openedx'),
            ('acb243a0-1234-5abe-099e-ffcae2a340d4', 'XSeries', 'Testing',
             'edX+Testing102', 'All about acceptance testing Part 3!',
             'course-v1:edX+Testing102x+1T2017', 'edX', 'openedx')
        ]
        self.assertEqual(expected, actual)
