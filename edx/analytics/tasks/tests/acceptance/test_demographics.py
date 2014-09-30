"""
End to end test of demographic trends.
"""

import datetime
import logging

from luigi.s3 import S3Target

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join


log = logging.getLogger(__name__)


class DemographicsAcceptanceTest(AcceptanceTestCase):
    """End to end test of demographic trends."""

    INPUT_FILE = 'enrollment_trends_tracking.log'

    def test_demographic_trends(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 8, 1))
        self.execute_sql_fixture_file('load_auth_userprofile.sql')

        blacklist_date = '2014-08-29'
        blacklist_url = url_path_join(
            self.warehouse_path, 'course_enrollment_blacklist', 'dt=' + blacklist_date, 'blacklist.tsv')
        with S3Target(blacklist_url).open('w') as f:
            f.write('edX/Open_DemoX/edx_demo_course3')

        config_override = {
            'enrollments': {
                'blacklist_date': blacklist_date,
            }
        }

        self.task.launch([
            'ImportDemographicsIntoMysql',
            '--interval', '2014-08-01-2014-08-06',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ], config_override=config_override)

        self.maxDiff = None

        self.validate_gender()
        self.validate_birth_year()
        self.validate_education_level()

    def validate_gender(self):
        """Ensure the gender breakdown is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT date, course_id, gender, count FROM course_enrollment_gender ORDER BY date, course_id, gender ASC')
            results = cursor.fetchall()

        expected = [
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', None, 2),
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 'm', 1),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', None, 2),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', None, 2),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 'm', 1),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'm', 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', None, 2),
            (datetime.date(2014, 8, 4), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'm', 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', None, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'm', 2),
        ]
        print 'Expected:', expected
        print 'Actual:', results
        self.assertItemsEqual(expected, results)

    def validate_birth_year(self):
        """Ensure the birth year breakdown is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT date, course_id, birth_year, count FROM course_enrollment_birth_year ORDER BY date, course_id, birth_year ASC')
            results = cursor.fetchall()

        expected = [
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 1975, 1),
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 2000, 2),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 2000, 2),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 1975, 1),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 2000, 2),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1975, 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', 2000, 2),
            (datetime.date(2014, 8, 4), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1975, 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 2000, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1975, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1984, 1),
        ]
        self.assertItemsEqual(expected, results)

    def validate_education_level(self):
        """Ensure the education level breakdown is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT date, course_id, education_level, count FROM course_enrollment_education_level ORDER BY date, course_id, education_level ASC')
            results = cursor.fetchall()

        expected = [
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', None, 1),
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 'b', 2),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', None, 1),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 'b', 1),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', None, 1),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 'b', 2),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'b', 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', None, 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', 'b', 1),
            (datetime.date(2014, 8, 4), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'b', 1),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', None, 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'a', 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 'b', 1),
        ]
        self.assertItemsEqual(expected, results)
