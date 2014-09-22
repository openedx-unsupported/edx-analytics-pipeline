
from datetime import datetime

from luigi.date_interval import Date

from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


class LocationByCourseAcceptanceTest(AcceptanceTestCase):

    INPUT_FILE = 'location_by_course_tracking.log'
    COURSE_ID = u'edX/Open_DemoX/edx_demo_course'
    COURSE_ID2 = u'course-v1:edX+Open_DemoX+edx_demo_course2'
    DATE_INTERVAL = Date(2014, 7, 21)
    START_DATE = DATE_INTERVAL.date_a
    END_DATE = DATE_INTERVAL.date_b
    SQL_FIXTURES = [
        'load_student_courseenrollment_for_location_by_course.sql',
        'load_auth_user_for_location_by_course.sql'
    ]

    def test_location_by_course(self):
        self.upload_tracking_log(self.INPUT_FILE, self.START_DATE)

        for fixture_file_name in self.SQL_FIXTURES:
            self.execute_sql_fixture_file(fixture_file_name)

        self.task.launch([
            'InsertToMysqlCourseEnrollByCountryWorkflow',
            '--source', self.test_src,
            '--interval', self.DATE_INTERVAL.to_string(),
            '--user-country-output', url_path_join(self.test_out, 'user'),
            '--course-country-output', url_path_join(self.test_out, 'country'),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT * FROM course_enrollment_location_current ORDER BY country_code')
            results = cursor.fetchall()

        self.maxDiff = None
        # TODO: what happens if the test starts near the UTC day boundary. The task sees that today is day "X", yet this
        # code sees the following day since the day boundary was crossed between then and now.
        today = datetime.utcnow().date()

        self.assertItemsEqual([
            row[1:5] for row in results
        ], [
            (today, self.COURSE_ID, '', 1),
            (today, self.COURSE_ID, 'IE', 1),
            (today, self.COURSE_ID, 'TH', 1),
            (today, self.COURSE_ID2, 'TH', 1),
        ])
