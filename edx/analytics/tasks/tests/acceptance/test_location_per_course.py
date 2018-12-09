
from datetime import datetime

from luigi.date_interval import Date

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, as_list_param, when_geolocation_data_available


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

    @when_geolocation_data_available
    def test_location_by_course(self):
        self.upload_tracking_log(self.INPUT_FILE, self.START_DATE)

        for fixture_file_name in self.SQL_FIXTURES:
            self.execute_sql_fixture_file(fixture_file_name)

        self.task.launch([
            'InsertToMysqlLastCountryPerCourseTask',
            '--source', as_list_param(self.test_src),
            '--interval', self.DATE_INTERVAL.to_string(),
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.maxDiff = None

        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT * FROM course_enrollment_location_current ORDER BY country_code, course_id')
            results = cursor.fetchall()

        # TODO: what happens if the test starts near the UTC day boundary. The task sees that today is day "X", yet this
        # code sees the following day since the day boundary was crossed between then and now.
        today = datetime.utcnow().date()

        self.assertItemsEqual([
            row[1:6] for row in results
        ], [
            (today, self.COURSE_ID, None, 1, 1),
            (today, self.COURSE_ID, 'UNKNOWN', 0, 1),
            (today, self.COURSE_ID, 'IE', 1, 1),
            (today, self.COURSE_ID2, 'TH', 1, 1),
            (today, self.COURSE_ID, 'TH', 1, 1),
        ])
