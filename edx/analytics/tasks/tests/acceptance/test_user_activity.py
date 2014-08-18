
import datetime

from luigi.date_interval import Date

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


class UserActivityAcceptanceTest(AcceptanceTestCase):

    INPUT_FILE = 'user_activity_tracking.log'
    DATE_INTERVAL = Date(2014, 6, 19)
    START_DATE = DATE_INTERVAL.date_a
    END_DATE = DATE_INTERVAL.date_b
    COURSE_ID = u'edX/Open_DemoX/edx_demo_course'
    NUM_REDUCERS = 1

    def test_user_activity(self):
        self.upload_tracking_log(self.INPUT_FILE, self.START_DATE)

        self.task.launch([
            'CountUserActivityPerIntervalTaskWorkflow',
            '--source', self.test_src,
            '--interval', self.DATE_INTERVAL.to_string(),
            '--credentials', self.export_db.credentials_file_url,
            '--output-root', self.test_out,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT * FROM course_activity')
            results = cursor.fetchall()

        self.maxDiff = None  # Disable unittest limit on length of diffs
        start_datetime = datetime.datetime(self.START_DATE.year, self.START_DATE.month, self.START_DATE.day, 0, 0)
        self.assertItemsEqual([
            row[1:6] for row in results
        ], [
            (self.COURSE_ID, start_datetime, None, 'ACTIVE', 4),
            (self.COURSE_ID, start_datetime, None, 'ATTEMPTED_PROBLEM', 2),
            (self.COURSE_ID, start_datetime, None, 'PLAYED_VIDEO', 3),
        ])
