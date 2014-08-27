"""Ensure we can compute activity for a set of events"""

import datetime

from luigi.date_interval import Custom

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


class UserActivityAcceptanceTest(AcceptanceTestCase):
    """Ensure we can compute activity for a set of events"""

    INPUT_FILE = 'user_activity_tracking.log'
    DATE_INTERVAL = Custom.parse('2014-06-06-2014-06-20')
    START_DATE = DATE_INTERVAL.date_a
    COURSE_ID = u'edX/Open_DemoX/edx_demo_course'
    COURSE_ID2 = u'course-v1:edX+DemoX+Test_2014'
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
            cursor.execute('SELECT * FROM course_activity ORDER BY course_id, interval_end, label')
            results = cursor.fetchall()

        # pylint: disable=line-too-long
        self.assertItemsEqual([
            row[1:6] for row in results
        ], [
            (self.COURSE_ID, datetime.datetime(2014, 6, 6, 0, 0), datetime.datetime(2014, 6, 13, 0, 0), 'ACTIVE', 1),
            (self.COURSE_ID, datetime.datetime(2014, 6, 6, 0, 0), datetime.datetime(2014, 6, 13, 0, 0), 'PLAYED_VIDEO', 1),
            (self.COURSE_ID, datetime.datetime(2014, 6, 13, 0, 0), datetime.datetime(2014, 6, 20, 0, 0), 'ACTIVE', 4),
            (self.COURSE_ID, datetime.datetime(2014, 6, 13, 0, 0), datetime.datetime(2014, 6, 20, 0, 0), 'ATTEMPTED_PROBLEM', 2),
            (self.COURSE_ID, datetime.datetime(2014, 6, 13, 0, 0), datetime.datetime(2014, 6, 20, 0, 0), 'PLAYED_VIDEO', 3),
            (self.COURSE_ID2, datetime.datetime(2014, 6, 13, 0, 0), datetime.datetime(2014, 6, 20, 0, 0), 'ACTIVE', 4),
            (self.COURSE_ID2, datetime.datetime(2014, 6, 13, 0, 0), datetime.datetime(2014, 6, 20, 0, 0), 'ATTEMPTED_PROBLEM', 1),
            (self.COURSE_ID2, datetime.datetime(2014, 6, 13, 0, 0), datetime.datetime(2014, 6, 20, 0, 0), 'PLAYED_VIDEO', 3),
        ])
