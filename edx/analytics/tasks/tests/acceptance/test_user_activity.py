"""Ensure we can compute activity for a set of events"""

import datetime

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


class UserActivityAcceptanceTest(AcceptanceTestCase):
    """Ensure we can compute activity for a set of events"""

    EMPTY_INPUT_FILE = 'module_engagement_acceptance_empty.log'
    INPUT_FILE = 'user_activity_tracking.log'
    END_DATE = datetime.date(2014, 6, 27)
    NUM_WEEKS = 1
    COURSE_ID = u'edX/Open_DemoX/edx_demo_course'
    COURSE_ID2 = u'course-v1:edX+DemoX+Test_2014'
    NUM_REDUCERS = 1

    def test_user_activity(self):
        self.maxDiff = None
        for day in range(16, 19):
            fake_date = datetime.date(2014, 6, day)
            self.upload_tracking_log(self.EMPTY_INPUT_FILE, fake_date)
        for day in range(20, 31):
            fake_date = datetime.date(2014, 6, day)
            self.upload_tracking_log(self.EMPTY_INPUT_FILE, fake_date)

        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 6, 19))

        self.task.launch([
            'InsertToMysqlCourseActivityTask',
            '--source', self.test_src,
            '--end-date', self.END_DATE.isoformat(),
            '--weeks', str(self.NUM_WEEKS),
            '--credentials', self.export_db.credentials_file_url,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--overwrite-n-days', '14'
        ])

        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT course_id, interval_start, interval_end, label, count FROM course_activity ORDER BY course_id, interval_end, label')
            results = cursor.fetchall()

        # pylint: disable=line-too-long
        self.assertItemsEqual([
            row for row in results
        ], [
            (self.COURSE_ID2, datetime.datetime(2014, 5, 19, 0, 0), datetime.datetime(2014, 5, 26, 0, 0), 'ACTIVE', 1),
            (self.COURSE_ID2, datetime.datetime(2014, 5, 19, 0, 0), datetime.datetime(2014, 5, 26, 0, 0), 'PLAYED_VIDEO', 1),
            (self.COURSE_ID2, datetime.datetime(2014, 6, 16, 0, 0), datetime.datetime(2014, 6, 23, 0, 0), 'ACTIVE', 4),
            (self.COURSE_ID2, datetime.datetime(2014, 6, 16, 0, 0), datetime.datetime(2014, 6, 23, 0, 0), 'ATTEMPTED_PROBLEM', 1),
            (self.COURSE_ID2, datetime.datetime(2014, 6, 16, 0, 0), datetime.datetime(2014, 6, 23, 0, 0), 'PLAYED_VIDEO', 3),
            (self.COURSE_ID, datetime.datetime(2014, 6, 9, 0, 0), datetime.datetime(2014, 6, 16, 0, 0), 'ACTIVE', 1),
            (self.COURSE_ID, datetime.datetime(2014, 6, 9, 0, 0), datetime.datetime(2014, 6, 16, 0, 0), 'PLAYED_VIDEO', 1),
            (self.COURSE_ID, datetime.datetime(2014, 6, 16, 0, 0), datetime.datetime(2014, 6, 23, 0, 0), 'ACTIVE', 4),
            (self.COURSE_ID, datetime.datetime(2014, 6, 16, 0, 0), datetime.datetime(2014, 6, 23, 0, 0), 'ATTEMPTED_PROBLEM', 2),
            (self.COURSE_ID, datetime.datetime(2014, 6, 16, 0, 0), datetime.datetime(2014, 6, 23, 0, 0), 'PLAYED_VIDEO', 3),
        ])

        # self.task.launch([
        #     'InsertToMysqlCourseActivityDailyTask',
        #     '--source', self.test_src,
        #     '--interval', '2014-05-25-' + self.END_DATE.isoformat(),
        #     '--credentials', self.export_db.credentials_file_url,
        #     '--n-reduce-tasks', str(self.NUM_REDUCERS),
        # ])
        #
        # with self.export_db.cursor() as cursor:
        #     cursor.execute('SELECT course_id, date, label, count FROM course_activity_daily ORDER BY course_id, date, label')
        #     results = cursor.fetchall()
        #
        # self.assertItemsEqual([
        #     row for row in results
        # ], [
        #     (self.COURSE_ID2, datetime.date(2014, 5, 25), 'ACTIVE', 1),
        #     (self.COURSE_ID2, datetime.date(2014, 5, 25), 'PLAYED_VIDEO', 1),
        #     (self.COURSE_ID2, datetime.date(2014, 6, 19), 'ACTIVE', 4),
        #     (self.COURSE_ID2, datetime.date(2014, 6, 19), 'ATTEMPTED_PROBLEM', 1),
        #     (self.COURSE_ID2, datetime.date(2014, 6, 19), 'PLAYED_VIDEO', 3),
        #     (self.COURSE_ID, datetime.date(2014, 6, 12), 'ACTIVE', 1),
        #     (self.COURSE_ID, datetime.date(2014, 6, 12), 'PLAYED_VIDEO', 1),
        #     (self.COURSE_ID, datetime.date(2014, 6, 19), 'ACTIVE', 4),
        #     (self.COURSE_ID, datetime.date(2014, 6, 19), 'ATTEMPTED_PROBLEM', 2),
        #     (self.COURSE_ID, datetime.date(2014, 6, 19), 'PLAYED_VIDEO', 3),
        # ])
        #
        # self.task.launch([
        #     'InsertToMysqlCourseActivityMonthlyTask',
        #     '--source', self.test_src,
        #     '--end-date', self.END_DATE.isoformat(),
        #     '--months', str(2),
        #     '--credentials', self.export_db.credentials_file_url,
        #     '--n-reduce-tasks', str(self.NUM_REDUCERS),
        # ])
        #
        # with self.export_db.cursor() as cursor:
        #     cursor.execute('SELECT course_id, year, month, label, count FROM course_activity_monthly ORDER BY course_id, year, month, label')
        #     results = cursor.fetchall()
        #
        # self.assertItemsEqual([
        #     row for row in results
        # ], [
        #     (self.COURSE_ID2, 2014, 5, 'ACTIVE', 1),
        #     (self.COURSE_ID2, 2014, 5, 'PLAYED_VIDEO', 1),
        #     (self.COURSE_ID2, 2014, 6, 'ACTIVE', 4),
        #     (self.COURSE_ID2, 2014, 6, 'ATTEMPTED_PROBLEM', 1),
        #     (self.COURSE_ID2, 2014, 6, 'PLAYED_VIDEO', 3),
        #     (self.COURSE_ID, 2014, 6, 'ACTIVE', 4),
        #     (self.COURSE_ID, 2014, 6, 'ATTEMPTED_PROBLEM', 2),
        #     (self.COURSE_ID, 2014, 6, 'PLAYED_VIDEO', 3),
        # ])
