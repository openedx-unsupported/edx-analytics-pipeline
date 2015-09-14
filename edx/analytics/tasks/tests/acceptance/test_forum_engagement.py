"""
End to end test of per-student forum engagement task.
"""
import datetime
import logging

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join

log = logging.getLogger(__name__)


class StudentForumEngagementAcceptanceTest(AcceptanceTestCase):
    """Acceptance test for the Student Fourm Engagement Task"""

    INPUT_FILE = 'student_forum_activity.log'
    NUM_REDUCERS = 1

    COURSE_ID = "course-v1:ForumX+1+1"

    def test_forum_engagement(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2015, 9, 14))
        self.execute_sql_fixture_file('load_student_engagement.sql')

        for interval_type in ['daily', 'weekly']:
            self.run_and_check()

    def run_and_check(self, interval_type):    
        self.task.launch([
            'StudentForumEngagementToMysqlTask',
            '--source', self.test_src,
            '--output-root', url_path_join(self.test_out, interval_type)
            '--credentials', self.export_db.credentials_file_url,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--interval', '2015-09-01-2015-09-16',
            '--interval-type', interal_type,
        ])

        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT end_date, course_id, username, '
                'forum_posts, forum_responses, forum_comments, '
                'forum_upvotes_given, forum_upvotes_received'
                'FROM student_forum_engagement_{interval_type} WHERE course_id="{course_id}"'
                'ORDER BY end_date, username;'
                .format(course_id=self.COURSE_ID, interval_type=interval_type)
            )
            results = cursor.fetchall()

        if interval_type == 'weekly':
            end_date_expected = '2015-09-15'
        elif interval_type == 'daily':
            end_date_expected = '2015-09-14'
        else:
            assert False, "Invalid interval type: {}".format(interval_type)

        self.assertItemsEqual(results, [
            (end_date_expected, self.COURSE_ID, 'audit',    1, 0, 0, 3, 1),
            (end_date_expected, self.COURSE_ID, 'honor',    1, 1, 0, 0, 2),
            (end_date_expected, self.COURSE_ID, 'staff',    2, 0, 0, 1, 2),
            (end_date_expected, self.COURSE_ID, 'verified', 0, 0, 1, 1, 0),
        ])
