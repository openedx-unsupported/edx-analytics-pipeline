"""End to end test for the trajectory pipeline"""

import datetime

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


class TrajectoryAcceptanceTest(AcceptanceTestCase):
    """Test TrajectoryPipelineTask and all its constituent tasks"""

    INPUT_FILE = 'trajectory_tracking.log'
    END_DATE = datetime.date(2015, 8, 7)
    COURSE_ID = u'course-v1:Typologex+TNT+20150803'
    NUM_REDUCERS = 1

    def test_user_activity(self):
        self.maxDiff = None
        self.upload_tracking_log(self.INPUT_FILE, self.END_DATE)

        NONE, SOME, ALL = 0, 1, 2

        self.task.launch([
            'TrajectoryPipelineTask',
            '--source', self.test_src,
            '--interval', '2015-07-21-2015-08-05',
            '--credentials', self.export_db.credentials_file_url,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT chapter_id, video_type, problem_type, num_users '
                'FROM trajectory WHERE course_id="{course_id}"'
                'ORDER BY chapter_id, video_type, problem_type;'
                .format(course_id=self.COURSE_ID)
            )
            results = cursor.fetchall()

        self.assertItemsEqual([
            row for row in results
        ], [
            ('week1', NONE, SOME, 2),
            ('week1', SOME, NONE, 1),
            ('week1', SOME, SOME, 1),
            ('week1', SOME, ALL, 1),
            ('week1', ALL, SOME, 3),
            ('week1', ALL, ALL, 2),
            ('week2', NONE, ALL, 1),
            ('week2', SOME, NONE, 1),
            ('week2', SOME, SOME, 3),
            ('week2', SOME, ALL, 2),
            ('week2', ALL, SOME, 1),
            ('week2', ALL, ALL, 1),
            ('week3', NONE, SOME, 4),
            ('week3', NONE, ALL, 4),
        ])

        # Now run a later interval.
        # The additional tracking logs will cause these two changes:
        # 1. In week 1, user 'Nonall' has now completed all the problems
        # 2. In week 2, user 'Completer1' has now finished all the videos

        self.task.launch([
            'TrajectoryPipelineTask',
            '--source', self.test_src,
            '--interval', '2015-08-06-2015-08-09',
            '--credentials', self.export_db.credentials_file_url,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT chapter_id, video_type, problem_type, num_users '
                'FROM trajectory WHERE course_id="{course_id}"'
                'ORDER BY chapter_id, video_type, problem_type;'
                .format(course_id=self.COURSE_ID)
            )
            results = cursor.fetchall()

        self.assertItemsEqual([
            row for row in results
        ], [
            ('week1', NONE, SOME, 1),  # Changed from 2 to 1
            ('week1', NONE, ALL, 1),  # Changed from 0 to 1
            ('week1', SOME, NONE, 1),
            ('week1', SOME, SOME, 1),
            ('week1', SOME, ALL, 1),
            ('week1', ALL, SOME, 3),
            ('week1', ALL, ALL, 2),
            ('week2', NONE, ALL, 1),
            ('week2', SOME, NONE, 1),
            ('week2', SOME, SOME, 3),
            ('week2', SOME, ALL, 1),  # Changed from 2 to 1
            ('week2', ALL, NONE, 1),  # Changed from 0 to 1
            ('week2', ALL, SOME, 1),
            ('week2', ALL, ALL, 1),
            ('week3', NONE, SOME, 4),
            ('week3', NONE, ALL, 4),
        ])
