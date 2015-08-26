"""
End to end test of user problem history computation.
"""

import datetime

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


class UserProblemHistoryImportAcceptanceTest(AcceptanceTestCase):
    """ End to end test of user problem history computation. """

    def test_problem_history_import(self):
        self.upload_tracking_log('user_problem_history_tracking.log', datetime.date(2015, 9, 1))

        self.task.launch([
            'UserProblemHistoryToMySQLTask',
            '--interval', '2015-08'
        ])

        self.maxDiff = None

        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT week_ending, course_id, user_id, problem_id, num_attempts, most_recent_score, max_score'
                ' FROM user_problem_weekly_data'
                ' ORDER BY week_ending;'
            )
            results = cursor.fetchall()

        expected = [
            (
                datetime.date(2015, 8, 10),
                'edX/DemoX/Demo_Course',
                4,
                'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02',
                4,
                0,
                1
            ),
            (
                datetime.date(2015, 8, 17),
                'edX/DemoX/Demo_Course',
                4,
                'i4x://edX/DemoX/problem/Sample_Algebraic_Problem',
                1,
                1,
                1
            ),
        ]
        self.assertItemsEqual(expected, results)
